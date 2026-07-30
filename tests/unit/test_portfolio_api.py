"""HTTP contract tests for the portfolio API."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.portfolio.api import router
from src.portfolio.ibkr_parser import normalize_entries, parse_ibkr_xml
from src.portfolio.portfolio_state import build_portfolio_state
from src.portfolio.schema import create_tables
from src.portfolio.transactions import insert_entries
from src.web_app.security import AppSettings, install_security

app = FastAPI()
app.include_router(router)
install_security(app, AppSettings.from_env())
client = TestClient(app)


def _configure_database(monkeypatch, portfolio_path: str, market_path: str) -> None:
    monkeypatch.setattr("src.portfolio.api.get_db3", lambda: portfolio_path)
    monkeypatch.setattr("src.portfolio.api.get_db2", lambda: market_path)
    monkeypatch.setattr("src.portfolio.price_fetcher.get_db2", lambda: market_path)
    monkeypatch.setattr("src.portfolio.portfolio_state.get_db3", lambda: portfolio_path)
    monkeypatch.setattr("src.portfolio.portfolio_state.get_db2", lambda: market_path)
    monkeypatch.setattr("src.portfolio.performance.get_db3", lambda: portfolio_path)
    monkeypatch.setattr("src.portfolio.performance.get_db2", lambda: market_path)


@pytest.fixture
def empty_api_database(monkeypatch, tmp_path: Path, market_db_path: str) -> str:
    path = str(tmp_path / "portfolio.db")
    create_tables(path)
    _configure_database(monkeypatch, path, market_db_path)
    monkeypatch.setattr(
        "src.portfolio.api.ensure_prices_for_tickers",
        lambda *_args, **_kwargs: {"fetched": [], "failed": []},
    )
    return path


@pytest.fixture
def populated_api_database(
    monkeypatch,
    tmp_path: Path,
    market_db_path: str,
    sample_ibkr_content: str,
) -> str:
    path = str(tmp_path / "portfolio.db")
    create_tables(path)
    _configure_database(monkeypatch, path, market_db_path)
    entries = normalize_entries(parse_ibkr_xml(sample_ibkr_content))
    insert_entries(
        path,
        entries,
        source_file="synthetic.xml",
        owner_user_id="local",
    )
    build_portfolio_state(
        path,
        db2_path=market_db_path,
        end_date="2024-01-20",
        base_currency="EUR",
        owner_user_id="local",
    )
    return path


class TestUpload:
    def test_upload_xml_success(
        self,
        empty_api_database: str,
        sample_ibkr_content: str,
    ) -> None:
        response = client.post(
            "/api/portfolio/upload",
            files={
                "file": (
                    "portfolio.xml",
                    sample_ibkr_content.encode(),
                    "application/xml",
                )
            },
        )

        assert response.status_code == 200, response.text
        data = response.json()
        assert data["source_file"] == "portfolio.xml"
        assert data["inserted"] == 8
        assert data["by_activity"]["TRADE"] == 3

    def test_upload_non_xml_rejected(self, empty_api_database: str) -> None:
        response = client.post(
            "/api/portfolio/upload",
            files={"file": ("test.txt", b"hello", "text/plain")},
        )
        assert response.status_code == 400

    def test_upload_rejects_oversized_content(
        self,
        monkeypatch,
        empty_api_database: str,
    ) -> None:
        monkeypatch.setattr("src.portfolio.api._MAX_XML_UPLOAD_BYTES", 16)
        response = client.post(
            "/api/portfolio/upload",
            files={"file": ("large.xml", b"x" * 17, "application/xml")},
        )
        assert response.status_code == 413

    def test_upload_rejects_unsafe_xml_without_leaking_parser_details(
        self,
        empty_api_database: str,
    ) -> None:
        content = b'<!DOCTYPE x [<!ENTITY y SYSTEM "file:///secret">]><x>&y;</x>'
        response = client.post(
            "/api/portfolio/upload",
            files={"file": ("unsafe.xml", content, "application/xml")},
        )
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid IBKR XML document"
        assert "secret" not in response.text

    def test_upload_stores_only_filename_basename(
        self,
        empty_api_database: str,
    ) -> None:
        response = client.post(
            "/api/portfolio/upload",
            files={
                "file": (
                    "..\\..\\portfolio.xml",
                    b"<FlexQueryResponse />",
                    "application/xml",
                )
            },
        )
        assert response.status_code == 200
        assert response.json()["source_file"] == "portfolio.xml"

    def test_upload_is_idempotent(
        self,
        empty_api_database: str,
        sample_ibkr_content: str,
    ) -> None:
        upload = {
            "file": (
                "portfolio.xml",
                sample_ibkr_content.encode(),
                "application/xml",
            )
        }
        first = client.post("/api/portfolio/upload", files=upload)
        second = client.post("/api/portfolio/upload", files=upload)

        assert first.status_code == 200
        assert first.json()["inserted"] == 8
        assert second.status_code == 200
        assert second.json()["inserted"] == 0
        assert second.json()["skipped"] == 8


class TestReadEndpoints:
    def test_transactions_are_bounded(self, populated_api_database: str) -> None:
        response = client.get("/api/portfolio/transactions?limit=5")
        assert response.status_code == 200
        assert len(response.json()) == 5

    def test_symbols_and_date_range(self, populated_api_database: str) -> None:
        symbols = client.get("/api/portfolio/symbols")
        date_range = client.get("/api/portfolio/date-range")

        assert symbols.status_code == 200
        symbol_names = {item["symbol"] for item in symbols.json()}
        assert {"AAA", "BBB", "SPIN"} <= symbol_names
        assert date_range.status_code == 200
        assert date_range.json()["min_date"] == "2024-01-02"
        assert date_range.json()["max_date"] == "2024-01-10"

    def test_activity_summary(self, populated_api_database: str) -> None:
        response = client.get("/api/portfolio/activity-summary")
        assert response.status_code == 200
        assert response.json()["by_activity"]["TRADE"] == 3

    def test_holdings_and_history(self, populated_api_database: str) -> None:
        holdings = client.get("/api/portfolio/holdings")
        history = client.get("/api/portfolio/holdings/history")

        assert holdings.status_code == 200
        assert any(row["symbol"] == "AAA" for row in holdings.json())
        assert history.status_code == 200
        assert history.json()

    def test_owner_scoped_holding_and_analytics_queries(
        self,
        populated_api_database: str,
    ) -> None:
        holding_history = client.get("/api/portfolio/holdings/AAA/history")
        dividend_history = client.get("/api/portfolio/dividends/history")
        dividend_yoy = client.get("/api/portfolio/dividends/yoy")
        returns = {
            path: client.get(f"/api/portfolio/returns/{path}")
            for path in ("by-company", "money-weighted", "contribution")
        }

        assert holding_history.status_code == 200
        assert holding_history.json()[0]["date"] == "2024-01-02"
        assert dividend_history.status_code == 200
        assert dividend_history.json()[0]["net"] == pytest.approx(15.3)
        assert dividend_yoy.status_code == 200
        assert dividend_yoy.json()["years"] == [2024]
        assert all(response.status_code == 200 for response in returns.values())
        assert all("years" in response.json() for response in returns.values())

    def test_performance(self, populated_api_database: str) -> None:
        response = client.get("/api/portfolio/performance?risk_free_rate=0.02")
        assert response.status_code == 200
        assert response.json()["total_dividend_income"] > 0
        assert "sharpe_ratio" in response.json()

    def test_rebuild(self, populated_api_database: str) -> None:
        response = client.post("/api/portfolio/rebuild")
        assert response.status_code == 200
        assert response.json()["daily_rows"] > 0

    def test_risk_free_rate(self, populated_api_database: str) -> None:
        response = client.get("/api/portfolio/risk-free-rate?base_currency=EUR")
        assert response.status_code == 200
        assert response.json()["risk_free_rate"] >= 0
