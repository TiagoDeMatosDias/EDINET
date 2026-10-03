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


class TestDataQualityAndBenchmarks:
    def test_data_quality_reports_the_valuation_and_each_holding(self, populated_api_database: str) -> None:
        response = client.get("/api/portfolio/data-quality?display_currency=EUR")
        assert response.status_code == 200
        body = response.json()
        assert body["valuation_date"] == "2024-01-20"
        assert {row["symbol"] for row in body["holdings"]} >= {"AAA", "BBB"}
        assert all(row["price_source"] for row in body["holdings"])
        assert any(issue["code"] == "valuation_behind" for issue in body["issues"])

    def test_benchmark_choices_say_whether_prices_are_stored(self, populated_api_database: str) -> None:
        response = client.get("/api/portfolio/benchmarks")
        assert response.status_code == 200
        choices = {choice["ticker"]: choice for choice in response.json()}
        assert choices["VWCE"]["available"] is False

    def test_performance_includes_the_series_and_inputs(self, populated_api_database: str) -> None:
        response = client.get("/api/portfolio/performance?base_currency=EUR&benchmark_ticker=BENCH")
        assert response.status_code == 200
        body = response.json()
        assert body["series"] and {"date", "cumulative_return", "drawdown", "value", "invested"} <= set(body["series"][0])
        assert body["benchmark"]["ticker"] == "BENCH"
        assert body["risk_free"]["kind"] in {"series", "missing"}

    def test_refreshing_market_data_is_operator_only(self, populated_api_database: str, monkeypatch) -> None:
        from src.auth.dependencies import current_user
        from src.auth.models import AuthenticatedUser

        calls: list[str] = []
        monkeypatch.setattr("src.portfolio.api._refresh_market_data", lambda *args: calls.append("refresh") or {})
        member = AuthenticatedUser("member-1", "member", None, "member", "active")
        app.dependency_overrides[current_user] = lambda: member
        try:
            response = client.post("/api/portfolio/refresh-market-data")
        finally:
            app.dependency_overrides.pop(current_user, None)
        assert response.status_code == 403
        assert calls == []

    def test_income_groups_dividends_with_their_withholding(self, populated_api_database: str) -> None:
        response = client.get("/api/portfolio/income?display_currency=EUR")
        assert response.status_code == 200
        body = response.json()
        assert [company["symbol"] for company in body["companies"]] == ["AAA"]
        assert body["companies"][0]["withholding_rate"] == pytest.approx(0.15)
        payment = body["payments"][0]
        assert payment["gross_native"] == pytest.approx(20.0) and payment["tax_native"] == pytest.approx(-3.0)
        assert body["total_net"] == pytest.approx(15.3)
