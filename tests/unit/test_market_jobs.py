"""Market-data jobs: handlers on real contexts, and the endpoints that queue them."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.web_app.api.screening as screening_api
from src.auth.models import AuthenticatedUser
from src.jobs.service import get_job_service
from src.jobs.user_api import install_job_error_handlers
from src.worker.context import JobContext, JobError
from src.worker.handlers import market as market_handlers
from src.worker.handlers import screening as screening_handler
from src.worker.inline import InlineWorker


def make_context(tmp_path: Path, kind: str, payload: dict) -> JobContext:
    output = tmp_path / "out"
    output.mkdir(exist_ok=True)
    return JobContext(
        job_id="job-1", kind=kind, attempt=1, payload=payload,
        workspace=tmp_path, output_dir=output, cancel_event=threading.Event(),
    )


@pytest.fixture
def market_db(tmp_path, monkeypatch) -> str:
    path = str(tmp_path / "market.db")
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE CompanyInfo (Company_Code TEXT PRIMARY KEY, Company_Ticker TEXT, Company_Name TEXT);
            CREATE TABLE FinancialStatements (Company_Code TEXT, docID TEXT UNIQUE, periodEnd TEXT);
            CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Currency TEXT, Price REAL, PRIMARY KEY (Date, Ticker));
            CREATE TABLE Company_Tags (edinetCode TEXT NOT NULL, tag TEXT NOT NULL, PRIMARY KEY (edinetCode, tag));
            INSERT INTO CompanyInfo VALUES ('E00001', '7203.T', 'Toyota'), ('E00002', '6758.T', 'Sony');
            INSERT INTO FinancialStatements VALUES ('E00001', 'D1', '2024-03-31'), ('E00002', 'D2', '2024-03-31');
            INSERT INTO Company_Tags VALUES ('E00001', 'Favorite');
            """
        )
    for module in (screening_api, screening_handler, market_handlers):
        monkeypatch.setattr(module, "get_market_db", lambda: path, raising=False)
    return path


SCREEN = {
    "criteria": [{
        "comparison_mode": "full_expression", "operator": ">",
        "left_side": [{"type": "column", "table": "Company_Tags", "column": "tag"}],
        "right_side": [{"type": "tag", "value": "Favorite"}],
    }],
    "columns": ["CompanyInfo.Company_Code"],
}


def _screen_request(**overrides) -> dict:
    request = screening_api.ScreeningRunRequest(**{**SCREEN, **overrides})
    return request.model_dump()


def test_screening_handler_uses_the_payload_tags_not_the_shared_table(tmp_path, market_db):
    context = make_context(tmp_path, "screening", {
        "request": _screen_request(), "owner_user_id": "u1", "company_tags": [["E00002", "Favorite"]],
    })
    result = screening_handler.run(context)

    assert [row[0] for row in result["rows"]] == ["E00002"]
    assert result["error"] is None and "last_run_at" not in result
    # The shared table is untouched.
    with sqlite3.connect(market_db) as conn:
        assert conn.execute("SELECT edinetCode FROM Company_Tags").fetchall() == [("E00001",)]


def test_screening_handler_rejects_a_missing_request(tmp_path, market_db):
    with pytest.raises(JobError):
        screening_handler.run(make_context(tmp_path, "screening", {}))


def test_price_refresh_handler_collects_per_ticker_results(tmp_path, market_db, monkeypatch):
    def fake(db_path, ticker):
        if ticker == "BAD":
            raise RuntimeError("provider down")
        return {"ok": True, "rows_inserted": 2, "message": f"updated {ticker}"}

    monkeypatch.setattr(market_handlers._security, "update_security_price", fake)
    result = market_handlers.price_refresh(make_context(tmp_path, "price_refresh", {"tickers": ["7203", "BAD"]}))

    assert result["results"]["7203"]["rows_inserted"] == 2
    assert result["results"]["BAD"] == {"ok": False, "rows_inserted": 0, "message": "provider down"}


def test_portfolio_prices_handler_passes_the_ticker_map(tmp_path, market_db, monkeypatch):
    seen = []
    monkeypatch.setattr(
        market_handlers, "ensure_prices_for_tickers",
        lambda db, mapping: seen.append((db, mapping)) or {"fetched": ["AAA"], "failed": [], "already_present": []},
    )
    result = market_handlers.portfolio_prices(make_context(tmp_path, "portfolio_prices", {"ticker_map": {"AAA": "USD"}}))

    assert result == {"fetched": ["AAA"], "failed": []}
    assert seen == [(market_db, {"AAA": "USD"})]


def test_portfolio_market_refresh_handler_returns_the_market_part(tmp_path, market_db, monkeypatch):
    calls = []
    monkeypatch.setattr(
        market_handlers, "refresh_market_prices",
        lambda db, tickers, currency: calls.append((db, tickers, currency)) or {"tickers": len(tickers), "updated": tickers},
    )
    result = market_handlers.portfolio_market_refresh(
        make_context(tmp_path, "portfolio_market_refresh", {"tickers": ["AAA"], "base_currency": "USD"})
    )
    assert result == {"tickers": 1, "updated": ["AAA"]} and calls == [(market_db, ["AAA"], "USD")]


def test_split_review_handler_confirms_a_split(tmp_path, market_db):
    from src.portfolio.split_schema import ensure_split_tables

    ensure_split_tables(market_db)
    with sqlite3.connect(market_db) as conn:
        conn.execute("INSERT INTO Stock_Splits (ticker, split_date, ratio_from, ratio_to) VALUES ('7203.T', '2024-05-01', 1, 2)")
    result = market_handlers.split_review(make_context(tmp_path, "split_review", {
        "split_id": 1, "confirmation": "confirmed", "reason": "checked", "reviewer": "op",
    }))

    assert result["split"]["confirmation"] == "confirmed"
    assert result["split"]["confirmed_by"] == "manual"
    assert "manual review by op: checked" in result["split"]["source_detail"]
    with pytest.raises(JobError, match="not found"):
        market_handlers.split_review(make_context(tmp_path, "split_review", {
            "split_id": 99, "confirmation": "rejected", "reviewer": "op",
        }))


def test_split_detection_handler_runs_the_scan(tmp_path, market_db, monkeypatch):
    monkeypatch.setattr(
        "src.portfolio.split_detection.run_split_detection",
        lambda **kwargs: {"new_pending": 1, "confirmed": 0, "mode": kwargs["mode"], "tickers": kwargs["tickers"]},
    )
    result = market_handlers.split_detection(make_context(tmp_path, "split_detection", {"ticker": "7203.T", "mode": "full"}))
    assert result == {"new_pending": 1, "confirmed": 0, "mode": "full", "tickers": ["7203.T"]}


def test_description_fetch_handler_caches_the_description(tmp_path, market_db, monkeypatch):
    from src.security_analysis import company_descriptions

    monkeypatch.setattr(company_descriptions, "fetch_yahoo_description", lambda ticker: "Makes cars.")
    result = market_handlers.description_fetch(
        make_context(tmp_path, "description_fetch", {"company_code": "E00001", "ticker": "7203.T"})
    )
    assert result == {"description": "Makes cars."}
    assert company_descriptions.read_cached_description(market_db, "E00001", "7203.T") == ("Makes cars.", False)


# --- endpoints ----------------------------------------------------------------


@pytest.fixture
def api(market_db):
    app = FastAPI()

    @app.middleware("http")
    async def sign_in(request, call_next):
        request.state.user = AuthenticatedUser("operator-1", "op", None, "operator", "active")
        return await call_next(request)

    app.include_router(screening_api.router)
    install_job_error_handlers(app)
    return TestClient(app)


def test_screening_without_a_worker_answers_202(api):
    response = api.post("/api/screening/run", json=_screen_request())
    assert response.status_code == 202
    body = response.json()
    assert body["code"] == "job_pending" and body["waiting_for_worker"] is True


def test_screening_run_is_finalized_as_the_last_result(api, monkeypatch):
    monkeypatch.setattr(
        screening_api._screening, "collect_user_company_tags", lambda db, owner: [("E00002", "Favorite")]
    )
    with InlineWorker(get_job_service()):
        response = api.post("/api/screening/run", json=_screen_request())
        assert response.status_code == 200, response.text
        body = response.json()
        assert [row[0] for row in body["rows"]] == ["E00002"]
        assert body["last_run_at"]
        stored = api.get("/api/screening/last-result").json()["result"]
    assert stored["result"]["row_count"] == 1
    assert stored["updated_at"] == body["last_run_at"]


def test_an_invalid_screen_is_a_400_without_queueing(api):
    response = api.post("/api/screening/run", json=_screen_request(
        criteria=[{"table": "NoSuchTable", "column": "x", "operator": ">", "value": 1}],
    ))
    assert response.status_code == 400


def test_update_prices_without_a_worker_answers_202(api):
    response = api.post("/api/screening/update-prices", json={"tickers": ["7203"]})
    assert response.status_code == 202
