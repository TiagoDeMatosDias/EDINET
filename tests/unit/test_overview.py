"""The signed-in overview's dashboard data."""

from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from src.portfolio.schema import create_tables
from src.research.storage import ResearchStore
from src.web_app.api import overview as overview_module


@pytest.fixture
def portfolio_db(tmp_path):
    path = str(tmp_path / "portfolio.db")
    create_tables(path)
    conn = sqlite3.connect(path)
    conn.executemany(
        "INSERT INTO Portfolio_Daily(date, owner_user_id, total_value, cash_balance, daily_return, cumulative_return) VALUES (?, ?, ?, ?, ?, ?)",
        [("2025-12-31", "u1", 900.0, 50.0, 0.0, 0.10), ("2026-03-02", "u1", 990.0, 40.0, -0.01, 0.20), ("2026-03-03", "u1", 1000.0, 40.0, 0.01, 0.21), ("2026-03-03", "u2", 5.0, 5.0, 0.0, 0.0)],
    )
    conn.executemany(
        "INSERT INTO Portfolio_Holdings(symbol, asset_category, owner_user_id, quantity, market_value, currency) VALUES (?, ?, ?, ?, ?, ?)",
        [("AAA", "STK", "u1", 10, 600.0, "USD"), ("BBB", "STK", "u1", 5, 360.0, "EUR"), ("OLD", "STK", "u1", 0, 0.0, "EUR")],
    )
    conn.commit()
    conn.close()
    return path


def test_portfolio_summary_gives_value_returns_and_weights(portfolio_db):
    summary = overview_module.portfolio_summary(portfolio_db, "u1")
    assert (summary["valuation_date"], summary["total_value"], summary["cash"], summary["day_return"]) == ("2026-03-03", 1000.0, 40.0, 0.01)
    assert summary["ytd_return"] == pytest.approx(1.21 / 1.10 - 1)
    assert summary["first_date"] == "2025-12-31"
    assert [(item["symbol"], item["weight"]) for item in summary["top_holdings"]] == [("AAA", 0.6), ("BBB", 0.36)]
    assert summary["holdings_count"] == 2
    assert overview_module.portfolio_summary(portfolio_db, "nobody") is None


def test_research_summary_counts_and_lists_due_reviews(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.db")
    monkeypatch.setattr("src.research.runtime.store", store)
    store.upsert_company_research("u1", "E1", thesis_status="watch", target_value=None, target_currency=None, review_on="2020-01-01", thesis=None)
    store.upsert_company_research("u1", "E2", thesis_status="own", target_value=None, target_currency=None, review_on="2999-01-01", thesis=None)
    store.set_company_tags("u1", "E3", ["Yield"])
    store.create_note("u1", "Margins", "Held up", "E1")
    store.create_alert("u1", "Cheap", "E1", '{"metric":"PERatio","operator":"<","value":12}')
    summary = overview_module.research_summary("u1")
    assert (summary["followed"], summary["alerts"], summary["notes"]) == (3, 1, 1)
    assert [item["edinet_code"] for item in summary["reviews_due"]] == ["E1"]
    assert summary["theses"] == {"watch": 1, "own": 1}
    assert overview_module.research_summary("u2")["followed"] == 0


def test_overview_route_answers_for_the_signed_in_user(portfolio_db, monkeypatch, market_db_path):
    from src.web_app.server import app

    monkeypatch.setattr(overview_module, "get_app_db", lambda: portfolio_db)
    monkeypatch.setattr(overview_module, "get_market_db", lambda: market_db_path)
    body = TestClient(app).get("/api/overview").json()
    assert set(body) == {"portfolio", "research", "data", "today"}
    assert body["data"]["latest_price_date"] is not None
