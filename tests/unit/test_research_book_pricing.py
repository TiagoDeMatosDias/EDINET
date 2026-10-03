from __future__ import annotations

import json
import math
import sqlite3

import pytest

from src.research import book as book_module
from src.research.book import build_book
from src.research.pricing import (
    credit_inputs,
    ewma_volatility,
    log_returns,
    realized_volatility,
    rolling_volatility,
    volatility_inputs,
)
from src.research.storage import ResearchStore


def test_realised_volatility_annualises_the_sample_deviation():
    returns = [0.01, -0.01] * 20
    # Alternating ±1% has a sample deviation of 1% × sqrt(40/39) per day.
    assert realized_volatility(returns, 40) == pytest.approx(0.01 * math.sqrt(40 / 39) * math.sqrt(252))
    assert realized_volatility(returns, 41) is None
    assert ewma_volatility(returns) == pytest.approx(0.01 * math.sqrt(252))
    assert log_returns([100.0, 110.0, 0.0, 121.0]) == pytest.approx([math.log(1.1)])


def test_rolling_volatility_is_dated_by_each_window_end():
    returns = [0.01, -0.01] * 10
    dates = [f"2026-01-{day:02d}" for day in range(1, 21)]
    points = rolling_volatility(dates, returns, window=10, step=5)
    assert [point["date"] for point in points] == ["2026-01-10", "2026-01-15", "2026-01-20"]


def test_volatility_inputs_list_every_window_and_skip_missing_prices():
    rows = [{"trade_date": f"2026-01-{day:02d}", "price": 100 * (1.01 if day % 2 else 1.0)} for day in range(1, 31)]
    rows.insert(3, {"trade_date": "2026-01-03b", "price": None})
    result = volatility_inputs(rows)
    windows = {item["window"]: item["value"] for item in result["estimates"]}
    assert set(windows) == {"1M", "3M", "6M", "1Y", "3Y", "EWMA"}
    assert windows["1M"] is not None and windows["3M"] is None
    assert result["observations"] == 29


def _history(periods, **lines):
    tables = {"BalanceSheet": [], "IncomeStatement": []}
    income = {"Net sales", "Operating income", "Non-operating expenses - Interest expenses"}
    for label, values in lines.items():
        tables["IncomeStatement" if label in income else "BalanceSheet"].append({"field": label, "values": values})
    return {"periods": periods, **tables}


def test_credit_inputs_read_one_fiscal_year_and_average_debt():
    history = _history(
        ["2024-03-31", "2025-03-31", "2026-03-31"],
        **{
            "Assets": [900.0, 1000.0, None],
            "Liabilities": [500.0, 600.0, 650.0],
            "Retained earnings": [100.0, 150.0, 170.0],
            "Net sales": [800.0, 820.0, 850.0],
            "Operating income": [80.0, 60.0, 90.0],
            "Non-operating expenses - Interest expenses": [10.0, 12.0, 14.0],
            "Short-term borrowings": [100.0, 120.0, 130.0],
            "Bonds payable": [200.0, 280.0, 300.0],
        },
    )
    result = credit_inputs(history)

    # The latest year has no total assets, so every line comes from the year before.
    assert result["period"] == "2025-03-31"
    assert result["lines"]["TotalLiabilities"] == 600.0
    assert result["lines"]["RetainedEarnings"] == 150.0
    assert result["debt"]["Bonds"] == 280.0 and result["debt"]["CommercialPaper"] is None
    assert result["debt_total"] == 400.0 and result["previous_debt_total"] == 300.0
    assert result["interest_coverage"] == pytest.approx(5.0)
    assert result["cost_of_debt"] == pytest.approx(12 / 350)


def test_credit_inputs_without_a_balance_sheet_are_empty():
    assert credit_inputs({"periods": ["2026-03-31"]})["period"] is None


def test_thesis_text_is_added_to_an_existing_database(tmp_path):
    database = tmp_path / "research.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TABLE company_research (user_id TEXT NOT NULL, edinet_code TEXT NOT NULL, thesis_status TEXT,"
            " target_value REAL, target_currency TEXT, review_on TEXT, version INTEGER NOT NULL DEFAULT 1,"
            " created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(user_id, edinet_code))"
        )
    store = ResearchStore(database)
    saved = store.upsert_company_research("user-a", "E1", thesis_status="buy", thesis="Pricing power in a consolidating market.")
    assert saved["thesis"] == "Pricing power in a consolidating market."
    assert [row["edinet_code"] for row in store.list_company_research("user-a")] == ["E1"]
    assert store.list_company_research("user-b") == []


def test_the_book_lists_followed_companies_with_alert_status(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.db")
    store.set_company_tags("u", "E1", ["Favorite", "Autos"])
    store.create_note("u", "Margins", "Watch the yen.", "E2")
    store.create_note("u", "Macro", "No company.")
    store.upsert_company_research("u", "E3", thesis_status="buy", target_value=3000, target_currency="JPY")
    store.create_alert("u", "Cheap", "E1", json.dumps({"metric": "PERatio", "operator": "<", "value": 12}))
    store.create_alert("u", "Pricey", "E2", json.dumps({"metric": "LatestPrice", "operator": ">", "value": 5000}))
    store.create_tag("u", "Empty")
    store.set_company_tags("other", "E9", ["Private"])

    monkeypatch.setattr(book_module, "_market_data", lambda _db, codes, _securities=None: (
        {code: {"company_code": code, "company_name": f"Company {code}", "ticker": f"{code[1:]}000"} for code in codes},
        {"E1": {"LatestPrice": 2500.0, "PERatio": 10.0}, "E2": {"LatestPrice": 4000.0}},
        {"1000": "JPY"},
    ))
    result = build_book(store, "u", "market.db")

    companies = {row["company_code"]: row for row in result["companies"]}
    assert set(companies) == {"E1", "E2", "E3"}
    assert companies["E1"]["tags"] == ["Autos", "Favorite"]
    assert companies["E1"]["alerts_triggered"] == 1 and companies["E1"]["price_currency"] == "JPY"
    assert companies["E2"]["note_count"] == 1 and companies["E2"]["alerts_triggered"] == 0
    assert companies["E3"]["thesis_status"] == "buy" and companies["E3"]["target_value"] == 3000
    assert {tag["name"]: tag["member_count"] for tag in result["tags"]} == {"Autos": 1, "Empty": 0, "Favorite": 1}
    alerts = {alert["name"]: alert for alert in result["alerts"]}
    assert alerts["Cheap"]["triggered"] and alerts["Cheap"]["current_value"] == 10.0
    assert alerts["Cheap"]["company_name"] == "Company E1"
    assert not alerts["Pricey"]["triggered"] and alerts["Pricey"]["current_value"] == 4000.0


def test_the_book_works_without_market_data(tmp_path):
    store = ResearchStore(tmp_path / "research.db")
    store.set_company_tags("u", "E1", ["Favorite"])
    store.create_alert("u", "Cheap", "E1", json.dumps({"metric": "PERatio", "operator": "<", "value": 12}))
    result = build_book(store, "u", None)
    assert result["companies"][0]["company_name"] == "E1"
    assert result["alerts"][0]["triggered"] is False


def test_pricing_endpoint_reports_unknown_companies(monkeypatch):
    from fastapi import HTTPException

    import src.research.api as research_api
    import src.research.pricing as pricing

    monkeypatch.setattr(research_api, "_market_db", lambda: "market.db")
    monkeypatch.setattr(pricing, "pricing_inputs", lambda _db, code: None if code == "E0" else {"company": {"company_code": code}})
    assert research_api.pricing_inputs(" E1 ")["company"]["company_code"] == "E1"
    with pytest.raises(HTTPException) as error:
        research_api.pricing_inputs("E0")
    assert error.value.status_code == 404
