from __future__ import annotations

import pytest

from src.comparison.peers import rank_peers
from src.comparison.service import statement_series

COMPANIES = [
    {"company_code": "E1", "ticker": "1000", "company_name": "Big Motor", "industry": "Autos"},
    {"company_code": "E2", "ticker": "2000", "company_name": "Small Motor", "industry": "Autos"},
    {"company_code": "E3", "ticker": "3000", "company_name": "Mid Motor", "industry": "Autos"},
    {"company_code": "E4", "ticker": "4000", "company_name": "Tiny Motor", "industry": "Autos"},
    {"company_code": "E5", "ticker": "", "company_name": "Unlisted Motor", "industry": "Autos"},
    {"company_code": "E6", "ticker": "6000", "company_name": "Bank", "industry": "Banks"},
    {"company_code": "E7", "ticker": "7000", "company_name": "Alpha Parts", "industry": "Autos"},
]
CAPS = {"E1": 1000.0, "E2": 10.0, "E3": 300.0, "E4": 1.0, "E6": 500.0, "E7": None}


def metrics_for(code: str, _ticker: str) -> dict:
    return {"MarketCap": CAPS.get(code), "PERatio": 12.0, "ReturnOnEquity": 0.1}


def test_peers_share_the_industry_and_come_closest_in_size_first():
    result = rank_peers(COMPANIES, ["E1"], metrics_for, limit=10)

    # Listed companies in the same industry only; no market cap ranks last.
    assert [row["company_code"] for row in result["peers"]] == ["E3", "E2", "E4", "E7"]
    assert result["industries"] == [{"industry": "Autos", "candidates": 4}]
    assert result["peers"][0]["nearest_code"] == "E1"
    assert result["peers"][0]["size_ratio"] == pytest.approx(0.3)
    assert result["peers"][0]["PERatio"] == 12.0
    assert result["peers"][-1]["size_ratio"] is None


def test_peers_of_a_set_measure_each_candidate_against_the_nearest_member():
    result = rank_peers(COMPANIES, ["E1", "E2"], metrics_for, limit=2)

    # Tiny (1) is 10x from Small (10); Mid (300) is 3.3x from Big (1000).
    assert [row["company_code"] for row in result["peers"]] == ["E3", "E4"]
    assert result["peers"][1]["nearest_code"] == "E2"
    assert result["total"] == 3


def test_companies_without_an_industry_have_no_peers():
    companies = [{**COMPANIES[0], "industry": ""}, COMPANIES[1]]
    assert rank_peers(companies, ["E1"], metrics_for)["peers"] == []


def test_statement_series_reads_each_year_and_derives_ratios():
    history = {
        "periods": ["2024-03-31", "2025-03-31", "2026-03-31"],
        "IncomeStatement": [
            {"field": "Net sales", "values": [100.0, 120.0, None]},
            {"field": "Total revenue", "values": [None, None, 150.0]},
            {"field": "Cost of sales", "values": [60.0, 70.0, 90.0]},
            {"field": "Operating income", "values": [10.0, 12.0, 15.0]},
            {"field": "Profit (loss)", "values": [5.0, -6.0, 9.0]},
        ],
        "BalanceSheet": [
            {"field": "Assets", "values": [200.0, 210.0, 220.0]},
            {"field": "Net assets", "values": [50.0, 0.0, 60.0]},
            {"field": "Liabilities", "values": [150.0, 210.0, 160.0]},
        ],
        "IncomeStatement_Rolling": [{"field": "Net sales_Growth_3_Year", "values": [None, 0.1, 0.2]}],
    }

    result = statement_series(history, ["IncomeStatement_Rolling.Net sales_Growth_3_Year"])
    series = result["series"]

    assert result["periods"] == ["2024-03-31", "2025-03-31", "2026-03-31"]
    assert series["Revenue"] == [100.0, 120.0, 150.0]
    assert series["GrossMargin"] == pytest.approx([0.4, 50 / 120, 0.4])
    assert series["OperatingMargin"] == pytest.approx([0.1, 0.1, 0.1])
    # Zero equity has no meaningful return or leverage.
    assert series["ReturnOnEquity"] == pytest.approx([0.1, None, 0.15])
    assert series["DebtToEquity"][1] is None
    assert series["IncomeStatement_Rolling.Net sales_Growth_3_Year"] == [None, 0.1, 0.2]


def test_trends_endpoint_returns_small_series_per_company(monkeypatch):
    import src.comparison.api as comparison_api

    history = {
        "periods": ["2025-03-31", "2026-03-31"],
        "IncomeStatement": [{"field": "Net sales", "values": [100.0, 110.0]}],
        "BalanceSheet": [],
        "ShareMetrics": [],
    }
    requested_sources = []

    def statements(_db, code, periods, statement_sources):
        requested_sources.append(statement_sources)
        if code == "E9":
            raise ValueError("no filings")
        return history

    monkeypatch.setattr(comparison_api, "_resolve_db", lambda: "fixture.db")
    monkeypatch.setattr(comparison_api, "_metric_catalog", lambda _db: {"IncomeStatement": ["Gross profit"]})
    monkeypatch.setattr(comparison_api, "get_security_statements", statements)

    response = comparison_api.trends(comparison_api.TrendsRequest(
        company_codes=["E1", "E9"], metrics=["IncomeStatement.Gross profit", "Unknown.Column"],
    ))

    assert [company["company_code"] for company in response["companies"]] == ["E1"]
    assert response["companies"][0]["series"]["Revenue"] == [100.0, 110.0]
    assert response["metrics"][-1] == "IncomeStatement.Gross profit"
    assert "Unknown.Column" not in response["metrics"]
    assert response["metric_definitions"]["ReturnOnEquity"]["direction"] == "higher"
    assert requested_sources[0]["IncomeStatement"] == "IncomeStatement"


def test_peers_endpoint_accepts_a_comma_separated_set(monkeypatch):
    import src.comparison.api as comparison_api

    calls = []
    monkeypatch.setattr(comparison_api, "_resolve_db", lambda: "fixture.db")
    monkeypatch.setattr(comparison_api, "find_peers", lambda _db, codes, limit: calls.append((codes, limit)) or {"peers": []})

    comparison_api.peers_for_set(codes=["E1,E2", "E2"], limit=5)
    comparison_api.peers("E3", limit=4)

    assert calls == [(["E1", "E2"], 5), (["E3"], 4)]


def test_snapshot_keeps_one_recent_entry_while_a_comparison_grows(monkeypatch):
    import src.comparison.api as comparison_api
    import src.research.runtime as research_runtime

    recorded = []

    class Store:
        def record_recent_work(self, *args, **_kwargs):
            recorded.append(args)

    class User:
        user_id = "u-1"

    class Request:
        class state:
            user = User()

    monkeypatch.setattr(comparison_api, "_resolve_db", lambda: "fixture.db")
    monkeypatch.setattr(comparison_api, "_metric_catalog", lambda _db: {})
    monkeypatch.setattr(comparison_api, "_snapshot_rows", lambda _db, codes, _metrics: ([{"company_code": code, "company": {}} for code in codes], []))
    monkeypatch.setattr(research_runtime, "store", Store())

    comparison_api.snapshot(comparison_api.ComparisonRequest(company_codes=["E1", "E2"]), Request())
    comparison_api.snapshot(comparison_api.ComparisonRequest(company_codes=["E1", "E2", "E3"]), Request())

    assert [args[2] for args in recorded] == ["comparison:E1", "comparison:E1"]
    assert recorded[-1][5] == "/compare?companies=E1,E2,E3"


def test_peer_metrics_are_cached_until_the_database_changes(tmp_path):
    import os

    from src.comparison.peers import _cached_metrics

    db = tmp_path / "standardized.db"
    db.write_text("v1")
    calls = []

    def compute(code, _ticker):
        calls.append(code)
        return {"MarketCap": 1.0}

    lookup = _cached_metrics(str(db), compute)
    lookup("E1", "1000")
    lookup("E1", "1000")
    assert calls == ["E1"]

    stamp = os.path.getmtime(db) + 10
    os.utime(db, (stamp, stamp))
    _cached_metrics(str(db), compute)("E1", "1000")
    assert calls == ["E1", "E1"]
