from __future__ import annotations

import sqlite3

from src.comparison.service import (
    extract_latest_statement_metrics,
    extract_latest_table_metrics,
    flatten_overview,
    normalize_companies,
)


def _overview(name: str, revenue: float, market_cap: float) -> dict:
    return {
        "company": {"company_name": name},
        "market": {"latest_price": 1000},
        "fundamentals_latest": {
            "Revenue": revenue,
            "OperatingIncome": revenue * 0.2,
            "NetIncome": revenue * 0.1,
            "TotalAssets": revenue * 2,
            "ShareholdersEquity": revenue,
            "SharesOutstanding": 100,
        },
        "valuation_latest": {
            "MarketCap": market_cap,
            "PERatio": market_cap / (revenue * 0.1),
            "PriceToBook": 2,
            "PriceToSales": market_cap / revenue,
            "DividendsYield": 0.02,
            "OperatingMargin": 0.2,
            "NetProfitMargin": 0.1,
        },
        "quality_latest": {
            "ReturnOnEquity": 0.1,
            "DebtToEquity": 0.5,
            "CurrentRatio": 1.5,
            "GrossMargin": 0.4,
        },
    }


def test_flatten_overview_maps_nested_analysis_payload():
    metrics = flatten_overview(_overview("Alpha", 100, 500))
    assert metrics["Revenue"] == 100
    assert metrics["TotalEquity"] == 100
    assert metrics["NetMargin"] == 0.1
    assert metrics["LatestPrice"] == 1000


def test_flatten_overview_prefers_the_analyze_metric_contract():
    overview = _overview("Alpha", 100, 500)
    overview["metrics"] = {
        "MarketCap": 900,
        "PERatio": 12,
        "PayoutRatio": 0.4,
        "ReturnOnAssets": 0.1,
    }

    metrics = flatten_overview(overview)

    assert metrics["MarketCap"] == 900
    assert metrics["PERatio"] == 12
    assert metrics["PayoutRatio"] == 0.4
    assert metrics["ReturnOnAssets"] == 0.1


def test_extract_latest_statement_metrics_skips_empty_latest_period():
    history = {
        "periods": ["2024-03-31", "2025-03-31", "2026-03-31"],
        "IncomeStatement": [
            {"field": "Net sales", "values": [100, 120, None]},
            {"field": "Operating Income - Operating profit (loss)", "values": [20, 24, None]},
            {"field": "Profit (loss)", "values": [10, 12, None]},
            {"field": "Cost of sales", "values": [60, 72, None]},
        ],
        "BalanceSheet": [
            {"field": "Assets", "values": [200, 240, None]},
            {"field": "Shareholders' equity", "values": [100, 120, None]},
            {"field": "Liabilities", "values": [100, 120, None]},
            {"field": "Current assets", "values": [80, 90, None]},
            {"field": "Current liabilities", "values": [40, 45, None]},
        ],
        "ShareMetrics": [
            {"field": "Number of issued shares as of filing date", "values": [10, 11, None]},
        ],
    }

    metrics, period = extract_latest_statement_metrics(history)

    assert period == "2025-03-31"
    assert metrics["Revenue"] == 120
    assert metrics["OperatingIncome"] == 24
    assert metrics["TotalEquity"] == 120
    assert metrics["SharesOutstanding"] == 11


def test_extract_latest_table_metrics_reads_arbitrary_table_columns():
    history = {
        "periods": ["2024-03-31", "2025-03-31", "2026-03-31"],
        "IncomeStatement": [
            {"field": "Gross profit", "values": [30, 40, None]},
            {"field": "Operating income", "values": [10, 12, None]},
        ],
    }

    values, period = extract_latest_table_metrics(
        history,
        ["IncomeStatement.Gross profit", "IncomeStatement.Operating income"],
    )

    assert values == {
        "IncomeStatement.Gross profit": 40,
        "IncomeStatement.Operating income": 12,
    }
    assert period == "2025-03-31"


def test_normalize_companies_calculates_common_size_and_peer_percentiles():
    overviews = {"E1": _overview("Alpha", 100, 500), "E2": _overview("Beta", 200, 1000)}
    result = normalize_companies(["E1", "E2"], overviews, ["Revenue", "TotalEquity", "MarketCap"])
    assert result["E1"]["common_size_income"]["OperatingIncome"] == 0.2
    assert result["E1"]["common_size_balance"]["TotalEquity"] == 0.5
    assert result["E1"]["percentiles"]["MarketCap"] == 0.0
    assert result["E2"]["percentiles"]["MarketCap"] == 0.5


def test_snapshot_endpoint_returns_real_metric_rows(monkeypatch):
    import src.comparison.api as comparison_api

    overviews = {"E1": _overview("Alpha", 100, 500), "E2": _overview("Beta", 200, 1000)}
    monkeypatch.setattr(comparison_api, "_resolve_db", lambda: "fixture.db")
    monkeypatch.setattr(comparison_api, "_metric_catalog", lambda _db: {})
    monkeypatch.setattr(comparison_api, "get_security_overview", lambda _db, company_code: overviews[company_code])

    response = comparison_api.snapshot(comparison_api.ComparisonRequest(
        company_codes=["E1", "E2"], metrics=["Revenue", "ReturnOnEquity"]
    ))
    assert response["missing"] == []
    assert response["companies"][0]["metrics"]["Revenue"] == 100
    assert response["companies"][1]["metrics"]["ReturnOnEquity"] == 0.1


def test_snapshot_endpoint_accepts_arbitrary_table_column_metric(monkeypatch):
    import src.comparison.api as comparison_api

    overviews = {"E1": _overview("Alpha", 100, 500), "E2": _overview("Beta", 200, 1000)}
    monkeypatch.setattr(comparison_api, "_resolve_db", lambda: "fixture.db")
    monkeypatch.setattr(
        comparison_api,
        "_metric_catalog",
        lambda _db: {"IncomeStatement": ["Gross profit"]},
    )
    monkeypatch.setattr(comparison_api, "get_security_overview", lambda _db, company_code: overviews[company_code])
    monkeypatch.setattr(comparison_api, "_enrich_overview", lambda _db, _code, overview: overview)
    monkeypatch.setattr(
        comparison_api,
        "_custom_metric_values",
        lambda _db, code, _metrics: ({"IncomeStatement.Gross profit": 40 if code == "E1" else 60}, "2025-03-31"),
    )

    response = comparison_api.snapshot(comparison_api.ComparisonRequest(
        company_codes=["E1", "E2"], metrics=["IncomeStatement.Gross profit"]
    ))

    assert response["metrics"] == ["IncomeStatement.Gross profit"]
    assert response["metric_definitions"]["IncomeStatement.Gross profit"] == {
        "label": "Gross profit",
        "group": "IncomeStatement",
    }
    assert response["companies"][0]["metrics"]["IncomeStatement.Gross profit"] == 40
    assert response["companies"][1]["metrics"]["IncomeStatement.Gross profit"] == 60


def test_metric_catalog_returns_joinable_table_columns(tmp_path):
    import src.comparison.api as comparison_api

    db_path = tmp_path / "standardized.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE IncomeStatement (docID TEXT, periodEnd TEXT, Revenue REAL, Label TEXT)"
        )
        conn.execute("CREATE TABLE CompanyInfo (Company_Code TEXT, Company_Name TEXT)")
        conn.execute("CREATE TABLE Unrelated (Value REAL)")
        conn.commit()
    finally:
        conn.close()

    catalog = comparison_api._metric_catalog(str(db_path))

    assert catalog["IncomeStatement"] == ["Revenue", "Label"]
    assert "CompanyInfo" not in catalog
    assert "Unrelated" not in catalog
