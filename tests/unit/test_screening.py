"""Tests for the screening package (src/screening/).

Each test uses an in-memory SQLite database with pre-populated sample data
so no external files are required.
"""

import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.screening import (
    DISCOVERED_SCREENING_MODULES,
    build_screening_query,
    delete_screening_criteria,
    export_screening_to_backtest_csv,
    export_screening_to_csv,
    format_financial_value,
    get_default_columns,
    get_available_metrics,
    get_available_periods,
    list_saved_screenings,
    load_screening_criteria,
    load_screening_history,
    run_screening,
    save_screening_criteria,
    save_screening_history,
)


def test_screening_package_discovery_exposes_main_module():
    assert "src.screening.screening" in DISCOVERED_SCREENING_MODULES


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_sample_db(path: str) -> str:
    """Create a minimal SQLite database with sample data for testing."""
    conn = sqlite3.connect(path)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE CompanyInfo (
            Company_Code TEXT PRIMARY KEY,
            Company_Ticker TEXT,
            CompanyName TEXT,
            Industry TEXT
        )
    """)
    c.execute("""
        CREATE TABLE FinancialStatements (
            Company_Code TEXT,
            docID TEXT UNIQUE,
            docTypeCode TEXT,
            Currency TEXT,
            periodStart TEXT,
            periodEnd TIMESTAMP,
            SharesOutstanding REAL,
            SharePrice REAL
        )
    """)
    c.execute("""
        CREATE TABLE Stock_Prices (
            Date TEXT,
            Ticker TEXT,
            Currency TEXT,
            Price REAL,
            PRIMARY KEY (Date, Ticker)
        )
    """)
    c.execute("""
        CREATE TABLE PerShare (
            docID TEXT UNIQUE,
            BookValue REAL,
            EPS REAL,
            Sales REAL
        )
    """)
    c.execute("""
        CREATE TABLE Valuation (
            docID TEXT UNIQUE,
            PERatio REAL,
            EarningsYield REAL,
            MarketCap REAL,
            PriceToBook REAL
        )
    """)
    c.execute("""
        CREATE TABLE Quality (
            docID TEXT UNIQUE,
            ReturnOnEquity REAL,
            GrossMargin REAL,
            DebtToEquity REAL
        )
    """)
    c.execute("""
        CREATE TABLE Valuation_Historical (
            docID TEXT UNIQUE,
            PERatio_5Year_Average REAL,
            PriceToBook REAL
        )
    """)

    # --- Insert sample data ---
    companies = [
        ("E00001", "10010", "Alpha Corp", "Industrial"),
        ("E00002", "20020", "Beta Co", "Retail"),
        ("E00003", "30030", "Gamma Ltd", "Industrial"),
    ]
    for code, ticker, company_name, industry in companies:
        c.execute(
            "INSERT INTO CompanyInfo VALUES (?, ?, ?, ?)",
            (code, ticker, company_name, industry),
        )

    filings = [
        ("E00001", "DOC001", "120", "2023-04-01", "2024-03-31", 1000000, 1500),
        ("E00002", "DOC002", "120", "2023-04-01", "2024-03-31", 2000000, 800),
        ("E00003", "DOC003", "120", "2023-04-01", "2024-03-31", 500000, 3200),
        ("E00001", "DOC004", "120", "2022-04-01", "2023-03-31", 1000000, 1200),
    ]
    for row in filings:
        c.execute(
            "INSERT INTO FinancialStatements (Company_Code, docID, docTypeCode, periodStart, periodEnd, SharesOutstanding, SharePrice) VALUES (?,?,?,?,?,?,?)",
            row,
        )

    prices = [
        ("2024-12-01", "10010", "JPY", 1600),
        ("2024-12-01", "20020", "JPY", 850),
        ("2024-12-01", "30030", "JPY", 3100),
        ("2024-11-01", "10010", "JPY", 1550),
    ]
    for row in prices:
        c.execute("INSERT INTO Stock_Prices VALUES (?,?,?,?)", row)

    per_share = [
        ("DOC001", 1200, 150, 5000),
        ("DOC002", 800, 80, 3000),
        ("DOC003", 2500, 300, 12000),
        ("DOC004", 1100, 120, 4500),
    ]
    for row in per_share:
        c.execute("INSERT INTO PerShare VALUES (?,?,?,?)", row)

    valuation = [
        ("DOC001", 10.0, 0.10, 1500000000, 1.25),
        ("DOC002", 10.0, 0.10, 1600000000, 0.95),
        ("DOC003", 10.67, 0.094, 1600000000, 1.40),
        ("DOC004", 10.0, 0.10, 1200000000, 1.10),
    ]
    for row in valuation:
        c.execute("INSERT INTO Valuation VALUES (?,?,?,?,?)", row)

    quality = [
        ("DOC001", 0.15, 0.40, 0.8),
        ("DOC002", 0.10, 0.35, 1.2),
        ("DOC003", 0.20, 0.50, 0.5),
        ("DOC004", 0.12, 0.38, 0.9),
    ]
    for row in quality:
        c.execute("INSERT INTO Quality VALUES (?,?,?,?)", row)

    valuation_historical = [
        ("DOC001", 11.0, 1.10),
        ("DOC002", 9.5, 0.90),
        ("DOC003", 12.0, 1.35),
        ("DOC004", 9.0, 1.00),
    ]
    for row in valuation_historical:
        c.execute("INSERT INTO Valuation_Historical VALUES (?, ?, ?)", row)

    conn.commit()
    conn.close()
    return path


@pytest.fixture
def sample_db(tmp_path):
    """Return path to a temporary sample database."""
    db_path = str(tmp_path / "test.db")
    _create_sample_db(db_path)
    return db_path


@pytest.fixture
def sample_db_companyinfo_variant(tmp_path):
    """Return a DB where CompanyInfo uses EdinetCode-style column names."""
    db_path = str(tmp_path / "test_variant.db")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE CompanyInfo (
            EdinetCode TEXT PRIMARY KEY,
            Company_Ticker TEXT,
            CompanyName TEXT,
            Industry TEXT
        )
    """)
    c.execute("""
        CREATE TABLE FinancialStatements (
            edinetCode TEXT,
            docID TEXT UNIQUE,
            docTypeCode TEXT,
            Currency TEXT,
            periodStart TEXT,
            periodEnd TIMESTAMP,
            SharesOutstanding REAL,
            SharePrice REAL
        )
    """)
    c.execute("""
        CREATE TABLE Stock_Prices (
            Date TEXT,
            Ticker TEXT,
            Currency TEXT,
            Price REAL,
            PRIMARY KEY (Date, Ticker)
        )
    """)
    c.execute("""
        CREATE TABLE Valuation (
            docID TEXT UNIQUE,
            PERatio REAL,
            EarningsYield REAL,
            MarketCap REAL
        )
    """)

    c.execute(
        "INSERT INTO CompanyInfo VALUES (?, ?, ?, ?)",
        ("E00001", "10010", "Alpha Corp", "Industrial"),
    )
    c.execute(
        "INSERT INTO FinancialStatements (edinetCode, docID, docTypeCode, periodStart, periodEnd, SharesOutstanding, SharePrice) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("E00001", "DOC001", "120", "2023-04-01", "2024-03-31", 1000000, 1500),
    )
    c.execute(
        "INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)",
        ("2024-12-01", "10010", "JPY", 1600),
    )
    c.execute(
        "INSERT INTO Valuation VALUES (?, ?, ?, ?)",
        ("DOC001", 10.0, 0.10, 1500000000),
    )

    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Tests — get_available_metrics
# ---------------------------------------------------------------------------

def test_get_available_metrics(sample_db):
    """All user tables should be returned with their columns."""
    metrics = get_available_metrics(sample_db)
    assert "CompanyInfo" in metrics
    assert "PerShare" in metrics
    assert "Valuation" in metrics
    assert "Quality" in metrics
    assert "FinancialStatements" in metrics
    assert "Stock_Prices" in metrics
    assert "CompanyName" in metrics["CompanyInfo"]
    assert "Industry" in metrics["CompanyInfo"]
    assert "BookValue" in metrics["PerShare"]
    assert "EPS" in metrics["PerShare"]
    assert "PERatio" in metrics["Valuation"]
    assert "ReturnOnEquity" in metrics["Quality"]
    # docID excluded from metric tables, kept in FinancialStatements/CompanyInfo
    for table in ("PerShare", "Valuation", "Quality"):
        assert "docID" not in metrics.get(table, [])
    assert "docID" in metrics.get("FinancialStatements", [])
    # Must return more than 1 table
    assert len(metrics) > 4


# ---------------------------------------------------------------------------
# Tests — get_available_periods
# ---------------------------------------------------------------------------

def test_get_available_periods(sample_db):
    """Should return distinct years from periodEnd."""
    periods = get_available_periods(sample_db)
    assert "2024" in periods
    assert "2023" in periods
    assert periods == sorted(periods)


# ---------------------------------------------------------------------------
# Tests — build_screening_query
# ---------------------------------------------------------------------------

def test_build_screening_query_single_criterion():
    """Single > filter should produce valid SQL with one param."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 5}]
    columns = ["CompanyInfo.Company_Code", "Valuation.PERatio"]
    sql, params = build_screening_query(criteria, columns)

    assert "SELECT" in sql
    assert "v.[PERatio] > ?" in sql
    assert params == [5]


def test_build_screening_query_multiple_criteria():
    """Multiple criteria should be ANDed together."""
    criteria = [
        {"table": "Valuation", "column": "PERatio", "operator": "<", "value": 15},
        {"table": "Quality", "column": "ReturnOnEquity", "operator": ">", "value": 0.1},
    ]
    columns = ["CompanyInfo.Company_Code"]
    sql, params = build_screening_query(criteria, columns)

    assert "v.[PERatio] < ?" in sql
    assert "q.[ReturnOnEquity] > ?" in sql
    assert len(params) == 2


def test_build_screening_query_between():
    """BETWEEN operator should generate correct SQL with two params."""
    criteria = [{
        "table": "Valuation", "column": "PERatio",
        "operator": "BETWEEN", "value": 5, "value2": 15,
    }]
    columns = ["CompanyInfo.Company_Code"]
    sql, params = build_screening_query(criteria, columns)

    assert "BETWEEN ? AND ?" in sql
    assert params == [5, 15]


def test_build_screening_query_with_period():
    """Period filter should be applied."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 5}]
    columns = ["CompanyInfo.Company_Code"]
    sql, params = build_screening_query(criteria, columns, period="2024")

    assert "SUBSTR(f.periodEnd, 1, 4) = ?" in sql
    assert "2024" in params


def test_build_screening_query_companyinfo_text_filter(sample_db):
    """CompanyInfo text columns should be available for filtering."""
    available = get_available_metrics(sample_db)
    criteria = [{
        "table": "CompanyInfo",
        "column": "Industry",
        "operator": "=",
        "value": "Industrial",
    }]
    columns = ["CompanyInfo.CompanyName", "CompanyInfo.Industry"]
    sql, params = build_screening_query(
        criteria, columns, available_metrics=available
    )

    assert "c.[Industry] = ?" in sql
    assert params == ["Industrial"]


def test_build_screening_query_dynamic_column_filter(sample_db):
    """Criteria can compare one company metric to another metric column."""
    available = get_available_metrics(sample_db)
    criteria = [{
        "table": "Valuation",
        "column": "PERatio",
        "operator": "<",
        "comparison_mode": "column",
        "compare_table": "Valuation_Historical",
        "compare_column": "PERatio_5Year_Average",
    }]
    columns = ["CompanyInfo.CompanyName", "Valuation.PERatio"]

    sql, params = build_screening_query(
        criteria, columns, available_metrics=available
    )

    assert "v.[PERatio] < vh.[PERatio_5Year_Average]" in sql
    assert params == []


def test_build_screening_query_validates_columns():
    """Invalid table/column names should raise ValueError."""
    available = {"Valuation": ["PERatio", "MarketCap"]}
    criteria = [{"table": "Valuation", "column": "FakeColumn", "operator": ">", "value": 5}]
    columns = ["CompanyInfo.Company_Code"]

    with pytest.raises(ValueError, match="Column"):
        build_screening_query(criteria, columns, available_metrics=available)


def test_build_screening_query_validates_operator():
    """Invalid operators should raise ValueError."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": "DROP", "value": 5}]
    columns = ["CompanyInfo.Company_Code"]

    with pytest.raises(ValueError, match="Invalid operator"):
        build_screening_query(criteria, columns)


def test_build_screening_query_safe_from_injection():
    """SQL injection via column names is blocked by bracket quoting + schema validation."""
    # Column names with special chars are now allowed (bracket-quoted),
    # but the column must exist in the schema — non-existent cols are rejected
    criteria = [{"table": "Valuation", "column": "PERatio; DROP TABLE", "operator": ">", "value": 5}]
    columns = ["CompanyInfo.Company_Code"]

    # Should not raise at identifier validation stage (bracket quoting handles it)
    # But should raise at schema validation if available_metrics is provided
    with pytest.raises(ValueError):
        build_screening_query(criteria, columns, available_metrics={"Valuation": ["PERatio"]})


def test_build_screening_query_allows_special_chars():
    """Column names with parentheses should be accepted (bracket-quoted)."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "CustomTable",
            "column": "Net cash provided by (used in) operating activities_Growth_10_Year",
            "operator": ">",
            "value": 0.05,
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    # The column name should appear bracket-quoted
    assert "Net cash provided by (used in) operating activities_Growth_10_Year" in sql
    assert "0.05" in str(params)


# ---------------------------------------------------------------------------
# Tests — run_screening
# ---------------------------------------------------------------------------

def test_run_screening_returns_dataframe(sample_db):
    """End-to-end test: run_screening should return a DataFrame."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 5}]
    columns = ["CompanyInfo.Company_Code", "Valuation.PERatio"]

    df = run_screening(sample_db, criteria, columns, period="2024")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "PERatio" in df.columns


def test_run_screening_empty_result(sample_db):
    """Query with impossible criteria should return empty DataFrame."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 999999}]
    columns = ["CompanyInfo.Company_Code"]

    df = run_screening(sample_db, criteria, columns)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_run_screening_sort(sample_db):
    """sort_by and sort_order parameters should work correctly."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}]
    columns = ["CompanyInfo.Company_Code", "Valuation.PERatio"]

    df_asc = run_screening(sample_db, criteria, columns, sort_by="PERatio", sort_order="ASC")
    df_desc = run_screening(sample_db, criteria, columns, sort_by="PERatio", sort_order="DESC")

    if len(df_asc) > 1:
        assert df_asc["PERatio"].iloc[0] <= df_asc["PERatio"].iloc[-1]
        assert df_desc["PERatio"].iloc[0] >= df_desc["PERatio"].iloc[-1]


def test_run_screening_companyinfo_text_filter(sample_db):
    """CompanyInfo string criteria should filter results correctly."""
    criteria = [{
        "table": "CompanyInfo",
        "column": "Industry",
        "operator": "=",
        "value": "Industrial",
    }]
    columns = ["CompanyInfo.CompanyName", "CompanyInfo.Industry"]

    df = run_screening(sample_db, criteria, columns, period="2024")

    assert list(df["Industry"].unique()) == ["Industrial"]
    assert set(df["CompanyName"]) == {"Alpha Corp", "Gamma Ltd"}


def test_run_screening_dynamic_column_filter(sample_db):
    """Dynamic criteria should compare the selected columns row-by-row."""
    criteria = [{
        "table": "Valuation",
        "column": "PERatio",
        "operator": "<",
        "comparison_mode": "column",
        "compare_table": "Valuation_Historical",
        "compare_column": "PERatio_5Year_Average",
    }]
    columns = ["CompanyInfo.CompanyName", "Valuation.PERatio"]

    df = run_screening(sample_db, criteria, columns, period="2024")

    assert set(df["CompanyName"]) == {"Alpha Corp", "Gamma Ltd"}


def test_run_screening_qualifies_duplicate_result_column_names(sample_db):
    """Duplicate metric names from different tables should stay distinct."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}]
    columns = [
        "CompanyInfo.CompanyName",
        "Valuation.PriceToBook",
        "Valuation_Historical.PriceToBook",
    ]

    df = run_screening(sample_db, criteria, columns, period="2024")

    assert "Valuation.PriceToBook" in df.columns
    assert "Valuation_Historical.PriceToBook" in df.columns
    assert "PriceToBook" not in df.columns

    alpha = df.loc[df["CompanyName"] == "Alpha Corp"].iloc[0]
    assert alpha["Valuation.PriceToBook"] == pytest.approx(1.25)
    assert alpha["Valuation_Historical.PriceToBook"] == pytest.approx(1.10)


def test_run_screening_with_weighted_ranking(sample_db):
    """Weighted ranking should add a rank column and sort by it."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}]
    columns = ["CompanyInfo.CompanyName"]
    ranking_rules = [
        {
            "table": "Valuation",
            "column": "PERatio",
            "weight": 1.0,
            "direction": "lower",
        },
        {
            "table": "Quality",
            "column": "ReturnOnEquity",
            "weight": 3.0,
            "direction": "higher",
        },
    ]

    df = run_screening(
        sample_db,
        criteria,
        columns,
        period="2024",
        ranking_algorithm="weighted_minmax",
        ranking_rules=ranking_rules,
    )

    assert "ScreeningRank" in df.columns
    assert "PERatio" not in df.columns
    assert "ReturnOnEquity" not in df.columns
    assert df.iloc[0]["CompanyName"] == "Gamma Ltd"
    assert list(df["ScreeningRank"]) == sorted(df["ScreeningRank"].tolist())


def test_run_screening_weighted_percentile_respects_lower_direction(sample_db):
    """Lower-is-better percentile ranking should favor smaller values."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}]
    df = run_screening(
        sample_db,
        criteria,
        ["CompanyInfo.CompanyName"],
        period="2024",
        ranking_algorithm="weighted_percentile",
        ranking_rules=[{
            "table": "Valuation",
            "column": "PERatio",
            "weight": 1.0,
            "direction": "lower",
        }],
    )

    assert list(df["ScreeningRank"]) == [1, 1, 2]
    assert df.iloc[-1]["CompanyName"] == "Gamma Ltd"


# ---------------------------------------------------------------------------
# Tests — export
# ---------------------------------------------------------------------------

def test_export_screening_to_csv(tmp_path, sample_db):
    """CSV export should create a file with correct contents."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}]
    columns = ["CompanyInfo.Company_Code", "Valuation.PERatio"]
    df = run_screening(sample_db, criteria, columns)

    output = str(tmp_path / "results.csv")
    result_path = export_screening_to_csv(df, output)

    assert Path(result_path).exists()
    loaded = pd.read_csv(result_path)
    assert len(loaded) == len(df)


def test_export_screening_to_backtest_csv_current_period(tmp_path, sample_db):
    """Current-period backtest export should produce the run_backtest_set CSV shape."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}]
    columns = [
        "CompanyInfo.Company_Ticker",
        "CompanyInfo.CompanyName",
        "Quality.ReturnOnEquity",
        "Valuation.PERatio",
    ]
    ranking_rules = [{
        "table": "Quality",
        "column": "ReturnOnEquity",
        "weight": 1.0,
        "direction": "higher",
    }]

    path = export_screening_to_backtest_csv(
        sample_db,
        criteria,
        columns,
        str(tmp_path / "backtest_current.csv"),
        period="2024",
        max_companies=2,
        ranking_algorithm="weighted_minmax",
        ranking_rules=ranking_rules,
    )

    exported = pd.read_csv(path)
    assert list(exported.columns[:4]) == ["Year", "Tickers", "Type", "Amount"]
    assert exported["Year"].astype(str).tolist() == ["2024", "2024"]
    assert exported["Tickers"].astype(str).tolist() == ["30030", "10010"]
    assert pytest.approx(exported["Amount"].sum()) == 1.0


def test_export_screening_to_backtest_csv_historical(tmp_path, sample_db):
    """Historical export should emit one portfolio slice per database year."""
    criteria = [{"table": "CompanyInfo", "column": "Industry", "operator": "=", "value": "Industrial"}]
    columns = [
        "CompanyInfo.Company_Ticker",
        "CompanyInfo.CompanyName",
        "Quality.ReturnOnEquity",
    ]
    ranking_rules = [{
        "table": "Quality",
        "column": "ReturnOnEquity",
        "weight": 1.0,
        "direction": "higher",
    }]

    path = export_screening_to_backtest_csv(
        sample_db,
        criteria,
        columns,
        str(tmp_path / "backtest_historical.csv"),
        max_companies=1,
        ranking_algorithm="weighted_minmax",
        ranking_rules=ranking_rules,
        historical=True,
    )

    exported = pd.read_csv(path)
    assert set(exported["Year"].astype(str)) == {"2023", "2024"}
    year_map = {
        str(row["Year"]): str(row["Tickers"])
        for _, row in exported.iterrows()
    }
    assert year_map["2023"] == "10010"
    assert year_map["2024"] == "30030"


# ---------------------------------------------------------------------------
# Tests — format_financial_value
# ---------------------------------------------------------------------------

def test_format_financial_value_percent():
    assert format_financial_value(0.15, "ReturnOnEquity") == "0.15"
    assert format_financial_value(0.035, "GrossMargin") == "0.035"
    assert format_financial_value(0.15, "ReturnOnEquity", formatted=True) == "15.00%"


def test_format_financial_value_currency():
    assert format_financial_value(1_500_000_000, "MarketCap") == "1500000000"
    assert format_financial_value(5_000_000, "EnterpriseValue") == "5000000"
    assert format_financial_value(999, "SharePrice") == "999"
    assert format_financial_value(1_500_000_000, "MarketCap", formatted=True) == "1,500,000,000"


def test_format_financial_value_ratio():
    result = format_financial_value(10.567, "PERatio")
    assert result == "10.567"
    assert format_financial_value(10.567, "PERatio", formatted=True) == "10.57"


def test_format_financial_value_screening_columns():
    assert format_financial_value(3, "ScreeningRank", formatted=True) == "3"
    assert format_financial_value(0.81234, "ScreeningScore", formatted=True) == "0.812"


def test_format_financial_value_none():
    assert format_financial_value(None, "PERatio") == "—"
    assert format_financial_value(float("nan"), "PERatio") == "—"


def test_get_default_columns_includes_company_name_and_industry(sample_db):
    """Resolved defaults should include CompanyInfo name and industry columns."""
    metrics = get_available_metrics(sample_db)

    columns = get_default_columns(metrics)

    assert "CompanyInfo.Company_Code" in columns
    assert "CompanyInfo.Company_Ticker" in columns
    assert "FinancialStatements.periodEnd" in columns
    assert "CompanyInfo.CompanyName" in columns
    assert "CompanyInfo.Industry" in columns


def test_run_screening_accepts_companyinfo_column_aliases(sample_db_companyinfo_variant):
    """CompanyInfo defaults should adapt to EdinetCode-style schemas."""
    metrics = get_available_metrics(sample_db_companyinfo_variant)
    columns = get_default_columns(metrics)

    assert "CompanyInfo.EdinetCode" in columns
    assert "CompanyInfo.Company_Ticker" in columns

    df = run_screening(
        sample_db_companyinfo_variant,
        [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}],
        columns,
        period="2024",
    )

    assert len(df) == 1


# ---------------------------------------------------------------------------
# Tests — persistence (criteria)
# ---------------------------------------------------------------------------

def test_save_and_load_criteria(tmp_path):
    """Round-trip: save, load, verify equality."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 5}]
    columns = ["CompanyInfo.Company_Code", "Valuation.PERatio"]
    period = "2024"
    ranking_rules = [{
        "table": "Valuation",
        "column": "PERatio",
        "weight": 1.0,
        "direction": "lower",
    }]

    save_screening_criteria(
        "test_screen",
        criteria,
        columns,
        period,
        str(tmp_path),
        ranking_algorithm="weighted_minmax",
        ranking_rules=ranking_rules,
    )
    loaded = load_screening_criteria("test_screen", str(tmp_path))

    assert loaded["criteria"] == criteria
    assert loaded["columns"] == columns
    assert loaded["period"] == period
    assert loaded["ranking_algorithm"] == "weighted_minmax"
    assert loaded["ranking_rules"] == ranking_rules


def test_list_saved_screenings(tmp_path):
    """Save multiple, list, verify sorted names."""
    for name in ["beta", "alpha", "gamma"]:
        save_screening_criteria(name, [], [], None, str(tmp_path))

    names = list_saved_screenings(str(tmp_path))
    assert names == ["alpha", "beta", "gamma"]


def test_saved_screening_name_sanitization_avoids_collisions(tmp_path):
    """Different names that sanitize to the same stem should both be preserved."""
    criteria_a = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 1}]
    criteria_b = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 5}]

    save_screening_criteria("A/B", criteria_a, [], None, str(tmp_path))
    save_screening_criteria("AB", criteria_b, [], "2025", str(tmp_path))

    assert len(list(Path(tmp_path).glob("*.json"))) == 2
    assert set(list_saved_screenings(str(tmp_path))) == {"A/B", "AB"}
    assert load_screening_criteria("A/B", str(tmp_path))["criteria"] == criteria_a
    assert load_screening_criteria("AB", str(tmp_path))["criteria"] == criteria_b


def test_delete_screening_criteria(tmp_path):
    """Save, delete, verify removed."""
    save_screening_criteria("to_delete", [], [], None, str(tmp_path))
    assert "to_delete" in list_saved_screenings(str(tmp_path))

    delete_screening_criteria("to_delete", str(tmp_path))
    assert "to_delete" not in list_saved_screenings(str(tmp_path))


def test_delete_nonexistent_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        delete_screening_criteria("nonexistent", str(tmp_path))


# ---------------------------------------------------------------------------
# Tests — persistence (history)
# ---------------------------------------------------------------------------

def test_save_and_load_history(tmp_path):
    """Save multiple entries, load, verify order and contents."""
    history_path = str(tmp_path / "history.jsonl")

    entry1 = {"criteria": [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 5}],
              "result_count": 10, "period": "2024"}
    entry2 = {"criteria": [{"table": "Quality", "column": "ROE", "operator": ">", "value": 0.1}],
              "result_count": 5, "period": "2023"}

    save_screening_history(entry1, history_path)
    save_screening_history(entry2, history_path)

    history = load_screening_history(history_path)
    assert len(history) == 2
    # Most recent first
    assert history[0]["result_count"] == 5
    assert history[1]["result_count"] == 10
    # Timestamps should have been added
    assert "timestamp" in history[0]
    assert "timestamp" in history[1]


def test_load_history_empty(tmp_path):
    """Loading from nonexistent file should return empty list."""
    history = load_screening_history(str(tmp_path / "nope.jsonl"))
    assert history == []


# ---------------------------------------------------------------------------
# Tests — screening_date (point-in-time screening)
# ---------------------------------------------------------------------------


def test_build_query_with_screening_date():
    """screening_date should select latest filing per company before date."""
    sql, params = build_screening_query(
        criteria=[],
        columns=["CompanyInfo.Company_Code"],
        screening_date="2020-01-01",
    )
    assert "WHERE date(periodEnd) <= ?" in sql
    assert "MAX(periodEnd) AS max_period" in sql
    assert params[0] == "2020-01-01"
    # Stock prices should also be capped
    assert "WHERE date([Date]) <= ?" in sql
    assert params[1] == "2020-01-01"


def test_build_query_with_screening_date_and_period():
    """screening_date + period should both apply."""
    sql, params = build_screening_query(
        criteria=[],
        columns=["CompanyInfo.Company_Code"],
        period="2020",
        screening_date="2020-01-01",
    )
    assert "SUBSTR(f.periodEnd, 1, 4) = ?" in sql
    assert params[-1] == "2020"  # period param is last


def test_run_screening_with_screening_date(sample_db):
    """Point-in-time screening should return correct results."""
    # sample_db has 2023 and 2024 periods. With date 2023-06-01,
    # only company E00001 (2023 period) should match, E00002 (2024) should not.
    df = run_screening(
        sample_db,
        criteria=[],
        columns=["CompanyInfo.Company_Code"],
        screening_date="2023-06-01",
    )
    # Both companies are in CompanyInfo, but only E00001 has financials <= date
    assert len(df) >= 1
    assert "E00001" in df["Company_Code"].values


def test_run_screening_date_without_results(sample_db):
    """screening_date before all data should return empty."""
    df = run_screening(
        sample_db,
        criteria=[],
        columns=["CompanyInfo.Company_Code"],
        screening_date="2010-01-01",
    )
    assert len(df) == 0


# ---------------------------------------------------------------------------
# Tests — column comparison with offset
# ---------------------------------------------------------------------------


def test_column_comparison_with_offset():
    """Column compare with offset should generate (right + offset) SQL."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "Valuation_Historical",
            "column": "Growth5Y",
            "operator": ">",
            "comparison_mode": "column",
            "compare_table": "Valuation_Historical",
            "compare_column": "Growth10Y",
            "offset": 0.02,
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    # Should contain (right_col + offset_param)
    assert "(vh.[Growth10Y] + ?)" in sql
    assert 0.02 in params


def test_column_comparison_without_offset():
    """Column compare without offset should NOT add + ?."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "Valuation_Historical",
            "column": "Growth5Y",
            "operator": ">",
            "comparison_mode": "column",
            "compare_table": "Valuation_Historical",
            "compare_column": "Growth10Y",
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "vh.[Growth5Y] > vh.[Growth10Y]" in sql
    # No offset param: " + ?" should NOT be in the column comparison part
    assert "Growth10Y] + ?" not in sql


def test_column_comparison_with_negative_offset():
    """Negative offset should work (e.g. A > B - 0.05)."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "PerShare",
            "column": "EPS",
            "operator": "<",
            "comparison_mode": "column",
            "compare_table": "PerShare",
            "compare_column": "BookValue",
            "offset": -0.5,
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "-0.5" in str(params) or any(p == -0.5 for p in params)


def test_column_comparison_between_rejected():
    """BETWEEN should not be allowed with column comparison mode."""
    with pytest.raises(ValueError, match="BETWEEN"):
        build_screening_query(
            criteria=[{
                "table": "PerShare",
                "column": "EPS",
                "operator": "BETWEEN",
                "comparison_mode": "column",
                "compare_table": "PerShare",
                "compare_column": "BookValue",
            }],
            columns=["CompanyInfo.Company_Code"],
        )


# ---------------------------------------------------------------------------
# Tests — computed columns
# ---------------------------------------------------------------------------


def test_computed_price_ratio_column():
    """Price ratio formula should generate CASE WHEN division."""
    sql, params = build_screening_query(
        criteria=[],
        columns=["CompanyInfo.Company_Code"],
        computed_columns=[{
            "name": "P/E Ratio",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "PerShare",
            "denominator_column": "EPS",
        }],
    )
    assert "P/E Ratio" in sql
    assert "s_p.[Price]" in sql
    assert "ps.[EPS]" in sql
    assert "CASE WHEN" in sql


def test_computed_custom_formula():
    """Custom formula should be injected directly."""
    sql, params = build_screening_query(
        criteria=[],
        columns=["CompanyInfo.Company_Code"],
        computed_columns=[{
            "name": "CustomScore",
            "formula_type": "custom",
            "formula": "ps.[EPS] * 2.0 + ps.[BookValue]",
        }],
    )
    assert "CustomScore" in sql
    assert "ps.[EPS] * 2.0 + ps.[BookValue]" in sql


def test_computed_column_appears_in_results(sample_db):
    """Computed P/E column should appear in run_screening output."""
    df = run_screening(
        sample_db,
        criteria=[],
        columns=["CompanyInfo.Company_Code"],
        period="2024",
        computed_columns=[{
            "name": "PE_Ratio",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "PerShare",
            "denominator_column": "EPS",
        }],
    )
    assert "PE_Ratio" in df.columns


def test_computed_column_visible_in_output(sample_db):
    """Computed column should be retained in visible columns."""
    df = run_screening(
        sample_db,
        criteria=[],
        columns=["CompanyInfo.Company_Code"],
        computed_columns=[{
            "name": "TestFormula",
            "formula_type": "custom",
            "formula": "1.0",
        }],
    )
    assert "TestFormula" in df.columns
    # Should evaluate to 1.0 for all rows
    assert (df["TestFormula"] == 1.0).all()


# ---------------------------------------------------------------------------
# Tests — combined features
# ---------------------------------------------------------------------------


def test_screening_date_with_criteria_and_computed(sample_db):
    """All features together: date, column-compare with offset, computed."""
    df = run_screening(
        sample_db,
        criteria=[{
            "table": "PerShare",
            "column": "BookValue",
            "operator": ">",
            "comparison_mode": "column",
            "compare_table": "PerShare",
            "compare_column": "EPS",
            "offset": -100,
        }],
        columns=["CompanyInfo.Company_Code", "PerShare.BookValue", "PerShare.EPS"],
        screening_date="2024-06-01",
        computed_columns=[{
            "name": "P/B",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "PerShare",
            "denominator_column": "BookValue",
        }],
    )
    # Just verify it doesn't crash
    assert isinstance(df, pd.DataFrame)
    if len(df) > 0:
        assert "P/B" in df.columns


# ---------------------------------------------------------------------------
# Tests — IN operator
# ---------------------------------------------------------------------------


def test_in_operator_with_values(sample_db):
    """IN operator should work with a list of values."""
    df = run_screening(
        sample_db,
        criteria=[{
            "table": "CompanyInfo",
            "column": "Industry",
            "operator": "IN",
            "comparison_mode": "in",
            "values": ["Industrial", "Technology"],
        }],
        columns=["CompanyInfo.Company_Code", "CompanyInfo.Industry"],
    )
    assert len(df) >= 1
    for _, row in df.iterrows():
        assert row["Industry"] in ("Industrial", "Technology")


def test_in_operator_query_building():
    """IN operator should generate proper SQL."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "CompanyInfo",
            "column": "Industry",
            "operator": "IN",
            "comparison_mode": "in",
            "values": ["Tech", "Finance"],
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "IN (?, ?)" in sql
    assert params == ["Tech", "Finance"]


def test_in_operator_falls_back_to_single_value():
    """IN with no values list should use single value."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "CompanyInfo",
            "column": "Industry",
            "operator": "IN",
            "value": "Tech",
            "comparison_mode": "in",
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "IN (?)" in sql
    assert params == ["Tech"]


def test_in_operator_empty_values_raises():
    """IN with no values should raise ValueError."""
    with pytest.raises(ValueError):
        build_screening_query(
            criteria=[{
                "table": "CompanyInfo",
                "column": "Industry",
                "operator": "IN",
                "comparison_mode": "in",
            }],
            columns=["CompanyInfo.Company_Code"],
        )


# ---------------------------------------------------------------------------
# Tests — LIKE operator
# ---------------------------------------------------------------------------


def test_like_operator():
    """LIKE operator should generate proper SQL."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "CompanyInfo",
            "column": "CompanyName",
            "operator": "LIKE",
            "value": "%Alpha%",
            "comparison_mode": "like",
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "LIKE ?" in sql
    assert params == ["%Alpha%"]


def test_full_expression_matches_user_scenario(sample_db):
    """Exact user scenario: Stock_Prices.Price < ShareMetrics_Rolling.EPS * 8."""
    # The sample DB doesn't have ShareMetrics_Rolling, but it has PerShare with EPS.
    # We use PerShare to validate the full_expression pipeline works end-to-end.
    df = run_screening(
        sample_db,
        criteria=[{
            "comparison_mode": "full_expression",
            "operator": "<",
            "left_side": [
                {"type": "column", "table": "Stock_Prices", "column": "Price"},
            ],
            "right_side": [
                {"type": "column", "table": "PerShare", "column": "EPS"},
                {"type": "op", "op": "*"},
                {"type": "value", "value": 8},
            ],
        }],
        columns=[
            "CompanyInfo.CompanyName",
            "Stock_Prices.Price",
            "PerShare.EPS",
        ],
        period="2024",
    )
    # Alpha: Price=1600, EPS*8=150*8=1200 → 1600 < 1200 is FALSE
    # Beta: Price=850, EPS*8=80*8=640 → 850 < 640 is FALSE
    # Gamma: Price=3100, EPS*8=300*8=2400 → 3100 < 2400 is FALSE
    # All three should be excluded (Price is NOT less than EPS*8)
    assert len(df) == 0

    # Now try with a value that should match: Price*0.5 < EPS*8
    df2 = run_screening(
        sample_db,
        criteria=[{
            "comparison_mode": "full_expression",
            "operator": "<",
            "left_side": [
                {"type": "column", "table": "Stock_Prices", "column": "Price"},
                {"type": "op", "op": "*"},
                {"type": "value", "value": 0.5},
            ],
            "right_side": [
                {"type": "column", "table": "PerShare", "column": "EPS"},
                {"type": "op", "op": "*"},
                {"type": "value", "value": 8},
            ],
        }],
        columns=["CompanyInfo.CompanyName", "Stock_Prices.Price", "PerShare.EPS"],
        period="2024",
    )
    # Alpha: 1600*0.5=800 < 1200 ✓
    # Beta: 850*0.5=425 < 640 ✓
    # Gamma: 3100*0.5=1550 < 2400 ✓
    assert len(df2) == 3
    assert set(df2["CompanyName"]) == {"Alpha Corp", "Beta Co", "Gamma Ltd"}


def test_full_expression_simple():
    """full_expression should build SQL from left and right token arrays."""
    sql, params = build_screening_query(
        criteria=[{
            "comparison_mode": "full_expression",
            "operator": ">",
            "left_side": [
                {"type": "column", "table": "PerShare", "column": "EPS"},
                {"type": "op", "op": "*"},
                {"type": "value", "value": 8},
            ],
            "right_side": [
                {"type": "column", "table": "Valuation", "column": "PERatio"},
            ],
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "(ps.[EPS] * ?) > (v.[PERatio])" in sql
    assert params == [8]


def test_full_expression_both_sides_complex():
    """Both sides with multiple tokens should work."""
    sql, params = build_screening_query(
        criteria=[{
            "comparison_mode": "full_expression",
            "operator": "<=",
            "left_side": [
                {"type": "column", "table": "PerShare", "column": "Sales"},
                {"type": "op", "op": "/"},
                {"type": "value", "value": 2},
            ],
            "right_side": [
                {"type": "column", "table": "Stock_Prices", "column": "Price"},
                {"type": "op", "op": "*"},
                {"type": "value", "value": 0.5},
            ],
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "(ps.[Sales] / ?) <= (s_p.[Price] * ?)" in sql
    assert params == [2, 0.5]


def test_full_expression_rejects_invalid_operators():
    """full_expression should reject BETWEEN and LIKE (IN is now allowed)."""
    for op in ("BETWEEN", "LIKE"):
        with pytest.raises(ValueError):
            build_screening_query(
                criteria=[{
                    "comparison_mode": "full_expression",
                    "operator": op,
                    "left_side": [{"type": "value", "value": 1}],
                    "right_side": [{"type": "value", "value": 2}],
                }],
                columns=["CompanyInfo.Company_Code"],
            )


def test_full_expression_requires_both_sides():
    """full_expression should raise if missing left_side or right_side."""
    with pytest.raises(ValueError, match="full_expression requires"):
        build_screening_query(
            criteria=[{
                "comparison_mode": "full_expression",
                "operator": ">",
                "left_side": [{"type": "value", "value": 1}],
            }],
            columns=["CompanyInfo.Company_Code"],
        )


def test_full_expression_end_to_end(sample_db):
    """End-to-end: full_expression with stock price comparison."""
    # Sales / 2 > Price (Alpha: 5000/2=2500 > 1600 ✓, Beta: 3000/2=1500 > 850 ✓, Gamma: 12000/2=6000 > 3100 ✓)
    df = run_screening(
        sample_db,
        criteria=[{
            "comparison_mode": "full_expression",
            "operator": ">",
            "left_side": [
                {"type": "column", "table": "PerShare", "column": "Sales"},
                {"type": "op", "op": "/"},
                {"type": "value", "value": 2},
            ],
            "right_side": [
                {"type": "column", "table": "Stock_Prices", "column": "Price"},
            ],
        }],
        columns=["CompanyInfo.CompanyName"],
        period="2024",
    )
    assert len(df) == 3
    assert set(df["CompanyName"]) == {"Alpha Corp", "Beta Co", "Gamma Ltd"}


def test_full_expression_with_stock_price_multiply(sample_db):
    """End-to-end: EPS * 8 > Price * 0.5."""
    # Alpha: 150*8=1200 > 1600*0.5=800 ✓
    # Beta: 80*8=640 > 850*0.5=425 ✓
    # Gamma: 300*8=2400 > 3100*0.5=1550 ✓
    df = run_screening(
        sample_db,
        criteria=[{
            "comparison_mode": "full_expression",
            "operator": ">",
            "left_side": [
                {"type": "column", "table": "PerShare", "column": "EPS"},
                {"type": "op", "op": "*"},
                {"type": "value", "value": 8},
            ],
            "right_side": [
                {"type": "column", "table": "Stock_Prices", "column": "Price"},
                {"type": "op", "op": "*"},
                {"type": "value", "value": 0.5},
            ],
        }],
        columns=["CompanyInfo.CompanyName"],
        period="2024",
    )
    assert len(df) == 3


def test_stock_price_mode_simple():
    """stock_price mode should compare a column against s_p.[Price]."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "PerShare",
            "column": "BookValue",
            "operator": "<",
            "comparison_mode": "stock_price",
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "ps.[BookValue] < s_p.[Price]" in sql
    assert params == []


def test_stock_price_mode_with_left_expression():
    """stock_price mode with left_expression should apply arithmetic."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "PerShare",
            "column": "Sales",
            "operator": "<=",
            "comparison_mode": "stock_price",
            "left_expression": "/ 2",
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "(ps.[Sales] / 2) <= s_p.[Price]" in sql
    assert params == []


def test_stock_price_mode_with_complex_expression():
    """stock_price mode should accept parentheses and decimals."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "PerShare",
            "column": "EPS",
            "operator": ">",
            "comparison_mode": "stock_price",
            "left_expression": "* 15.5 + 100",
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "(ps.[EPS] * 15.5 + 100) > s_p.[Price]" in sql


def test_stock_price_rejects_injection():
    """stock_price left_expression should reject non-arithmetic characters."""
    with pytest.raises(ValueError, match="Invalid stock_price expression"):
        build_screening_query(
            criteria=[{
                "table": "PerShare",
                "column": "BookValue",
                "operator": ">",
                "comparison_mode": "stock_price",
                "left_expression": "; DROP TABLE x--",
            }],
            columns=["CompanyInfo.Company_Code"],
        )


def test_stock_price_rejects_between():
    """stock_price mode should not support BETWEEN."""
    with pytest.raises(ValueError, match="stock_price"):
        build_screening_query(
            criteria=[{
                "table": "PerShare",
                "column": "BookValue",
                "operator": "BETWEEN",
                "comparison_mode": "stock_price",
            }],
            columns=["CompanyInfo.Company_Code"],
        )


def test_stock_price_mode_end_to_end(sample_db):
    """End-to-end: stock_price mode should filter results correctly."""
    # BookValue for Alpha Corp (DOC001) = 1200, stock price = 1600
    # BookValue < Price (1200 < 1600) → Alpha should match
    # BookValue for Beta Co (DOC002) = 800, stock price = 850
    # BookValue < Price (800 < 850) → Beta should match
    # BookValue for Gamma Ltd (DOC003) = 2500, stock price = 3100
    # BookValue < Price (2500 < 3100) → Gamma should match (all three match)
    df = run_screening(
        sample_db,
        criteria=[{
            "table": "PerShare",
            "column": "BookValue",
            "operator": "<",
            "comparison_mode": "stock_price",
        }],
        columns=["CompanyInfo.Company_Code", "CompanyInfo.CompanyName"],
        period="2024",
    )
    assert len(df) >= 1
    # All three companies have BookValue < their current stock price
    assert set(df["CompanyName"]) == {"Alpha Corp", "Beta Co", "Gamma Ltd"}


def test_stock_price_mode_with_expression_end_to_end(sample_db):
    """End-to-end: stock_price with left_expression should filter correctly."""
    # Sales for Alpha Corp (DOC001) = 5000
    # Sales / 2 = 2500 > Current Price 1600 → matches
    # Sales for Beta Co (DOC002) = 3000
    # Sales / 2 = 1500 > Current Price 850 → matches
    # Sales for Gamma Ltd (DOC003) = 12000
    # Sales / 2 = 6000 > Current Price 3100 → matches
    df = run_screening(
        sample_db,
        criteria=[{
            "table": "PerShare",
            "column": "Sales",
            "operator": ">",
            "comparison_mode": "stock_price",
            "left_expression": "/ 2",
        }],
        columns=["CompanyInfo.CompanyName"],
        period="2024",
    )
    assert len(df) == 3
    assert set(df["CompanyName"]) == {"Alpha Corp", "Beta Co", "Gamma Ltd"}


def test_is_null_operator():
    """IS should generate col IS NULL without a param."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "PerShare",
            "column": "BookValue",
            "operator": "IS",
            "comparison_mode": "fixed",
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "ps.[BookValue] IS NULL" in sql
    assert params == []


def test_is_not_null_operator():
    """IS NOT should generate col IS NOT NULL without a param."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "PerShare",
            "column": "BookValue",
            "operator": "IS NOT",
            "comparison_mode": "fixed",
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "ps.[BookValue] IS NOT NULL" in sql
    assert params == []


def test_is_null_end_to_end(sample_db):
    """IS NULL should filter correctly in run_screening."""
    # In our sample, all BookValue values are non-NULL, so IS NULL should return 0 rows
    df = run_screening(
        sample_db,
        criteria=[{
            "table": "PerShare",
            "column": "BookValue",
            "operator": "IS",
            "comparison_mode": "fixed",
        }],
        columns=["CompanyInfo.CompanyName"],
        period="2024",
    )
    assert len(df) == 0

    # IS NOT NULL should return all companies with data
    df2 = run_screening(
        sample_db,
        criteria=[{
            "table": "PerShare",
            "column": "BookValue",
            "operator": "IS NOT",
            "comparison_mode": "fixed",
        }],
        columns=["CompanyInfo.CompanyName"],
        period="2024",
    )
    assert len(df2) >= 1


def test_like_operator_results(sample_db):
    """LIKE should match patterns in text columns."""
    df = run_screening(
        sample_db,
        criteria=[{
            "table": "CompanyInfo",
            "column": "CompanyName",
            "operator": "LIKE",
            "value": "%Alpha%",
            "comparison_mode": "like",
        }],
        columns=["CompanyInfo.Company_Code", "CompanyInfo.CompanyName"],
    )
    assert len(df) >= 1
    found = False
    for _, row in df.iterrows():
        if "Alpha" in str(row.get("CompanyName", "")):
            found = True
    assert found


# ---------------------------------------------------------------------------
# Tests — expression mode
# ---------------------------------------------------------------------------


def test_expression_mode_simple():
    """Expression mode: Column > 0.75 * OtherColumn."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "PerShare",
            "column": "BookValue",
            "operator": ">",
            "comparison_mode": "expression",
            "right_side": [
                {"type": "value", "value": 0.75},
                {"type": "op", "op": "*"},
                {"type": "column", "table": "PerShare", "column": "EPS"},
            ],
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "0.75" in str(params) or any(p == 0.75 for p in params)
    assert "ps.[EPS]" in sql
    assert "*" in sql


def test_expression_mode_with_addition():
    """Expression mode: Column > OtherColumn + value."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "PerShare",
            "column": "BookValue",
            "operator": "<=",
            "comparison_mode": "expression",
            "right_side": [
                {"type": "column", "table": "PerShare", "column": "Sales"},
                {"type": "op", "op": "+"},
                {"type": "value", "value": 1000},
            ],
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "ps.[Sales]" in sql
    assert "+" in sql
    assert 1000 in params


# ---------------------------------------------------------------------------
# Tests — arbitrary / custom tables (not in hardcoded list)
# ---------------------------------------------------------------------------


def test_get_metrics_includes_arbitrary_table(tmp_path):
    """Tables with any name should appear in available metrics."""
    db_path = str(tmp_path / "custom.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE CompanyInfo (Company_Code TEXT, Ticker TEXT)")
    conn.execute("CREATE TABLE FinancialStatements (Company_Code TEXT, docID TEXT, periodEnd TEXT)")
    conn.execute("CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Price REAL)")
    conn.execute("CREATE TABLE Financial_Ratios_Rolling (docID TEXT, Net_Margin_3Y REAL, ROA_10Y REAL)")
    conn.commit()
    conn.close()

    metrics = get_available_metrics(db_path)
    assert "Financial_Ratios_Rolling" in metrics
    assert "Net_Margin_3Y" in metrics["Financial_Ratios_Rolling"]
    assert "ROA_10Y" in metrics["Financial_Ratios_Rolling"]
    # docID excluded from arbitrary tables
    assert "docID" not in metrics["Financial_Ratios_Rolling"]
    # More than 1 table
    assert len(metrics) > 3


def test_build_query_with_arbitrary_table(tmp_path):
    """Query building should work with arbitrary table names."""
    sql, params = build_screening_query(
        criteria=[{
            "table": "Financial_Ratios_Rolling",
            "column": "Net_Margin_3Y",
            "operator": ">",
            "value": 0.05,
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    # Should use table name as alias
    assert "[Financial_Ratios_Rolling]" in sql
    assert "Net_Margin_3Y" in sql

def test_parenthesized_expression_uses_standard_precedence():
    """Parenthesis tokens should be retained and values parameterised."""
    sql, params = build_screening_query(
        criteria=[{
            "comparison_mode": "full_expression",
            "operator": ">",
            "left_side": [
                {"type": "paren", "value": "("},
                {"type": "value", "value": 2},
                {"type": "op", "op": "+"},
                {"type": "value", "value": 3},
                {"type": "paren", "value": ")"},
                {"type": "op", "op": "*"},
                {"type": "value", "value": 4},
            ],
            "right_side": [{"type": "value", "value": 19}],
        }],
        columns=["CompanyInfo.Company_Code"],
    )
    assert "( ? + ? ) * ?" in sql
    assert params == [2, 3, 4, 19]


def test_unbalanced_expression_parentheses_are_rejected():
    """Malformed formulas must fail before reaching SQLite."""
    with pytest.raises(ValueError, match="unmatched"):
        build_screening_query(
            criteria=[{
                "comparison_mode": "full_expression",
                "operator": ">",
                "left_side": [
                    {"type": "paren", "value": "("},
                    {"type": "value", "value": 1},
                    {"type": "op", "op": "+"},
                    {"type": "value", "value": 2},
                ],
                "right_side": [{"type": "value", "value": 0}],
            }],
            columns=["CompanyInfo.Company_Code"],
        )


def test_computed_expression_column_supports_metrics_values_and_parentheses(sample_db):
    """Derived output fields use the same token grammar as screening rules."""
    df = run_screening(
        sample_db,
        criteria=[],
        columns=["CompanyInfo.CompanyName"],
        period="2024",
        computed_columns=[{
            "name": "Adjusted EPS",
            "formula_type": "expression",
            "expression_tokens": [
                {"type": "paren", "value": "("},
                {"type": "column", "table": "PerShare", "column": "EPS"},
                {"type": "op", "op": "+"},
                {"type": "value", "value": 10},
                {"type": "paren", "value": ")"},
                {"type": "op", "op": "*"},
                {"type": "value", "value": 2},
            ],
        }],
    )
    values = dict(zip(df["CompanyName"], df["Adjusted EPS"]))
    assert values == {"Alpha Corp": 320, "Beta Co": 180, "Gamma Ltd": 620}
