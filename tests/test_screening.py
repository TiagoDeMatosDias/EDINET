"""Tests for the screening module (src/screening.py).

Each test uses an in-memory SQLite database with pre-populated sample data
so no external files are required.
"""

import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.screening import (
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_sample_db(path: str) -> str:
    """Create a minimal SQLite database with sample data for testing."""
    conn = sqlite3.connect(path)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE CompanyInfo (
            edinetCode TEXT PRIMARY KEY,
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
            MarketCap REAL
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
            PERatio_5Year_Average REAL
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
        c.execute("INSERT INTO FinancialStatements VALUES (?,?,?,?,?,?,?)", row)

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
        ("DOC001", 10.0, 0.10, 1500000000),
        ("DOC002", 10.0, 0.10, 1600000000),
        ("DOC003", 10.67, 0.094, 1600000000),
        ("DOC004", 10.0, 0.10, 1200000000),
    ]
    for row in valuation:
        c.execute("INSERT INTO Valuation VALUES (?,?,?,?)", row)

    quality = [
        ("DOC001", 0.15, 0.40, 0.8),
        ("DOC002", 0.10, 0.35, 1.2),
        ("DOC003", 0.20, 0.50, 0.5),
        ("DOC004", 0.12, 0.38, 0.9),
    ]
    for row in quality:
        c.execute("INSERT INTO Quality VALUES (?,?,?,?)", row)

    valuation_historical = [
        ("DOC001", 11.0),
        ("DOC002", 9.5),
        ("DOC003", 12.0),
        ("DOC004", 9.0),
    ]
    for row in valuation_historical:
        c.execute("INSERT INTO Valuation_Historical VALUES (?, ?)", row)

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
        "INSERT INTO FinancialStatements VALUES (?, ?, ?, ?, ?, ?, ?)",
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
    """Known tables should return their column lists."""
    metrics = get_available_metrics(sample_db)
    assert "CompanyInfo" in metrics
    assert "PerShare" in metrics
    assert "Valuation" in metrics
    assert "Quality" in metrics
    assert "CompanyName" in metrics["CompanyInfo"]
    assert "Industry" in metrics["CompanyInfo"]
    assert "BookValue" in metrics["PerShare"]
    assert "EPS" in metrics["PerShare"]
    assert "PERatio" in metrics["Valuation"]
    assert "ReturnOnEquity" in metrics["Quality"]
    # docID should be excluded
    for table_cols in metrics.values():
        assert "docID" not in table_cols


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
    columns = ["CompanyInfo.edinetCode", "Valuation.PERatio"]
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
    columns = ["CompanyInfo.edinetCode"]
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
    columns = ["CompanyInfo.edinetCode"]
    sql, params = build_screening_query(criteria, columns)

    assert "BETWEEN ? AND ?" in sql
    assert params == [5, 15]


def test_build_screening_query_with_period():
    """Period filter should be applied."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 5}]
    columns = ["CompanyInfo.edinetCode"]
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
    columns = ["CompanyInfo.edinetCode"]

    with pytest.raises(ValueError, match="Column"):
        build_screening_query(criteria, columns, available_metrics=available)


def test_build_screening_query_validates_operator():
    """Invalid operators should raise ValueError."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": "DROP", "value": 5}]
    columns = ["CompanyInfo.edinetCode"]

    with pytest.raises(ValueError, match="Invalid operator"):
        build_screening_query(criteria, columns)


def test_build_screening_query_validates_identifier():
    """SQL injection via column names should be blocked."""
    criteria = [{"table": "Valuation", "column": "PERatio; DROP TABLE", "operator": ">", "value": 5}]
    columns = ["CompanyInfo.edinetCode"]

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        build_screening_query(criteria, columns)


# ---------------------------------------------------------------------------
# Tests — run_screening
# ---------------------------------------------------------------------------

def test_run_screening_returns_dataframe(sample_db):
    """End-to-end test: run_screening should return a DataFrame."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 5}]
    columns = ["CompanyInfo.edinetCode", "Valuation.PERatio"]

    df = run_screening(sample_db, criteria, columns, period="2024")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "PERatio" in df.columns


def test_run_screening_empty_result(sample_db):
    """Query with impossible criteria should return empty DataFrame."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 999999}]
    columns = ["CompanyInfo.edinetCode"]

    df = run_screening(sample_db, criteria, columns)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_run_screening_sort(sample_db):
    """sort_by and sort_order parameters should work correctly."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}]
    columns = ["CompanyInfo.edinetCode", "Valuation.PERatio"]

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


def test_run_screening_with_weighted_ranking(sample_db):
    """Weighted ranking should add score columns and sort by score."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}]
    columns = [
        "CompanyInfo.CompanyName",
        "Valuation.PERatio",
        "Quality.ReturnOnEquity",
    ]
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

    assert "ScreeningScore" in df.columns
    assert "ScreeningRank" in df.columns
    assert df.iloc[0]["CompanyName"] == "Gamma Ltd"


# ---------------------------------------------------------------------------
# Tests — export
# ---------------------------------------------------------------------------

def test_export_screening_to_csv(tmp_path, sample_db):
    """CSV export should create a file with correct contents."""
    criteria = [{"table": "Valuation", "column": "PERatio", "operator": ">", "value": 0}]
    columns = ["CompanyInfo.edinetCode", "Valuation.PERatio"]
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


def test_format_financial_value_currency():
    assert format_financial_value(1_500_000_000, "MarketCap") == "1500000000"
    assert format_financial_value(5_000_000, "EnterpriseValue") == "5000000"
    assert format_financial_value(999, "SharePrice") == "999"


def test_format_financial_value_ratio():
    result = format_financial_value(10.567, "PERatio")
    assert result == "10.567"


def test_format_financial_value_none():
    assert format_financial_value(None, "PERatio") == "—"
    assert format_financial_value(float("nan"), "PERatio") == "—"


def test_get_default_columns_includes_company_name_and_industry(sample_db):
    """Resolved defaults should include CompanyInfo name and industry columns."""
    metrics = get_available_metrics(sample_db)

    columns = get_default_columns(metrics)

    assert "CompanyInfo.edinetCode" in columns
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
    columns = ["CompanyInfo.edinetCode", "Valuation.PERatio"]
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
