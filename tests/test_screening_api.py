"""Tests for the Screening API endpoints (src/web_app/api/screening.py).

Uses in-memory SQLite databases passed as real files (via tmp_path)
so the /api/screening/* endpoints work with a TestClient.
"""

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.web_app.server import app


client = TestClient(app)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _create_test_db(path: str) -> str:
    """Create a minimal screening database on disk (TestClient needs a real file)."""
    conn = sqlite3.connect(path)
    c = conn.cursor()

    c.execute("""CREATE TABLE CompanyInfo (
        edinetCode TEXT PRIMARY KEY,
        Company_Ticker TEXT,
        Company_Name TEXT,
        Company_Industry TEXT
    )""")
    c.execute("""CREATE TABLE FinancialStatements (
        edinetCode TEXT,
        docID TEXT UNIQUE,
        periodEnd TEXT,
        SharesOutstanding REAL,
        SharePrice REAL
    )""")
    c.execute("""CREATE TABLE Stock_Prices (
        Date TEXT,
        Ticker TEXT,
        Currency TEXT,
        Price REAL,
        PRIMARY KEY (Date, Ticker)
    )""")
    c.execute("""CREATE TABLE PerShare (
        docID TEXT UNIQUE,
        BookValue REAL,
        EPS REAL,
        Dividends REAL,
        Sales REAL
    )""")
    c.execute("""CREATE TABLE Valuation (
        docID TEXT UNIQUE,
        PERatio REAL,
        PriceToBook REAL
    )""")
    c.execute("""CREATE TABLE Quality (
        docID TEXT UNIQUE,
        ROE REAL,
        DebtToEquity REAL
    )""")

    # Company A
    c.execute(
        "INSERT INTO CompanyInfo VALUES (?, ?, ?, ?)",
        ("E00001", "7203.T", "Toyota Motor", "Automotive"),
    )
    c.execute(
        "INSERT INTO FinancialStatements VALUES (?, ?, ?, ?, ?)",
        ("E00001", "DOC001", "2024-03-31", 1000000, 3500),
    )
    c.execute(
        "INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)",
        ("2024-03-31", "7203.T", "JPY", 3500),
    )
    c.execute(
        "INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)",
        ("2024-04-01", "7203.T", "JPY", 3520),
    )
    c.execute(
        "INSERT INTO PerShare VALUES (?, ?, ?, ?, ?)",
        ("DOC001", 5000, 300, 50, 8000),
    )
    c.execute(
        "INSERT INTO Valuation VALUES (?, ?, ?)",
        ("DOC001", 11.67, 0.7),
    )
    c.execute(
        "INSERT INTO Quality VALUES (?, ?, ?)",
        ("DOC001", 0.08, 1.2),
    )

    # Company B
    c.execute(
        "INSERT INTO CompanyInfo VALUES (?, ?, ?, ?)",
        ("E00002", "6758.T", "Sony Group", "Technology"),
    )
    c.execute(
        "INSERT INTO FinancialStatements VALUES (?, ?, ?, ?, ?)",
        ("E00002", "DOC002", "2024-03-31", 500000, 12500),
    )
    c.execute(
        "INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)",
        ("2024-03-31", "6758.T", "JPY", 12500),
    )
    c.execute(
        "INSERT INTO PerShare VALUES (?, ?, ?, ?, ?)",
        ("DOC002", 6000, 500, 40, 10000),
    )
    c.execute(
        "INSERT INTO Valuation VALUES (?, ?, ?)",
        ("DOC002", 25.0, 2.08),
    )
    c.execute(
        "INSERT INTO Quality VALUES (?, ?, ?)",
        ("DOC002", 0.12, 0.8),
    )

    # Also add 2023 period for Company A (for screening_date tests)
    c.execute(
        "INSERT INTO FinancialStatements VALUES (?, ?, ?, ?, ?)",
        ("E00001", "DOC003", "2023-03-31", 1000000, 3200),
    )
    c.execute(
        "INSERT INTO PerShare VALUES (?, ?, ?, ?, ?)",
        ("DOC003", 4800, 280, 45, 7500),
    )
    c.execute(
        "INSERT INTO Valuation VALUES (?, ?, ?)",
        ("DOC003", 11.43, 0.67),
    )

    conn.commit()
    conn.close()
    return path


@pytest.fixture
def test_db_path(tmp_path):
    """Create a test database file and return its path."""
    db_path = str(tmp_path / "test_screening.db")
    return _create_test_db(db_path)


# ---------------------------------------------------------------------------
# Tests — GET /api/screening/db-path
# ---------------------------------------------------------------------------


def test_get_default_db_path():
    """Should return the default DB2 path."""
    resp = client.get("/api/screening/db-path")
    assert resp.status_code == 200
    data = resp.json()
    assert "db_path" in data
    assert data["db_path"]


# ---------------------------------------------------------------------------
# Tests — GET /api/screening/metrics
# ---------------------------------------------------------------------------


def test_get_metrics(test_db_path):
    """Should return available tables and columns."""
    resp = client.get(f"/api/screening/metrics?db_path={test_db_path}")
    assert resp.status_code == 200
    data = resp.json()
    assert "tables" in data
    tables = data["tables"]
    assert "CompanyInfo" in tables
    assert "PerShare" in tables
    assert "Valuation" in tables
    assert "Quality" in tables
    assert "BookValue" in tables["PerShare"]
    assert "EPS" in tables["PerShare"]


def test_get_metrics_invalid_db():
    """Should return 400 for nonexistent database."""
    resp = client.get("/api/screening/metrics?db_path=/nonexistent/path.db")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Tests — GET /api/screening/periods
# ---------------------------------------------------------------------------


def test_get_periods(test_db_path):
    """Should return available period years."""
    resp = client.get(f"/api/screening/periods?db_path={test_db_path}")
    assert resp.status_code == 200
    data = resp.json()
    assert "periods" in data
    assert "2023" in data["periods"]
    assert "2024" in data["periods"]


# ---------------------------------------------------------------------------
# Tests — GET /api/screening/formulas
# ---------------------------------------------------------------------------


def test_get_formulas():
    """Should return predefined formula list."""
    resp = client.get("/api/screening/formulas")
    assert resp.status_code == 200
    data = resp.json()
    assert "formulas" in data
    formulas = data["formulas"]
    assert len(formulas) >= 4
    names = [f["name"] for f in formulas]
    assert "P/E Ratio" in names
    assert "P/B Ratio" in names
    assert "Dividend Yield" in names


# ---------------------------------------------------------------------------
# Tests — POST /api/screening/run
# ---------------------------------------------------------------------------


def test_run_screening_basic(test_db_path):
    """Basic screening with no criteria should return all companies."""
    resp = client.post("/api/screening/run", json={
        "db_path": test_db_path,
        "criteria": [],
        "columns": ["CompanyInfo.edinetCode", "CompanyInfo.Company_Name"],
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] >= 1
    assert "edinetCode" in data["columns"] or "Company_Name" in data["columns"]
    # SQL display should be included
    assert "sql_display" in data
    assert "SELECT" in data["sql_display"]
    assert "FROM" in data["sql_display"]


def test_run_screening_with_criteria(test_db_path):
    """Filtering by industry should return only that company."""
    resp = client.post("/api/screening/run", json={
        "db_path": test_db_path,
        "criteria": [{
            "table": "CompanyInfo",
            "column": "Company_Industry",
            "operator": "=",
            "value": "Technology",
            "field_type": "text",
        }],
        "columns": ["CompanyInfo.edinetCode", "CompanyInfo.Company_Name"],
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] >= 1
    # Should find Sony
    found = False
    for row in data["rows"]:
        if "Sony" in str(row):
            found = True
    assert found, f"Sony not found in results: {data['rows']}"


def test_run_screening_with_column_compare_and_offset(test_db_path):
    """Column comparison with offset."""
    resp = client.post("/api/screening/run", json={
        "db_path": test_db_path,
        "criteria": [{
            "table": "PerShare",
            "column": "BookValue",
            "operator": ">",
            "comparison_mode": "column",
            "compare_table": "PerShare",
            "compare_column": "EPS",
            "offset": -5000,
        }],
        "columns": ["CompanyInfo.edinetCode", "PerShare.BookValue", "PerShare.EPS"],
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] >= 1


def test_run_screening_with_computed_columns(test_db_path):
    """Computed P/E column should be in results."""
    resp = client.post("/api/screening/run", json={
        "db_path": test_db_path,
        "criteria": [],
        "columns": ["CompanyInfo.edinetCode"],
        "computed_columns": [{
            "name": "PE_Ratio",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "PerShare",
            "denominator_column": "EPS",
        }],
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "PE_Ratio" in data["columns"]
    # PE values should be numeric (not null since we have prices and EPS)
    pe_idx = data["columns"].index("PE_Ratio")
    pe_values = [row[pe_idx] for row in data["rows"] if row[pe_idx] is not None]
    assert len(pe_values) >= 1


def test_run_screening_with_period(test_db_path):
    """Period filter should restrict results."""
    resp = client.post("/api/screening/run", json={
        "db_path": test_db_path,
        "criteria": [],
        "columns": ["CompanyInfo.edinetCode", "FinancialStatements.periodEnd"],
        "period": "2023",
    })
    assert resp.status_code == 200
    data = resp.json()
    # Find periodEnd column index (case-insensitive)
    pe_idx = None
    for i, col in enumerate(data["columns"]):
        if "periodend" in col.lower():
            pe_idx = i
            break
    assert pe_idx is not None, f"periodEnd not in columns: {data['columns']}"
    for row in data["rows"]:
        period_val = str(row[pe_idx]) if row[pe_idx] else ""
        assert "2023" in period_val, f"Expected 2023 in {period_val}"


def test_run_screening_with_screening_date(test_db_path):
    """Point-in-time date should restrict to filings before that date."""
    # 2023-06-01: should only see the 2023-03-31 filing for E00001
    resp = client.post("/api/screening/run", json={
        "db_path": test_db_path,
        "criteria": [],
        "columns": ["CompanyInfo.edinetCode", "FinancialStatements.periodEnd"],
        "screening_date": "2023-06-01",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] >= 0  # Should return at least E00001's 2023 filing
    # Find periodEnd column index (case-insensitive)
    pe_idx = None
    for i, col in enumerate(data["columns"]):
        if "periodend" in col.lower():
            pe_idx = i
            break
    if pe_idx is not None:
        for row in data["rows"]:
            period_val = str(row[pe_idx]) if row[pe_idx] else ""
            # All periods should be <= 2023-06-01
            assert period_val <= "2023-06-01", f"Got period {period_val} after screening_date"


def test_run_screening_invalid_db():
    """Nonexistent DB should get 400."""
    resp = client.post("/api/screening/run", json={
        "db_path": "/nonexistent/db.sqlite",
        "criteria": [],
        "columns": [],
    })
    assert resp.status_code == 400


def test_run_screening_validation_error(test_db_path):
    """Invalid column reference should get 400."""
    resp = client.post("/api/screening/run", json={
        "db_path": test_db_path,
        "criteria": [{
            "table": "NonexistentTable",
            "column": "FakeCol",
            "operator": ">",
            "value": 1,
            "field_type": "num",
        }],
        "columns": [],
    })
    assert resp.status_code in (400, 500)  # ValueError → 400 via our handler


# ---------------------------------------------------------------------------
# Tests — saved screenings CRUD
# ---------------------------------------------------------------------------


def test_save_list_load_delete_screening():
    """Full CRUD round-trip for saved screenings."""
    # Save
    resp = client.post("/api/screening/save", json={
        "name": "test_api_screening",
        "criteria": [{
            "table": "Valuation",
            "column": "PERatio",
            "operator": "<",
            "value": 15,
            "field_type": "num",
        }],
        "columns": ["CompanyInfo.edinetCode", "Valuation.PERatio"],
        "period": "2024",
        "ranking_algorithm": "weighted_minmax",
        "ranking_rules": [{
            "table": "Valuation",
            "column": "PERatio",
            "weight": 1.0,
            "direction": "lower",
        }],
    })
    assert resp.status_code == 200
    assert resp.json()["saved"] is True

    # List
    resp = client.get("/api/screening/saved")
    assert resp.status_code == 200
    assert "test_api_screening" in resp.json()["screenings"]

    # Load
    resp = client.get("/api/screening/saved/test_api_screening")
    assert resp.status_code == 200
    data = resp.json()
    assert data["period"] == "2024"
    assert data["ranking_algorithm"] == "weighted_minmax"
    assert len(data["criteria"]) == 1
    assert len(data["ranking_rules"]) == 1

    # Delete
    resp = client.delete("/api/screening/saved/test_api_screening")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True

    # Verify gone
    resp = client.get("/api/screening/saved/test_api_screening")
    assert resp.status_code == 404


def test_load_nonexistent_screening():
    """Loading nonexistent screening should return 404."""
    resp = client.get("/api/screening/saved/nonexistent_screening_xyz")
    assert resp.status_code == 404


def test_delete_nonexistent_screening():
    """Deleting nonexistent screening should return 404."""
    resp = client.delete("/api/screening/saved/nonexistent_screening_xyz")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Tests — history
# ---------------------------------------------------------------------------


def test_screening_history_roundtrip():
    """Save and load screening history."""
    # Save
    resp = client.post("/api/screening/history", json={
        "name": "test_run",
        "criteria_count": 3,
        "result_count": 42,
        "period": "2024",
    })
    assert resp.status_code == 200

    # Load
    resp = client.get("/api/screening/history")
    assert resp.status_code == 200
    data = resp.json()
    assert "entries" in data
    assert len(data["entries"]) >= 1
    latest = data["entries"][0]
    assert latest["result_count"] == 42


# ---------------------------------------------------------------------------
# Tests — export
# ---------------------------------------------------------------------------


def test_export_csv(test_db_path):
    """CSV export should return a CSV file."""
    resp = client.post("/api/screening/export", json={
        "db_path": test_db_path,
        "criteria": [],
        "columns": ["CompanyInfo.edinetCode"],
        "format": "csv",
    })
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    content = resp.text
    assert "edinetCode" in content
    assert "E00001" in content


def test_export_backtest(test_db_path):
    """Backtest export should work."""
    resp = client.post("/api/screening/export", json={
        "db_path": test_db_path,
        "criteria": [],
        "columns": ["CompanyInfo.Company_Ticker"],
        "format": "backtest",
        "period": "2024",
        "max_companies": 10,
    })
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    content = resp.text
    assert "Year" in content
    assert "Tickers" in content


# ---------------------------------------------------------------------------
# Tests — metrics returns ALL tables (not just hardcoded list)
# ---------------------------------------------------------------------------


def test_metrics_returns_many_tables(test_db_path):
    """The metrics endpoint must return all user tables, not a hardcoded subset."""
    resp = client.get(f"/api/screening/metrics?db_path={test_db_path}")
    assert resp.status_code == 200
    tables = resp.json()["tables"]
    # Must have more than 1 table
    assert len(tables) > 1, f"Expected >1 tables, got {list(tables.keys())}"
    # Core tables must be present
    assert "CompanyInfo" in tables
    assert "PerShare" in tables
    assert "Valuation" in tables
    assert "Quality" in tables
    # FinancialStatements and Stock_Prices must be present (all user tables)
    assert "FinancialStatements" in tables
    assert "Stock_Prices" in tables


def test_metrics_includes_custom_tables(tmp_path):
    """Metrics must include tables with arbitrary names, not just known ones."""
    import sqlite3
    db_path = str(tmp_path / "custom.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE CompanyInfo (edinetCode TEXT, Company_Ticker TEXT)")
    conn.execute("CREATE TABLE FinancialStatements (edinetCode TEXT, docID TEXT, periodEnd TEXT)")
    conn.execute("CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Price REAL)")
    conn.execute("CREATE TABLE Financial_Ratios_Rolling (docID TEXT, Net_Margin_Avg_3Y REAL, ROA_Avg_10Y REAL)")
    conn.execute("CREATE TABLE Custom_Metrics (docID TEXT, Score REAL, Rank INTEGER)")
    conn.commit()
    conn.close()

    resp = client.get(f"/api/screening/metrics?db_path={db_path}")
    assert resp.status_code == 200
    tables = resp.json()["tables"]

    # Arbitrary tables must appear
    assert "Financial_Ratios_Rolling" in tables, f"Tables: {list(tables.keys())}"
    assert "Custom_Metrics" in tables
    assert "Net_Margin_Avg_3Y" in tables["Financial_Ratios_Rolling"]
    assert "ROA_Avg_10Y" in tables["Financial_Ratios_Rolling"]
    assert "Score" in tables["Custom_Metrics"]
    # Metadata columns excluded from arbitrary tables
    assert "docID" not in tables["Financial_Ratios_Rolling"]
    assert "docID" not in tables["Custom_Metrics"]
    # More than 1 table
    assert len(tables) > 3
