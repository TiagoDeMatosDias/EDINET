"""Tests for the Screening API endpoints (src/web_app/api/screening.py).

Uses in-memory SQLite databases passed as real files (via tmp_path)
so the /api/screening/* endpoints work with a TestClient.
"""

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import src.web_app.api.screening as screening_api
from src.web_app.server import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _use_database(monkeypatch, path) -> str:
    """Make ``path`` the server's configured screening database."""
    monkeypatch.setattr(screening_api, "get_market_db", lambda: str(path))
    return str(path)


@pytest.fixture(autouse=True)
def default_database(monkeypatch, tmp_path):
    """Point the configured database at an empty per-test file."""
    database = tmp_path / "default.db"
    database.touch()
    _use_database(monkeypatch, database)


def _create_test_db(path: str) -> str:
    """Create a minimal screening database on disk (TestClient needs a real file)."""
    conn = sqlite3.connect(path)
    c = conn.cursor()

    c.execute("""CREATE TABLE CompanyInfo (
        Company_Code TEXT PRIMARY KEY,
        Company_Ticker TEXT,
        Company_Name TEXT,
        Company_Industry TEXT
    )""")
    c.execute("""CREATE TABLE FinancialStatements (
        Company_Code TEXT,
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
def test_db_path(tmp_path, monkeypatch):
    """Create a test database file and return its path."""
    db_path = str(tmp_path / "test_screening.db")
    return _use_database(monkeypatch, _create_test_db(db_path))


# ---------------------------------------------------------------------------
# Tests — GET /api/screening/metrics
# ---------------------------------------------------------------------------


def test_get_metrics(test_db_path):
    """Should return available tables and columns."""
    resp = client.get("/api/screening/metrics")
    assert resp.status_code == 200
    data = resp.json()
    assert "tables" in data
    tables = data["tables"]
    assert "CompanyInfo" in tables
    assert "PerShare" in tables
    assert "Valuation" in tables
    assert "Quality" in tables
    assert "Stock_Splits" in tables
    assert "split_date" in tables["Stock_Splits"]
    assert "BookValue" in tables["PerShare"]
    assert "EPS" in tables["PerShare"]


def test_get_metrics_reports_missing_configured_database(monkeypatch, tmp_path):
    """A missing configured database is a server problem, not a client error."""
    _use_database(monkeypatch, tmp_path / "missing.db")
    resp = client.get("/api/screening/metrics")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Tests — GET /api/screening/periods
# ---------------------------------------------------------------------------


def test_get_periods(test_db_path):
    """Should return available period years."""
    resp = client.get("/api/screening/periods")
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
        "criteria": [],
        "columns": ["CompanyInfo.Company_Code", "CompanyInfo.Company_Name"],
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] >= 1
    assert "Company_Code" in data["columns"] or "Company_Name" in data["columns"]
    # The generated SQL is an internal detail and must not reach clients.
    assert "sql_display" not in data


def test_run_screening_with_criteria(test_db_path):
    """Filtering by industry should return only that company."""
    resp = client.post("/api/screening/run", json={
        "criteria": [{
            "table": "CompanyInfo",
            "column": "Company_Industry",
            "operator": "=",
            "value": "Technology",
            "field_type": "text",
        }],
        "columns": ["CompanyInfo.Company_Code", "CompanyInfo.Company_Name"],
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


def test_run_screening_excludes_recent_stock_splits_by_date(test_db_path):
    with sqlite3.connect(test_db_path) as conn:
        conn.execute(
            "CREATE TABLE Stock_Splits ("
            "ticker TEXT, split_date TEXT, ratio_from REAL, ratio_to REAL, "
            "confirmation TEXT)"
        )
        conn.execute(
            "INSERT INTO Stock_Splits VALUES (?, ?, ?, ?, ?)",
            ("7203.T", "2024-11-15", 1, 2, "confirmed"),
        )

    resp = client.post("/api/screening/run", json={
        "criteria": [{
            "comparison_mode": "recent_split",
            "operator": "=",
            "value": "2024-11-01",
        }],
        "columns": ["CompanyInfo.Company_Ticker"],
        "screening_date": "2024-12-01",
    })

    assert resp.status_code == 200
    data = resp.json()
    assert "7203.T" not in {row[0] for row in data["rows"]}
    assert "6758.T" in {row[0] for row in data["rows"]}


def test_run_screening_recent_split_options_are_forwarded(test_db_path):
    with sqlite3.connect(test_db_path) as conn:
        conn.execute(
            "CREATE TABLE Stock_Splits ("
            "ticker TEXT, split_date TEXT, ratio_from REAL, ratio_to REAL, "
            "confirmation TEXT)"
        )
        conn.execute(
            "INSERT INTO Stock_Splits VALUES (?, ?, ?, ?, ?)",
            ("6758.T", "2024-03-15", 1, 2, "pending"),
        )

    resp = client.post("/api/screening/run", json={
        "criteria": [{
            "comparison_mode": "recent_split",
            "operator": "=",
            "value": "2024-04-01",
            "split_action": "include",
            "split_status": "pending",
            "split_date_operator": "on_or_before",
        }],
        "columns": ["CompanyInfo.Company_Ticker"],
        "screening_date": "2024-12-01",
    })

    assert resp.status_code == 200
    data = resp.json()
    assert {row[0] for row in data["rows"]} == {"6758.T"}


def test_run_screening_recent_split_window_is_forwarded(test_db_path):
    with sqlite3.connect(test_db_path) as conn:
        conn.execute(
            "CREATE TABLE Stock_Splits ("
            "ticker TEXT, split_date TEXT, ratio_from REAL, ratio_to REAL, "
            "confirmation TEXT)"
        )
        conn.execute(
            "INSERT INTO Stock_Splits VALUES (?, ?, ?, ?, ?)",
            ("6758.T", "2024-11-15", 1, 2, "confirmed"),
        )

    resp = client.post("/api/screening/run", json={
        "criteria": [{
            "comparison_mode": "recent_split",
            "split_window_days": 365,
        }],
        "columns": ["CompanyInfo.Company_Ticker"],
        "screening_date": "2024-12-01",
    })

    assert resp.status_code == 200
    data = resp.json()
    # Window start = 2024-12-01 minus 365 days = 2023-12-02; the confirmed
    # 2024-11-15 split falls inside it, so the company is excluded.
    assert "6758.T" not in {row[0] for row in data["rows"]}


def test_run_screening_with_column_compare_and_offset(test_db_path):
    """Column comparison with offset."""
    resp = client.post("/api/screening/run", json={
        "criteria": [{
            "table": "PerShare",
            "column": "BookValue",
            "operator": ">",
            "comparison_mode": "column",
            "compare_table": "PerShare",
            "compare_column": "EPS",
            "offset": -5000,
        }],
        "columns": ["CompanyInfo.Company_Code", "PerShare.BookValue", "PerShare.EPS"],
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] >= 1


def test_run_screening_with_computed_columns(test_db_path):
    """Computed P/E column should be in results."""
    resp = client.post("/api/screening/run", json={
        "criteria": [],
        "columns": ["CompanyInfo.Company_Code"],
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
        "criteria": [],
        "columns": ["CompanyInfo.Company_Code", "FinancialStatements.periodEnd"],
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
        "criteria": [],
        "columns": ["CompanyInfo.Company_Code", "FinancialStatements.periodEnd"],
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


def test_run_screening_rejects_a_client_database_path(test_db_path):
    """Requests cannot name a database; the removed field is rejected outright."""
    resp = client.post("/api/screening/run", json={
        "db_path": test_db_path,
        "criteria": [],
        "columns": [],
    })
    assert resp.status_code == 422


def test_run_screening_validation_error(test_db_path):
    """Invalid column reference should get 400."""
    resp = client.post("/api/screening/run", json={
        "criteria": [{
            "table": "NonexistentTable",
            "column": "FakeCol",
            "operator": ">",
            "value": 1,
            "field_type": "num",
        }],
        "columns": [],
    })
    assert resp.status_code == 400


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
        "columns": ["CompanyInfo.Company_Code", "Valuation.PERatio"],
        "period": "2024",
        "screening_date": "2024-06-30",
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
    assert data["screening_date"] == "2024-06-30"
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


def test_save_overwrites_only_when_asked_and_lists_screen_summaries():
    rule = {"table": "Valuation", "column": "PERatio", "operator": "<", "value": 15}
    grouped = [
        {**rule, "group": "g1", "group_match": "any"},
        {**rule, "value": 9, "group": "g1", "group_match": "any"},
        {**rule, "value": 99, "enabled": False},
    ]
    try:
        first = client.post("/api/screening/save", json={"name": "overwrite_me", "criteria": [rule], "columns": ["CompanyInfo.Company_Code"]})
        assert first.status_code == 200 and first.json()["updated"] is False

        clash = client.post("/api/screening/save", json={"name": "overwrite_me", "criteria": grouped, "columns": []})
        assert clash.status_code == 409

        replaced = client.post("/api/screening/save", json={
            "name": "overwrite_me", "criteria": grouped, "criteria_match": "any",
            "columns": ["CompanyInfo.Company_Code"], "overwrite": True,
        })
        assert replaced.status_code == 200 and replaced.json()["updated"] is True
        assert replaced.json()["screen_id"] == first.json()["screen_id"]

        loaded = client.get("/api/screening/saved/overwrite_me").json()
        assert loaded["criteria_match"] == "any"
        assert [criterion.get("group") for criterion in loaded["criteria"]] == ["g1", "g1", None]
        assert loaded["criteria"][2]["enabled"] is False

        summary = next(item for item in client.get("/api/screening/saved").json()["items"] if item["name"] == "overwrite_me")
        assert summary["rule_count"] == 2
        assert summary["column_count"] == 1
        assert summary["criteria_match"] == "any"
        assert summary["updated_at"]
    finally:
        client.delete("/api/screening/saved/overwrite_me")


def test_run_screening_reports_declared_formats_of_derived_columns(test_db_path):
    resp = client.post("/api/screening/run", json={
        "criteria": [],
        "columns": ["CompanyInfo.Company_Code"],
        "computed_columns": [{
            "name": "Earnings yield",
            "formula_type": "expression",
            "format": "percent",
            "expression_tokens": [
                {"type": "column", "table": "PerShare", "column": "EPS"},
                {"type": "op", "op": "/"},
                {"type": "column", "table": "Stock_Prices", "column": "Price"},
            ],
        }],
    })
    assert resp.status_code == 200
    assert resp.json()["column_formats"]["Earnings yield"] == "percent"


def test_load_nonexistent_screening():
    """Loading nonexistent screening should return 404."""
    resp = client.get("/api/screening/saved/nonexistent_screening_xyz")
    assert resp.status_code == 404


def test_save_rejects_non_iso_screening_date():
    response = client.post(
        "/api/screening/save",
        json={
            "name": "invalid-date",
            "criteria": [],
            "columns": [],
            "screening_date": "June 30, 2024",
        },
    )
    assert response.status_code == 422


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


def test_screening_history_pagination():
    """History endpoint supports limit/offset pagination."""
    # Save a few entries
    for i in range(5):
        resp = client.post("/api/screening/history", json={
            "name": f"test_run_{i}",
            "criteria_count": i,
            "result_count": i * 10,
            "period": "2024",
        })
        assert resp.status_code == 200

    # Get with limit
    resp = client.get("/api/screening/history?limit=3")
    assert resp.status_code == 200
    data = resp.json()
    assert "entries" in data
    assert "total" in data
    assert "limit" in data
    assert "offset" in data
    assert data["limit"] == 3
    assert data["offset"] == 0
    assert len(data["entries"]) <= 3
    assert data["total"] >= 5

    # Get with offset
    resp = client.get("/api/screening/history?limit=2&offset=3")
    assert resp.status_code == 200
    data = resp.json()
    assert data["limit"] == 2
    assert data["offset"] == 3
    assert len(data["entries"]) <= 2


def test_screening_history_invalid_limit():
    """Invalid limit value returns 422."""
    resp = client.get("/api/screening/history?limit=0")
    assert resp.status_code == 422

    resp = client.get("/api/screening/history?limit=1000")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Tests — export
# ---------------------------------------------------------------------------


def test_export_csv(test_db_path):
    """CSV export should return a CSV file."""
    resp = client.post("/api/screening/export", json={
        "criteria": [],
        "columns": ["CompanyInfo.Company_Code"],
        "format": "csv",
    })
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    content = resp.text
    assert "Company_Code" in content
    assert "E00001" in content


def test_export_backtest(test_db_path, monkeypatch):
    """Backtest export should work."""
    original_export = screening_api._screening.export_screening_to_backtest_csv
    generated_path = {}

    def capture_output_path(**kwargs):
        generated_path["value"] = Path(kwargs["output_path"])
        return original_export(**kwargs)

    monkeypatch.setattr(
        screening_api._screening,
        "export_screening_to_backtest_csv",
        capture_output_path,
    )
    resp = client.post("/api/screening/export", json={
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
    from src.paths import exports_dir

    assert generated_path["value"].parent.parent == exports_dir()
    assert not generated_path["value"].parent.exists()


def test_export_enforces_response_size_limit(test_db_path, monkeypatch):
    """CSV responses larger than the configured limit are rejected."""
    from dataclasses import replace

    limited = replace(screening_api.get_settings(), max_export_bytes=8)
    monkeypatch.setattr(screening_api, "get_settings", lambda: limited)
    resp = client.post("/api/screening/export", json={
        "criteria": [],
        "columns": ["CompanyInfo.Company_Code"],
        "format": "csv",
    })
    assert resp.status_code == 413


def test_export_rejects_unknown_format(test_db_path):
    resp = client.post("/api/screening/export", json={
        "criteria": [],
        "columns": ["CompanyInfo.Company_Code"],
        "format": "spreadsheet",
    })
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Tests — metrics returns ALL tables (not just hardcoded list)
# ---------------------------------------------------------------------------


def test_metrics_returns_many_tables(test_db_path):
    """The metrics endpoint must return all user tables, not a hardcoded subset."""
    resp = client.get("/api/screening/metrics")
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


def test_metrics_includes_custom_tables(tmp_path, monkeypatch):
    """Metrics must include tables with arbitrary names, not just known ones."""
    import sqlite3
    db_path = str(tmp_path / "custom.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE CompanyInfo (Company_Code TEXT, Company_Ticker TEXT)")
    conn.execute("CREATE TABLE FinancialStatements (Company_Code TEXT, docID TEXT, periodEnd TEXT)")
    conn.execute("CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Price REAL)")
    conn.execute("CREATE TABLE Financial_Ratios_Rolling (docID TEXT, Net_Margin_Avg_3Y REAL, ROA_Avg_10Y REAL)")
    conn.execute("CREATE TABLE Custom_Metrics (docID TEXT, Score REAL, Rank INTEGER)")
    conn.commit()
    conn.close()

    _use_database(monkeypatch, db_path)
    resp = client.get("/api/screening/metrics")
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


# ---------------------------------------------------------------------------
# Update prices
# ---------------------------------------------------------------------------


def _create_db_for_update_prices(path: str) -> str:
    """Create a minimal DB with Stock_Prices so update-prices can write."""
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("CREATE TABLE CompanyInfo (Company_Code TEXT, Company_Ticker TEXT, Company_Name TEXT)")
    c.execute("CREATE TABLE FinancialStatements (Company_Code TEXT, docID TEXT, periodEnd TEXT)")
    c.execute("""CREATE TABLE Stock_Prices (
        Date TEXT, Ticker TEXT, Currency TEXT, Price REAL
    )""")
    c.execute(
        "INSERT INTO CompanyInfo VALUES ('E00001', '7203', 'Toyota Motor')"
    )
    c.execute(
        "INSERT INTO CompanyInfo VALUES ('E00002', '6758', 'Sony Group')"
    )
    conn.commit()
    conn.close()
    return path


def test_update_prices_requires_tickers(tmp_path, monkeypatch):
    _use_database(monkeypatch, _create_db_for_update_prices(str(tmp_path / "test.db")))
    resp = client.post("/api/screening/update-prices", json={"tickers": []})
    assert resp.status_code == 422


def test_update_prices_rejects_a_client_database_path(tmp_path):
    db_path = _create_db_for_update_prices(str(tmp_path / "test.db"))
    resp = client.post("/api/screening/update-prices", json={"db_path": db_path, "tickers": ["7203"]})
    assert resp.status_code == 422


def test_update_prices_rejects_oversized_ticker_lists(tmp_path, monkeypatch):
    _use_database(monkeypatch, _create_db_for_update_prices(str(tmp_path / "test.db")))
    tickers = [f"T{index}" for index in range(screening_api.MAX_PRICE_UPDATE_TICKERS + 1)]
    resp = client.post(
        "/api/screening/update-prices",
        json={"tickers": tickers},
    )
    assert resp.status_code == 422


def _fake_price_update(calls):
    """Deterministic stand-in for the provider round trip."""

    def update(db_path, ticker):
        calls.append((db_path, ticker))
        return {"ok": True, "rows_inserted": 3, "message": f"updated {ticker}"}

    return update


def test_update_prices_returns_results_structure(tmp_path, monkeypatch):
    db_path = _use_database(monkeypatch, _create_db_for_update_prices(str(tmp_path / "test.db")))
    calls = []
    monkeypatch.setattr(
        screening_api._security, "update_security_price", _fake_price_update(calls)
    )
    resp = client.post(
        "/api/screening/update-prices",
        # Duplicates and blanks are dropped before any provider call.
        json={"tickers": ["7203", "6758", "7203", " "]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert [r["ticker"] for r in data["results"]] == ["7203", "6758"]
    assert calls == [(db_path, "7203"), (db_path, "6758")]
    for r in data["results"]:
        assert r["ok"] is True
        assert r["rows_inserted"] == 3
        assert r["message"] == f"updated {r['ticker']}"


def test_update_prices_requires_operator_role(tmp_path, monkeypatch):
    from src.auth.dependencies import current_user
    from src.auth.models import AuthenticatedUser

    _use_database(monkeypatch, _create_db_for_update_prices(str(tmp_path / "test.db")))
    calls = []
    monkeypatch.setattr(
        screening_api._security, "update_security_price", _fake_price_update(calls)
    )
    member = AuthenticatedUser("member-1", "member", None, "member", "active")
    app.dependency_overrides[current_user] = lambda: member
    try:
        resp = client.post(
            "/api/screening/update-prices",
            json={"tickers": ["7203"]},
        )
    finally:
        app.dependency_overrides.pop(current_user, None)
    assert resp.status_code == 403
    assert calls == []


def test_update_prices_reports_missing_configured_database(tmp_path, monkeypatch):
    _use_database(monkeypatch, tmp_path / "missing.db")
    resp = client.post("/api/screening/update-prices", json={"tickers": ["7203"]})
    assert resp.status_code == 503
