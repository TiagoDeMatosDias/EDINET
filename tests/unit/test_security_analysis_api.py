"""Tests for Security Analysis API endpoints.

Uses in-memory SQLite databases injected via monkeypatched get_db2().
The frontend never sends database paths — they are resolved server-side.
"""

from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from src.web_app.server import app

client = TestClient(app)


def _create_db(path: str) -> str:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE CompanyInfo(Company_Code TEXT PRIMARY KEY, Company_Name TEXT, [Submitter Name] TEXT, Company_Industry TEXT, Company_Ticker TEXT, Listed TEXT)")
    cur.execute("CREATE TABLE FinancialStatements(Company_Code TEXT, docID TEXT UNIQUE, periodEnd TEXT, SharesOutstanding REAL, SharePrice REAL)")
    cur.execute("CREATE TABLE Stock_Prices(Date TEXT, Ticker TEXT, Currency TEXT, Price REAL, PRIMARY KEY(Date, Ticker))")
    cur.execute("CREATE TABLE IncomeStatement(docID TEXT UNIQUE, [Net sales] REAL, [Operating income] REAL, [Net income (loss)] REAL)")
    cur.execute("CREATE TABLE BalanceSheet(docID TEXT UNIQUE, [Net assets] REAL)")
    cur.execute("CREATE TABLE ShareMetrics(docID TEXT UNIQUE, [Basic earnings (loss) per share] REAL, [Net assets per share] REAL, [Dividend paid per share] REAL, [Number of issued shares as of filing date] REAL)")
    cur.execute("CREATE TABLE PerShare_Metrics(docID TEXT UNIQUE, [Sales Per Share] REAL)")
    cur.execute("CREATE TABLE Financial_Ratios(docID TEXT UNIQUE, [Current Ratio] REAL)")
    cur.execute("CREATE TABLE Financial_Ratios_Rolling(docID TEXT UNIQUE, [Return on Assets_Average_3_Year] REAL, [Return on Equity_Average_3_Year] REAL)")

    cur.execute("INSERT INTO CompanyInfo VALUES(?,?,?,?,?,?)", ("E00001", "Alpha Corp", "Alpha Submitter", "Tech", "1001.T", "TSE"))
    cur.execute("INSERT INTO FinancialStatements VALUES(?,?,?,?,?)", ("E00001", "DOC1", "2024-03-31", 5000000, 1500))
    cur.execute("INSERT INTO Stock_Prices VALUES(?,?,?,?)", ("2024-03-31", "1001.T", "JPY", 1500))
    cur.execute("INSERT INTO Stock_Prices VALUES(?,?,?,?)", ("2024-04-01", "1001.T", "JPY", 1520))
    cur.execute("INSERT INTO IncomeStatement VALUES(?,?,?,?)", ("DOC1", 10e9, 1e9, 0.8e9))
    cur.execute("INSERT INTO BalanceSheet VALUES(?,?)", ("DOC1", 8e9))
    cur.execute("INSERT INTO ShareMetrics VALUES(?,?,?,?,?)", ("DOC1", 100.0, 650.0, 25.0, 5000000))
    cur.execute("INSERT INTO PerShare_Metrics VALUES(?,?)", ("DOC1", 2000.0))
    cur.execute("INSERT INTO Financial_Ratios VALUES(?,?)", ("DOC1", 1.85))
    cur.execute("INSERT INTO Financial_Ratios_Rolling VALUES(?,?,?)", ("DOC1", 0.04, 0.125))
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = str(tmp_path / "test.db")
    _create_db(p)
    import src.web_app.api.security_analysis as m
    monkeypatch.setattr(m, "get_db2", lambda: p)
    return p


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def test_search_matches_name(db):
    data = client.get("/api/security/search", params={"q": "Alpha"}).json()
    assert len(data["results"]) >= 1
    assert data["results"][0]["company_name"] == "Alpha Corp"

def test_search_matches_ticker(db):
    data = client.get("/api/security/search", params={"q": "1001"}).json()
    assert data["results"][0]["ticker"] == "1001.T"

def test_search_empty(db):
    assert client.get("/api/security/search", params={"q": ""}).json()["results"] == []

def test_search_nonexistent(db):
    assert client.get("/api/security/search", params={"q": "xyznope"}).json()["results"] == []

def test_search_limit(db):
    assert len(client.get("/api/security/search", params={"q": "a", "limit": 1}).json()["results"]) <= 1

# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

def test_overview_basic(db):
    data = client.get("/api/security/overview", params={"company_code": "E00001"}).json()
    assert data["company"]["company_code"] == "E00001"
    assert data["market"]["latest_price"] == 1520.0
    assert "metrics" in data

def test_overview_metrics_computed(db):
    data = client.get("/api/security/overview", params={"company_code": "E00001"}).json()
    m = data["metrics"]
    # Price * shares = 1520 * 5M = 7.6B
    assert m["MarketCap"] == pytest.approx(7600000000, rel=1e-4)
    # Price / EPS = 1520 / 100 = 15.2
    assert m["PERatio"] == pytest.approx(15.2, rel=1e-4)
    # Price / BVPS = 1520 / 650 = 2.338
    assert m["PriceToBook"] == pytest.approx(1520 / 650, rel=1e-4)
    # Price / SPS = 1520 / 2000 = 0.76
    assert m["PriceToSales"] == pytest.approx(0.76, rel=1e-4)
    # DPS / Price = 25 / 1520 = 0.0164
    assert m["DividendsYield"] == pytest.approx(25 / 1520, rel=1e-4)
    # DPS / EPS = 25 / 100 = 0.25
    assert m["PayoutRatio"] == pytest.approx(0.25, rel=1e-4)
    # ROA from rolling
    assert m["ReturnOnAssets"] == pytest.approx(0.04, rel=1e-4)
    # ROE from rolling
    assert m["ReturnOnEquity"] == pytest.approx(0.125, rel=1e-4)
    # Current Ratio from Financial_Ratios
    assert m["CurrentRatio"] == pytest.approx(1.85, rel=1e-4)
    # Latest Price
    assert m["LatestPrice"] == 1520.0


def test_overview_metrics_project_report_values_onto_current_share_basis(db):
    """A reviewed post-filing split adjusts per-share metrics, not source rows."""
    from src.portfolio.split_schema import ensure_split_tables

    conn = sqlite3.connect(db)
    ensure_split_tables(conn=conn)
    conn.execute(
        "INSERT INTO Stock_Splits "
        "(ticker, split_date, ratio_from, ratio_to, confirmation, price_basis) "
        "VALUES (?, ?, ?, ?, 'confirmed', 'raw')",
        ("1001.T", "2024-06-01", 1, 2),
    )
    conn.execute(
        "INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)",
        ("2024-07-01", "1001.T", "JPY", 300),
    )
    conn.commit()
    conn.close()

    data = client.get("/api/security/overview", params={"company_code": "E00001"}).json()
    metrics = data["metrics"]
    # Report EPS/DPS are halved and issued shares doubled for the 2-for-1 split.
    assert metrics["LatestPrice"] == 300.0
    assert metrics["MarketCap"] == pytest.approx(3_000_000_000.0)
    assert metrics["SharesOutstanding"] == pytest.approx(10_000_000.0)
    assert metrics["PERatio"] == pytest.approx(6.0)
    assert metrics["PriceToBook"] == pytest.approx(300 / 325)
    assert metrics["PriceToSales"] == pytest.approx(300 / 1000)
    assert metrics["DividendsYield"] == pytest.approx(12.5 / 300)
    assert metrics["PayoutRatio"] == pytest.approx(0.25)

def test_overview_404(db):
    assert client.get("/api/security/overview", params={"company_code": "E99999"}).status_code == 404

# ---------------------------------------------------------------------------
# Formulas
# ---------------------------------------------------------------------------

def test_formulas_all_metric_ids(db):
    data = client.get("/api/security/formulas").json()
    ids = {f["id"] for f in data["formulas"]}
    assert ids >= {"LatestPrice", "MarketCap", "PERatio", "PriceToBook",
                   "PriceToSales", "DividendsYield", "PayoutRatio",
                   "ReturnOnAssets", "ReturnOnEquity", "CurrentRatio"}

def test_formulas_each_has_format(db):
    for f in client.get("/api/security/formulas").json()["formulas"]:
        assert "name" in f and "id" in f and "format" in f

# ---------------------------------------------------------------------------
# Price history
# ---------------------------------------------------------------------------

def test_price_history(db):
    prices = client.get("/api/security/price-history", params={"ticker": "1001.T"}).json()["prices"]
    assert len(prices) == 2
    assert prices[0]["price"] == 1500.0

def test_price_history_empty(db):
    assert client.get("/api/security/price-history", params={"ticker": "UNKNOWN"}).json()["prices"] == []

# ---------------------------------------------------------------------------
# Update price
# ---------------------------------------------------------------------------

def test_update_price(db, monkeypatch):
    def fake(ticker, prices_table, conn):
        # Named columns: the update migrates Stock_Prices to the provenance
        # schema before loading, so positional inserts no longer fit.
        conn.execute(
            f"INSERT INTO {prices_table} (Date, Ticker, Currency, Price) VALUES(?,?,?,?)",
            ("2025-01-01", ticker, "JPY", 999),
        )
        return True
    monkeypatch.setattr("src.security_analysis.security_analysis.load_ticker_data", fake)
    monkeypatch.setattr("src.security_analysis.load_ticker_data", fake)
    monkeypatch.setattr("src.utilities.stock_prices.load_ticker_data", fake)
    response = client.post("/api/security/update-price", json={"ticker": "1001.T"})
    assert response.status_code == 200
    r = response.json()
    assert r["ok"] is True
    assert r["rows_inserted"] == 1
    assert r["max_date"] == "2025-01-01"

def test_update_price_requires_ticker(db):
    assert client.post("/api/security/update-price", json={"ticker": ""}).status_code == 400


def test_update_price_is_operator_only(db, monkeypatch):
    from src.auth.dependencies import current_user
    from src.auth.models import AuthenticatedUser

    calls = []
    monkeypatch.setattr(
        "src.web_app.api.security_analysis._security.update_security_price",
        lambda db_path, ticker: calls.append(ticker) or {"ok": True},
    )
    member = AuthenticatedUser("member-1", "member", None, "member", "active")
    app.dependency_overrides[current_user] = lambda: member
    try:
        response = client.post("/api/security/update-price", json={"ticker": "1001.T"})
    finally:
        app.dependency_overrides.pop(current_user, None)
    assert response.status_code == 403
    assert calls == []

# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

def test_history_table_groups(db):
    data = client.get("/api/security/history", params={"company_code": "E00001"}).json()
    assert "tables" in data and "periods" in data
    tables = data["tables"]
    assert "ShareMetrics" in tables
    assert tables["ShareMetrics"]["display_name"] == "Share Metrics"
    assert len(tables["ShareMetrics"]["metrics"]) >= 3

def test_history_periods(db):
    data = client.get("/api/security/history", params={"company_code": "E00001"}).json()
    assert data["periods"] == sorted(data["periods"])

def test_history_empty(db):
    data = client.get("/api/security/history", params={"company_code": "E99999"}).json()
    assert data["periods"] == [] and data["tables"] == {}

# ---------------------------------------------------------------------------
# Page serving
# ---------------------------------------------------------------------------

def test_page_html(db):
    r = client.get("/security")
    assert r.status_code == 200
    assert '<div id="root"></div>' in r.text
    assert "/app-assets/" in r.text
    # Must not leak db_path
    assert "db_path" not in r.text


def test_overview_prefers_filing_description_without_external_lookup(db, monkeypatch):
    import src.web_app.api.security_analysis as m

    original = m._security.get_security_overview

    def with_filing_text(*args, **kwargs):
        result = original(*args, **kwargs)
        result["company"]["filing_description_en"] = "Alpha makes industrial sensors."
        result["company"]["description"] = "Alpha makes industrial sensors."
        return result

    calls: list[tuple] = []
    monkeypatch.setattr(m._security, "get_security_overview", with_filing_text)
    monkeypatch.setattr(
        m,
        "get_or_fetch_description",
        lambda *args: calls.append(args) or "External profile text.",
    )

    company = client.get("/api/security/overview", params={"company_code": "E00001"}).json()["company"]

    assert calls == []
    assert company["yahoo_description"] == ""
    assert company["description_source"] == {
        "kind": "filing",
        "label": "EDINET annual report (English translation)",
    }


def test_overview_labels_external_description_fallback(db, monkeypatch):
    import src.web_app.api.security_analysis as m

    calls: list[tuple] = []
    monkeypatch.setattr(
        m,
        "get_or_fetch_description",
        lambda *args: calls.append(args) or "External profile text.",
    )

    company = client.get("/api/security/overview", params={"company_code": "E00001"}).json()["company"]

    assert calls and calls[0][1:] == ("E00001", "1001.T")
    assert company["yahoo_description"] == "External profile text."
    assert company["yahoo_symbol"] == "1001.T"
    assert company["description_source"] == {
        "kind": "external",
        "label": "Yahoo Finance profile",
        "symbol": "1001.T",
    }


def test_overview_reports_price_and_reporting_currencies(db):
    with sqlite3.connect(db) as conn:
        conn.execute("ALTER TABLE FinancialStatements ADD COLUMN Currency TEXT")
        conn.execute("UPDATE FinancialStatements SET Currency = 'USD'")

    data = client.get("/api/security/overview", params={"company_code": "E00001"}).json()

    assert data["market"]["price_currency"] == "JPY"
    assert data["metadata"]["reporting_currency"] == "USD"
    assert data["metric_definitions"]["MarketCap"] == {
        "label": "Market cap", "group": "Market", "format": "money", "currency": "price",
        "description": "Latest price × shares issued as of the latest annual filing date.",
    }
    assert data["metric_definitions"]["Revenue"]["currency"] == "reporting"
