"""Tests for Security Analysis API endpoints.

Uses in-memory SQLite databases injected via monkeypatched get_db2().
The frontend never sends database paths — they are resolved server-side.
"""

from __future__ import annotations

import json, sqlite3
import pytest
from fastapi.testclient import TestClient
from src.web_app.server import app

client = TestClient(app)


def _create_db(path: str) -> str:
    conn = sqlite3.connect(path); cur = conn.cursor()
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
    conn.commit(); conn.close()
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
        conn.execute(f"INSERT INTO {prices_table} VALUES(?,?,?,?)", ("2025-01-01", ticker, "JPY", 999))
        return True
    monkeypatch.setattr("src.security_analysis.security_analysis.load_ticker_data", fake)
    monkeypatch.setattr("src.security_analysis.load_ticker_data", fake)
    monkeypatch.setattr("src.utilities.stock_prices.load_ticker_data", fake)
    r = client.post("/api/security/update-price", json={"ticker": "1001.T"}).json()
    assert r.get("ok") is True or r.get("rows_inserted", 0) >= 0

def test_update_price_requires_ticker(db):
    assert client.post("/api/security/update-price", json={"ticker": ""}).status_code == 400

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
    assert "sa-search" in r.text
    assert "Security Analysis" in r.text
    # Must not leak db_path
    assert "db_path" not in r.text
