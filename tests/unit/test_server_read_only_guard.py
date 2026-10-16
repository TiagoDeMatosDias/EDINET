"""With the read-only guard armed, the web server reads market data and queues every write."""

from __future__ import annotations

import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.portfolio.portfolio_state as portfolio_state
import src.web_app.api.screening as screening_api
import src.web_app.api.security_analysis as security_api
import src.web_app.api.splits as splits_api
from src.auth.models import AuthenticatedUser
from src.jobs.service import get_job_service
from src.jobs.user_api import install_job_error_handlers
from src.orchestrator.common.sqlite import (
    clear_protected_database_paths,
    connect_write,
    protect_database_paths,
)
from src.screening.screening import _prepare_screening_database


@pytest.fixture
def market_db(tmp_path, monkeypatch):
    path = str(tmp_path / "market.db")
    filings = str(tmp_path / "filings.db")
    with sqlite3.connect(filings) as conn:
        conn.execute("CREATE TABLE t (x)")
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE CompanyInfo (Company_Code TEXT PRIMARY KEY, Company_Name TEXT, [Submitter Name] TEXT,
                Company_Industry TEXT, Company_Ticker TEXT, Listed TEXT);
            CREATE TABLE FinancialStatements (Company_Code TEXT, docID TEXT UNIQUE, periodEnd TEXT,
                SharesOutstanding REAL, SharePrice REAL);
            CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Currency TEXT, Price REAL, PRIMARY KEY (Date, Ticker));
            INSERT INTO CompanyInfo VALUES ('E00001', 'Alpha Corp', 'Alpha', 'Tech', '1001.T', 'TSE');
            INSERT INTO FinancialStatements VALUES ('E00001', 'DOC1', '2024-03-31', 5000000, 1500);
            INSERT INTO Stock_Prices VALUES ('2024-03-31', '1001.T', 'JPY', 1500), ('2024-04-01', '1001.T', 'JPY', 1520);
            """
        )
    for module in (screening_api, security_api, splits_api):
        monkeypatch.setattr(module, "get_market_db", lambda: path)
    portfolio_state._split_table_checked.discard(path)
    yield path, filings
    clear_protected_database_paths()


@pytest.fixture
def client(market_db):
    app = FastAPI()

    @app.middleware("http")
    async def sign_in(request, call_next):
        request.state.user = AuthenticatedUser("operator-1", "op", None, "operator", "active")
        return await call_next(request)

    for module in (screening_api, security_api, splits_api):
        app.include_router(module.router)
    install_job_error_handlers(app)
    return TestClient(app)


def _arm(market_db) -> None:
    protect_database_paths(list(market_db))


def test_the_guard_makes_a_direct_write_fail(market_db):
    _arm(market_db)
    conn = connect_write(market_db[0])
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("INSERT INTO Stock_Prices VALUES ('2024-05-01', '1001.T', 'JPY', 1)")
    finally:
        conn.close()


def test_screening_reads_work_on_an_unprepared_database(client, market_db):
    _arm(market_db)
    assert "CompanyInfo" in client.get("/api/screening/metrics").json()["tables"]
    assert client.get("/api/screening/periods").json()["periods"] == ["2024"]
    assert client.get("/api/screening/last-result").status_code == 200


def test_a_screening_export_runs_on_a_prepared_database(client, market_db):
    _prepare_screening_database(market_db[0])
    _arm(market_db)
    response = client.post("/api/screening/export", json={"columns": ["CompanyInfo.Company_Code"]})
    assert response.status_code == 200 and "E00001" in response.text


def test_security_analysis_reads_skip_missing_indexes(client, market_db):
    _arm(market_db)
    assert client.get("/api/security/search", params={"q": "Alpha"}).json()["results"]
    overview = client.get("/api/security/overview", params={"company_code": "E00001"})
    assert overview.status_code == 200, overview.text
    # The server only queued the description fetch; no index was written.
    with sqlite3.connect(market_db[0]) as conn:
        names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
    assert not any(name.startswith("idx_sa_") for name in names)


def test_split_reads_treat_a_missing_table_as_empty(client, market_db):
    _arm(market_db)
    assert client.get("/api/splits/").json() == {"splits": [], "total": 0}
    assert client.get("/api/splits/1").status_code == 404
    with sqlite3.connect(market_db[0]) as conn:
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'Stock_Splits'").fetchone() is None


def test_split_review_and_detection_are_queued_not_run(client, market_db):
    from src.portfolio.split_schema import ensure_split_tables

    ensure_split_tables(market_db[0])
    with sqlite3.connect(market_db[0]) as conn:
        conn.execute("INSERT INTO Stock_Splits (ticker, split_date, ratio_from, ratio_to) VALUES ('1001.T', '2024-05-01', 1, 2)")
    _arm(market_db)

    review = client.put("/api/splits/1", json={"confirmation": "confirmed"})
    detect = client.post("/api/splits/detect", json={"mode": "incremental"})

    assert review.status_code == 202 and detect.status_code == 202
    jobs = get_job_service()
    assert jobs.get(review.json()["job_id"]).kind == "split_review"
    assert jobs.get(detect.json()["job_id"]).kind == "split_detection"
    with sqlite3.connect(market_db[0]) as conn:
        assert conn.execute("SELECT confirmation FROM Stock_Splits").fetchone()[0] == "pending"


def test_price_updates_are_queued_not_run(client, market_db):
    _arm(market_db)
    single = client.post("/api/security/update-price", json={"ticker": "1001.T"})
    several = client.post("/api/screening/update-prices", json={"tickers": ["1001.T"]})
    assert single.status_code == 202 and several.status_code == 202
    assert get_job_service().get(several.json()["job_id"]).kind == "price_refresh"


def test_the_ledger_rebuild_does_not_migrate_splits_in_the_server(market_db):
    _arm(market_db)
    portfolio_state._ensure_split_table_exists(market_db[0])
    with sqlite3.connect(market_db[0]) as conn:
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'Stock_Splits'").fetchone() is None
