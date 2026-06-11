"""Tests for src/portfolio/api.py — HTTP-level route verification.

Uses FastAPI's TestClient for in-process testing without a running server.
"""

import os
import sys
import pytest
from fastapi.testclient import TestClient

# Import the FastAPI app
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from src.portfolio.schema import create_tables
from src.portfolio.api import router

from fastapi import FastAPI

app = FastAPI()
app.include_router(router)

client = TestClient(app)


class TestUpload:
    @pytest.fixture(autouse=True)
    def setup_db(self, monkeypatch):
        import tempfile
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        create_tables(path)

        def _mock_get_db3():
            return path

        def _mock_get_db2():
            # Use real db2 for price lookups (read-only)
            from src.orchestrator.common.db_config import get_db2 as _real_db2
            return _real_db2()

        monkeypatch.setattr("src.portfolio.api.get_db3", _mock_get_db3)
        monkeypatch.setattr("src.portfolio.api.get_db2", _mock_get_db2)
        monkeypatch.setattr("src.portfolio.price_fetcher.get_db2", _mock_get_db2)

    def _read_test_xml(self):
        ibkr_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../..", "data", "ibkr"
        )
        with open(os.path.join(ibkr_dir, "2024.xml"), "rb") as f:
            return f.read()

    def test_upload_xml_success(self):
        content = self._read_test_xml()
        resp = client.post(
            "/api/portfolio/upload",
            files={"file": ("2024.xml", content, "application/xml")},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["source_file"] == "2024.xml"
        assert data["inserted"] > 0
        assert "TRADE" in str(data["by_activity"])

    def test_upload_non_xml_rejected(self):
        resp = client.post(
            "/api/portfolio/upload",
            files={"file": ("test.txt", b"hello", "text/plain")},
        )
        assert resp.status_code == 400

    def test_upload_twice_no_duplicates(self, monkeypatch):
        """Uploading the same file twice should skip on second insert."""
        content = self._read_test_xml()
        r1 = client.post(
            "/api/portfolio/upload",
            files={"file": ("2024.xml", content, "application/xml")},
        )
        inserted1 = r1.json()["inserted"]
        r2 = client.post(
            "/api/portfolio/upload",
            files={"file": ("2024.xml", content, "application/xml")},
        )
        inserted2 = r2.json()["inserted"]
        assert inserted2 == 0
        assert inserted1 > 0


class TestReadEndpoints:
    @pytest.fixture(autouse=True)
    def setup_db(self, monkeypatch):
        import tempfile
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        create_tables(path)

        def _mock_get_db3():
            return path

        def _mock_get_db2():
            from src.orchestrator.common.db_config import get_db2 as _real_db2
            return _real_db2()

        monkeypatch.setattr("src.portfolio.api.get_db3", _mock_get_db3)
        monkeypatch.setattr("src.portfolio.api.get_db2", _mock_get_db2)
        monkeypatch.setattr("src.portfolio.portfolio_state.get_db3", _mock_get_db3)
        monkeypatch.setattr("src.portfolio.portfolio_state.get_db2", _mock_get_db2)
        monkeypatch.setattr("src.portfolio.performance.get_db3", _mock_get_db3)
        monkeypatch.setattr("src.portfolio.performance.get_db2", _mock_get_db2)

        # Load and build portfolio state
        from src.portfolio.ibkr_parser import parse_ibkr_xml_file, normalize_entries
        from src.portfolio.transactions import insert_entries
        from src.portfolio.portfolio_state import build_portfolio_state

        ibkr_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../..", "data", "ibkr"
        )
        for year in ["2024"]:
            fpath = os.path.join(ibkr_dir, f"{year}.xml")
            result = parse_ibkr_xml_file(fpath)
            entries = normalize_entries(result)
            insert_entries(path, entries, source_file=f"{year}.xml")

        build_portfolio_state(path, base_currency="EUR")

    def test_get_transactions(self):
        resp = client.get("/api/portfolio/transactions?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) <= 5

    def test_get_symbols(self):
        resp = client.get("/api/portfolio/symbols")
        assert resp.status_code == 200
        assert len(resp.json()) > 0

    def test_date_range(self):
        resp = client.get("/api/portfolio/date-range")
        assert resp.status_code == 200
        data = resp.json()
        assert data["min_date"] is not None

    def test_activity_summary(self):
        resp = client.get("/api/portfolio/activity-summary")
        assert resp.status_code == 200
        data = resp.json()
        assert "TRADE" in data["by_activity"]

    def test_holdings(self):
        resp = client.get("/api/portfolio/holdings")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        if data:
            assert "symbol" in data[0]

    def test_holdings_history(self):
        resp = client.get("/api/portfolio/holdings/history")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_performance(self):
        resp = client.get("/api/portfolio/performance?risk_free_rate=0.02")
        assert resp.status_code == 200
        data = resp.json()
        assert "sharpe_ratio" in data
        assert "total_dividend_income" in data

    def test_rebuild(self):
        resp = client.post("/api/portfolio/rebuild")
        assert resp.status_code == 200
        data = resp.json()
        assert data["daily_rows"] > 0

    def test_risk_free_rate(self):
        resp = client.get("/api/portfolio/risk-free-rate?base_currency=EUR")
        assert resp.status_code == 200
        data = resp.json()
        assert data["risk_free_rate"] is not None
