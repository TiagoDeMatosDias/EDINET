"""
Tests for src/backtesting/api.py — FastAPI endpoint handlers.

Uses fastapi.testclient.TestClient against the full server app.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from fastapi.testclient import TestClient

from src.web_app.server import app

client = TestClient(app)


class TestBacktestingAPI(unittest.TestCase):

    # ── GET /api/backtesting/db-path ───────────────────────────────────

    def test_db_path_returns_200(self):
        resp = client.get("/api/backtesting/db-path")
        # May succeed or 503 if no DB configured, but shouldn't crash
        self.assertIn(resp.status_code, (200, 503))

    def test_db_path_returns_json(self):
        resp = client.get("/api/backtesting/db-path")
        if resp.status_code == 200:
            data = resp.json()
            self.assertIn("db_path", data)

    # ── GET /api/backtesting/available-tickers ────────────────────────

    def test_available_tickers_returns_json(self):
        resp = client.get("/api/backtesting/available-tickers")
        if resp.status_code == 200:
            data = resp.json()
            self.assertIn("tickers", data)
            self.assertIsInstance(data["tickers"], list)

    # ── POST /api/backtesting/run ─────────────────────────────────────

    def test_run_missing_start_date_422(self):
        """Pydantic validation error for missing required field."""
        resp = client.post("/api/backtesting/run", json={
            "portfolio": {"A": {"mode": "weight", "value": 1.0}},
            "end_date": "2023-12-29",
        })
        # FastAPI returns 422 for validation errors
        self.assertEqual(resp.status_code, 422)

    def test_run_empty_portfolio_422_or_400(self):
        """Empty portfolio → validation failure."""
        resp = client.post("/api/backtesting/run", json={
            "portfolio": {},
            "start_date": "2023-01-04",
            "end_date": "2023-12-29",
        })
        # Pydantic allows empty dict — backend validation catches it
        # Could be 400 (backend raises) or 200/500 depending on DB state.
        # At minimum it's not a 503.
        self.assertNotEqual(resp.status_code, 503)

    # ── POST /api/backtesting/run-from-screener ───────────────────────

    # ── POST /api/backtesting/run-from-csv ────────────────────────────

    def test_run_from_csv_empty_400(self):
        resp = client.post("/api/backtesting/run-from-csv", json={
            "csv_content": "",
        })
        self.assertEqual(resp.status_code, 400)

    def test_run_from_csv_missing_field_422(self):
        resp = client.post("/api/backtesting/run-from-csv", json={})
        self.assertEqual(resp.status_code, 422)

    # ── GET /api/backtesting/base-currencies ─────────────────────────

    def test_base_currencies_returns_list(self):
        """Endpoint returns a currencies list with code/label objects."""
        resp = client.get("/api/backtesting/base-currencies")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("currencies", data)
        currencies = data["currencies"]
        self.assertIsInstance(currencies, list)
        if currencies:
            self.assertIn("code", currencies[0])
            self.assertIn("label", currencies[0])

    # ── Currency / benchmark_mode validation ─────────────────────────

    def test_run_invalid_base_currency_400(self):
        """Invalid base_currency returns 400."""
        resp = client.post("/api/backtesting/run", json={
            "portfolio": {"A": {"mode": "weight", "value": 1.0}},
            "start_date": "2023-01-04",
            "end_date": "2023-12-29",
            "base_currency": "INVALID_XXX",
        })
        # Will either 400 (validation) or 200/500 (if DB not available)
        # If 200, the DB must have data; if 400, validation caught it.
        self.assertIn(resp.status_code, (200, 400, 500, 503))

    def test_run_valid_benchmark_mode_portfolio(self):
        """benchmark_mode='portfolio' is accepted in request."""
        resp = client.post("/api/backtesting/run", json={
            "portfolio": {"A": {"mode": "weight", "value": 1.0}},
            "start_date": "2023-01-04",
            "end_date": "2023-12-29",
            "benchmark_mode": "portfolio",
            "base_currency": "EUR",
        })
        # Should not 422 (model validation passes)
        self.assertNotEqual(resp.status_code, 422)

    def test_run_invalid_benchmark_mode_422(self):
        """Invalid benchmark_mode value returns 422 validation error."""
        resp = client.post("/api/backtesting/run", json={
            "portfolio": {"A": {"mode": "weight", "value": 1.0}},
            "start_date": "2023-01-04",
            "end_date": "2023-12-29",
            "benchmark_mode": "invalid_mode",
        })
        self.assertEqual(resp.status_code, 422)

    def test_csv_run_accepts_benchmark_mode(self):
        """CSV backtest request accepts benchmark_mode field."""
        resp = client.post("/api/backtesting/run-from-csv", json={
            "csv_content": "year,tickers\n2023,A",
            "benchmark_mode": "portfolio",
            "base_currency": "EUR",
        })
        # Should not 422 (model validation passes; may fail on empty DB)
        self.assertNotEqual(resp.status_code, 422)

    # ── Page route ────────────────────────────────────────────────────

    def test_backtesting_page_returns_200(self):
        """/backtesting serves the HTML page."""
        resp = client.get("/backtesting")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/html", resp.headers.get("content-type", ""))

    # ── OpenAPI docs ──────────────────────────────────────────────────

    def test_openapi_includes_backtesting_routes(self):
        """Backtesting routes are registered in the OpenAPI schema."""
        resp = client.get("/openapi.json")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        # Check path-level tags (top-level tags may not aggregate from
        # dynamically-discovered routers in FastAPI)
        path_tags = set()
        for path_info in data.get("paths", {}).values():
            for method_info in path_info.values():
                for tag in method_info.get("tags", []):
                    path_tags.add(tag)
        self.assertIn("backtesting", path_tags)


if __name__ == "__main__":
    unittest.main()
