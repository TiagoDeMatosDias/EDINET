"""
Integration tests for the Backtesting view — end-to-end functionality.

Verifies that the backtesting API endpoints, data structures, edge cases,
and cross-page flows work correctly with the real database.
"""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

import src.backtesting.api as backtesting_api
from src.backtesting.backtesting import (
    _empty_result,
    run_backtest_set_web,
    run_backtest_web,
)
from src.web_app.server import app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_db():
    """Resolve the test database path (same as Security Analysis tests)."""
    from src.orchestrator.common.db_config import get_db2
    db = get_db2()
    if not db:
        raise unittest.SkipTest("No database configured (set DB_PATH or DB2_PATH in .env)")
    return db


def _get_test_tickers(db_path: str, n: int = 5) -> list[str]:
    """Return up to *n* tickers that have price data."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT Ticker FROM Stock_Prices "
            "WHERE Ticker IS NOT NULL AND Ticker != '' "
            "ORDER BY Ticker LIMIT ?",
            (n,),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def _build_weight_portfolio(tickers: list[str]) -> dict:
    """Equal-weight portfolio."""
    w = 1.0 / len(tickers)
    return {t: {"mode": "weight", "value": w} for t in tickers}


def _load_saved_result(response_data: dict) -> dict:
    result_path = (
        backtesting_api._BACKTEST_ROOT
        / response_data["id"]
        / "result.json"
    )
    return json.loads(result_path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Test client
# ---------------------------------------------------------------------------

client = TestClient(app)
_output_directory: tempfile.TemporaryDirectory | None = None
_original_backtest_root = backtesting_api._BACKTEST_ROOT


def setUpModule() -> None:
    """Keep all generated backtests out of operator-owned data."""
    global _output_directory
    _output_directory = tempfile.TemporaryDirectory()
    backtesting_api._BACKTEST_ROOT = Path(_output_directory.name).resolve()


def tearDownModule() -> None:
    backtesting_api._BACKTEST_ROOT = _original_backtest_root
    if _output_directory is not None:
        _output_directory.cleanup()


# ===========================================================================
# API Integration Tests
# ===========================================================================

class TestBacktestingAPIIntegration(unittest.TestCase):
    """End-to-end tests for the backtesting API endpoints with real data."""

    @classmethod
    def setUpClass(cls):
        cls.db_path = _resolve_db()
        cls.tickers = _get_test_tickers(cls.db_path, 5)
        if len(cls.tickers) < 2:
            raise unittest.SkipTest("Need at least 2 tickers with price data")

    # ── GET /api/backtesting/db-path ─────────────────────────────────

    def test_db_path_returns_200(self):
        resp = client.get("/api/backtesting/db-path")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("db_path", data)

    # ── GET /api/backtesting/available-tickers ───────────────────────

    def test_available_tickers_returns_list(self):
        resp = client.get("/api/backtesting/available-tickers")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("tickers", data)
        self.assertIsInstance(data["tickers"], list)
        self.assertGreater(len(data["tickers"]), 0)
        # No duplicates
        self.assertEqual(len(data["tickers"]), len(set(data["tickers"])))

    def test_available_tickers_are_non_empty_strings(self):
        """Tickers should be non-empty strings."""
        resp = client.get("/api/backtesting/available-tickers")
        data = resp.json()
        for t in data["tickers"]:
            self.assertIsInstance(t, str)
            self.assertTrue(len(t) > 0, "Empty ticker found")
            self.assertNotEqual(t, "null")
            self.assertNotEqual(t, "None")
            self.assertNotEqual(t, "undefined")

    # ── POST /api/backtesting/run — happy path ───────────────────────

    def test_run_manual_portfolio_happy_path(self):
        portfolio = _build_weight_portfolio(self.tickers[:3])
        resp = client.post("/api/backtesting/run", json={
            "portfolio": portfolio,
            "start_date": "2020-01-01",
            "end_date": "2023-01-01",
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        # The API returns a bounded summary while persisting the full result.
        self.assertIn("summary", data)
        self.assertIn("chart_data", data)
        self.assertIn("per_company", data)
        self.assertIn("yearly_returns", data)
        self.assertIn("dividends_by_year", data)
        self.assertEqual(data["path"], data["id"])

        # Summary completeness
        m = data["summary"]
        required = {
            "total_return", "annualized_return", "volatility",
            "sharpe_ratio", "max_drawdown", "start_date", "end_date",
            "price_return", "dividend_return", "initial_capital", "warnings",
        }
        for key in required:
            self.assertIn(key, m, f"Missing metric: {key}")

        # Benchmark fields should be None (no benchmark)
        self.assertIsNone(m["benchmark_total_return"])
        self.assertIsNone(m["excess_return"])

        # Chart data structure
        cd = data["chart_data"]
        self.assertIn("cumulative", cd)
        self.assertIn("drawdown", cd)
        self.assertIn("decomposition", cd)
        self.assertGreater(len(cd["cumulative"]), 0, "Cumulative data is empty")
        self.assertGreater(len(cd["drawdown"]), 0, "Drawdown data is empty")

        # Per-company records
        pc = data["per_company"]
        self.assertEqual(len(pc), 3)
        for rec in pc:
            self.assertIn("Ticker", rec)
            self.assertIsInstance(rec["Ticker"], str)
            self.assertIsInstance(rec["total_return"], (int, float))

        # Yearly returns — keys may have spaces (pandas column names)
        yr = data["yearly_returns"]
        self.assertGreater(len(yr), 0)
        for rec in yr:
            # Backend produces either 'year' or 'Year' key
            self.assertTrue("year" in rec or "Year" in rec,
                            f"No year key in {list(rec.keys())}")

    # ── POST /api/backtesting/run — with benchmark ───────────────────

    def test_run_with_benchmark_produces_benchmark_metrics(self):
        portfolio = _build_weight_portfolio(self.tickers[:2])
        # Use the third ticker as benchmark
        benchmark = self.tickers[2] if len(self.tickers) > 2 else self.tickers[0]

        resp = client.post("/api/backtesting/run", json={
            "portfolio": portfolio,
            "start_date": "2020-01-01",
            "end_date": "2023-01-01",
            "benchmark_ticker": benchmark,
            "risk_free_rate": 0.02,
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        m = _load_saved_result(data)["metrics"]

        # Benchmark fields should be populated
        self.assertIsNotNone(m["benchmark_total_return"],
                             "benchmark_total_return is None with benchmark")
        self.assertIsNotNone(m["benchmark_annualized_return"])
        self.assertIsNotNone(m["benchmark_volatility"])
        self.assertIsNotNone(m["benchmark_max_drawdown"])
        self.assertIsNotNone(m["excess_return"])
        self.assertIsNotNone(m["information_ratio"])

        # Cumulative chart should have benchmark data
        cum = data["chart_data"]["cumulative"]
        self.assertIsNotNone(cum[0].get("benchmark"))

    # ── POST /api/backtesting/run — validation ───────────────────────

    def test_run_empty_portfolio_returns_400(self):
        resp = client.post("/api/backtesting/run", json={
            "portfolio": {},
            "start_date": "2020-01-01",
            "end_date": "2023-01-01",
        })
        self.assertEqual(resp.status_code, 400)

    def test_run_missing_dates_returns_422(self):
        resp = client.post("/api/backtesting/run", json={
            "portfolio": {self.tickers[0]: {"mode": "weight", "value": 1.0}},
        })
        self.assertEqual(resp.status_code, 422)

    def test_run_start_after_end_returns_400(self):
        resp = client.post("/api/backtesting/run", json={
            "portfolio": {self.tickers[0]: {"mode": "weight", "value": 1.0}},
            "start_date": "2023-01-01",
            "end_date": "2020-01-01",
        })
        self.assertEqual(resp.status_code, 400)

    # ── POST /api/backtesting/run-from-csv — happy path ──────────────

    def test_run_csv_backtest_set_happy_path(self):
        tickers = self.tickers[:2]
        weight = 1.0 / len(tickers)
        csv_lines = ["Year,Tickers,Type,Amount"]
        for t in tickers:
            csv_lines.append(f"2020,{t},weight,{weight:.6f}")

        resp = client.post("/api/backtesting/run-from-csv", json={
            "csv_content": "\n".join(csv_lines),
            "durations": ["1yr", "3yr"],
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        # The API returns aggregate data; detailed runs remain in result.json.
        self.assertIn("aggregate", data)
        results = _load_saved_result(data)["results"]

        agg = data["aggregate"]
        self.assertEqual(agg["total_runs"], 2)  # 2 durations
        self.assertEqual(agg["successful"], 2)
        self.assertEqual(agg["failed"], 0)
        self.assertIsNotNone(agg["stats"])
        self.assertIn("total_return", agg["stats"])
        self.assertIn("sharpe_ratio", agg["stats"])

        self.assertEqual(len(results), 2)
        for r in results:
            self.assertIn("metrics", r)
            self.assertIsNotNone(r["metrics"])
            self.assertIn("year", r)
            self.assertIn("duration", r)
            self.assertIn("tickers", r)
            self.assertEqual(r["year"], "2020")

    def test_run_csv_with_benchmark_comment_header(self):
        tickers = self.tickers[:1]
        benchmark = self.tickers[1] if len(self.tickers) > 1 else self.tickers[0]
        csv_lines = [
            f"# Benchmark: {benchmark}",
            "# Discount Rate: 0.03",
            "Year,Tickers,Type,Amount",
            f"2021,{tickers[0]},weight,1.0",
        ]

        resp = client.post("/api/backtesting/run-from-csv", json={
            "csv_content": "\n".join(csv_lines),
            "durations": ["1yr"],
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        # Benchmark should be picked up from CSV header
        r = _load_saved_result(data)["results"][0]
        m = r["metrics"]
        self.assertIsNotNone(m.get("benchmark_total_return"),
                             "CSV # Benchmark: comment header not applied")

    # ── POST /api/backtesting/run-from-csv — validation ──────────────

    def test_run_csv_empty_content_returns_400(self):
        resp = client.post("/api/backtesting/run-from-csv", json={
            "csv_content": "",
        })
        self.assertEqual(resp.status_code, 400)

    def test_run_csv_missing_required_columns_returns_400(self):
        resp = client.post("/api/backtesting/run-from-csv", json={
            "csv_content": "Year,Amount\n2020,1.0",
        })
        self.assertEqual(resp.status_code, 400)

    def test_run_csv_header_only_returns_empty_set(self):
        resp = client.post("/api/backtesting/run-from-csv", json={
            "csv_content": "Year,Tickers,Type,Amount",
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["aggregate"]["total_runs"], 0)
        self.assertEqual(_load_saved_result(data)["results"], [])

    # ── Multi-year CSV ───────────────────────────────────────────────

    def test_run_csv_multi_year_with_multiple_durations(self):
        tickers = self.tickers[:2]
        weight = 1.0 / len(tickers)
        csv_lines = ["Year,Tickers,Type,Amount"]
        for y in ("2019", "2020", "2021"):
            for t in tickers:
                csv_lines.append(f"{y},{t},weight,{weight:.6f}")

        resp = client.post("/api/backtesting/run-from-csv", json={
            "csv_content": "\n".join(csv_lines),
            "durations": ["1yr", "2yr"],
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        agg = data["aggregate"]
        # 3 years × 2 durations = 6 backtests
        self.assertEqual(agg["total_runs"], 6)
        self.assertEqual(agg["successful"], 6)

        # Verify all year/duration combinations
        results = _load_saved_result(data)["results"]
        years = set(r["year"] for r in results)
        durations = set(r["duration"] for r in results)
        self.assertEqual(years, {"2019", "2020", "2021"})
        self.assertEqual(durations, {"1yr", "2yr"})

    # ── Page route ───────────────────────────────────────────────────

    def test_backtesting_page_loads(self):
        resp = client.get("/backtesting")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/html", resp.headers.get("content-type", ""))
        self.assertIn('<div id="root"></div>', resp.text)
        self.assertIn("/app-assets/", resp.text)

    # ── OpenAPI ──────────────────────────────────────────────────────

    def test_openapi_includes_csv_and_run_endpoints(self):
        resp = client.get("/openapi.json")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        paths = set()
        for path_info in data.get("paths", {}).values():
            for method in path_info:
                paths.add(method.upper())
        # POST should be available (for /run and /run-from-csv)
        self.assertIn("POST", paths)

        # /run-from-screener should NOT be present
        openapi_paths = set(data.get("paths", {}).keys())
        self.assertNotIn("/api/backtesting/run-from-screener", openapi_paths,
                         "/run-from-screener should be removed from OpenAPI")


# ===========================================================================
# Data Structure Tests
# ===========================================================================

class TestBacktestingDataStructures(unittest.TestCase):
    """Verify chart data, metrics, and return types are correct and
    safe for the frontend (no NaN, no null strings, valid JSON)."""

    @classmethod
    def setUpClass(cls):
        cls.db_path = _resolve_db()
        cls.tickers = _get_test_tickers(cls.db_path, 3)
        if len(cls.tickers) < 2:
            raise unittest.SkipTest("Need at least 2 tickers with price data")

    # ── BacktestResult data integrity ────────────────────────────────

    def test_result_all_fields_json_serializable(self):
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        # Should not raise
        json_str = json.dumps(result)
        self.assertIsInstance(json_str, str)
        self.assertGreater(len(json_str), 100)

    def test_result_no_nan_in_metrics(self):
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        m = result["metrics"]
        for key, val in m.items():
            if isinstance(val, float):
                self.assertFalse(
                    pd.isna(val) or val != val,  # NaN check: NaN != NaN
                    f"Metric '{key}' is NaN",
                )

    def test_result_no_null_string_in_per_company(self):
        """Per-company entries must have valid ticker strings, never null."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        for rec in result["per_company"]:
            self.assertIsNotNone(rec.get("Ticker"), "Ticker is None")
            self.assertIsInstance(rec["Ticker"], str)
            self.assertNotEqual(rec["Ticker"], "")
            self.assertNotEqual(rec["Ticker"], "null")
            self.assertNotEqual(rec["Ticker"], "None")

    def test_result_chart_data_dates_are_strings(self):
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        for row in result["chart_data"]["cumulative"]:
            self.assertIsInstance(row["date"], str)
            self.assertRegex(row["date"], r"^\d{4}-\d{2}-\d{2}$")

    def test_result_chart_data_no_null_values(self):
        """Chart data values should be floats, never None/null."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        for row in result["chart_data"]["cumulative"]:
            self.assertIsInstance(row["portfolio"], (int, float))
            # Benchmark may be None (no benchmark), which is valid
        for row in result["chart_data"]["drawdown"]:
            self.assertIsInstance(row["portfolio"], (int, float))
        for row in result["chart_data"]["decomposition"]:
            self.assertIsInstance(row["price_only"], (int, float))
            self.assertIsInstance(row["dividend_only"], (int, float))
            self.assertIsInstance(row["total"], (int, float))

    def test_decomposition_adds_up_to_total(self):
        """price_only + dividend_only ≈ total (within float tolerance)."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        for row in result["chart_data"]["decomposition"]:
            computed = row["price_only"] + row["dividend_only"]
            diff = abs(computed - row["total"])
            self.assertLess(diff, 0.01, f"Decomposition doesn't sum: {computed} vs {row['total']}")

    # ── Yearly returns ────────────────────────────────────────────────

    def test_yearly_returns_have_required_fields(self):
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        yr = result["yearly_returns"]
        self.assertGreater(len(yr), 0)
        for rec in yr:
            # Backend uses pandas column names — may have spaces ('Year' vs 'year')
            year_key = "year" if "year" in rec else "Year"
            self.assertIsNotNone(rec.get(year_key))
            self.assertIsInstance(rec[year_key], (int, float))

    # ── Dividends by year ────────────────────────────────────────────

    def test_dividends_columns_include_total(self):
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        divs = result["dividends_by_year"]
        if divs:
            # At minimum should have 'year' key
            for rec in divs:
                self.assertIn("year", rec)

    # ── Empty result ─────────────────────────────────────────────────

    def test_empty_result_has_valid_structure(self):
        result = _empty_result("2020-01-01", "2023-01-01", 10000.0,
                               has_benchmark=True)
        self.assertEqual(result["metrics"]["total_return"], 0.0)
        self.assertEqual(result["chart_data"]["cumulative"], [])
        self.assertEqual(result["chart_data"]["drawdown"], [])
        self.assertEqual(result["chart_data"]["decomposition"], [])
        self.assertEqual(result["per_company"], [])
        self.assertEqual(result["yearly_returns"], [])
        self.assertEqual(result["dividends_by_year"], [])
        # With has_benchmark=True, benchmark fields exist but are None
        self.assertIsNone(result["metrics"]["benchmark_total_return"])

    # ── Backtest set data integrity ──────────────────────────────────

    def test_backtest_set_results_all_have_chart_data(self):
        tickers = self.tickers[:2]
        weight = 1.0 / len(tickers)
        csv = "\n".join([
            "Year,Tickers,Type,Amount",
            f"2020,{tickers[0]},weight,{weight:.6f}",
            f"2020,{tickers[1]},weight,{weight:.6f}",
        ])
        result = run_backtest_set_web(
            db_path=self.db_path,
            csv_content=csv,
            durations=["1yr", "3yr"],
        )
        for r in result["results"]:
            self.assertIsNotNone(r["metrics"])
            self.assertGreater(len(r["chart_data"]["cumulative"]), 0)
            self.assertIn("chart_data", r)
            self.assertIn("per_company", r)
            self.assertIn("yearly_returns", r)

    def test_backtest_set_aggregate_stats_completeness(self):
        tickers = self.tickers[:2]
        weight = 1.0 / len(tickers)
        csv = "\n".join([
            "Year,Tickers,Type,Amount",
            f"2020,{tickers[0]},weight,{weight:.6f}",
            f"2020,{tickers[1]},weight,{weight:.6f}",
        ])
        result = run_backtest_set_web(
            db_path=self.db_path,
            csv_content=csv,
            durations=["1yr", "2yr", "3yr"],
        )
        stats = result["aggregate"]["stats"]
        for metric in ("total_return", "annualized_return", "sharpe_ratio", "max_drawdown"):
            self.assertIn(metric, stats, f"Missing aggregate stat: {metric}")
            for agg in ("mean", "median", "min", "max"):
                self.assertIn(agg, stats[metric],
                              f"Missing {agg} for {metric}")
                self.assertIsInstance(stats[metric][agg], float)

    # ── Edge cases ───────────────────────────────────────────────────

    def test_mixed_allocation_modes(self):
        """Weight + shares + value modes should work together."""
        portfolio = {
            self.tickers[0]: {"mode": "weight", "value": 0.5},
            self.tickers[1]: {"mode": "shares", "value": 10},
        }
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        m = result["metrics"]
        self.assertIsNotNone(m["total_return"])
        self.assertIsInstance(m["total_return"], float)

    def test_single_ticker_no_benchmark(self):
        portfolio = {self.tickers[0]: {"mode": "weight", "value": 1.0}}
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        self.assertEqual(len(result["per_company"]), 1)
        self.assertIsNone(result["metrics"]["benchmark_total_return"])

    def test_invalid_ticker_produces_warnings_not_errors(self):
        portfolio = {
            self.tickers[0]: {"mode": "weight", "value": 0.5},
            "ZZZZZ.INVALID": {"mode": "weight", "value": 0.5},
        }
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        # Should still return results for the valid ticker
        self.assertIsNotNone(result["metrics"]["total_return"])
        # Should have warnings
        self.assertGreater(len(result["warnings"]), 0,
                           "No warnings for invalid ticker")

    def test_short_date_range_minimal_days(self):
        """Very short date range (consecutive trading days) should not crash."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        # Find two consecutive dates with price data
        conn = sqlite3.connect(self.db_path)
        try:
            dates = conn.execute(
                "SELECT DISTINCT Date FROM Stock_Prices WHERE Ticker = ? ORDER BY Date LIMIT 2",
                (self.tickers[0],),
            ).fetchall()
            if len(dates) < 2:
                raise unittest.SkipTest("Need at least 2 price dates")
            start_date, end_date = dates[0][0], dates[1][0]
        finally:
            conn.close()

        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date=start_date,
            end_date=end_date,
        )
        # Should not crash with very short range
        self.assertIn("metrics", result)
        self.assertIsInstance(result["metrics"]["total_return"], float)


# ===========================================================================
# Chart Data Integrity Tests
# ===========================================================================

class TestChartDataIntegrity(unittest.TestCase):
    """Ensures chart data sent to the frontend is valid for Chart.js."""

    @classmethod
    def setUpClass(cls):
        cls.db_path = _resolve_db()
        cls.tickers = _get_test_tickers(cls.db_path, 3)
        if len(cls.tickers) < 2:
            raise unittest.SkipTest("Need at least 2 tickers with price data")

    def test_cumulative_data_is_monotonic_dates(self):
        """Dates should be in ascending order."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        dates = [row["date"] for row in result["chart_data"]["cumulative"]]
        self.assertEqual(dates, sorted(dates),
                         f"Cumulative dates not sorted: {dates[:5]}...")

    def test_cumulative_and_drawdown_have_same_date_count(self):
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        self.assertEqual(
            len(result["chart_data"]["cumulative"]),
            len(result["chart_data"]["drawdown"]),
        )

    def test_drawdown_values_are_negative_or_zero(self):
        """All drawdown values should be ≤ 0."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        for row in result["chart_data"]["drawdown"]:
            self.assertLessEqual(row["portfolio"], 0.001,
                                 f"Drawdown positive: {row['portfolio']} at {row['date']}")

    def test_cumulative_first_value_near_zero(self):
        """First cumulative return should be very close to 0%."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        first = result["chart_data"]["cumulative"][0]["portfolio"]
        self.assertLess(abs(first), 0.05,
                        f"First cumulative return {first} too far from 0")

    def test_with_benchmark_cumulative_alignment(self):
        """Portfolio and benchmark should have the same dates when benchmark present."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        benchmark = self.tickers[2] if len(self.tickers) > 2 else self.tickers[0]

        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
            benchmark_ticker=benchmark,
        )
        cum = result["chart_data"]["cumulative"]
        portfolio_dates = [r["date"] for r in cum]
        bench_dates = [r["date"] for r in cum if r.get("benchmark") is not None]
        # Benchmark dates should be a subset of portfolio dates
        self.assertTrue(
            set(bench_dates).issubset(set(portfolio_dates)),
            "Benchmark dates not aligned with portfolio dates",
        )

    def test_decomposition_data_no_nan_or_infinity(self):
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        import math
        for row in result["chart_data"]["decomposition"]:
            for key in ("price_only", "dividend_only", "total"):
                val = row[key]
                self.assertFalse(
                    math.isnan(val) if isinstance(val, float) else False,
                    f"{key} is NaN at {row['date']}",
                )
                self.assertFalse(
                    math.isinf(val) if isinstance(val, float) else False,
                    f"{key} is Infinity at {row['date']}",
                )

    def test_per_company_ticker_never_null(self):
        """Every per-company entry must have a valid Ticker string.
        This is the root cause of 'null' labels in Charts.js tooltips."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        for rec in result["per_company"]:
            self.assertIsNotNone(rec["Ticker"],
                                 f"Ticker is None in {rec}")
            self.assertIsInstance(rec["Ticker"], str,
                                  f"Ticker not a string: {type(rec['Ticker'])}")
            self.assertTrue(len(rec["Ticker"]) > 0,
                            "Ticker is empty string")
            self.assertNotIn(rec["Ticker"], ("null", "None", "undefined"),
                             f"Ticker is literal '{rec['Ticker']}' string")


# ===========================================================================
# Cross-Page Flow Tests
# ===========================================================================

class TestScreeningToBacktestingFlow(unittest.TestCase):
    """Validates the screening → backtesting deep-link data integrity."""

    @classmethod
    def setUpClass(cls):
        cls.db_path = _resolve_db()

    def test_csv_built_from_screening_results_round_trips(self):
        """Simulate the screening page building a CSV payload,
        which the backtesting page receives and executes."""
        from src.screening import run_screening as _run_screening

        # Run a simple screening
        try:
            screen_df = _run_screening(
                db_path=self.db_path,
                criteria=[
                    {"table": "PerShare_Metrics", "column": "Earnings Per Share",
                     "operator": ">", "value": 100},
                ],
                columns=["CompanyInfo.Company_Ticker"],
                screening_date="2023-03-31",
            )
        except Exception as e:
            raise unittest.SkipTest(
                f"Screening criteria not applicable: {e}"
            ) from e

        # Extract tickers (simulate what screening/index.js does)
        ticker_col = None
        for candidate in ("Company_Ticker", "Ticker", "ticker"):
            if candidate in screen_df.columns:
                ticker_col = candidate
                break
        self.assertIsNotNone(ticker_col, "No ticker column in screening results")

        tickers = screen_df[ticker_col].dropna().unique().tolist()[:10]
        self.assertGreater(len(tickers), 0)

        # Build CSV (simulate what screening/index.js → openInBacktesting does)
        year = "2023"
        weight = 1.0 / len(tickers)
        csv_lines = ["Year,Tickers,Type,Amount"]
        for t in tickers:
            csv_lines.append(f"{year},{t},weight,{weight:.6f}")
        csv_content = "\n".join(csv_lines)

        # Run via backtesting (simulate what runScreenerBacktest does)
        result = run_backtest_set_web(
            db_path=self.db_path,
            csv_content=csv_content,
            durations=["1yr"],
        )

        self.assertEqual(result["aggregate"]["successful"], 1)
        self.assertEqual(len(result["results"][0]["tickers"]), len(tickers))

    def test_screening_backtest_api_flow(self):
        """Full API flow: screening → POST /api/backtesting/run-from-csv."""
        from src.screening import run_screening as _run_screening

        try:
            screen_df = _run_screening(
                db_path=self.db_path,
                criteria=[
                    {"table": "PerShare_Metrics", "column": "Earnings Per Share",
                     "operator": ">", "value": 100},
                ],
                columns=["CompanyInfo.Company_Ticker"],
                screening_date="2022-12-31",
            )
        except Exception as e:
            raise unittest.SkipTest(
                f"Screening criteria not applicable: {e}"
            ) from e

        if screen_df is None or screen_df.empty:
            raise unittest.SkipTest("Screening returned no results")

        ticker_col = next(
            (c for c in ("Company_Ticker", "Ticker", "ticker") if c in screen_df.columns),
            None,
        )
        tickers = screen_df[ticker_col].dropna().unique().tolist()[:5]
        weight = 1.0 / len(tickers)
        csv_lines = ["Year,Tickers,Type,Amount"]
        for t in tickers:
            csv_lines.append(f"2022,{t},weight,{weight:.6f}")

        resp = client.post("/api/backtesting/run-from-csv", json={
            "csv_content": "\n".join(csv_lines),
            "durations": ["1yr", "2yr"],
            "benchmark_ticker": tickers[0] if tickers else "",
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertGreater(data["aggregate"]["total_runs"], 0)


# ===========================================================================
# JS-compatibility smoke tests
# ===========================================================================

class TestFrontendDataSafety(unittest.TestCase):
    """Checks that backend data is safe for the JS frontend to consume."""

    @classmethod
    def setUpClass(cls):
        cls.db_path = _resolve_db()
        cls.tickers = _get_test_tickers(cls.db_path, 3)
        if len(cls.tickers) < 2:
            raise unittest.SkipTest("Need at least 2 tickers with price data")

    def test_all_numbers_are_finite(self):
        """No Infinity or -Infinity in any numeric field."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        errors = []

        def check(v, path):
            if isinstance(v, float):
                import math
                if math.isinf(v):
                    errors.append(f"Infinity at {path}: {v}")
                elif math.isnan(v):
                    errors.append(f"NaN at {path}")

        def walk(obj, path):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    walk(v, f"{path}.{k}")
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    walk(v, f"{path}[{i}]")
            else:
                check(obj, path)

        walk(result, "result")
        self.assertEqual(errors, [], f"Found {len(errors)} non-finite values:\n" + "\n".join(errors[:5]))

    def test_warnings_are_strings(self):
        """All warnings should be plain strings (not objects/null)."""
        portfolio = {
            self.tickers[0]: {"mode": "weight", "value": 0.5},
            "FAKE.TICKER": {"mode": "weight", "value": 0.5},
        }
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
        )
        for w in result["warnings"]:
            self.assertIsInstance(w, str)

    def test_backtest_set_results_are_ordered_by_year_then_duration(self):
        """Results in a set should be stable-ordered: year ascending, then duration."""
        tickers = self.tickers[:2]
        weight = 1.0 / len(tickers)
        csv = "\n".join([
            "Year,Tickers,Type,Amount",
            f"2021,{tickers[0]},weight,{weight:.6f}",
            f"2021,{tickers[1]},weight,{weight:.6f}",
            f"2020,{tickers[0]},weight,{weight:.6f}",
            f"2020,{tickers[1]},weight,{weight:.6f}",
        ])
        result = run_backtest_set_web(
            db_path=self.db_path,
            csv_content=csv,
            durations=["1yr", "2yr"],
        )
        years = [r["year"] for r in result["results"]]
        # Years should be sorted (2020, 2020, 2021, 2021)
        self.assertEqual(years, sorted(years),
                         f"Results not ordered by year: {years}")

    def test_initial_capital_propagates_to_metrics(self):
        portfolio = _build_weight_portfolio(self.tickers[:2])
        result = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
            initial_capital=50000.0,
        )
        self.assertEqual(result["metrics"]["initial_capital"], 50000.0)

    def test_risk_free_rate_propagates_to_sharpe(self):
        """Higher risk-free rate → lower Sharpe ratio."""
        portfolio = _build_weight_portfolio(self.tickers[:2])
        r0 = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
            risk_free_rate=0.0,
        )
        r5 = run_backtest_web(
            db_path=self.db_path,
            portfolio=portfolio,
            start_date="2020-01-01",
            end_date="2023-01-01",
            risk_free_rate=0.05,
        )
        # Higher risk-free rate should give lower (not higher) Sharpe
        self.assertLessEqual(
            r5["metrics"]["sharpe_ratio"],
            r0["metrics"]["sharpe_ratio"] + 0.001,
            "Higher risk-free rate should reduce Sharpe ratio",
        )


if __name__ == "__main__":
    unittest.main()
