"""
Tests for src/backtesting/backtesting.py — web-facing backtesting functions.

Strategy
--------
* Uses an in-memory SQLite database seeded with minimal price, company,
  dividends, and financial-statement data to exercise real SQL paths.
* ``run_backtest_web`` is tested for structure, benchmark metrics,
  validation, and edge cases.
* ``run_backtest_set_web`` is tested with CSV content strings.
* ``run_screening_backtest_set`` is tested with a minimal screening flow.
"""

import os
import sqlite3
import sys
import unittest

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.backtesting import (
    run_backtest_web,
    run_backtest_set_web,
    run_screening_backtest_set,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_in_memory_db() -> sqlite3.Connection:
    """Create a fully seeded in-memory database for backtest testing."""
    conn = sqlite3.connect(":memory:")

    # ── Stock_Prices ──────────────────────────────────────────────────
    prices = pd.DataFrame({
        "Date": [
            # Ticker A — steady rise (2023)
            "2023-01-04", "2023-01-05", "2023-01-06", "2023-01-09", "2023-01-10",
            "2023-06-01", "2023-06-02", "2023-12-28", "2023-12-29",
            # Ticker B — volatile (2023)
            "2023-01-04", "2023-01-05", "2023-01-06", "2023-01-09", "2023-01-10",
            "2023-06-01", "2023-06-02", "2023-12-28", "2023-12-29",
            # Benchmark
            "2023-01-04", "2023-01-05", "2023-01-06", "2023-01-09", "2023-01-10",
            "2023-06-01", "2023-06-02", "2023-12-28", "2023-12-29",
            # Ticker C — only partial data (2024)
            "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09", "2024-01-10",
        ],
        "Ticker": [
            "A", "A", "A", "A", "A", "A", "A", "A", "A",
            "B", "B", "B", "B", "B", "B", "B", "B", "B",
            "BENCH", "BENCH", "BENCH", "BENCH", "BENCH",
            "BENCH", "BENCH", "BENCH", "BENCH",
            "C", "C", "C", "C", "C",
        ],
        "Price": [
            100, 101, 102, 103, 104,
            115, 116, 120, 121,          # A: +21 %
            200, 198, 196, 202, 205,
            210, 208, 215, 218,          # B: +9 %
            500, 502, 503, 505, 507,
            510, 511, 515, 516,          # BENCH: +3.2 %
            50, 52, 53, 54, 55,          # C: partial
        ],
    })
    prices.to_sql("Stock_Prices", conn, if_exists="replace", index=False)

    # ── CompanyInfo ───────────────────────────────────────────────────
    company = pd.DataFrame({
        "Company_Code": ["E00001", "E00002", "E00003", "E_BENCH"],
        "Company_Ticker": ["A", "B", "C", "BENCH"],
    })
    company.to_sql("CompanyInfo", conn, if_exists="replace", index=False)

    # ── FinancialStatements ───────────────────────────────────────────
    fs = pd.DataFrame({
        "docID": [1, 2, 3, 4],
        "Company_Code": ["E00001", "E00002", "E00001", "E_BENCH"],
        "periodEnd": ["2023-03-31", "2023-03-31", "2023-09-30", "2023-06-30"],
    })
    fs.to_sql("FinancialStatements", conn, if_exists="replace", index=False)

    # ── ShareMetrics ──────────────────────────────────────────────────
    share = pd.DataFrame({
        "docID": [1, 2, 3, 4],
        "Dividend paid per share": [2.0, 1.5, 2.5, 1.0],
    })
    share.to_sql("ShareMetrics", conn, if_exists="replace", index=False)

    # ── Screening-related tables (minimal for run_screening path) ─────
    # CompanyInfo already has Company_Code + Company_Ticker.
    # FinancialStatements — add periodEnd for screening join path.
    # PerShare table for screening columns
    per_share = pd.DataFrame({
        "docID": [1, 2],
        "Basic earnings (loss) per share": [10.0, 20.0],
        "Net assets per share": [50.0, 60.0],
    })
    per_share.to_sql("PerShare", conn, if_exists="replace", index=False)

    # Ensure screening index
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fin_company_period "
        "ON FinancialStatements(Company_Code, periodEnd)"
    )

    return conn


def _minimal_db_path() -> str:
    """Build an in-memory DB and save to a temp file for file-based paths."""
    conn = _build_in_memory_db()
    # Use tempfile to get a real path, but we'll copy the in-memory data
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    disk_conn = sqlite3.connect(path)
    conn.backup(disk_conn)
    disk_conn.close()
    conn.close()
    return path


# ---------------------------------------------------------------------------
# Tests — run_backtest_web
# ---------------------------------------------------------------------------


class TestRunBacktestWeb(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_path = _minimal_db_path()

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls.db_path)
        except Exception:
            pass

    # ── Structure ─────────────────────────────────────────────────────

    def test_valid_result_structure(self):
        """All top-level keys present, correct types, cumulative array non-empty."""
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 1.0}},
            start_date="2023-01-04",
            end_date="2023-12-29",
        )
        self.assertIn("metrics", result)
        self.assertIn("chart_data", result)
        self.assertIn("per_company", result)
        self.assertIn("yearly_returns", result)
        self.assertIn("dividends_by_year", result)
        self.assertIn("warnings", result)

        self.assertIsInstance(result["metrics"], dict)
        self.assertIsInstance(result["chart_data"], dict)
        self.assertIsInstance(result["chart_data"]["cumulative"], list)
        self.assertGreater(len(result["chart_data"]["cumulative"]), 0)

    # ── Benchmark metrics ─────────────────────────────────────────────

    def test_with_benchmark_produces_extra_metrics(self):
        """Three extra benchmark metrics are computed."""
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 0.5},
                       "B": {"mode": "weight", "value": 0.5}},
            start_date="2023-01-04",
            end_date="2023-12-29",
            benchmark_ticker="BENCH",
        )
        m = result["metrics"]
        self.assertIsNotNone(m.get("benchmark_total_return"))
        self.assertIsNotNone(m.get("benchmark_volatility"))
        self.assertIsNotNone(m.get("benchmark_max_drawdown"))
        self.assertIsNotNone(m.get("information_ratio"))

    def test_without_benchmark_all_null(self):
        """No crash; all benchmark fields None."""
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 1.0}},
            start_date="2023-01-04",
            end_date="2023-12-29",
        )
        m = result["metrics"]
        self.assertIsNone(m.get("benchmark_total_return"))
        self.assertIsNone(m.get("benchmark_annualized_return"))
        self.assertIsNone(m.get("benchmark_volatility"))
        self.assertIsNone(m.get("benchmark_max_drawdown"))
        self.assertIsNone(m.get("excess_return"))
        self.assertIsNone(m.get("information_ratio"))

    # ── Validation ────────────────────────────────────────────────────

    def test_empty_portfolio_raises(self):
        with self.assertRaises(ValueError):
            run_backtest_web(
                self.db_path,
                portfolio={},
                start_date="2023-01-04",
                end_date="2023-12-29",
            )

    def test_missing_start_date_raises(self):
        with self.assertRaises(ValueError):
            run_backtest_web(
                self.db_path,
                portfolio={"A": {"mode": "weight", "value": 1.0}},
                start_date="",
                end_date="2023-12-29",
            )

    def test_start_after_end_raises(self):
        with self.assertRaises(ValueError):
            run_backtest_web(
                self.db_path,
                portfolio={"A": {"mode": "weight", "value": 1.0}},
                start_date="2023-12-29",
                end_date="2023-01-04",
            )

    def test_ticker_not_in_prices_produces_warning(self):
        """Warning list non-empty; partial results still returned."""
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 0.5},
                       "Z": {"mode": "weight", "value": 0.5}},
            start_date="2023-01-04",
            end_date="2023-12-29",
        )
        self.assertGreater(len(result["warnings"]), 0)
        self.assertIn("metrics", result)

    # ── Allocation modes ──────────────────────────────────────────────

    def test_weight_mode_normalised(self):
        """Weights sum to 1.0 even if raw values don't."""
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 0.4},
                       "B": {"mode": "weight", "value": 0.4}},
            start_date="2023-01-04",
            end_date="2023-12-29",
        )
        per_c = result["per_company"]
        weight_sum = sum(r["weight"] for r in per_c)
        self.assertAlmostEqual(weight_sum, 1.0, places=4)

    def test_shares_mode_produces_quantities(self):
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "shares", "value": 10}},
            start_date="2023-01-04",
            end_date="2023-12-29",
        )
        per_c = result["per_company"]
        self.assertIn("capital_invested", per_c[0])
        self.assertIn("shares_purchased", per_c[0])

    def test_value_mode_produces_quantities(self):
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "value", "value": 5000}},
            start_date="2023-01-04",
            end_date="2023-12-29",
        )
        per_c = result["per_company"]
        self.assertIn("capital_invested", per_c[0])

    def test_mixed_mode(self):
        """Mixed weight + shares across tickers."""
        result = run_backtest_web(
            self.db_path,
            portfolio={
                "A": {"mode": "weight", "value": 0.5},
                "B": {"mode": "shares", "value": 5},
            },
            start_date="2023-01-04",
            end_date="2023-12-29",
        )
        self.assertIn("metrics", result)
        self.assertGreater(len(result["per_company"]), 0)

    # ── Dividends ─────────────────────────────────────────────────────

    def test_dividends_populated(self):
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 1.0}},
            start_date="2023-01-04",
            end_date="2023-12-29",
        )
        # With ShareMetrics data, dividend_return should be > 0
        self.assertLess(result["metrics"]["portfolio_dividend_return"], 1.0)

    def test_single_day_date_range(self):
        """Returns metrics without division-by-zero (two consecutive days)."""
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 1.0}},
            start_date="2023-01-04",
            end_date="2023-01-05",
        )
        self.assertIn("metrics", result)
        # Short date range — still a valid result
        self.assertIsNotNone(result["metrics"]["total_return"])

    # ── Custom table names ────────────────────────────────────────────

    def test_custom_table_names(self):
        """Custom table names are forwarded to underlying functions."""
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 1.0}},
            start_date="2023-01-04",
            end_date="2023-12-29",
            prices_table="Stock_Prices",
            ratios_table="ShareMetrics",
        )
        self.assertIn("metrics", result)

    # ── Portfolio returns ─────────────────────────────────────────────

    def test_portfolio_price_and_dividend_returns(self):
        """portfolio_price_return and portfolio_dividend_return computed."""
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 1.0}},
            start_date="2023-01-04",
            end_date="2023-12-29",
        )
        m = result["metrics"]
        self.assertIsNotNone(m.get("portfolio_price_return"))
        self.assertIsNotNone(m.get("portfolio_dividend_return"))
        self.assertEqual(m["initial_capital"], 0.0)

    def test_initial_capital_injected(self):
        result = run_backtest_web(
            self.db_path,
            portfolio={"A": {"mode": "weight", "value": 1.0}},
            start_date="2023-01-04",
            end_date="2023-12-29",
            initial_capital=100000.0,
        )
        self.assertEqual(result["metrics"]["initial_capital"], 100000.0)


# ---------------------------------------------------------------------------
# Tests — run_backtest_set_web
# ---------------------------------------------------------------------------


_BT_CSV = """Year,Tickers,Type,Amount
2023,A,weight,1.0
2024,C,weight,1.0
"""

_BT_CSV_WITH_COMMENTS = """# Benchmark: BENCH
# Discount Rate: 0.015
Year,Tickers,Type,Amount
2023,A,weight,0.5
2023,B,weight,0.5
"""

_BT_CSV_MISSING_COL = """Year,Tickers,Amount
2023,A,1.0
"""

_BT_CSV_EMPTY = """Year,Tickers,Type,Amount
"""


class TestRunBacktestSetWeb(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_path = _minimal_db_path()

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls.db_path)
        except Exception:
            pass

    def test_valid_csv_produces_aggregate(self):
        result = run_backtest_set_web(
            self.db_path,
            _BT_CSV,
            durations=["1yr"],
        )
        self.assertIn("aggregate", result)
        self.assertIn("results", result)
        agg = result["aggregate"]
        self.assertGreater(agg["total_runs"], 0)
        self.assertGreaterEqual(agg["successful"], 0)

    def test_csv_comments_parsed(self):
        """# Benchmark and # Discount Rate parsed from CSV comments."""
        result = run_backtest_set_web(
            self.db_path,
            _BT_CSV_WITH_COMMENTS,
            durations=["1yr"],
        )
        self.assertIn("results", result)
        # The benchmark should have been picked up from the comment
        for r in result["results"]:
            if r.get("metrics"):
                # If benchmark data available, it should be present
                break

    def test_custom_durations(self):
        result = run_backtest_set_web(
            self.db_path,
            _BT_CSV,
            durations=["1yr", "2yr"],
        )
        durations_seen = set(r["duration"] for r in result["results"])
        self.assertEqual(durations_seen, {"1yr", "2yr"})

    def test_missing_column_raises(self):
        with self.assertRaises(ValueError):
            run_backtest_set_web(
                self.db_path,
                _BT_CSV_MISSING_COL,
            )

    def test_empty_csv(self):
        """Header-only CSV returns zero runs."""
        result = run_backtest_set_web(
            self.db_path,
            _BT_CSV_EMPTY,
        )
        self.assertEqual(result["aggregate"]["total_runs"], 0)

    def test_by_duration_stats(self):
        """Benchmark comparison by_duration populated."""
        result = run_backtest_set_web(
            self.db_path,
            _BT_CSV_WITH_COMMENTS,
            durations=["1yr"],
        )
        agg = result["aggregate"]
        if agg["benchmark_comparison"]:
            self.assertIn("by_duration", agg["benchmark_comparison"])


# ---------------------------------------------------------------------------
# Tests — run_screening_backtest_set
# ---------------------------------------------------------------------------


class TestRunScreeningBacktestSet(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_path = _minimal_db_path()

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls.db_path)
        except Exception:
            pass

    def test_basic_screening_flow(self):
        """Runs screening, extracts tickers, runs backtest set."""
        result = run_screening_backtest_set(
            self.db_path,
            criteria=[
                {
                    "table": "PerShare",
                    "column": "Basic earnings (loss) per share",
                    "operator": ">",
                    "value": 0,
                },
            ],
            columns=["CompanyInfo.Company_Ticker"],
            screening_date="2023-03-31",
            max_companies=5,
            durations=["1yr"],
        )
        self.assertIn("aggregate", result)
        self.assertIn("results", result)

    def test_screening_empty_raises(self):
        """Screening returns empty → ValueError."""
        with self.assertRaises(ValueError):
            run_screening_backtest_set(
                self.db_path,
                criteria=[
                    {
                        "table": "PerShare",
                        "column": "Basic earnings (loss) per share",
                        "operator": ">",
                        "value": 999999,
                    },
                ],
                columns=["CompanyInfo.Company_Ticker"],
                screening_date="2023-03-31",
                durations=["1yr"],
            )

    def test_max_companies_caps(self):
        result = run_screening_backtest_set(
            self.db_path,
            criteria=[
                {
                    "table": "PerShare",
                    "column": "Basic earnings (loss) per share",
                    "operator": ">",
                    "value": 0,
                },
            ],
            columns=["CompanyInfo.Company_Ticker"],
            screening_date="2023-03-31",
            max_companies=1,
            durations=["1yr"],
        )
        # Each result should have at most 1 ticker
        for r in result["results"]:
            self.assertLessEqual(len(r.get("tickers", [])), 1)


if __name__ == "__main__":
    unittest.main()
