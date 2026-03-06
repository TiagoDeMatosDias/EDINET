"""
Tests for src/backtesting.py

Strategy
--------
* Data-retrieval functions (get_portfolio_prices, get_dividend_data) are tested
  with an in-memory SQLite database so real SQL paths are exercised without
  file-system side-effects.
* Calculation functions (calculate_portfolio_returns, calculate_benchmark_returns,
  calculate_metrics) are tested with hand-crafted DataFrames so the maths can
  be verified precisely.
* generate_report is tested by writing to a temporary file and checking the
  content.
* run_backtest is tested end-to-end with an in-memory database seeded with
  minimal data.
"""

import os
import shutil
import sys
import sqlite3
import tempfile
import unittest

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.backtesting import (
    get_portfolio_prices,
    get_dividend_data,
    calculate_portfolio_returns,
    calculate_benchmark_returns,
    calculate_metrics,
    generate_report,
    run_backtest,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_prices(conn: sqlite3.Connection, table: str = "stock_prices") -> None:
    """Insert sample price rows into an in-memory database."""
    df = pd.DataFrame(
        {
            "Date": [
                "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
                "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
                "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
            ],
            "Ticker": [
                "1001", "1001", "1001", "1001", "1001",
                "2002", "2002", "2002", "2002", "2002",
                "BENCH", "BENCH", "BENCH", "BENCH", "BENCH",
            ],
            "Currency": ["JPY"] * 15,
            "Price": [
                100, 102, 101, 105, 110,    # 1001: +10 %
                200, 198, 202, 206, 210,    # 2002: +5 %
                500, 505, 503, 510, 520,    # BENCH: +4 %
            ],
        }
    )
    df.to_sql(table, conn, if_exists="replace", index=False)


def _seed_company_info(conn: sqlite3.Connection, table: str = "companyInfo") -> None:
    df = pd.DataFrame(
        {
            "edinetCode": ["E00001", "E00002"],
            "Company_Ticker": ["1001", "2002"],
        }
    )
    df.to_sql(table, conn, if_exists="replace", index=False)


def _seed_ratios(conn: sqlite3.Connection, table: str = "Standard_Data_Ratios") -> None:
    df = pd.DataFrame(
        {
            "edinetCode": ["E00001", "E00002"],
            "periodEnd": ["2024-01-03", "2024-01-04"],
            "PerShare_Dividends": [5.0, 3.0],
        }
    )
    df.to_sql(table, conn, if_exists="replace", index=False)


# ---------------------------------------------------------------------------
# get_portfolio_prices
# ---------------------------------------------------------------------------

class TestGetPortfolioPrices(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        _seed_prices(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_returns_correct_tickers(self):
        df = get_portfolio_prices(
            ":memory:", "stock_prices", ["1001"], "2024-01-01", "2024-01-05",
            conn=self.conn,
        )
        self.assertEqual(list(df["Ticker"].unique()), ["1001"])

    def test_filters_by_date_range(self):
        df = get_portfolio_prices(
            ":memory:", "stock_prices", ["1001"], "2024-01-02", "2024-01-04",
            conn=self.conn,
        )
        self.assertEqual(len(df), 3)
        self.assertEqual(df["Date"].min(), pd.Timestamp("2024-01-02"))
        self.assertEqual(df["Date"].max(), pd.Timestamp("2024-01-04"))

    def test_multiple_tickers(self):
        df = get_portfolio_prices(
            ":memory:", "stock_prices", ["1001", "2002"],
            "2024-01-01", "2024-01-05", conn=self.conn,
        )
        self.assertEqual(set(df["Ticker"].unique()), {"1001", "2002"})

    def test_returns_empty_for_unknown_ticker(self):
        df = get_portfolio_prices(
            ":memory:", "stock_prices", ["UNKNOWN"],
            "2024-01-01", "2024-01-05", conn=self.conn,
        )
        self.assertTrue(df.empty)

    def test_dates_are_datetime(self):
        df = get_portfolio_prices(
            ":memory:", "stock_prices", ["1001"], "2024-01-01", "2024-01-05",
            conn=self.conn,
        )
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df["Date"]))


# ---------------------------------------------------------------------------
# get_dividend_data
# ---------------------------------------------------------------------------

class TestGetDividendData(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        _seed_company_info(self.conn)
        _seed_ratios(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_returns_dividends_for_tickers(self):
        df = get_dividend_data(
            ":memory:", "Standard_Data_Ratios", "companyInfo",
            ["1001"], "2024-01-01", "2024-01-05", conn=self.conn,
        )
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df["PerShare_Dividends"].iloc[0], 5.0)

    def test_returns_both_tickers(self):
        df = get_dividend_data(
            ":memory:", "Standard_Data_Ratios", "companyInfo",
            ["1001", "2002"], "2024-01-01", "2024-01-05", conn=self.conn,
        )
        self.assertEqual(len(df), 2)

    def test_returns_empty_outside_date_range(self):
        df = get_dividend_data(
            ":memory:", "Standard_Data_Ratios", "companyInfo",
            ["1001"], "2025-01-01", "2025-12-31", conn=self.conn,
        )
        self.assertTrue(df.empty)


# ---------------------------------------------------------------------------
# calculate_portfolio_returns
# ---------------------------------------------------------------------------

class TestCalculatePortfolioReturns(unittest.TestCase):

    def _make_prices_df(self):
        return pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-01", "2024-01-02", "2024-01-03",
                "2024-01-01", "2024-01-02", "2024-01-03",
            ]),
            "Ticker": ["A", "A", "A", "B", "B", "B"],
            "Price": [100.0, 110.0, 121.0, 200.0, 210.0, 220.0],
        })

    def test_equally_weighted_portfolio(self):
        prices = self._make_prices_df()
        result = calculate_portfolio_returns(prices, {"A": 0.5, "B": 0.5})
        # Day 2: A +10%, B +5%  → weighted 7.5%
        # Day 3: A +10%, B ~4.76% → weighted ~7.38%
        self.assertEqual(len(result), 2)
        self.assertAlmostEqual(result["portfolio_return"].iloc[0], 0.075, places=4)

    def test_single_ticker_full_weight(self):
        prices = self._make_prices_df()
        result = calculate_portfolio_returns(prices, {"A": 1.0})
        # Day 2: +10%, Day 3: +10%
        self.assertAlmostEqual(result["portfolio_return"].iloc[0], 0.10, places=4)
        self.assertAlmostEqual(result["portfolio_return"].iloc[1], 0.10, places=4)

    def test_cumulative_return(self):
        prices = self._make_prices_df()
        result = calculate_portfolio_returns(prices, {"A": 1.0})
        # After 2 days of +10%: (1.1)(1.1) = 1.21
        self.assertAlmostEqual(result["cumulative_return"].iloc[-1], 1.21, places=4)

    def test_with_dividends(self):
        prices = self._make_prices_df()
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-02"]),
            "PerShare_Dividends": [10.0],  # 10 JPY on price of 110
        })
        result = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        # Day 2: price return 10% + dividend yield 10/110 ≈ 9.09%
        expected_day2 = 0.10 + 10.0 / 110.0
        self.assertAlmostEqual(result["portfolio_return"].iloc[0], expected_day2, places=4)

    def test_missing_ticker_in_weights_is_ignored(self):
        prices = self._make_prices_df()
        # Ticker "C" not in data – weights should be renormalised to A only
        result = calculate_portfolio_returns(prices, {"A": 0.5, "C": 0.5})
        # All weight falls on A after renormalisation
        self.assertAlmostEqual(result["portfolio_return"].iloc[0], 0.10, places=4)

    def test_empty_prices_returns_empty(self):
        prices = pd.DataFrame({"Date": [], "Ticker": [], "Price": []})
        prices["Date"] = pd.to_datetime(prices["Date"])
        result = calculate_portfolio_returns(prices, {"A": 1.0})
        self.assertTrue(result.empty)


# ---------------------------------------------------------------------------
# calculate_benchmark_returns
# ---------------------------------------------------------------------------

class TestCalculateBenchmarkReturns(unittest.TestCase):

    def _make_prices(self):
        return pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "Ticker": ["BM", "BM", "BM"],
            "Price": [100.0, 105.0, 110.0],
        })

    def test_daily_returns(self):
        prices = self._make_prices()
        result = calculate_benchmark_returns(prices, "BM")
        # Day 2: +5%, Day 3: ~4.76%
        self.assertEqual(len(result), 2)
        self.assertAlmostEqual(result["benchmark_return"].iloc[0], 0.05, places=4)

    def test_cumulative_return(self):
        prices = self._make_prices()
        result = calculate_benchmark_returns(prices, "BM")
        # (1.05) * (110/105) ≈ 1.10
        self.assertAlmostEqual(result["cumulative_return"].iloc[-1], 1.10, places=4)


# ---------------------------------------------------------------------------
# calculate_metrics
# ---------------------------------------------------------------------------

class TestCalculateMetrics(unittest.TestCase):

    def _portfolio_df(self, returns):
        """Build a portfolio DataFrame from a list of daily returns."""
        cum = np.cumprod([1 + r for r in returns])
        return pd.DataFrame({
            "portfolio_return": returns,
            "cumulative_return": cum,
        }, index=pd.date_range("2024-01-02", periods=len(returns)))

    def _benchmark_df(self, returns):
        cum = np.cumprod([1 + r for r in returns])
        return pd.DataFrame({
            "benchmark_return": returns,
            "cumulative_return": cum,
        }, index=pd.date_range("2024-01-02", periods=len(returns)))

    def test_total_return(self):
        pf = self._portfolio_df([0.10, 0.05])
        metrics = calculate_metrics(pf, None, "2024-01-01", "2024-01-03")
        # (1.10)(1.05) - 1 = 0.155
        self.assertAlmostEqual(metrics["total_return"], 0.155, places=4)

    def test_excess_return(self):
        pf = self._portfolio_df([0.10])
        bm = self._benchmark_df([0.05])
        metrics = calculate_metrics(pf, bm, "2024-01-01", "2024-01-02")
        self.assertAlmostEqual(metrics["excess_return"], 0.05, places=4)

    def test_no_benchmark(self):
        pf = self._portfolio_df([0.05])
        metrics = calculate_metrics(pf, None, "2024-01-01", "2024-01-02")
        self.assertIsNone(metrics["benchmark_total_return"])
        self.assertIsNone(metrics["excess_return"])

    def test_max_drawdown(self):
        # Up 10%, then down 20% from peak
        pf = self._portfolio_df([0.10, -0.20])
        metrics = calculate_metrics(pf, None, "2024-01-01", "2024-01-03")
        # Peak at 1.10, trough at 1.10*0.80=0.88  → DD = (0.88-1.10)/1.10 = -0.2
        self.assertAlmostEqual(metrics["max_drawdown"], -0.20, places=4)

    def test_sharpe_ratio_positive(self):
        # Varying positive returns so std > 0
        returns = [0.01, 0.02, 0.005, 0.015] * 63  # 252 days
        pf = self._portfolio_df(returns)
        metrics = calculate_metrics(pf, None, "2024-01-01", "2025-01-01")
        self.assertGreater(metrics["sharpe_ratio"], 0)

    def test_zero_volatility(self):
        pf = self._portfolio_df([0.0, 0.0])
        metrics = calculate_metrics(pf, None, "2024-01-01", "2024-01-03")
        self.assertEqual(metrics["sharpe_ratio"], 0.0)

    def test_empty_portfolio(self):
        pf = pd.DataFrame({"portfolio_return": [], "cumulative_return": []})
        metrics = calculate_metrics(pf, None, "2024-01-01", "2024-01-02")
        self.assertEqual(metrics["total_return"], 0.0)


# ---------------------------------------------------------------------------
# generate_report
# ---------------------------------------------------------------------------

class TestGenerateReport(unittest.TestCase):

    def test_writes_file(self):
        metrics = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "total_return": 0.15,
            "annualized_return": 0.15,
            "volatility": 0.20,
            "sharpe_ratio": 0.75,
            "max_drawdown": -0.10,
            "benchmark_total_return": 0.10,
            "benchmark_annualized_return": 0.10,
            "excess_return": 0.05,
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "report.txt")
            text = generate_report(metrics, path)
            self.assertTrue(os.path.isfile(path))
            with open(path, encoding="utf-8") as f:
                content = f.read()
            self.assertIn("BACKTESTING REPORT", content)
            self.assertIn("+15.00%", content)

    def test_report_without_benchmark(self):
        metrics = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "total_return": 0.05,
            "annualized_return": 0.05,
            "volatility": 0.10,
            "sharpe_ratio": 0.50,
            "max_drawdown": -0.03,
            "benchmark_total_return": None,
            "benchmark_annualized_return": None,
            "excess_return": None,
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "report.txt")
            text = generate_report(metrics, path)
            self.assertNotIn("Benchmark", text)

    def test_creates_intermediate_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "sub", "dir", "report.txt")
            generate_report(
                {
                    "start_date": "2024-01-01", "end_date": "2024-12-31",
                    "total_return": 0.0, "annualized_return": 0.0,
                    "volatility": 0.0, "sharpe_ratio": 0.0,
                    "max_drawdown": 0.0,
                    "benchmark_total_return": None,
                    "benchmark_annualized_return": None,
                    "excess_return": None,
                },
                path,
            )
            self.assertTrue(os.path.isfile(path))


# ---------------------------------------------------------------------------
# run_backtest  (end-to-end with in-memory database)
# ---------------------------------------------------------------------------

class TestRunBacktest(unittest.TestCase):

    def setUp(self):
        # Create a temporary DB file (run_backtest opens its own connection)
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, "test.db")
        conn = sqlite3.connect(self.db_path)
        _seed_prices(conn)
        _seed_company_info(conn)
        _seed_ratios(conn)
        conn.commit()
        conn.close()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_basic_backtest(self):
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "portfolio": {"1001": 0.6, "2002": 0.4},
            "benchmark_ticker": "BENCH",
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        metrics = run_backtest(
            config, self.db_path,
            prices_table="stock_prices",
            ratios_table="Standard_Data_Ratios",
            company_table="companyInfo",
        )
        self.assertIn("total_return", metrics)
        self.assertIn("excess_return", metrics)
        self.assertTrue(os.path.isfile(config["output_file"]))

    def test_backtest_without_benchmark(self):
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "portfolio": {"1001": 1.0},
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        metrics = run_backtest(config, self.db_path)
        self.assertIsNone(metrics["benchmark_total_return"])
        # Single ticker 1001: 100 → 110 = +10% price return, plus dividends
        self.assertGreater(metrics["total_return"], 0.10)

    def test_backtest_no_data(self):
        config = {
            "start_date": "2030-01-01",
            "end_date": "2030-12-31",
            "portfolio": {"NONE": 1.0},
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        metrics = run_backtest(config, self.db_path)
        self.assertEqual(metrics["total_return"], 0.0)

    def test_report_file_content(self):
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "portfolio": {"1001": 0.5, "2002": 0.5},
            "benchmark_ticker": "BENCH",
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        run_backtest(config, self.db_path)
        with open(config["output_file"], encoding="utf-8") as f:
            content = f.read()
        self.assertIn("BACKTESTING REPORT", content)
        self.assertIn("Benchmark", content)
        self.assertIn("Excess Return", content)

    def test_empty_portfolio_raises(self):
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "portfolio": {},
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        with self.assertRaises(ValueError) as ctx:
            run_backtest(config, self.db_path)
        self.assertIn("empty", str(ctx.exception).lower())

    def test_missing_dates_raises(self):
        config = {
            "portfolio": {"1001": 1.0},
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        with self.assertRaises(ValueError) as ctx:
            run_backtest(config, self.db_path)
        self.assertIn("start_date", str(ctx.exception))

    def test_weights_not_summing_to_one_raises(self):
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "portfolio": {"1001": 0.3, "2002": 0.3},
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        with self.assertRaises(ValueError) as ctx:
            run_backtest(config, self.db_path)
        self.assertIn("sum to 1.0", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
