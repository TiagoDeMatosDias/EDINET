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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.orchestrator.common.backtesting import (
    get_portfolio_prices,
    get_dividend_data,
    build_daily_portfolio_tracker,
    calculate_portfolio_returns,
    calculate_benchmark_returns,
    calculate_return_decomposition,
    calculate_per_company_returns,
    calculate_yearly_returns,
    calculate_dividends_by_company_year,
    calculate_metrics,
    generate_report,
    generate_backtest_charts,
    run_backtest,
    resolve_portfolio_allocations,
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
            "Company_Code": ["E00001", "E00002"],
            "Company_Ticker": ["1001", "2002"],
        }
    )
    df.to_sql(table, conn, if_exists="replace", index=False)


def _seed_financial_statements(
    conn: sqlite3.Connection,
    table: str = "FinancialStatements",
) -> None:
    df = pd.DataFrame(
        {
            "docID": ["D1", "D2"],
            "Company_Code": ["E00001", "E00002"],
            "periodEnd": ["2024-01-03", "2024-01-04"],
        }
    )
    df.to_sql(table, conn, if_exists="replace", index=False)


def _seed_share_metrics(
    conn: sqlite3.Connection,
    table: str = "ShareMetrics",
) -> None:
    df = pd.DataFrame(
        {
            "docID": ["D1", "D2"],
            "Dividend paid per share": [5.0, 3.0],
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
        _seed_financial_statements(self.conn)
        _seed_share_metrics(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_returns_dividends_for_tickers(self):
        df = get_dividend_data(
            ":memory:", "ShareMetrics", "companyInfo",
            ["1001"], "2024-01-01", "2024-01-05", conn=self.conn,
        )
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df["PerShare_Dividends"].iloc[0], 5.0)

    def test_returns_both_tickers(self):
        df = get_dividend_data(
            ":memory:", "ShareMetrics", "companyInfo",
            ["1001", "2002"], "2024-01-01", "2024-01-05", conn=self.conn,
        )
        self.assertEqual(len(df), 2)

    def test_returns_empty_outside_date_range(self):
        df = get_dividend_data(
            ":memory:", "ShareMetrics", "companyInfo",
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
        # Day 2: A +10%, B +5%  â†’ weighted 7.5%
        # Day 3: A +10%, B ~4.76% â†’ weighted ~7.38%
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
            "PerShare_Dividends": [10.0],  # 10 JPY on initial price of 100
        })
        result = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        # Day 2: price return 10% + dividend yield 10/100 = 10%
        expected_day2 = 0.10 + 10.0 / 100.0
        self.assertAlmostEqual(result["portfolio_return"].iloc[0], expected_day2, places=4)

    def test_missing_ticker_in_weights_is_ignored(self):
        prices = self._make_prices_df()
        # Ticker "C" not in data â€“ weights should be renormalised to A only
        result = calculate_portfolio_returns(prices, {"A": 0.5, "C": 0.5})
        # All weight falls on A after renormalisation
        self.assertAlmostEqual(result["portfolio_return"].iloc[0], 0.10, places=4)

    def test_empty_prices_returns_empty(self):
        prices = pd.DataFrame({"Date": [], "Ticker": [], "Price": []})
        prices["Date"] = pd.to_datetime(prices["Date"])
        result = calculate_portfolio_returns(prices, {"A": 1.0})
        self.assertTrue(result.empty)

    # â”€â”€ Shares-based path (initial_capital > 0) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def test_shares_based_matches_normalized_no_dividends(self):
        """Shares-based and normalized paths produce same results."""
        prices = self._make_prices_df()
        norm = calculate_portfolio_returns(prices, {"A": 0.5, "B": 0.5})
        shares = calculate_portfolio_returns(
            prices, {"A": 0.5, "B": 0.5},
            initial_capital=1_000_000,
        )
        # Cumulative returns should match
        self.assertAlmostEqual(
            norm["cumulative_return"].iloc[-1],
            shares["cumulative_return"].iloc[-1],
            places=6,
        )

    def test_shares_based_with_dividends(self):
        """Shares-based path correctly includes dividends."""
        prices = self._make_prices_df()
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-02"]),
            "PerShare_Dividends": [10.0],
        })
        # Shares-based: 500k in A at 100 = 5000 shares; 500k in B at 200 = 2500 shares
        result = calculate_portfolio_returns(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        # A: 5000 shares Ã— (121-100) = 105,000 cap gain + 5000Ã—10 = 50,000 div
        # B: 2500 shares Ã— (220-200) = 50,000 cap gain
        # End value: A=605k, B=550k, +50k div cash = 1,205,000
        # Return: 205,000 / 1,000,000 = 20.5%
        expected_return = 0.205
        self.assertAlmostEqual(
            result["cumulative_return"].iloc[-1] - 1,
            expected_return,
            places=4,
        )

    def test_shares_based_dividends_increase_cumulative_return(self):
        """With dividends, cumulative return > price-only cumulative return."""
        prices = self._make_prices_df()
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-02"]),
            "PerShare_Dividends": [10.0],
        })
        with_div = calculate_portfolio_returns(
            prices, {"A": 1.0}, dividends,
            initial_capital=500_000,
        )
        without_div = calculate_portfolio_returns(
            prices, {"A": 1.0},
            initial_capital=500_000,
        )
        self.assertGreater(
            with_div["cumulative_return"].iloc[-1],
            without_div["cumulative_return"].iloc[-1],
        )

    def test_shares_based_consistency_with_per_company(self):
        """Shares-based portfolio return = sum(per_company weighted_total)."""
        prices = self._make_prices_df()
        dividends = pd.DataFrame({
            "Ticker": ["A", "B"],
            "periodEnd": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "PerShare_Dividends": [5.0, 3.0],
        })
        capital = 1_000_000.0
        pf = calculate_portfolio_returns(
            prices, {"A": 0.6, "B": 0.4}, dividends,
            initial_capital=capital,
        )
        per_co = calculate_per_company_returns(
            prices, {"A": 0.6, "B": 0.4}, dividends,
            initial_capital=capital,
        )
        pf_total = pf["cumulative_return"].iloc[-1] - 1
        per_co_sum = per_co["weighted_total"].sum()
        self.assertAlmostEqual(pf_total, per_co_sum, places=6,
            msg="Shares-based portfolio return must equal sum(weighted_total)")

    def test_shares_based_single_ticker(self):
        """Shares-based: single ticker, verify share count and returns."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "Ticker": ["X", "X", "X"],
            "Price": [200.0, 210.0, 220.0],
        })
        # 1M capital, all in X:
        # shares = 1,000,000 / 200 = 5,000
        # end market value = 5,000 Ã— 220 = 1,100,000
        # return = 100,000 / 1,000,000 = 10%
        result = calculate_portfolio_returns(
            prices, {"X": 1.0},
            initial_capital=1_000_000,
        )
        self.assertAlmostEqual(
            result["cumulative_return"].iloc[-1] - 1, 0.10, places=6,
        )


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
        # (1.05) * (110/105) â‰ˆ 1.10
        self.assertAlmostEqual(result["cumulative_return"].iloc[-1], 1.10, places=4)

    def test_price_return_columns_present(self):
        prices = self._make_prices()
        result = calculate_benchmark_returns(prices, "BM")
        for col in ("price_return", "cum_price_return",
                     "dividend_return", "cum_dividend_return"):
            self.assertIn(col, result.columns)

    def test_no_dividends_means_price_equals_total(self):
        prices = self._make_prices()
        result = calculate_benchmark_returns(prices, "BM")
        # Without dividends, total and price-only should be identical
        self.assertAlmostEqual(
            result["cumulative_return"].iloc[-1],
            result["cum_price_return"].iloc[-1],
            places=6,
        )
        self.assertAlmostEqual(
            result["cum_dividend_return"].iloc[-1], 1.0, places=6,
        )

    def test_with_dividends(self):
        prices = self._make_prices()
        dividends = pd.DataFrame({
            "Ticker": ["BM"],
            "periodEnd": pd.to_datetime(["2024-01-02"]),
            "PerShare_Dividends": [5.0],
        })
        result = calculate_benchmark_returns(prices, "BM", dividends)
        # Day 2: price return 5% + dividend 5/100 (prev day price) = 5%
        expected_total = 0.05 + 5.0 / 100.0
        self.assertAlmostEqual(
            result["benchmark_return"].iloc[0], expected_total, places=4
        )
        # Dividend cumulative should be > 1
        self.assertGreater(result["cum_dividend_return"].iloc[0], 1.0)


# ---------------------------------------------------------------------------
# calculate_return_decomposition
# ---------------------------------------------------------------------------

class TestCalculateReturnDecomposition(unittest.TestCase):

    def _make_prices_df(self):
        return pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-01", "2024-01-02", "2024-01-03",
                "2024-01-01", "2024-01-02", "2024-01-03",
            ]),
            "Ticker": ["A", "A", "A", "B", "B", "B"],
            "Price": [100.0, 110.0, 121.0, 200.0, 210.0, 220.0],
        })

    def test_returns_three_keys(self):
        prices = self._make_prices_df()
        result = calculate_return_decomposition(prices, {"A": 0.5, "B": 0.5})
        self.assertIn("total", result)
        self.assertIn("price_only", result)
        self.assertIn("dividend_only", result)

    def test_no_dividends_means_dividend_component_neutral(self):
        prices = self._make_prices_df()
        result = calculate_return_decomposition(prices, {"A": 1.0})
        # Without dividends, dividend component should be ~0
        div = result["dividend_only"]
        if not div.empty:
            self.assertAlmostEqual(
                div["cumulative_return"].iloc[-1], 1.0, places=6
            )

    def test_with_dividends_total_exceeds_price_only(self):
        prices = self._make_prices_df()
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-02"]),
            "PerShare_Dividends": [10.0],
        })
        result = calculate_return_decomposition(
            prices, {"A": 1.0}, dividends
        )
        total_ret = result["total"]["cumulative_return"].iloc[-1]
        price_ret = result["price_only"]["cumulative_return"].iloc[-1]
        self.assertGreater(total_ret, price_ret)

    def test_total_matches_calculate_portfolio_returns(self):
        prices = self._make_prices_df()
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-02"]),
            "PerShare_Dividends": [5.0],
        })
        decomp = calculate_return_decomposition(
            prices, {"A": 0.5, "B": 0.5}, dividends
        )
        direct = calculate_portfolio_returns(
            prices, {"A": 0.5, "B": 0.5}, dividends
        )
        self.assertAlmostEqual(
            decomp["total"]["cumulative_return"].iloc[-1],
            direct["cumulative_return"].iloc[-1],
            places=6,
        )


# ---------------------------------------------------------------------------
# calculate_per_company_returns
# ---------------------------------------------------------------------------

class TestCalculatePerCompanyReturns(unittest.TestCase):

    def _make_prices_df(self):
        return pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-01", "2024-01-02", "2024-01-03",
                "2024-01-01", "2024-01-02", "2024-01-03",
            ]),
            "Ticker": ["A", "A", "A", "B", "B", "B"],
            "Price": [100.0, 110.0, 120.0, 200.0, 210.0, 220.0],
        })

    def test_returns_all_tickers(self):
        prices = self._make_prices_df()
        result = calculate_per_company_returns(
            prices, {"A": 0.6, "B": 0.4}
        )
        self.assertEqual(set(result["Ticker"]), {"A", "B"})

    def test_price_return_correct(self):
        prices = self._make_prices_df()
        result = calculate_per_company_returns(prices, {"A": 1.0})
        row = result[result["Ticker"] == "A"].iloc[0]
        # A: 100 â†’ 120 = +20%
        self.assertAlmostEqual(row["price_return"], 0.20, places=4)

    def test_dividend_return(self):
        prices = self._make_prices_df()
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-02"]),
            "PerShare_Dividends": [11.0],  # 11 / 100 (start_price) = 11%
        })
        result = calculate_per_company_returns(
            prices, {"A": 1.0}, dividends
        )
        row = result[result["Ticker"] == "A"].iloc[0]
        self.assertAlmostEqual(row["dividend_return"], 0.11, places=4)

    def test_weighted_contribution(self):
        prices = self._make_prices_df()
        result = calculate_per_company_returns(
            prices, {"A": 0.6, "B": 0.4}
        )
        a_row = result[result["Ticker"] == "A"].iloc[0]
        # A: price_return=20%, weight=60% â†’ weighted_price = 12%
        self.assertAlmostEqual(a_row["weighted_price"], 0.12, places=4)

    def test_columns_present(self):
        prices = self._make_prices_df()
        result = calculate_per_company_returns(prices, {"A": 1.0})
        expected_cols = {
            "Ticker", "start_price", "end_price", "price_return",
            "dividend_return", "total_return", "weight",
            "weighted_price", "weighted_dividend", "weighted_total",
        }
        self.assertTrue(expected_cols.issubset(set(result.columns)))

    def test_initial_capital_columns(self):
        prices = self._make_prices_df()
        result = calculate_per_company_returns(
            prices, {"A": 0.6, "B": 0.4}, initial_capital=1_000_000
        )
        for col in ("capital_invested", "shares_purchased", "dividends_received",
                     "market_value"):
            self.assertIn(col, result.columns)

    def test_shares_purchased(self):
        prices = self._make_prices_df()
        result = calculate_per_company_returns(
            prices, {"A": 0.6, "B": 0.4}, initial_capital=1_000_000
        )
        a_row = result[result["Ticker"] == "A"].iloc[0]
        # A: 60% of 1M = 600,000 / 100 = 6,000 shares
        self.assertAlmostEqual(a_row["capital_invested"], 600_000, places=0)
        self.assertAlmostEqual(a_row["shares_purchased"], 6_000, places=0)
        b_row = result[result["Ticker"] == "B"].iloc[0]
        # B: 40% of 1M = 400,000 / 200 = 2,000 shares
        self.assertAlmostEqual(b_row["capital_invested"], 400_000, places=0)
        self.assertAlmostEqual(b_row["shares_purchased"], 2_000, places=0)

        # Market value = shares Ã— end_price
        # A: 6,000 Ã— 120 = 720,000
        self.assertAlmostEqual(a_row["market_value"], 720_000, places=0)
        # B: 2,000 Ã— 220 = 440,000
        self.assertAlmostEqual(b_row["market_value"], 440_000, places=0)

    def test_dividends_received(self):
        prices = self._make_prices_df()
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-02"]),
            "PerShare_Dividends": [10.0],
        })
        result = calculate_per_company_returns(
            prices, {"A": 1.0}, dividends, initial_capital=1_000_000
        )
        row = result[result["Ticker"] == "A"].iloc[0]
        # 1M / 100 = 10,000 shares, 10 JPY dividend each = 100,000
        self.assertAlmostEqual(row["dividends_received"], 100_000, places=0)

    def test_no_capital_means_no_extra_columns(self):
        prices = self._make_prices_df()
        result = calculate_per_company_returns(prices, {"A": 1.0})
        self.assertNotIn("capital_invested", result.columns)

    def test_empty_prices(self):
        prices = pd.DataFrame({"Date": [], "Ticker": [], "Price": []})
        prices["Date"] = pd.to_datetime(prices["Date"])
        result = calculate_per_company_returns(prices, {"A": 1.0})
        self.assertTrue(result.empty)

    def test_dividend_on_first_day_still_positive(self):
        """Dividends with periodEnd on the first price date must still
        produce a total cumulative return >= price-only return."""
        prices = self._make_prices_df()
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-01"]),  # day 0
            "PerShare_Dividends": [10.0],
        })
        with_div = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        without_div = calculate_portfolio_returns(prices, {"A": 1.0})
        self.assertGreater(
            with_div["cumulative_return"].iloc[-1],
            without_div["cumulative_return"].iloc[-1],
        )


# ---------------------------------------------------------------------------
# calculate_yearly_returns
# ---------------------------------------------------------------------------

class TestCalculateYearlyReturns(unittest.TestCase):

    def _make_decomposition(self, multi_year=False):
        if multi_year:
            dates = pd.to_datetime([
                "2024-06-01", "2024-06-02", "2024-12-31",
                "2025-01-02", "2025-06-01",
            ])
            total = pd.DataFrame({
                "daily_return": [0.01, 0.005, 0.02, 0.013, -0.003],
                "cumulative_return": [1.01, 1.015, 1.035, 1.048, 1.045],
            }, index=dates)
            price_only = pd.DataFrame({
                "daily_return": [0.01, 0.005, 0.015, 0.008, -0.003],
                "cumulative_return": [1.01, 1.015, 1.030, 1.038, 1.035],
            }, index=dates)
        else:
            dates = pd.date_range("2024-01-02", periods=4)
            total = pd.DataFrame({
                "daily_return": [0.02, 0.01, -0.005, 0.015],
                "cumulative_return": np.cumprod([1.02, 1.01, 0.995, 1.015]),
            }, index=dates)
            price_only = pd.DataFrame({
                "daily_return": [0.02, 0.01, -0.005, 0.01],
                "cumulative_return": np.cumprod([1.02, 1.01, 0.995, 1.01]),
            }, index=dates)

        # Derive dividend_only consistently from total - price_only
        div_contribution = (
            total["cumulative_return"].values
            - price_only["cumulative_return"].values
        )
        div_daily = np.diff(div_contribution, prepend=0.0)
        div_only = pd.DataFrame({
            "daily_return": div_daily,
            "cumulative_return": 1.0 + div_contribution,
        }, index=dates)
        return {"total": total, "price_only": price_only, "dividend_only": div_only}

    def test_returns_correct_columns(self):
        decomp = self._make_decomposition()
        result = calculate_yearly_returns(decomp)
        for col in ("Year", "Price Return", "Dividend Return", "Total Return"):
            self.assertIn(col, result.columns)

    def test_single_year(self):
        decomp = self._make_decomposition()
        result = calculate_yearly_returns(decomp)
        self.assertEqual(len(result), 1)
        self.assertEqual(int(result.iloc[0]["Year"]), 2024)

    def test_multi_year(self):
        decomp = self._make_decomposition(multi_year=True)
        result = calculate_yearly_returns(decomp)
        self.assertEqual(len(result), 2)
        years = list(result["Year"].astype(int))
        self.assertEqual(years, [2024, 2025])

    def test_total_equals_price_plus_dividend(self):
        decomp = self._make_decomposition(multi_year=True)
        result = calculate_yearly_returns(decomp)
        for _, row in result.iterrows():
            self.assertAlmostEqual(
                row["Total Return"],
                row["Price Return"] + row["Dividend Return"],
                places=10,
            )

    def test_empty_decomposition(self):
        decomp = {
            "total": pd.DataFrame(columns=["daily_return", "cumulative_return"]),
            "price_only": pd.DataFrame(columns=["daily_return", "cumulative_return"]),
            "dividend_only": pd.DataFrame(columns=["daily_return", "cumulative_return"]),
        }
        result = calculate_yearly_returns(decomp)
        self.assertTrue(result.empty)

    def test_yearly_dividend_returns_non_negative(self):
        """With realistic (non-decreasing) dividend contribution, yearly
        dividend returns must never be negative."""
        decomp = self._make_decomposition(multi_year=True)
        result = calculate_yearly_returns(decomp)
        for _, row in result.iterrows():
            self.assertGreaterEqual(
                row["Dividend Return"], 0.0,
                f"Dividend return for {int(row['Year'])} is negative: "
                f"{row['Dividend Return']:.6f}",
            )


# ---------------------------------------------------------------------------
# calculate_dividends_by_company_year
# ---------------------------------------------------------------------------

class TestCalculateDividendsByCompanyYear(unittest.TestCase):

    def test_basic_pivot(self):
        dividends = pd.DataFrame({
            "Ticker": ["A", "B", "A"],
            "periodEnd": pd.to_datetime(["2024-03-01", "2024-06-01", "2025-03-01"]),
            "PerShare_Dividends": [5.0, 3.0, 6.0],
        })
        result = calculate_dividends_by_company_year(dividends)
        self.assertIn("Total", result.columns)
        self.assertEqual(len(result), 2)  # 2024 and 2025
        self.assertAlmostEqual(result.loc[2024, "A"], 5.0)
        self.assertAlmostEqual(result.loc[2024, "B"], 3.0)
        self.assertAlmostEqual(result.loc[2024, "Total"], 8.0)
        self.assertAlmostEqual(result.loc[2025, "A"], 6.0)
        self.assertAlmostEqual(result.loc[2025, "Total"], 6.0)

    def test_none_returns_empty(self):
        result = calculate_dividends_by_company_year(None)
        self.assertTrue(result.empty)

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame(columns=["Ticker", "periodEnd", "PerShare_Dividends"])
        df["periodEnd"] = pd.to_datetime(df["periodEnd"])
        result = calculate_dividends_by_company_year(df)
        self.assertTrue(result.empty)

    def test_single_company_single_year(self):
        dividends = pd.DataFrame({
            "Ticker": ["X"],
            "periodEnd": pd.to_datetime(["2024-06-15"]),
            "PerShare_Dividends": [10.0],
        })
        result = calculate_dividends_by_company_year(dividends)
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result.loc[2024, "X"], 10.0)
        self.assertAlmostEqual(result.loc[2024, "Total"], 10.0)

    def test_multiple_dividends_same_year_summed(self):
        dividends = pd.DataFrame({
            "Ticker": ["A", "A"],
            "periodEnd": pd.to_datetime(["2024-03-01", "2024-09-01"]),
            "PerShare_Dividends": [4.0, 6.0],
        })
        result = calculate_dividends_by_company_year(dividends)
        self.assertAlmostEqual(result.loc[2024, "A"], 10.0)
        self.assertAlmostEqual(result.loc[2024, "Total"], 10.0)

    def test_with_shares_purchased(self):
        """When shares_purchased is given, values should be cash amounts."""
        dividends = pd.DataFrame({
            "Ticker": ["A", "B"],
            "periodEnd": pd.to_datetime(["2024-03-01", "2024-06-01"]),
            "PerShare_Dividends": [5.0, 3.0],
        })
        shares = {"A": 100.0, "B": 200.0}  # 100 shares of A, 200 of B
        result = calculate_dividends_by_company_year(
            dividends, shares_purchased=shares
        )
        # A: 5.0 * 100 = 500, B: 3.0 * 200 = 600
        self.assertAlmostEqual(result.loc[2024, "A"], 500.0)
        self.assertAlmostEqual(result.loc[2024, "B"], 600.0)
        self.assertAlmostEqual(result.loc[2024, "Total"], 1100.0)

    def test_without_shares_shows_per_share(self):
        """Without shares_purchased, values remain per-share amounts."""
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-03-01"]),
            "PerShare_Dividends": [5.0],
        })
        result = calculate_dividends_by_company_year(dividends)
        self.assertAlmostEqual(result.loc[2024, "A"], 5.0)


# ---------------------------------------------------------------------------
# generate_backtest_charts
# ---------------------------------------------------------------------------

class TestGenerateBacktestCharts(unittest.TestCase):

    def _make_decomposition(self):
        dates = pd.date_range("2024-01-02", periods=4)
        total = pd.DataFrame({
            "daily_return": [0.02, 0.01, -0.005, 0.015],
            "cumulative_return": np.cumprod([1.02, 1.01, 0.995, 1.015]),
        }, index=dates)
        price_only = pd.DataFrame({
            "daily_return": [0.02, 0.01, -0.005, 0.01],
            "cumulative_return": np.cumprod([1.02, 1.01, 0.995, 1.01]),
        }, index=dates)
        div_only = pd.DataFrame({
            "daily_return": [0.0, 0.0, 0.0, 0.005],
            "cumulative_return": np.cumprod([1.0, 1.0, 1.0, 1.005]),
        }, index=dates)
        return {"total": total, "price_only": price_only, "dividend_only": div_only}

    def _make_per_company(self):
        return pd.DataFrame({
            "Ticker": ["A", "B"],
            "start_price": [100.0, 200.0],
            "end_price": [110.0, 210.0],
            "price_return": [0.10, 0.05],
            "dividend_return": [0.02, 0.01],
            "total_return": [0.12, 0.06],
            "weight": [0.6, 0.4],
            "weighted_price": [0.06, 0.02],
            "weighted_dividend": [0.012, 0.004],
            "weighted_total": [0.072, 0.024],
        })

    def test_creates_chart_files(self):
        decomp = self._make_decomposition()
        per_co = self._make_per_company()
        with tempfile.TemporaryDirectory() as tmp:
            files = generate_backtest_charts(
                decomp, None, per_co, tmp, "2024-01-01", "2024-01-05"
            )
            self.assertTrue(len(files) >= 3)
            for f in files:
                self.assertTrue(os.path.isfile(f))

    def test_creates_charts_with_benchmark(self):
        decomp = self._make_decomposition()
        dates = pd.date_range("2024-01-02", periods=4)
        bench = pd.DataFrame({
            "benchmark_return": [0.01, 0.005, -0.002, 0.008],
            "cumulative_return": np.cumprod([1.01, 1.005, 0.998, 1.008]),
            "price_return": [0.01, 0.005, -0.002, 0.008],
            "cum_price_return": np.cumprod([1.01, 1.005, 0.998, 1.008]),
            "dividend_return": [0.0, 0.0, 0.0, 0.0],
            "cum_dividend_return": [1.0, 1.0, 1.0, 1.0],
        }, index=dates)
        with tempfile.TemporaryDirectory() as tmp:
            files = generate_backtest_charts(
                decomp, bench, None, tmp, "2024-01-01", "2024-01-05"
            )
            # Should have cumulative + drawdown + decomposition (no per_company)
            self.assertTrue(len(files) >= 3)


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
            "price_return": returns,
            "cum_price_return": cum,
            "dividend_return": [0.0] * len(returns),
            "cum_dividend_return": [1.0] * len(returns),
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
        self.assertIsNone(metrics["benchmark_price_return"])
        self.assertIsNone(metrics["benchmark_dividend_return"])

    def test_max_drawdown(self):
        # Up 10%, then down 20% from peak
        pf = self._portfolio_df([0.10, -0.20])
        metrics = calculate_metrics(pf, None, "2024-01-01", "2024-01-03")
        # Peak at 1.10, trough at 1.10*0.80=0.88  â†’ DD = (0.88-1.10)/1.10 = -0.2
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
        _seed_financial_statements(conn)
        _seed_share_metrics(conn)
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
            ratios_table="ShareMetrics",
            company_table="companyInfo",
        )
        self.assertIn("total_return", metrics)
        self.assertIn("excess_return", metrics)
        self.assertTrue(os.path.isfile(config["output_file"]))

        # New decomposition fields
        self.assertIn("portfolio_price_return", metrics)
        self.assertIn("portfolio_dividend_return", metrics)
        self.assertIn("benchmark_price_return", metrics)
        self.assertIn("benchmark_dividend_return", metrics)
        self.assertIn("per_company", metrics)
        self.assertIsInstance(metrics["per_company"], list)
        self.assertTrue(len(metrics["per_company"]) > 0)

        # Chart files should have been created
        self.assertIn("chart_files", metrics)
        for f in metrics["chart_files"]:
            self.assertTrue(os.path.isfile(f))

    def test_backtest_without_benchmark(self):
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "portfolio": {"1001": 1.0},
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        metrics = run_backtest(config, self.db_path)
        self.assertIsNone(metrics["benchmark_total_return"])
        # Single ticker 1001: 100 â†’ 110 = +10% price return, plus dividends
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
        # New decomposition sections
        self.assertIn("Portfolio Return Decomposition", content)
        self.assertIn("Per-Company Breakdown", content)
        self.assertIn("1001", content)
        self.assertIn("2002", content)
        # Yearly returns and dividends tables
        self.assertIn("Yearly Returns", content)
        self.assertIn("Dividends Per Company Per Year", content)

    def test_backtest_decomposition_values(self):
        """Price + dividend return should sum close to total return."""
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "portfolio": {"1001": 1.0},
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        metrics = run_backtest(config, self.db_path)
        # Price return should be 10% (100 â†’ 110)
        self.assertAlmostEqual(
            metrics["portfolio_price_return"], 0.10, places=2
        )
        # Dividend return should be positive (5 JPY on 101 or similar)
        self.assertGreater(metrics["portfolio_dividend_return"], 0.0)
        # Per-company list should have 1 entry
        self.assertEqual(len(metrics["per_company"]), 1)
        self.assertEqual(metrics["per_company"][0]["Ticker"], "1001")

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

    def test_weights_not_summing_to_one_warns_and_normalises(self):
        """Weights that don't sum to 100% should still run (normalised)."""
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "portfolio": {"1001": 0.3, "2002": 0.3},
            "output_file": os.path.join(self.tmp_dir, "report.txt"),
        }
        # Should NOT raise â€” weights are auto-normalised
        metrics = run_backtest(config, self.db_path)
        self.assertIn("total_return", metrics)


# ---------------------------------------------------------------------------
# resolve_portfolio_allocations
# ---------------------------------------------------------------------------

class TestResolvePortfolioAllocations(unittest.TestCase):
    """Tests for the mixed-mode portfolio resolution logic."""

    def test_plain_float_backward_compat(self):
        """Legacy dict[str, float] format is treated as weight mode."""
        config = {"A": 0.6, "B": 0.4}
        weights, cap, warns = resolve_portfolio_allocations(config, {})
        self.assertAlmostEqual(weights["A"], 0.6)
        self.assertAlmostEqual(weights["B"], 0.4)
        self.assertEqual(cap, 0.0)  # no capital derived for pure weight
        self.assertEqual(warns, [])

    def test_new_weight_format(self):
        """New dict-of-dict weight entries work like plain floats."""
        config = {
            "A": {"mode": "weight", "value": 0.7},
            "B": {"mode": "weight", "value": 0.3},
        }
        weights, cap, warns = resolve_portfolio_allocations(config, {})
        self.assertAlmostEqual(weights["A"], 0.7)
        self.assertAlmostEqual(weights["B"], 0.3)

    def test_shares_mode(self):
        """Shares are converted to capital using start prices."""
        config = {
            "A": {"mode": "shares", "value": 100},
            "B": {"mode": "shares", "value": 200},
        }
        prices = {"A": 10.0, "B": 5.0}  # A=1000, B=1000
        weights, cap, warns = resolve_portfolio_allocations(config, prices)
        self.assertAlmostEqual(weights["A"], 0.5)
        self.assertAlmostEqual(weights["B"], 0.5)
        self.assertAlmostEqual(cap, 2000.0)

    def test_value_mode(self):
        """Fixed currency amounts are converted to weights."""
        config = {
            "A": {"mode": "value", "value": 30000},
            "B": {"mode": "value", "value": 10000},
        }
        weights, cap, warns = resolve_portfolio_allocations(config, {})
        self.assertAlmostEqual(weights["A"], 0.75)
        self.assertAlmostEqual(weights["B"], 0.25)
        self.assertAlmostEqual(cap, 40000.0)

    def test_mixed_weight_and_shares(self):
        """Weight + shares mix: implied capital derived correctly."""
        # A = 50% weight, B = 100 shares @ 1000 = 100,000
        # 50% of total = A, so total = 100,000 / 0.5 = 200,000
        # A = 100,000, B = 100,000 â†’ each 50%
        config = {
            "A": {"mode": "weight", "value": 0.5},
            "B": {"mode": "shares", "value": 100},
        }
        prices = {"B": 1000.0}
        weights, cap, warns = resolve_portfolio_allocations(config, prices)
        self.assertAlmostEqual(cap, 200000.0)
        self.assertAlmostEqual(weights["A"], 0.5)
        self.assertAlmostEqual(weights["B"], 0.5)

    def test_mixed_weight_shares_value(self):
        """All three modes combined."""
        # A = 50% weight, B = 100 shares @ 500, C = 25000 value
        # Fixed capital = 50,000 + 25,000 = 75,000
        # 50% means: 0.5 * total = A_capital
        # total = fixed / (1 - 0.5) = 75,000 / 0.5 = 150,000
        # A = 75,000, B = 50,000, C = 25,000
        config = {
            "A": {"mode": "weight", "value": 0.5},
            "B": {"mode": "shares", "value": 100},
            "C": {"mode": "value", "value": 25000},
        }
        prices = {"B": 500.0}
        weights, cap, warns = resolve_portfolio_allocations(config, prices)
        self.assertAlmostEqual(cap, 150000.0)
        self.assertAlmostEqual(weights["A"], 75000 / 150000)
        self.assertAlmostEqual(weights["B"], 50000 / 150000)
        self.assertAlmostEqual(weights["C"], 25000 / 150000)

    def test_explicit_initial_capital(self):
        """When initial_capital is given, weight-mode uses it as base."""
        config = {
            "A": {"mode": "weight", "value": 0.5},
            "B": {"mode": "value", "value": 100000},
        }
        # With 1M capital: A = 500k, B = 100k â†’ total allocated = 600k
        weights, cap, warns = resolve_portfolio_allocations(
            config, {}, initial_capital=1_000_000
        )
        self.assertAlmostEqual(weights["A"], 500000 / 600000, places=4)
        self.assertAlmostEqual(weights["B"], 100000 / 600000, places=4)
        # Warning about mismatch
        self.assertTrue(any("differs" in w for w in warns))

    def test_missing_price_for_shares_warns(self):
        """Shares mode without price data emits a warning and skips."""
        config = {
            "A": {"mode": "shares", "value": 100},
            "B": {"mode": "value", "value": 5000},
        }
        weights, cap, warns = resolve_portfolio_allocations(
            config, {}  # no prices!
        )
        self.assertNotIn("A", weights)
        self.assertIn("B", weights)
        self.assertTrue(any("No start-price" in w for w in warns))

    def test_run_backtest_with_shares_mode(self):
        """End-to-end backtest using shares allocation mode."""
        tmp_dir = tempfile.mkdtemp()
        db_path = os.path.join(tmp_dir, "test.db")
        conn = sqlite3.connect(db_path)
        _seed_prices(conn)
        _seed_company_info(conn)
        _seed_financial_statements(conn)
        _seed_share_metrics(conn)
        conn.commit()
        conn.close()
        try:
            config = {
                "start_date": "2024-01-01",
                "end_date": "2024-01-05",
                "portfolio": {
                    "1001": {"mode": "shares", "value": 50},
                    "2002": {"mode": "shares", "value": 25},
                },
                "output_file": os.path.join(tmp_dir, "report.txt"),
            }
            metrics = run_backtest(config, db_path)
            self.assertIn("total_return", metrics)
            self.assertGreater(metrics["total_return"], 0)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_run_backtest_with_mixed_modes(self):
        """End-to-end backtest with weight + shares + value."""
        tmp_dir = tempfile.mkdtemp()
        db_path = os.path.join(tmp_dir, "test.db")
        conn = sqlite3.connect(db_path)
        _seed_prices(conn)
        _seed_company_info(conn)
        _seed_financial_statements(conn)
        _seed_share_metrics(conn)
        conn.commit()
        conn.close()
        try:
            config = {
                "start_date": "2024-01-01",
                "end_date": "2024-01-05",
                "portfolio": {
                    "1001": {"mode": "weight", "value": 0.5},
                    "2002": {"mode": "value", "value": 10000},
                },
                "benchmark_ticker": "BENCH",
                "output_file": os.path.join(tmp_dir, "report.txt"),
            }
            metrics = run_backtest(config, db_path)
            self.assertIn("total_return", metrics)
            self.assertIn("per_company", metrics)
            self.assertEqual(len(metrics["per_company"]), 2)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Dividend edge-case tests (non-trading-day bug fix)
# ---------------------------------------------------------------------------

class TestDividendDateMatching(unittest.TestCase):
    """Verify dividends on non-trading days are NOT silently dropped."""

    def test_dividend_on_weekend_not_dropped(self):
        """Pay date falls on Saturday â€” should map to Monday."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-05",  # Friday
                "2024-01-08",  # Monday
                "2024-01-09",  # Tuesday
            ]),
            "Ticker": ["A", "A", "A"],
            "Price": [100.0, 102.0, 104.0],
        })
        # Dividend paid on Saturday Jan 6 (non-trading day)
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-06"]),  # Saturday
            "PerShare_Dividends": [5.0],
        })
        with_div = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        without_div = calculate_portfolio_returns(prices, {"A": 1.0})
        # With dividend should have higher cumulative return
        self.assertGreater(
            with_div["cumulative_return"].iloc[-1],
            without_div["cumulative_return"].iloc[-1],
        )
        # Dividend of 5 on initial price of 100 = 5% contribution
        # Total return = price return (4%) + dividend (5%) = 9%
        expected_cum = 1.04 + 0.05  # 1.09
        self.assertAlmostEqual(
            with_div["cumulative_return"].iloc[-1],
            expected_cum,
            places=4,
        )

    def test_dividend_on_sunday_mapped_to_monday(self):
        """Pay date on Sunday â€” should map to Monday."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-05",  # Friday
                "2024-01-08",  # Monday
                "2024-01-09",  # Tuesday
            ]),
            "Ticker": ["A", "A", "A"],
            "Price": [100.0, 103.0, 105.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-07"]),  # Sunday
            "PerShare_Dividends": [10.0],
        })
        result = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        # Price return: 105/100 - 1 = 5%
        # Dividend: 10/100 = 10%
        # Total cumulative = 1.05 + 0.10 = 1.15
        self.assertAlmostEqual(
            result["cumulative_return"].iloc[-1], 1.15, places=4,
        )

    def test_dividend_after_last_trading_day_skipped(self):
        """Pay date after the last trading day is correctly skipped."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-08", "2024-01-09", "2024-01-10",
            ]),
            "Ticker": ["A", "A", "A"],
            "Price": [100.0, 102.0, 104.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-15"]),  # After last trading day
            "PerShare_Dividends": [10.0],
        })
        with_div = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        without_div = calculate_portfolio_returns(prices, {"A": 1.0})
        # Should be identical â€” dividend skipped
        self.assertAlmostEqual(
            with_div["cumulative_return"].iloc[-1],
            without_div["cumulative_return"].iloc[-1],
            places=6,
        )

    def test_dividend_before_first_trading_day(self):
        """Pay date before first trading day â€” maps to first trading day."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-05", "2024-01-08", "2024-01-09",
            ]),
            "Ticker": ["A", "A", "A"],
            "Price": [100.0, 102.0, 104.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-01"]),  # Before trading starts
            "PerShare_Dividends": [5.0],
        })
        result = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        # Dividend should be applied from day 1 (2024-01-05)
        # Cumulative: (104/100) + 5/100 = 1.04 + 0.05 = 1.09
        self.assertAlmostEqual(
            result["cumulative_return"].iloc[-1], 1.09, places=4,
        )

    def test_dividend_on_exact_trading_day(self):
        """Pay date exactly on trading day â€” works as before."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-05", "2024-01-08", "2024-01-09",
            ]),
            "Ticker": ["A", "A", "A"],
            "Price": [100.0, 102.0, 104.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-08"]),  # Exact trading day
            "PerShare_Dividends": [5.0],
        })
        result = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        # Price return: 4%, Dividend: 5% starting from Jan 8
        # Cumulative: (104/100) + 5/100 = 1.09
        self.assertAlmostEqual(
            result["cumulative_return"].iloc[-1], 1.09, places=4,
        )

    def test_benchmark_dividend_on_weekend(self):
        """Same fix for calculate_benchmark_returns."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-05", "2024-01-08", "2024-01-09",
            ]),
            "Ticker": ["BM", "BM", "BM"],
            "Price": [500.0, 505.0, 510.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["BM"],
            "periodEnd": pd.to_datetime(["2024-01-06"]),  # Saturday
            "PerShare_Dividends": [10.0],
        })
        result = calculate_benchmark_returns(prices, "BM", dividends)
        # Total return > price-only return because dividend was captured
        self.assertGreater(
            result["cumulative_return"].iloc[-1],
            result["cum_price_return"].iloc[-1],
        )

    def test_multiple_dividends_mixed_trading_and_non_trading(self):
        """Mix of trading and non-trading pay dates, all should count."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-05", "2024-01-08", "2024-01-09", "2024-01-10",
            ]),
            "Ticker": ["A", "A", "A", "A"],
            "Price": [100.0, 102.0, 104.0, 106.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["A", "A"],
            "periodEnd": pd.to_datetime(["2024-01-06", "2024-01-08"]),
            "PerShare_Dividends": [3.0, 4.0],  # Sat + Mon = 7 total
        })
        result = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        # Price: 6%, Dividends: 7% â†’ total cumulative = 1.06 + 0.07 = 1.13
        self.assertAlmostEqual(
            result["cumulative_return"].iloc[-1], 1.13, places=4,
        )


# ---------------------------------------------------------------------------
# Portfolio-to-Per-Company consistency tests
# ---------------------------------------------------------------------------

class TestPortfolioPerCompanyConsistency(unittest.TestCase):
    """Verify per-company weighted totals match portfolio-level metrics."""

    def _make_data(self):
        """Create sample prices and dividends for 3 tickers over 5 days."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08",
                "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08",
                "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08",
            ]),
            "Ticker": ["A"] * 5 + ["B"] * 5 + ["C"] * 5,
            "Price": [
                100.0, 102.0, 101.0, 105.0, 110.0,  # A: +10%
                200.0, 198.0, 202.0, 206.0, 210.0,  # B: +5%
                50.0,  51.0,  52.0,  53.0,  54.0,   # C: +8%
            ],
        })
        dividends = pd.DataFrame({
            "Ticker": ["A", "B"],
            "periodEnd": pd.to_datetime(["2024-01-04", "2024-01-05"]),
            "PerShare_Dividends": [5.0, 3.0],
        })
        return prices, dividends

    def test_weighted_total_sums_to_portfolio_return_no_dividends(self):
        """Without dividends, sum(weighted_total) â‰ˆ portfolio total_return."""
        prices, _ = self._make_data()
        weights = {"A": 0.5, "B": 0.3, "C": 0.2}
        pf = calculate_portfolio_returns(prices, weights)
        per_co = calculate_per_company_returns(prices, weights)
        portfolio_total = pf["cumulative_return"].iloc[-1] - 1
        per_co_sum = per_co["weighted_total"].sum()
        self.assertAlmostEqual(portfolio_total, per_co_sum, places=6,
            msg="sum(weighted_total) must equal portfolio total_return")

    def test_weighted_total_sums_to_portfolio_return_with_dividends(self):
        """With dividends, sum(weighted_total) â‰ˆ portfolio total_return."""
        prices, dividends = self._make_data()
        weights = {"A": 0.5, "B": 0.3, "C": 0.2}
        pf = calculate_portfolio_returns(prices, weights, dividends)
        per_co = calculate_per_company_returns(prices, weights, dividends)
        portfolio_total = pf["cumulative_return"].iloc[-1] - 1
        per_co_sum = per_co["weighted_total"].sum()
        self.assertAlmostEqual(portfolio_total, per_co_sum, places=6,
            msg="sum(weighted_total) must equal portfolio total_return even with dividends")

    def test_weighted_price_sums_to_price_only_return(self):
        """sum(weighted_price) â‰ˆ portfolio price return (no dividends)."""
        prices, _ = self._make_data()
        weights = {"A": 0.5, "B": 0.3, "C": 0.2}
        pf_no_div = calculate_portfolio_returns(prices, weights)
        per_co = calculate_per_company_returns(prices, weights)
        price_total = pf_no_div["cumulative_return"].iloc[-1] - 1
        per_co_price_sum = per_co["weighted_price"].sum()
        self.assertAlmostEqual(price_total, per_co_price_sum, places=6)

    def test_weighted_dividend_matches_dividend_contribution(self):
        """sum(weighted_dividend) should match the total dividend contribution."""
        prices, dividends = self._make_data()
        weights = {"A": 0.5, "B": 0.3, "C": 0.2}
        pf_with = calculate_portfolio_returns(prices, weights, dividends)
        pf_without = calculate_portfolio_returns(prices, weights)
        per_co = calculate_per_company_returns(prices, weights, dividends)
        # Dividend contribution = total - price_only at portfolio level
        div_contribution = (
            (pf_with["cumulative_return"].iloc[-1] - 1)
            - (pf_without["cumulative_return"].iloc[-1] - 1)
        )
        per_co_div_sum = per_co["weighted_dividend"].sum()
        self.assertAlmostEqual(div_contribution, per_co_div_sum, places=6)

    def test_three_ticker_equal_weight_consistency(self):
        """Equal-weight 3 ticker portfolio: per-company sums match."""
        prices, dividends = self._make_data()
        weights = {"A": 1.0/3, "B": 1.0/3, "C": 1.0/3}
        pf = calculate_portfolio_returns(prices, weights, dividends)
        per_co = calculate_per_company_returns(prices, weights, dividends)
        pf_total = pf["cumulative_return"].iloc[-1] - 1
        per_co_total = per_co["weighted_total"].sum()
        self.assertAlmostEqual(pf_total, per_co_total, places=6)
        # Weights should sum to ~1
        self.assertAlmostEqual(per_co["weight"].sum(), 1.0, places=6)

    def test_single_ticker_consistency(self):
        """Single ticker: per-company should exactly match portfolio."""
        prices, dividends = self._make_data()
        pf = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        per_co = calculate_per_company_returns(prices, {"A": 1.0}, dividends)
        pf_total = pf["cumulative_return"].iloc[-1] - 1
        self.assertAlmostEqual(pf_total, per_co["weighted_total"].sum(), places=6)
        self.assertEqual(len(per_co), 1)


# ---------------------------------------------------------------------------
# Enhanced per-company returns tests
# ---------------------------------------------------------------------------

class TestEnhancedPerCompanyReturns(unittest.TestCase):
    """Verify new/improved per-company columns and calculations."""

    def test_all_columns_present_with_capital(self):
        """With initial_capital, all expected columns exist."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "Ticker": ["X", "X", "X"],
            "Price": [100.0, 105.0, 110.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["X"],
            "periodEnd": pd.to_datetime(["2024-01-03"]),
            "PerShare_Dividends": [3.0],
        })
        result = calculate_per_company_returns(
            prices, {"X": 1.0}, dividends, initial_capital=500_000,
        )
        expected = {
            "Ticker", "start_price", "end_price",
            "price_return", "dividend_return", "total_return",
            "weight", "weighted_price", "weighted_dividend", "weighted_total",
            "capital_invested", "shares_purchased",
            "dividends_received", "market_value",
        }
        self.assertTrue(expected.issubset(set(result.columns)))
        # Verify values
        row = result.iloc[0]
        self.assertAlmostEqual(row["capital_invested"], 500_000)
        self.assertAlmostEqual(row["shares_purchased"], 5000)  # 500k / 100
        self.assertAlmostEqual(row["dividends_received"], 15000)  # 5000 * 3
        self.assertAlmostEqual(row["market_value"], 550000)  # 5000 * 110

    def test_dividends_received_zero_when_no_dividends(self):
        """dividends_received should be 0 when no dividends exist."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "Ticker": ["X", "X"],
            "Price": [100.0, 110.0],
        })
        result = calculate_per_company_returns(
            prices, {"X": 1.0}, initial_capital=100_000,
        )
        self.assertAlmostEqual(result.iloc[0]["dividends_received"], 0.0)

    def test_multiple_dividends_summed_correctly(self):
        """Two dividends for same ticker: dividends_received sums both."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "Ticker": ["X", "X", "X"],
            "Price": [200.0, 205.0, 210.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["X", "X"],
            "periodEnd": pd.to_datetime(["2024-01-03", "2024-01-04"]),
            "PerShare_Dividends": [4.0, 6.0],
        })
        result = calculate_per_company_returns(
            prices, {"X": 1.0}, dividends, initial_capital=1_000_000,
        )
        row = result.iloc[0]
        # shares = 1M / 200 = 5000
        # dividends_received = 5000 * (4 + 6) = 50000
        self.assertAlmostEqual(row["shares_purchased"], 5000)
        self.assertAlmostEqual(row["dividends_received"], 50000)

    def test_weighted_columns_are_weight_times_return(self):
        """weighted_* = weight * *_return for each row."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-02", "2024-01-03",
                "2024-01-02", "2024-01-03",
            ]),
            "Ticker": ["A", "A", "B", "B"],
            "Price": [100.0, 120.0, 50.0, 55.0],
        })
        result = calculate_per_company_returns(
            prices, {"A": 0.7, "B": 0.3},
        )
        for _, row in result.iterrows():
            self.assertAlmostEqual(
                row["weighted_price"],
                row["price_return"] * row["weight"],
                places=10,
            )
            self.assertAlmostEqual(
                row["weighted_dividend"],
                row["dividend_return"] * row["weight"],
                places=10,
            )
            self.assertAlmostEqual(
                row["weighted_total"],
                row["total_return"] * row["weight"],
                places=10,
            )


# ---------------------------------------------------------------------------
# Dividends by year long-format tests
# ---------------------------------------------------------------------------

class TestDividendsByYearLongFormat(unittest.TestCase):
    """Tests for the long-format dividends table."""

    def test_basic_long_format(self):
        """Two tickers, two years â€” correct rows."""
        dividends = pd.DataFrame({
            "Ticker": ["A", "B", "A"],
            "periodEnd": pd.to_datetime(["2024-03-01", "2024-06-01", "2025-03-01"]),
            "PerShare_Dividends": [5.0, 3.0, 6.0],
        })
        from src.orchestrator.common.backtesting import (
            calculate_dividends_by_company_year_long,
        )
        result = calculate_dividends_by_company_year_long(dividends)
        self.assertIn("Year", result.columns)
        self.assertIn("Ticker", result.columns)
        self.assertIn("Amount", result.columns)
        self.assertIn("Total", result.columns)
        # 2024: A=5, B=3; 2025: A=6
        self.assertEqual(len(result), 3)
        totals = result.groupby("Year")["Total"].first()
        self.assertAlmostEqual(totals.loc[2024], 8.0)
        self.assertAlmostEqual(totals.loc[2025], 6.0)

    def test_with_shares(self):
        """Amounts should be multiplied by shares when provided."""
        dividends = pd.DataFrame({
            "Ticker": ["A", "B"],
            "periodEnd": pd.to_datetime(["2024-03-01", "2024-06-01"]),
            "PerShare_Dividends": [5.0, 3.0],
        })
        from src.orchestrator.common.backtesting import (
            calculate_dividends_by_company_year_long,
        )
        result = calculate_dividends_by_company_year_long(
            dividends, shares_purchased={"A": 100, "B": 200},
        )
        row_a = result[(result["Year"] == 2024) & (result["Ticker"] == "A")]
        row_b = result[(result["Year"] == 2024) & (result["Ticker"] == "B")]
        self.assertAlmostEqual(row_a["Amount"].iloc[0], 500.0)
        self.assertAlmostEqual(row_b["Amount"].iloc[0], 600.0)

    def test_empty_input(self):
        """Empty dividends => empty DataFrame with correct columns."""
        from src.orchestrator.common.backtesting import (
            calculate_dividends_by_company_year_long,
        )
        result = calculate_dividends_by_company_year_long(None)
        self.assertTrue(result.empty)
        for col in ("Year", "Ticker", "Amount", "Total"):
            self.assertIn(col, result.columns)


# ---------------------------------------------------------------------------
# Per-company per-year (new primary calculation engine)
# ---------------------------------------------------------------------------

class TestPerCompanyPerYear(unittest.TestCase):
    """Tests for the per-company-per-year primary calculation."""

    def _make_data(self):
        """Two tickers, two years."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-02", "2024-01-03", "2024-12-30",
                "2025-01-02", "2025-12-30",
                "2024-01-02", "2024-01-03", "2024-12-30",
                "2025-01-02", "2025-12-30",
            ]),
            "Ticker": ["A"] * 5 + ["B"] * 5,
            "Price": [
                100.0, 102.0, 110.0,  # A 2024
                112.0, 120.0,          # A 2025
                200.0, 198.0, 210.0,  # B 2024
                212.0, 220.0,          # B 2025
            ],
        })
        dividends = pd.DataFrame({
            "Ticker": ["A", "A", "B"],
            "periodEnd": pd.to_datetime(["2024-06-30", "2025-06-30", "2024-12-31"]),
            "PerShare_Dividends": [5.0, 6.0, 3.0],
        })
        return prices, dividends

    def test_returns_rows_for_each_year_and_ticker_plus_cash(self):
        prices, dividends = self._make_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        result = tracker["per_company_per_year"]
        # 2 tickers + 1 cash row per year Ã— 2 years = 6 rows
        self.assertEqual(len(result), 6)
        self.assertIn("CASH", list(result["Ticker"]))

    def test_price_return_correct(self):
        prices, dividends = self._make_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 1.0}, dividends,
            initial_capital=1_000_000,
        )
        result = tracker["per_company_per_year"]
        row_2024 = result[(result["Year"] == 2024) & (result["Ticker"] == "A")].iloc[0]
        # A 2024: Start=100, End=110 â†’ price return = 10%
        self.assertAlmostEqual(row_2024["Price_Return_Pct"], 0.10, places=4)

    def test_dividend_return_correct(self):
        prices, dividends = self._make_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 1.0}, dividends,
            initial_capital=1_000_000,
        )
        result = tracker["per_company_per_year"]
        row_2024 = result[(result["Year"] == 2024) & (result["Ticker"] == "A")].iloc[0]
        # A 2024: Div/Share=5, Start Price=100 â†’ dividend return = 5%
        self.assertAlmostEqual(row_2024["Dividend_Return_Pct"], 0.05, places=4)

    def test_total_return_is_price_plus_dividend(self):
        prices, dividends = self._make_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        result = tracker["per_company_per_year"]
        for _, row in result.iterrows():
            if row["Ticker"] == "CASH":
                continue
            self.assertAlmostEqual(
                row["Total_Return_Pct"],
                row["Price_Return_Pct"] + row["Dividend_Return_Pct"],
                places=10,
            )

    def test_cash_row_accumulates_dividends(self):
        prices, dividends = self._make_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        result = tracker["per_company_per_year"]
        # Year 2024 cash row
        cash_2024 = result[(result["Year"] == 2024) & (result["Ticker"] == "CASH")].iloc[0]
        self.assertGreater(cash_2024["Ending_Shares"], 0)  # dividends received
        self.assertAlmostEqual(cash_2024["Starting_Shares"], 0.0)  # starts at 0

        # Year 2025 cash row â€” carries forward
        cash_2025 = result[(result["Year"] == 2025) & (result["Ticker"] == "CASH")].iloc[0]
        self.assertAlmostEqual(cash_2025["Starting_Shares"], cash_2024["Ending_Shares"])
        self.assertGreater(cash_2025["Ending_Shares"], cash_2025["Starting_Shares"])

    def test_shares_dont_change(self):
        """Buy-and-hold: ending shares == starting shares for tickers."""
        prices, dividends = self._make_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        result = tracker["per_company_per_year"]
        for _, row in result.iterrows():
            if row["Ticker"] == "CASH":
                continue
            self.assertAlmostEqual(row["Starting_Shares"], row["Ending_Shares"], places=4)

    def test_dividends_received_correct(self):
        prices, dividends = self._make_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 1.0}, dividends,
            initial_capital=1_000_000,
        )
        result = tracker["per_company_per_year"]
        row_2024 = result[(result["Year"] == 2024) & (result["Ticker"] == "A")].iloc[0]
        # Shares = 1,000,000 / 100 = 10,000
        # Div/Share = 5
        # Total Divs = 10,000 Ã— 5 = 50,000
        self.assertAlmostEqual(row_2024["Total_Dividends_Received"], 50_000, places=0)

    def test_market_values_correct(self):
        prices, dividends = self._make_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 1.0}, dividends,
            initial_capital=500_000,
        )
        result = tracker["per_company_per_year"]
        row_2024 = result[(result["Year"] == 2024) & (result["Ticker"] == "A")].iloc[0]
        # Shares = 500,000 / 100 = 5,000
        # Start Mkt Val = 5,000 Ã— 100 = 500,000
        # End Mkt Val = 5,000 Ã— 110 = 550,000
        self.assertAlmostEqual(row_2024["Starting_Market_Value"], 500_000, places=0)
        self.assertAlmostEqual(row_2024["Ending_Market_Value"], 550_000, places=0)

    def test_empty_input(self):
        """Empty prices should produce empty result."""
        prices = pd.DataFrame({"Date": [], "Ticker": [], "Price": []})
        prices["Date"] = pd.to_datetime(prices["Date"])
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 1.0}, None, initial_capital=1_000_000,
        )
        self.assertTrue(tracker["daily"].empty)


class TestPortfolioMetricsFromPerCompany(unittest.TestCase):
    """Verify portfolio metrics from build_daily_portfolio_tracker."""

    def test_dividend_return_is_nonzero_when_dividends_exist(self):
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-02", "2024-12-30",
                "2024-01-02", "2024-12-30",
            ]),
            "Ticker": ["A", "A", "B", "B"],
            "Price": [100.0, 110.0, 200.0, 220.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["A", "B"],
            "periodEnd": pd.to_datetime(["2024-06-30", "2024-12-31"]),
            "PerShare_Dividends": [5.0, 3.0],
        })
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        metrics = tracker["metrics"]
        self.assertGreater(metrics["portfolio_dividend_return"], 0.0)
        self.assertAlmostEqual(
            metrics["total_return"],
            metrics["portfolio_price_return"] + metrics["portfolio_dividend_return"],
            places=10,
        )

    def test_total_return_matches_manual_calculation(self):
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-02", "2024-12-30",
            ]),
            "Ticker": ["A", "A"],
            "Price": [100.0, 115.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-06-30"]),
            "PerShare_Dividends": [5.0],
        })
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 1.0}, dividends, initial_capital=1_000_000,
        )
        metrics = tracker["metrics"]
        # Shares = 10,000, End Mkt = 1,150,000, Cash = 50,000
        # Final = 1,200,000, Return = 20%
        self.assertAlmostEqual(metrics["total_return"], 0.20, places=4)
        self.assertAlmostEqual(metrics["portfolio_dividend_return"], 0.05, places=4)
        self.assertAlmostEqual(metrics["portfolio_price_return"], 0.15, places=4)


# ---------------------------------------------------------------------------
# Accuracy tests — verify virtual portfolio produces correct, consistent results
# ---------------------------------------------------------------------------

class TestVirtualPortfolioAccuracy(unittest.TestCase):
    """Verify the tracker-based virtual portfolio produces accurate results.

    These test a "virtual buy-and-hold portfolio" — concrete shares,
    daily market values, and accumulated cash dividends.
    """

    def _make_two_ticker_data(self):
        """A rising 2-ticker portfolio over 3 days with one dividend."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-02", "2024-01-03", "2024-01-04",
                "2024-01-02", "2024-01-03", "2024-01-04",
            ]),
            "Ticker": ["A", "A", "A", "B", "B", "B"],
            "Price": [100.0, 105.0, 110.0,     # A: +10%
                       50.0,  52.0,  55.0],    # B: +10%
        })
        dividends = pd.DataFrame({
            "Ticker": ["A"],
            "periodEnd": pd.to_datetime(["2024-01-03"]),
            "PerShare_Dividends": [5.0],
        })
        return prices, dividends

    def test_total_return_is_price_plus_dividend(self):
        """total_return === price_return + dividend_return (to 1e-10)."""
        prices, dividends = self._make_two_ticker_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        m = tracker["metrics"]
        total = m["total_return"]
        price = m["portfolio_price_return"]
        div = m["portfolio_dividend_return"]
        self.assertAlmostEqual(total, price + div, places=10,
            msg=f"total={total:.10f} != price={price:.10f} + div={div:.10f}")

    def test_per_company_from_tracker_matches_legacy(self):
        """_derive_per_company_from_tracker matches calculate_per_company_returns."""
        prices, dividends = self._make_two_ticker_data()

        # Legacy
        weights = {"A": 0.5, "B": 0.5}
        legacy = calculate_per_company_returns(
            prices, weights, dividends,
            initial_capital=1_000_000,
        )

        # Tracker-based (the new helper from the web layer)
        from src.backtesting.backtesting import _derive_per_company_from_tracker
        tracker = build_daily_portfolio_tracker(
            prices, weights, dividends, initial_capital=1_000_000,
        )
        derived = _derive_per_company_from_tracker(
            tracker["daily"], ["A", "B"], weights,
            initial_capital=1_000_000, dividends_df=dividends,
        )

        # Compare key numeric columns
        for col in ["price_return", "dividend_return", "total_return",
                     "weight", "shares_purchased", "capital_invested",
                     "dividends_received"]:
            for tk in ["A", "B"]:
                legacy_val = float(legacy[legacy["Ticker"] == tk][col].iloc[0])
                derived_val = float(derived[derived["Ticker"] == tk][col].iloc[0])
                self.assertAlmostEqual(legacy_val, derived_val, places=8,
                    msg=f"{tk}.{col}: legacy={legacy_val:.8f} vs derived={derived_val:.8f}")

    def test_shares_times_price_equals_market_value(self):
        """shares * price == mktval for each ticker on each day."""
        prices, dividends = self._make_two_ticker_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        daily = tracker["daily"]
        for tk in ["A", "B"]:
            for idx in daily.index:
                shares = daily.loc[idx, f"shares_{tk}"]
                price = daily.loc[idx, f"price_{tk}"]
                mktval = daily.loc[idx, f"mktval_{tk}"]
                expected = shares * price
                self.assertAlmostEqual(mktval, expected, places=2,
                    msg=f"{tk} on {idx.date()}: {shares} * {price} = {expected}, got {mktval}")

    def test_portfolio_total_equals_mktval_plus_cash(self):
        """portfolio_total == sum(mktval) + cash on every day."""
        prices, dividends = self._make_two_ticker_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        daily = tracker["daily"]
        for idx in daily.index:
            mktval_sum = daily.loc[idx, "mktval_A"] + daily.loc[idx, "mktval_B"]
            cash = daily.loc[idx, "cash"]
            portfolio_total = daily.loc[idx, "portfolio_total"]
            self.assertAlmostEqual(portfolio_total, mktval_sum + cash, places=2,
                msg=f"{idx.date()}: mkt={mktval_sum} + cash={cash} = {mktval_sum + cash}, got {portfolio_total}")

    def test_cumulative_return_from_portfolio_total(self):
        """cumulative_return == portfolio_total / initial_capital."""
        prices, dividends = self._make_two_ticker_data()
        capital = 1_000_000.0
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=capital,
        )
        daily = tracker["daily"]
        for idx in daily.index:
            expected_cum = daily.loc[idx, "portfolio_total"] / capital
            actual_cum = daily.loc[idx, "cumulative_return"]
            self.assertAlmostEqual(expected_cum, actual_cum, places=8,
                msg=f"{idx.date()}: expected {expected_cum:.8f}, got {actual_cum:.8f}")

    def test_dividend_cash_equals_sum_of_per_ticker_divs(self):
        """cash column == sum(div_cash_{t}) for all tickers."""
        prices, dividends = self._make_two_ticker_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        daily = tracker["daily"]
        for idx in daily.index:
            div_sum = daily.loc[idx, "div_cash_A"] + daily.loc[idx, "div_cash_B"]
            cash = daily.loc[idx, "cash"]
            self.assertAlmostEqual(cash, div_sum, places=2,
                msg=f"{idx.date()}: div_sum={div_sum}, cash={cash}")

    def test_dividend_cum_contribution_adds_up(self):
        """cumulative_return == price_only_cum_return + dividend_cum_contribution."""
        prices, dividends = self._make_two_ticker_data()
        tracker = build_daily_portfolio_tracker(
            prices, {"A": 0.5, "B": 0.5}, dividends,
            initial_capital=1_000_000,
        )
        daily = tracker["daily"]
        for idx in daily.index:
            total = daily.loc[idx, "cumulative_return"]
            price = daily.loc[idx, "price_only_cum_return"]
            div = daily.loc[idx, "dividend_cum_contribution"]
            self.assertAlmostEqual(total, price + div, places=10,
                msg=f"{idx.date()}: total={total:.10f} != price={price:.10f} + div={div:.10f}")

    def test_single_ticker_no_dividends_exact(self):
        """Single ticker, no dividends: check every number manually."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "Ticker": ["X", "X", "X"],
            "Price": [200.0, 210.0, 220.0],
        })
        capital = 500_000.0
        tracker = build_daily_portfolio_tracker(
            prices, {"X": 1.0}, None, initial_capital=capital,
        )
        daily = tracker["daily"]
        metrics = tracker["metrics"]

        # Shares = 500k / 200 = 2500
        self.assertAlmostEqual(float(daily["shares_X"].iloc[0]), 2500.0, places=2)

        # Day 2: mktval = 2500 * 210 = 525,000; cum return = 525k/500k = 1.05
        self.assertAlmostEqual(float(daily["mktval_X"].iloc[0]), 525000.0, places=0)
        self.assertAlmostEqual(float(daily["cumulative_return"].iloc[0]), 1.05, places=6)

        # Day 3: mktval = 2500 * 220 = 550,000; cum return = 550k/500k = 1.10
        self.assertAlmostEqual(float(daily["mktval_X"].iloc[1]), 550000.0, places=0)
        self.assertAlmostEqual(float(daily["cumulative_return"].iloc[1]), 1.10, places=6)

        # Metrics: total return = 10%
        self.assertAlmostEqual(metrics["total_return"], 0.10, places=6)
        self.assertAlmostEqual(metrics["portfolio_price_return"], 0.10, places=6)
        self.assertAlmostEqual(metrics["portfolio_dividend_return"], 0.0, places=6)

    def test_benchmark_comparison_math(self):
        """Verify benchmark_total_return and excess_return are correct."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-02", "2024-01-03", "2024-01-04",
                "2024-01-02", "2024-01-03", "2024-01-04",
            ]),
            "Ticker": ["PF", "PF", "PF", "BENCH", "BENCH", "BENCH"],
            "Price": [100.0, 110.0, 120.0,     # Portfolio: +20%
                       50.0,  52.0,  53.0],    # Benchmark: +6%
        })
        # Portfolio: 20% return
        bench_returns = calculate_benchmark_returns(prices, "BENCH")
        pf_returns = calculate_portfolio_returns(prices, {"PF": 1.0})
        metrics = calculate_metrics(
            pf_returns, bench_returns,
            "2024-01-02", "2024-01-04",
        )

        # Benchmark: 50 -> 53 = 6.0%
        self.assertAlmostEqual(metrics["benchmark_total_return"], 0.06, places=4)
        # Portfolio: 100 -> 120 = 20%
        self.assertAlmostEqual(metrics["total_return"], 0.20, places=4)
        # Excess = 20% - 6% = 14%
        self.assertAlmostEqual(metrics["excess_return"], 0.14, places=4)

    def test_three_ticker_portfolio_weighted_return(self):
        """3-ticker portfolio: total return = weighted sum of individual returns."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime([
                "2024-01-02", "2024-01-04",
                "2024-01-02", "2024-01-04",
                "2024-01-02", "2024-01-04",
            ]),
            "Ticker": ["A", "A", "B", "B", "C", "C"],
            "Price": [100.0, 120.0,    # A: +20%
                       80.0,  88.0,    # B: +10%
                       50.0,  55.0],   # C: +10%
        })
        weights = {"A": 0.5, "B": 0.3, "C": 0.2}  # sum = 1.0
        tracker = build_daily_portfolio_tracker(
            prices, weights, None, initial_capital=1_000_000,
        )

        # Expected: 0.5*20% + 0.3*10% + 0.2*10% = 10% + 3% + 2% = 15%
        expected = 0.5 * 0.20 + 0.3 * 0.10 + 0.2 * 0.10
        self.assertAlmostEqual(tracker["metrics"]["total_return"], expected, places=6)

        # per_company_per_year should match individual returns
        pyp = tracker["per_company_per_year"]
        for tk, exp_ret in [("A", 0.20), ("B", 0.10), ("C", 0.10)]:
            row = pyp[(pyp["Ticker"] == tk)].iloc[0]
            self.assertAlmostEqual(row["Total_Return_Pct"], exp_ret, places=6,
                msg=f"{tk}: expected {exp_ret}, got {row['Total_Return_Pct']}")

    def test_dividends_add_to_cash_and_dont_change_shares(self):
        """Dividends increase cash, shares stay constant (buy-and-hold)."""
        prices = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "Ticker": ["X", "X", "X"],
            "Price": [100.0, 105.0, 108.0],
        })
        dividends = pd.DataFrame({
            "Ticker": ["X"],
            "periodEnd": pd.to_datetime(["2024-01-03"]),
            "PerShare_Dividends": [5.0],
        })
        tracker = build_daily_portfolio_tracker(
            prices, {"X": 1.0}, dividends, initial_capital=1_000_000,
        )
        daily = tracker["daily"]

        # Shares constant (10,000 = 1M / 100)
        for idx in daily.index:
            self.assertAlmostEqual(float(daily.loc[idx, "shares_X"]), 10000.0, places=2)

        # Cash: daily_df starts at 2nd trading day (1st dropped for pct_change).
        # Dividend on Jan 3 maps to first row of daily_df (Jan 3) → 50k.
        # Both rows (Jan 3 and Jan 4) carry the 50k cash.
        self.assertAlmostEqual(float(daily["cash"].iloc[0]), 50000.0, places=0)
        self.assertAlmostEqual(float(daily["cash"].iloc[1]), 50000.0, places=0)

        # Portfolio total: mktval + cash
        # Row 0 (Jan 3): 10000 * 105 + 50000 = 1,100,000
        self.assertAlmostEqual(
            float(daily["portfolio_total"].iloc[0]), 1100000.0, places=0,
        )
        # Row 1 (Jan 4): 10000 * 108 + 50000 = 1,130,000
        self.assertAlmostEqual(
            float(daily["portfolio_total"].iloc[1]), 1130000.0, places=0,
        )
        # Return = 130k / 1M = 13%
        self.assertAlmostEqual(tracker["metrics"]["total_return"], 0.13, places=6)
        # Price return = 8% (100 → 108)
        self.assertAlmostEqual(tracker["metrics"]["portfolio_price_return"], 0.08, places=6)
        # Dividend return = 5% (50000 / 1M)
        self.assertAlmostEqual(tracker["metrics"]["portfolio_dividend_return"], 0.05, places=6)

    def test_initial_capital_derived_from_shares_mode(self):
        """Shares-mode portfolio: verify capital and shares are correct."""
        config = {"X": {"mode": "shares", "value": 100}}
        prices_data = {"X": 50.0}
        weights, capital, warns = resolve_portfolio_allocations(config, prices_data)
        # 100 shares * 50 = 5000 capital
        self.assertAlmostEqual(capital, 5000.0)
        self.assertAlmostEqual(weights["X"], 1.0)

    def test_initial_capital_derived_from_value_mode(self):
        """Value-mode portfolio: verify capital and weights."""
        config = {
            "A": {"mode": "value", "value": 3000},
            "B": {"mode": "value", "value": 7000},
        }
        weights, capital, warns = resolve_portfolio_allocations(config, {})
        self.assertAlmostEqual(capital, 10000.0)
        self.assertAlmostEqual(weights["A"], 0.3)
        self.assertAlmostEqual(weights["B"], 0.7)


if __name__ == "__main__":
    unittest.main()


