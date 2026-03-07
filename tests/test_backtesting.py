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
    calculate_return_decomposition,
    calculate_per_company_returns,
    calculate_yearly_returns,
    calculate_dividends_by_company_year,
    calculate_metrics,
    generate_report,
    generate_backtest_charts,
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
            "PerShare_Dividends": [10.0],  # 10 JPY on initial price of 100
        })
        result = calculate_portfolio_returns(prices, {"A": 1.0}, dividends)
        # Day 2: price return 10% + dividend yield 10/100 = 10%
        expected_day2 = 0.10 + 10.0 / 100.0
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
        # A: 100 → 120 = +20%
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
        # A: price_return=20%, weight=60% → weighted_price = 12%
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
        for col in ("capital_invested", "shares_purchased", "dividends_received"):
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
        # Price return should be 10% (100 → 110)
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
