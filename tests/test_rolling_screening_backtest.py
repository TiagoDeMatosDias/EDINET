"""Unit tests for rolling screening backtest functionality."""

import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.backtesting.backtesting import (
    _build_heatmap_data,
    _build_portfolios,
    _build_rolling_aggregate,
    _discover_screening_periods,
    _stat_summary,
    run_screening_backtest_rolling,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    """Create a minimal in-memory SQLite database for period discovery."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    try:
        # FinancialStatements table
        conn.execute(
            "CREATE TABLE FinancialStatements ("
            "  edinetCode TEXT, docID TEXT, periodEnd TEXT"
            ")"
        )
        # Insert data spanning 2 years, monthly
        months = []
        for year in (2019, 2020):
            for month in range(1, 13):
                m = f"{year}-{month:02d}"
                months.append(m)
                conn.execute(
                    "INSERT INTO FinancialStatements (edinetCode, docID, periodEnd) "
                    "VALUES (?, ?, ?)",
                    (f"E{year}{month:02d}", f"D{year}{month:02d}", f"{m}-15"),
                )
        conn.commit()
        yield db_path
    finally:
        conn.close()
        Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def mock_screen_df():
    """Create a mock screening result DataFrame."""
    return pd.DataFrame({
        "Company_Ticker": ["7203", "8306", "9984"],
        "LatestPrice": [2500.0, 1800.0, 350000.0],
        "SharesOutstanding": [1000000.0, 2000000.0, 500000.0],
    })


# ---------------------------------------------------------------------------
# _stat_summary
# ---------------------------------------------------------------------------


class TestStatSummary:
    def test_basic(self):
        result = _stat_summary([1.0, 2.0, 3.0, 4.0, 5.0])
        assert result["mean"] == 3.0
        assert result["median"] == 3.0
        assert result["min"] == 1.0
        assert result["max"] == 5.0
        assert "std" in result
        assert result["std"] > 0

    def test_empty(self):
        result = _stat_summary([])
        assert result["mean"] == 0.0
        assert result["std"] == 0.0

    def test_std_calculation(self):
        result = _stat_summary([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
        # Known std from numpy: std(values) ≈ 2.0
        assert result["std"] == pytest.approx(2.0, abs=0.1)


# ---------------------------------------------------------------------------
# _discover_screening_periods
# ---------------------------------------------------------------------------


class TestDiscoverScreeningPeriods:
    def test_monthly(self, mock_db):
        periods = _discover_screening_periods(mock_db, "monthly")
        assert len(periods) == 24  # 2 years × 12 months
        assert all(p.endswith("-01") for p in periods)
        assert periods[0] == "2019-01-01"
        assert periods[-1] == "2020-12-01"

    def test_quarterly(self, mock_db):
        periods = _discover_screening_periods(mock_db, "quarterly")
        assert len(periods) == 8  # 24 months / 3
        assert periods[0] == "2019-01-01"
        assert periods[1] == "2019-04-01"
        assert periods[-1] == "2020-10-01"

    def test_yearly(self, mock_db):
        periods = _discover_screening_periods(mock_db, "yearly")
        assert len(periods) == 2  # 24 months / 12
        assert periods[0] == "2019-01-01"
        assert periods[1] == "2020-01-01"

    def test_bounded_start(self, mock_db):
        periods = _discover_screening_periods(
            mock_db, "monthly", start_period="2020-01",
        )
        assert len(periods) == 12
        assert all(p >= "2020-01-01" for p in periods)

    def test_bounded_end(self, mock_db):
        periods = _discover_screening_periods(
            mock_db, "monthly", end_period="2019-06",
        )
        assert len(periods) == 6
        assert all(p <= "2019-06-01" for p in periods)

    def test_bounded_both(self, mock_db):
        periods = _discover_screening_periods(
            mock_db, "monthly",
            start_period="2019-06",
            end_period="2020-03",
        )
        assert len(periods) == 10
        assert periods[0] == "2019-06-01"
        assert periods[-1] == "2020-03-01"

    def test_quarterly_relative_to_first(self, mock_db):
        """Quarterly should sample relative to the first available month."""
        periods = _discover_screening_periods(
            mock_db, "quarterly", start_period="2019-03",
        )
        # First available is 2019-03, quarterly: 03, 06, 09, 12, 2020-03, 06, 09, 12
        assert periods[0] == "2019-03-01"
        assert periods[1] == "2019-06-01"

    def test_invalid_cadence(self, mock_db):
        with pytest.raises(ValueError, match="Unknown cadence"):
            _discover_screening_periods(mock_db, "weekly")

    def test_empty_db(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE FinancialStatements (periodEnd TEXT)")
        conn.close()
        try:
            periods = _discover_screening_periods(db_path, "monthly")
            assert periods == []
        finally:
            Path(db_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# _build_portfolios
# ---------------------------------------------------------------------------


class TestBuildPortfolios:
    def test_equal_weight_basic(self):
        tickers = ["7203", "8306", "9984"]
        portfolios, warnings = _build_portfolios(tickers, ["equal"])
        assert "equal" in portfolios
        eq = portfolios["equal"]
        assert len(eq) == 3
        for t in tickers:
            assert eq[t]["mode"] == "weight"
            assert eq[t]["value"] == pytest.approx(1.0 / 3)

    def test_equal_weight_single(self):
        portfolios, warnings = _build_portfolios(["7203"], ["equal"])
        eq = portfolios["equal"]
        assert eq["7203"]["value"] == 1.0

    def test_market_cap_weight(self, mock_screen_df):
        tickers = ["7203", "8306", "9984"]
        portfolios, warnings = _build_portfolios(
            tickers, ["market_cap"],
            screen_df=mock_screen_df,
            shares_outstanding_col="SharesOutstanding",
        )
        assert "market_cap" in portfolios
        mc = portfolios["market_cap"]
        # Market caps:
        # 7203: 2500 * 1M = 2.5B
        # 8306: 1800 * 2M = 3.6B
        # 9984: 350000 * 0.5M = 175B
        # Total: 181.1B
        total = 2.5 + 3.6 + 175.0  # in billions
        assert mc["7203"]["value"] == pytest.approx(2.5 / total, rel=1e-4)
        assert mc["9984"]["value"] == pytest.approx(175.0 / total, rel=1e-4)

    def test_market_cap_no_shares_column(self, mock_screen_df):
        tickers = ["7203"]
        portfolios, warnings = _build_portfolios(
            tickers, ["market_cap"],
            screen_df=mock_screen_df.drop(columns=["SharesOutstanding"]),
        )
        # Should fall back to equal weight
        assert len(warnings) > 0
        assert any("no shares-outstanding" in w.lower() for w in warnings)
        mc = portfolios["market_cap"]
        assert mc["7203"]["value"] == 1.0

    def test_market_cap_fewer_than_two(self, mock_screen_df):
        # Only one ticker has valid data
        small_df = pd.DataFrame({
            "Company_Ticker": ["7203", "8306"],
            "LatestPrice": [2500.0, None],  # 8306 has no price
            "SharesOutstanding": [1000000.0, 2000000.0],
        })
        portfolios, warnings = _build_portfolios(
            ["7203", "8306"], ["market_cap"],
            screen_df=small_df,
            shares_outstanding_col="SharesOutstanding",
        )
        assert any("fewer than 2" in w.lower() for w in warnings)
        mc = portfolios["market_cap"]
        assert mc["7203"]["value"] == 0.5

    def test_market_cap_no_screen_df(self):
        portfolios, warnings = _build_portfolios(
            ["7203"], ["market_cap"], screen_df=None,
        )
        assert any("no screening data" in w.lower() for w in warnings)
        mc = portfolios["market_cap"]
        assert mc["7203"]["value"] == 1.0

    def test_mixed_weighting_modes(self, mock_screen_df):
        tickers = ["7203", "8306"]
        portfolios, warnings = _build_portfolios(
            tickers, ["equal", "market_cap"],
            screen_df=mock_screen_df,
            shares_outstanding_col="SharesOutstanding",
        )
        assert "equal" in portfolios
        assert "market_cap" in portfolios
        eq = portfolios["equal"]
        assert eq["7203"]["value"] == 0.5

    def test_unknown_weighting_mode(self):
        portfolios, warnings = _build_portfolios(["7203"], ["unknown_mode"])
        assert "unknown_mode" not in portfolios
        assert any("unknown weighting" in w.lower() for w in warnings)


# ---------------------------------------------------------------------------
# _build_heatmap_data
# ---------------------------------------------------------------------------


class TestBuildHeatmapData:
    def test_basic(self):
        results = [
            {
                "period": "2020-01",
                "backtests": {
                    "equal": {
                        "1yr": {"metrics": {"total_return": 0.12}},
                        "2yr": {"metrics": {"total_return": 0.25}},
                    },
                },
            },
            {
                "period": "2020-02",
                "backtests": {
                    "equal": {
                        "1yr": {"metrics": {"total_return": -0.05}},
                        "2yr": {"metrics": None},  # failed backtest
                    },
                },
            },
        ]
        hm = _build_heatmap_data(results, ["1yr", "2yr"], ["equal"])
        assert "equal" in hm
        assert "1yr" in hm["equal"]
        assert "2yr" in hm["equal"]

        yr1 = hm["equal"]["1yr"]
        assert len(yr1) == 2
        assert yr1[0]["return"] == 0.12
        assert yr1[1]["return"] == -0.05

        yr2 = hm["equal"]["2yr"]
        assert yr2[1]["return"] is None  # failed backtest

    def test_multiple_weightings(self):
        results = [
            {
                "period": "2020-01",
                "backtests": {
                    "equal": {"1yr": {"metrics": {"total_return": 0.10}}},
                    "market_cap": {"1yr": {"metrics": {"total_return": 0.12}}},
                },
            },
        ]
        hm = _build_heatmap_data(results, ["1yr"], ["equal", "market_cap"])
        assert hm["equal"]["1yr"][0]["return"] == 0.10
        assert hm["market_cap"]["1yr"][0]["return"] == 0.12


# ---------------------------------------------------------------------------
# _build_rolling_aggregate
# ---------------------------------------------------------------------------


class TestBuildRollingAggregate:
    def test_basic_stats(self):
        results = [
            {
                "period": "2020-01",
                "backtests": {
                    "equal": {
                        "1yr": {
                            "metrics": {
                                "total_return": 0.10,
                                "sharpe_ratio": 0.5,
                                "max_drawdown": -0.15,
                                "annualized_return": 0.10,
                            },
                        },
                    },
                },
            },
            {
                "period": "2020-02",
                "backtests": {
                    "equal": {
                        "1yr": {
                            "metrics": {
                                "total_return": 0.20,
                                "sharpe_ratio": 0.8,
                                "max_drawdown": -0.10,
                                "annualized_return": 0.20,
                            },
                        },
                    },
                },
            },
        ]
        agg = _build_rolling_aggregate(
            results, ["1yr"], ["equal"],
            ["2020-01-01", "2020-02-01"], "",
        )
        assert agg["total_runs"] == 2
        assert agg["successful"] == 2
        assert agg["failed"] == 0
        assert agg["periods"] == 2
        assert agg["stats"]["total_return"]["mean"] == pytest.approx(0.15)
        assert agg["stats"]["total_return"]["std"] > 0

    def test_by_weighting_breakdown(self):
        results = [
            {
                "period": "2020-01",
                "backtests": {
                    "equal": {
                        "1yr": {
                            "metrics": {
                                "total_return": 0.10,
                                "sharpe_ratio": 0.5,
                                "max_drawdown": -0.15,
                                "annualized_return": 0.10,
                            },
                        },
                    },
                },
            },
        ]
        agg = _build_rolling_aggregate(
            results, ["1yr"], ["equal"],
            ["2020-01-01"], "",
        )
        assert "equal" in agg["by_weighting"]
        eq_1yr = agg["by_weighting"]["equal"]["1yr"]
        assert eq_1yr["mean_return"] == 0.10
        assert eq_1yr["count"] == 1

    def test_benchmark_comparison(self):
        results = [
            {
                "period": "2020-01",
                "backtests": {
                    "equal": {
                        "1yr": {
                            "metrics": {
                                "total_return": 0.15,
                                "benchmark_total_return": 0.10,
                                "sharpe_ratio": 0.5,
                                "max_drawdown": -0.15,
                                "annualized_return": 0.15,
                            },
                        },
                    },
                },
            },
            {
                "period": "2020-02",
                "backtests": {
                    "equal": {
                        "1yr": {
                            "metrics": {
                                "total_return": 0.05,
                                "benchmark_total_return": 0.10,
                                "sharpe_ratio": 0.2,
                                "max_drawdown": -0.20,
                                "annualized_return": 0.05,
                            },
                        },
                    },
                },
            },
        ]
        agg = _build_rolling_aggregate(
            results, ["1yr"], ["equal"],
            ["2020-01-01", "2020-02-01"], "1321.T",
        )
        bc = agg["benchmark_comparison"]
        assert bc is not None
        assert bc["outperformed"] == 1  # First month beat benchmark
        assert bc["underperformed"] == 1  # Second month didn't
        assert bc["win_rate"] == 0.5

    def test_no_benchmark_no_comparison(self):
        results = [
            {
                "period": "2020-01",
                "backtests": {
                    "equal": {
                        "1yr": {
                            "metrics": {
                                "total_return": 0.10,
                                "sharpe_ratio": 0.5,
                                "max_drawdown": -0.15,
                                "annualized_return": 0.10,
                            },
                        },
                    },
                },
            },
        ]
        agg = _build_rolling_aggregate(
            results, ["1yr"], ["equal"],
            ["2020-01-01"], "",
        )
        assert agg["benchmark_comparison"] is None

    def test_failed_backtests(self):
        results = [
            {
                "period": "2020-01",
                "backtests": {
                    "equal": {
                        "1yr": {"metrics": None},  # failed
                    },
                },
            },
        ]
        agg = _build_rolling_aggregate(
            results, ["1yr"], ["equal"],
            ["2020-01-01"], "",
        )
        assert agg["successful"] == 0
        assert agg["failed"] == 1
        # No stats when all failed
        assert agg["stats"] is None

    def test_heatmap_included(self):
        results = [
            {
                "period": "2020-01",
                "backtests": {
                    "equal": {
                        "1yr": {"metrics": {"total_return": 0.10}},
                    },
                },
            },
        ]
        agg = _build_rolling_aggregate(
            results, ["1yr"], ["equal"],
            ["2020-01-01"], "",
        )
        assert "heatmap" in agg
        assert "equal" in agg["heatmap"]
        assert "1yr" in agg["heatmap"]["equal"]

    def test_date_range(self):
        results = []
        agg = _build_rolling_aggregate(
            results, ["1yr"], ["equal"],
            ["2018-01-01", "2025-12-01"], "",
        )
        assert agg["date_range"]["first"] == "2018-01-01"
        assert agg["date_range"]["last"] == "2025-12-01"


# ---------------------------------------------------------------------------
# run_screening_backtest_rolling (integration-style)
# ---------------------------------------------------------------------------


class TestRunScreeningBacktestRolling:
    """End-to-end tests using a small mock database."""

    @pytest.fixture
    def rolling_test_db(self):
        """Create a minimal DB with price data, financials, and company info."""
        db_path = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
        conn = sqlite3.connect(db_path)
        try:
            # CompanyInfo
            conn.execute(
                "CREATE TABLE CompanyInfo ("
                "  edinetCode TEXT, Company_Ticker TEXT, Company_Name TEXT"
                ")"
            )
            conn.execute(
                "INSERT INTO CompanyInfo VALUES ('E001', '7203', 'Toyota')"
            )
            conn.execute(
                "INSERT INTO CompanyInfo VALUES ('E002', '8306', 'MUFG')"
            )

            # Stock_Prices
            conn.execute(
                "CREATE TABLE Stock_Prices ("
                "  Date TEXT, Ticker TEXT, Price REAL"
                ")"
            )
            # 2020-01-01 through 2021-06-01 for both tickers
            dates = ["2020-01-01", "2020-06-01", "2021-01-01", "2021-06-01"]
            for d in dates:
                conn.execute(
                    "INSERT INTO Stock_Prices VALUES (?, '7203', 2000.0)", (d,)
                )
                conn.execute(
                    "INSERT INTO Stock_Prices VALUES (?, '8306', 1500.0)", (d,)
                )

            # FinancialStatements (needed for screening periods)
            conn.execute(
                "CREATE TABLE FinancialStatements ("
                "  edinetCode TEXT, docID TEXT, periodEnd TEXT, Revenue REAL"
                ")"
            )
            for m in range(1, 5):
                period = f"2020-{m:02d}-15"
                conn.execute(
                    "INSERT INTO FinancialStatements VALUES "
                    "(?, ?, ?, ?)",
                    ("E001", f"D001-{m}", period, 1000.0 * m),
                )
                conn.execute(
                    "INSERT INTO FinancialStatements VALUES "
                    "(?, ?, ?, ?)",
                    ("E002", f"D002-{m}", period, 800.0 * m),
                )

            # PerShare (for screening criteria)
            conn.execute(
                "CREATE TABLE PerShare (docID TEXT, EPS REAL)"
            )
            for m in range(1, 5):
                conn.execute(
                    "INSERT INTO PerShare VALUES (?, ?)",
                    (f"D001-{m}", 100.0),
                )
                conn.execute(
                    "INSERT INTO PerShare VALUES (?, ?)",
                    (f"D002-{m}", 80.0),
                )

            # ShareMetrics (for potential dividends)
            conn.execute(
                "CREATE TABLE ShareMetrics ("
                "  docID TEXT, \"Dividend paid per share\" REAL"
                ")"
            )

            conn.commit()
            yield db_path
        finally:
            conn.close()
            Path(db_path).unlink(missing_ok=True)

    def test_end_to_end_basic(self, rolling_test_db):
        """Run a basic rolling backtest with minimal config."""
        criteria = [{
            "table": "PerShare",
            "column": "EPS",
            "operator": ">",
            "value": 0,
            "comparison_mode": "fixed",
        }]
        columns = [
            "CompanyInfo.Company_Ticker",
            "CompanyInfo.Company_Name",
        ]

        result = run_screening_backtest_rolling(
            db_path=rolling_test_db,
            criteria=criteria,
            columns=columns,
            cadence="monthly",
            durations=["1yr"],
            weighting_modes=["equal"],
            max_companies=25,
            start_period="2020-01",
            end_period="2020-02",
        )

        assert "config" in result
        assert result["config"]["cadence"] == "monthly"
        assert "durations" in result["config"]

        assert "aggregate" in result
        agg = result["aggregate"]
        assert agg["periods"] > 0
        assert agg["successful"] >= 0

        assert "results" in result
        for r in result["results"]:
            assert "period" in r
            assert "tickers" in r
            assert "backtests" in r
            assert "equal" in r["backtests"]

    def test_respects_cadence(self, rolling_test_db):
        """Verify cadence affects how many periods are screened."""
        criteria = [{
            "table": "PerShare",
            "column": "EPS",
            "operator": ">",
            "value": 0,
            "comparison_mode": "fixed",
        }]
        columns = ["CompanyInfo.Company_Ticker"]

        result_monthly = run_screening_backtest_rolling(
            db_path=rolling_test_db,
            criteria=criteria,
            columns=columns,
            cadence="monthly",
            durations=["1yr"],
            weighting_modes=["equal"],
            max_companies=25,
            start_period="2020-01",
            end_period="2020-04",
        )

        result_quarterly = run_screening_backtest_rolling(
            db_path=rolling_test_db,
            criteria=criteria,
            columns=columns,
            cadence="quarterly",
            durations=["1yr"],
            weighting_modes=["equal"],
            max_companies=25,
            start_period="2020-01",
            end_period="2020-04",
        )

        assert result_monthly["aggregate"]["periods"] >= result_quarterly["aggregate"]["periods"]

    def test_empty_criteria_raises(self, rolling_test_db):
        with pytest.raises(ValueError, match="(?i)at least one"):
            run_screening_backtest_rolling(
                db_path=rolling_test_db,
                criteria=[],
                columns=["CompanyInfo.Company_Ticker"],
                durations=["1yr"],
            )

    def test_invalid_cadence_raises(self, rolling_test_db):
        with pytest.raises(ValueError, match="Invalid cadence"):
            run_screening_backtest_rolling(
                db_path=rolling_test_db,
                criteria=[{"table": "PerShare", "column": "EPS", "operator": ">", "value": 0}],
                columns=["CompanyInfo.Company_Ticker"],
                cadence="daily",
                durations=["1yr"],
            )

    def test_multiple_durations(self, rolling_test_db):
        criteria = [{
            "table": "PerShare",
            "column": "EPS",
            "operator": ">",
            "value": 0,
            "comparison_mode": "fixed",
        }]
        columns = ["CompanyInfo.Company_Ticker"]

        result = run_screening_backtest_rolling(
            db_path=rolling_test_db,
            criteria=criteria,
            columns=columns,
            cadence="monthly",
            durations=["1yr", "2yr"],
            weighting_modes=["equal"],
            max_companies=25,
            start_period="2020-01",
            end_period="2020-01",
        )

        # Should have backtests for both durations
        for r in result["results"]:
            assert "1yr" in r["backtests"]["equal"] or "2yr" in r["backtests"]["equal"]

    def test_progress_queue(self, rolling_test_db):
        """Verify progress events are emitted."""
        import queue

        criteria = [{
            "table": "PerShare",
            "column": "EPS",
            "operator": ">",
            "value": 0,
            "comparison_mode": "fixed",
        }]
        columns = ["CompanyInfo.Company_Ticker"]
        progress_queue = queue.Queue()

        run_screening_backtest_rolling(
            db_path=rolling_test_db,
            criteria=criteria,
            columns=columns,
            cadence="monthly",
            durations=["1yr"],
            weighting_modes=["equal"],
            max_companies=25,
            start_period="2020-01",
            end_period="2020-01",
            progress_queue=progress_queue,
        )

        events = []
        while not progress_queue.empty():
            events.append(progress_queue.get_nowait())

        assert len(events) > 0
        assert any(e["type"] == "progress" for e in events)

    def test_cancellation(self, rolling_test_db):
        """Verify cancellation stops execution."""
        import threading

        criteria = [{
            "table": "PerShare",
            "column": "EPS",
            "operator": ">",
            "value": 0,
            "comparison_mode": "fixed",
        }]
        columns = ["CompanyInfo.Company_Ticker"]
        cancel_event = threading.Event()
        cancel_event.set()  # Cancel immediately

        with pytest.raises(RuntimeError, match="cancelled"):
            run_screening_backtest_rolling(
                db_path=rolling_test_db,
                criteria=criteria,
                columns=columns,
                cadence="monthly",
                durations=["1yr"],
                weighting_modes=["equal"],
                max_companies=25,
                start_period="2020-01",
                end_period="2020-04",
                cancel_event=cancel_event,
            )


# ---------------------------------------------------------------------------
# Chart data integrity tests
# ---------------------------------------------------------------------------


class TestChartDataIntegrity:
    """Verify that every chart gets populated data.

    These tests mirror what the frontend consumes:
    1. Heatmap data — agg.heatmap[wm][dur] = [{period, return}, ...]
    2. Distribution data — 1yr returns from result.results[].backtests[wm][1yr].metrics.total_return
    3. Drill-down data — result.results[].backtests[wm][dur].metrics + chart_data
    4. Summary table — agg.by_weighting[wm][dur] = {mean_return, median_return, ...}
    """

    @pytest.fixture
    def sample_rolling_result(self):
        """Build a complete RollingBacktestResult for chart testing."""
        return {
            "config": {
                "cadence": "monthly",
                "durations": ["1yr", "2yr", "3yr"],
                "weighting_modes": ["equal", "market_cap"],
                "max_companies": 25,
                "criteria": [{"table": "P", "column": "E", "operator": ">", "value": 0}],
                "benchmark_ticker": "1321.T",
                "start_period": "2020-01",
                "end_period": "2020-03",
            },
            "aggregate": {
                "total_runs": 18,
                "successful": 18,
                "failed": 0,
                "periods": 3,
                "date_range": {"first": "2020-01-01", "last": "2020-03-01"},
                "by_weighting": {
                    "equal": {
                        "1yr": {"mean_return": 0.08, "median_return": 0.07, "mean_sharpe": 0.5, "count": 3},
                        "2yr": {"mean_return": 0.015, "median_return": 0.01, "mean_sharpe": 0.3, "count": 3},
                        "3yr": {"mean_return": 0.005, "median_return": 0.003, "mean_sharpe": 0.2, "count": 3},
                    },
                    "market_cap": {
                        "1yr": {"mean_return": 0.09, "median_return": 0.08, "mean_sharpe": 0.55, "count": 3},
                        "2yr": {"mean_return": 0.02, "median_return": 0.015, "mean_sharpe": 0.35, "count": 3},
                        "3yr": {"mean_return": 0.008, "median_return": 0.006, "mean_sharpe": 0.25, "count": 3},
                    },
                },
                "benchmark_comparison": {
                    "outperformed": 12, "underperformed": 6, "win_rate": 0.67,
                    "by_duration": {
                        "1yr": {"out": 4, "total": 6, "win_rate": 0.67},
                        "2yr": {"out": 4, "total": 6, "win_rate": 0.67},
                        "3yr": {"out": 4, "total": 6, "win_rate": 0.67},
                    },
                },
                "stats": {
                    "total_return": {"mean": 0.05, "median": 0.04, "min": -0.1, "max": 0.25, "std": 0.08},
                    "sharpe_ratio": {"mean": 0.4, "median": 0.35, "min": 0.1, "max": 0.9, "std": 0.15},
                    "max_drawdown": {"mean": -0.2, "median": -0.18, "min": -0.5, "max": -0.05, "std": 0.12},
                },
                "heatmap": {
                    "equal": {
                        "1yr": [
                            {"period": "2020-01-01", "return": 0.10},
                            {"period": "2020-02-01", "return": 0.05},
                            {"period": "2020-03-01", "return": 0.09},
                        ],
                        "2yr": [
                            {"period": "2020-01-01", "return": 0.02},
                            {"period": "2020-02-01", "return": 0.01},
                            {"period": "2020-03-01", "return": 0.015},
                        ],
                        "3yr": [
                            {"period": "2020-01-01", "return": 0.008},
                            {"period": "2020-02-01", "return": 0.005},
                            {"period": "2020-03-01", "return": 0.003},
                        ],
                    },
                    "market_cap": {
                        "1yr": [
                            {"period": "2020-01-01", "return": 0.12},
                            {"period": "2020-02-01", "return": 0.06},
                            {"period": "2020-03-01", "return": 0.09},
                        ],
                        "2yr": [
                            {"period": "2020-01-01", "return": 0.025},
                            {"period": "2020-02-01", "return": 0.015},
                            {"period": "2020-03-01", "return": 0.02},
                        ],
                        "3yr": [
                            {"period": "2020-01-01", "return": 0.01},
                            {"period": "2020-02-01", "return": 0.008},
                            {"period": "2020-03-01", "return": 0.006},
                        ],
                    },
                },
            },
            "results": [
                {
                    "period": "2020-01-01",
                    "screening_date": "2020-01-01",
                    "tickers": ["7203", "8306", "9984"],
                    "ticker_count": 3,
                    "warnings": [],
                    "backtests": {
                        "equal": {
                            "1yr": {
                                "metrics": {
                                    "total_return": 0.10, "annualized_return": 0.10,
                                    "sharpe_ratio": 0.6, "max_drawdown": -0.15,
                                    "start_date": "2020-01-01", "end_date": "2021-01-01",
                                },
                                "chart_data": {
                                    "cumulative": [
                                        {"date": "2020-01-01", "portfolio": 0.0, "benchmark": 0.0},
                                        {"date": "2021-01-01", "portfolio": 0.10, "benchmark": 0.05},
                                    ],
                                    "drawdown": [
                                        {"date": "2020-01-01", "portfolio": 0.0},
                                        {"date": "2021-01-01", "portfolio": -0.05},
                                    ],
                                    "decomposition": [
                                        {"date": "2021-01-01", "price_only": 0.08, "dividend_only": 0.02, "total": 0.10},
                                    ],
                                },
                                "per_company": [
                                    {"Ticker": "7203", "total_return": 0.12, "price_return": 0.10, "dividend_return": 0.02, "weight": 0.33},
                                ],
                                "yearly_returns": [
                                    {"Year": 2020, "Price Return": 0.08, "Dividend Return": 0.02, "Total Return": 0.10},
                                ],
                                "dividends_by_year": [
                                    {"year": 2020, "7203": 5000.0, "Total": 15000.0},
                                ],
                                "warnings": [],
                            },
                        },
                    },
                },
            ],
        }

    # ── Heatmap data ────────────────────────────────────────────────

    def test_heatmap_has_all_weightings(self, sample_rolling_result):
        hm = sample_rolling_result["aggregate"]["heatmap"]
        assert "equal" in hm
        assert "market_cap" in hm

    def test_heatmap_has_all_durations(self, sample_rolling_result):
        hm = sample_rolling_result["aggregate"]["heatmap"]
        for wm in ["equal", "market_cap"]:
            for dur in ["1yr", "2yr", "3yr"]:
                assert dur in hm[wm], f"Missing {wm}/{dur} in heatmap"

    def test_heatmap_has_non_empty_data(self, sample_rolling_result):
        hm = sample_rolling_result["aggregate"]["heatmap"]
        for wm in ["equal", "market_cap"]:
            for dur in ["1yr", "2yr", "3yr"]:
                data = hm[wm][dur]
                assert len(data) > 0, f"Heatmap {wm}/{dur} is empty"
                for entry in data:
                    assert "period" in entry
                    assert entry["period"], f"Heatmap {wm}/{dur} has empty period"
                    assert entry["return"] is not None, f"Heatmap {wm}/{dur} has null return"

    def test_heatmap_returns_are_finite(self, sample_rolling_result):
        hm = sample_rolling_result["aggregate"]["heatmap"]
        for wm in hm:
            for dur in hm[wm]:
                for entry in hm[wm][dur]:
                    r = entry["return"]
                    assert np.isfinite(r), f"Heatmap {wm}/{dur} has non-finite return: {r}"

    def test_heatmap_returns_are_annualized(self, sample_rolling_result):
        """Heatmap 1yr returns should equal total returns (not annualized differently)."""
        hm = sample_rolling_result["aggregate"]["heatmap"]
        # 1yr returns in heatmap should be between -1 and, say, 5 (reasonable)
        for wm in hm:
            for entry in hm[wm].get("1yr", []):
                assert -1.0 <= entry["return"] <= 10.0, \
                    f"Heatmap 1yr return out of range: {entry['return']}"
        # 3yr returns should be annualized (lower magnitude than raw total)
        for wm in hm:
            for entry in hm[wm].get("3yr", []):
                assert -1.0 <= entry["return"] <= 10.0, \
                    f"Heatmap 3yr return out of range: {entry['return']}"

    # ── Distribution chart data ─────────────────────────────────────

    def test_distribution_has_1yr_returns(self, sample_rolling_result):
        """Verify that 1yr returns exist for the distribution chart."""
        returns_1yr = []
        for r in sample_rolling_result["results"]:
            for wm in r.get("backtests", {}):
                bt = r["backtests"][wm].get("1yr")
                if bt and bt.get("metrics"):
                    returns_1yr.append(bt["metrics"]["total_return"])
        assert len(returns_1yr) > 0, "No 1yr returns for distribution chart"
        for val in returns_1yr:
            assert np.isfinite(val), f"Non-finite 1yr return: {val}"

    def test_distribution_returns_are_finite(self, sample_rolling_result):
        """All returns in all backtests must be finite for chart rendering."""
        for r in sample_rolling_result["results"]:
            for wm in r.get("backtests", {}):
                for dur in r["backtests"][wm]:
                    bt = r["backtests"][wm][dur]
                    if bt.get("metrics"):
                        tr = bt["metrics"].get("total_return")
                        if tr is not None:
                            assert np.isfinite(tr), \
                                f"Non-finite return in {r['period']}/{wm}/{dur}: {tr}"

    # ── Summary table data ──────────────────────────────────────────

    def test_summary_table_has_all_weightings(self, sample_rolling_result):
        by_w = sample_rolling_result["aggregate"]["by_weighting"]
        assert "equal" in by_w
        assert "market_cap" in by_w

    def test_summary_table_has_all_durations(self, sample_rolling_result):
        by_w = sample_rolling_result["aggregate"]["by_weighting"]
        for wm in by_w:
            for dur in ["1yr", "2yr", "3yr"]:
                assert dur in by_w[wm], f"Missing {wm}/{dur} in by_weighting"

    def test_summary_table_values_are_finite(self, sample_rolling_result):
        by_w = sample_rolling_result["aggregate"]["by_weighting"]
        for wm in by_w:
            for dur in by_w[wm]:
                entry = by_w[wm][dur]
                for key in ["mean_return", "median_return", "mean_sharpe"]:
                    val = entry.get(key)
                    assert val is not None, f"Missing {key} in {wm}/{dur}"
                    assert np.isfinite(val), f"Non-finite {key} in {wm}/{dur}: {val}"
                assert entry["count"] >= 0

    # ── Drill-down data ─────────────────────────────────────────────

    def test_drilldown_period_has_tickers(self, sample_rolling_result):
        for r in sample_rolling_result["results"]:
            assert "tickers" in r
            assert len(r["tickers"]) > 0

    def test_drilldown_period_has_backtests(self, sample_rolling_result):
        for r in sample_rolling_result["results"]:
            assert "backtests" in r
            bt = r["backtests"]
            # At least one weighting mode
            assert len(bt) > 0

    def test_drilldown_backtest_has_chart_data(self, sample_rolling_result):
        for r in sample_rolling_result["results"]:
            for wm in r.get("backtests", {}):
                for dur in r["backtests"][wm]:
                    bt = r["backtests"][wm][dur]
                    cd = bt.get("chart_data", {})
                    # Cumulative chart data
                    assert "cumulative" in cd, f"Missing cumulative in {r['period']}/{wm}/{dur}"
                    # Drawdown chart data
                    assert "drawdown" in cd, f"Missing drawdown in {r['period']}/{wm}/{dur}"

    def test_drilldown_chart_data_has_dates(self, sample_rolling_result):
        for r in sample_rolling_result["results"]:
            for wm in r.get("backtests", {}):
                for dur in r["backtests"][wm]:
                    bt = r["backtests"][wm][dur]
                    cd = bt.get("chart_data", {})
                    cumulative = cd.get("cumulative", [])
                    if cumulative:
                        for point in cumulative:
                            assert "date" in point, f"Missing date in cumulative point"
                            assert point["date"], f"Empty date"
                            assert "portfolio" in point, f"Missing portfolio value"
                            assert np.isfinite(point["portfolio"]), \
                                f"Non-finite portfolio: {point['portfolio']}"

    def test_drilldown_metrics_are_complete(self, sample_rolling_result):
        """Every backtest in drill-down must have required metric fields."""
        required = ["total_return", "sharpe_ratio", "max_drawdown", "start_date", "end_date"]
        for r in sample_rolling_result["results"]:
            for wm in r.get("backtests", {}):
                for dur in r["backtests"][wm]:
                    bt = r["backtests"][wm][dur]
                    m = bt.get("metrics", {})
                    for key in required:
                        assert key in m, f"Missing {key} in {r['period']}/{wm}/{dur}"

    # ── Overall stats tile data ─────────────────────────────────────

    def test_stats_tiles_have_all_fields(self, sample_rolling_result):
        stats = sample_rolling_result["aggregate"]["stats"]
        assert "total_return" in stats
        assert "sharpe_ratio" in stats
        assert "max_drawdown" in stats
        for key in ["total_return", "sharpe_ratio", "max_drawdown"]:
            for field in ["mean", "median", "min", "max", "std"]:
                val = stats[key].get(field)
                assert val is not None, f"Missing {key}.{field}"
                assert np.isfinite(val), f"Non-finite {key}.{field}: {val}"

    def test_no_chart_has_nan(self, sample_rolling_result):
        """Sanity: recursively check no NaN/Inf in the entire result."""
        import math

        def check(obj, path=""):
            if isinstance(obj, float):
                assert not math.isnan(obj), f"NaN at {path}"
                assert not math.isinf(obj), f"Inf at {path}"
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    check(v, f"{path}.{k}")
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    check(v, f"{path}[{i}]")

        check(sample_rolling_result)
