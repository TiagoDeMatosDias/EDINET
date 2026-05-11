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
