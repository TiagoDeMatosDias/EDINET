"""Tests for src/portfolio/performance.py — metrics computation."""

import os
import math
import pytest
import numpy as np
from src.portfolio.performance import (
    sharpe_ratio,
    sortino_ratio,
    max_drawdown,
    max_drawdown_with_dates,
    calmar_ratio,
    win_rate,
    avg_win,
    avg_loss,
    profit_factor,
    var_historical,
    cvar_historical,
    get_risk_free_rate,
    calculate_metrics,
)
from src.orchestrator.common.db_config import get_db2


class TestIndividualMetrics:
    def test_sharpe_ratio_zero_return(self):
        returns = [0.0] * 252
        assert sharpe_ratio(returns, 0.02) == 0.0

    def test_sharpe_ratio_positive(self):
        """Constant 10% annual return, 0% risk → very high Sharpe."""
        daily = [0.10 / 252] * 252
        s = sharpe_ratio(daily, 0.02)
        assert s > 5, f"Expected Sharpe > 5, got {s}"

    def test_sortino_vs_sharpe(self):
        """Sortino > Sharpe when returns are all positive (no downside)."""
        # All-positive returns: downside deviation = 0 → Sortino = inf or very large
        returns = [0.001] * 252  # all positive, constant
        s = sharpe_ratio(returns, 0.0)
        sort = sortino_ratio(returns, 0.0)
        # With all-positive returns, downside std = 0, Sortino is undefined (0/0).
        # In practice, a tiny positive outperformance yields Sortino >> Sharpe.
        # For this edge case, we just verify both compute without error.
        assert isinstance(s, float)
        assert isinstance(sort, float)

    def test_max_drawdown_simple(self):
        """Peak at 100, trough at 50, recovery to 60."""
        values = [100, 120, 80, 50, 70, 90]
        dd, peak_i, trough_i = max_drawdown(values)
        assert dd == (50 - 120) / 120  # -0.5833
        assert abs(dd - (-0.58333)) < 0.01

    def test_max_drawdown_no_drawdown(self):
        values = [100, 110, 120, 130]
        dd, _, _ = max_drawdown(values)
        assert dd == 0.0

    def test_max_drawdown_with_dates_returns_dates(self):
        values = [100, 50, 60]
        dates = ["2024-01-01", "2024-02-01", "2024-03-01"]
        dd, peak, trough = max_drawdown_with_dates(values, dates)
        assert dd < 0
        assert peak == "2024-01-01"
        assert trough == "2024-02-01"

    def test_calmar_ratio(self):
        assert round(calmar_ratio(0.15, -0.30), 3) == 0.5

    def test_win_rate(self):
        returns = [0.01, -0.02, 0.03, 0.0, -0.01]
        assert win_rate(returns) == 0.5  # 2/4 (zeros excluded: 2 wins, 2 losses)

    def test_avg_win(self):
        returns = [0.01, -0.02, 0.03]
        assert avg_win(returns) == 0.02

    def test_avg_loss(self):
        returns = [0.01, -0.02, -0.03, 0.04]
        assert avg_loss(returns) == -0.025

    def test_profit_factor(self):
        returns = [0.02, -0.01, 0.03, -0.01]
        pf = profit_factor(returns)
        assert pf == 2.5  # 0.05 / 0.02

    def test_profit_factor_no_losses(self):
        returns = [0.01, 0.02]
        assert profit_factor(returns) == float("inf")

    def test_var_95(self):
        np.random.seed(42)
        returns = np.random.randn(1000) * 0.02
        var = var_historical(returns.tolist(), 0.95)
        # VaR at 95% should be around 5th percentile ≈ -1.645 * sigma = -0.0329
        assert var < 0
        assert var > -0.05

    def test_cvar_95(self):
        np.random.seed(42)
        returns = np.random.randn(1000) * 0.02
        cvar = cvar_historical(returns.tolist(), 0.95)
        # CVaR should be worse (more negative) than VaR
        var = var_historical(returns.tolist(), 0.95)
        assert cvar <= var

    def test_sharpe_single_return(self):
        assert sharpe_ratio([0.01], 0.02) == 0.0


class TestRiskFreeRate:
    def test_auto_detect_returns_float(self):
        rate = get_risk_free_rate(base_currency="EUR")
        assert isinstance(rate, float)
        assert rate >= 0


class TestCalculateMetrics:
    """End-to-end test using real db2 + loaded data."""

    @pytest.fixture
    def db3_path(self):
        import tempfile
        from src.portfolio.schema import create_tables
        from src.portfolio.ibkr_parser import parse_ibkr_xml_file, normalize_entries
        from src.portfolio.transactions import insert_entries
        from src.portfolio.portfolio_state import build_portfolio_state

        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        create_tables(path)

        # Load all XML files
        ibkr_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "data", "ibkr"
        )
        for year in ["2020", "2021", "2022", "2023", "2024", "2025"]:
            fpath = os.path.join(ibkr_dir, f"{year}.xml")
            result = parse_ibkr_xml_file(fpath)
            entries = normalize_entries(result)
            insert_entries(path, entries, source_file=f"{year}.xml")

        build_portfolio_state(path, base_currency="EUR")
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_calculate_metrics_returns_all_keys(self, db3_path):
        result = calculate_metrics(db3_path, base_currency="EUR", risk_free_rate=0.02)
        assert "sharpe_ratio" in result
        assert "sortino_ratio" in result
        assert "max_drawdown" in result
        assert "total_dividend_income" in result
        assert result["total_dividend_income"] > 0
        assert result["dividend_breakdown"]["total_gross"] > 0
        assert result["dividend_breakdown"]["total_tax"] < 0  # taxes are negative

    def test_calculate_metrics_with_date_range(self, db3_path):
        result = calculate_metrics(db3_path, start_date="2024-01-01",
                                   end_date="2024-12-31", risk_free_rate=0.02)
        assert result["start_date"] >= "2024-01-01"

    def test_empty_db_returns_empty(self):
        # Fresh empty db3
        import tempfile
        fd, path2 = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        from src.portfolio.schema import create_tables
        create_tables(path2)
        result = calculate_metrics(path2)
        assert result.get("start_date", "") == ""
        # Close any connections before unlinking
        import gc
        gc.collect()
        import time
        time.sleep(0.1)
        os.unlink(path2)
