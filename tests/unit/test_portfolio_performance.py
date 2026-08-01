"""Tests for portfolio performance metrics."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np

from src.portfolio.performance import (
    avg_loss,
    avg_win,
    calculate_metrics,
    calmar_ratio,
    cvar_historical,
    get_risk_free_rate,
    max_drawdown,
    max_drawdown_with_dates,
    profit_factor,
    sharpe_ratio,
    sortino_ratio,
    var_historical,
    win_rate,
)
from src.portfolio.schema import create_tables


class TestIndividualMetrics:
    def test_sharpe_ratio_zero_or_single_return(self) -> None:
        assert sharpe_ratio([0.0] * 252, 0.02) == 0.0
        assert sharpe_ratio([0.01], 0.02) == 0.0

    def test_sharpe_ratio_positive(self) -> None:
        returns = [0.002, 0.001, 0.003, -0.0005] * 63
        assert sharpe_ratio(returns, 0.02) > 0

    def test_sortino_exceeds_sharpe_when_upside_volatility_dominates(self) -> None:
        returns = [0.01, 0.02, -0.001] * 84
        sharpe = sharpe_ratio(returns, 0.0)
        sortino = sortino_ratio(returns, 0.0)
        assert sortino > sharpe > 0

    def test_max_drawdown_finds_peak_and_trough(self) -> None:
        drawdown, peak, trough = max_drawdown([100, 120, 80, 50, 70, 90])
        assert drawdown == (50 - 120) / 120
        assert peak == "1"
        assert trough == "3"

        dated_drawdown, peak_date, trough_date = max_drawdown_with_dates(
            [100, 50, 60],
            ["2024-01-01", "2024-02-01", "2024-03-01"],
        )
        assert dated_drawdown == -0.5
        assert peak_date == "2024-01-01"
        assert trough_date == "2024-02-01"

    def test_max_drawdown_is_zero_for_monotonic_growth(self) -> None:
        drawdown, _, _ = max_drawdown([100, 110, 120, 130])
        assert drawdown == 0.0

    def test_return_distribution_helpers(self) -> None:
        returns = [0.01, -0.02, 0.03, 0.0, -0.01]
        assert win_rate(returns) == 0.5
        assert avg_win(returns) == 0.02
        assert avg_loss(returns) == -0.015
        assert profit_factor([0.02, -0.01, 0.03, -0.01]) == 2.5
        assert profit_factor([0.01, 0.02]) == float("inf")

    def test_calmar_ratio(self) -> None:
        assert calmar_ratio(0.15, -0.30) == 0.5

    def test_historical_var_and_cvar(self) -> None:
        returns = (np.random.default_rng(42).standard_normal(1000) * 0.02).tolist()
        value_at_risk = var_historical(returns, 0.95)
        conditional_var = cvar_historical(returns, 0.95)

        assert -0.05 < value_at_risk < 0
        assert conditional_var <= value_at_risk


def test_risk_free_rate_uses_generated_inflation_series(market_db_path: str) -> None:
    rate = get_risk_free_rate(market_db_path, base_currency="EUR")
    assert isinstance(rate, float)
    assert rate >= 0


class TestCalculateMetrics:
    def test_calculate_metrics_returns_complete_result(
        self,
        populated_db3: str,
        market_db_path: str,
    ) -> None:
        result = calculate_metrics(
            populated_db3,
            db2_path=market_db_path,
            base_currency="EUR",
            risk_free_rate=0.02,
        )

        required = {
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown",
            "total_dividend_income",
            "dividend_breakdown",
        }
        assert required <= result.keys()
        assert result["total_dividend_income"] > 0
        assert result["dividend_breakdown"]["total_gross"] == 18
        assert result["dividend_breakdown"]["total_tax"] == -2.7

    def test_calculate_metrics_honors_date_range(
        self,
        populated_db3: str,
        market_db_path: str,
    ) -> None:
        result = calculate_metrics(
            populated_db3,
            db2_path=market_db_path,
            start_date="2024-01-05",
            end_date="2024-01-15",
            risk_free_rate=0.02,
        )
        assert result["start_date"] == "2024-01-05"
        assert result["end_date"] == "2024-01-15"

    def test_drawdown_ignores_withdrawals(
        self,
        tmp_path: Path,
        market_db_path: str,
    ) -> None:
        path = tmp_path / "flow-adjusted-drawdown.db"
        create_tables(str(path))
        with sqlite3.connect(path) as conn:
            conn.executemany(
                "INSERT INTO Portfolio_Daily "
                "(date, total_value, daily_return, cumulative_return, net_inflow) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    ("2024-01-01", 100.0, 0.0, 0.0, 100.0),
                    ("2024-01-02", 50.0, 0.0, 0.0, -50.0),
                    ("2024-01-03", 45.0, -0.1, -0.1, 0.0),
                ],
            )

        result = calculate_metrics(str(path), db2_path=market_db_path, risk_free_rate=0.0)

        assert np.isclose(result["max_drawdown"], -0.1)
        assert result["max_dd_peak_date"] == "2024-01-01"
        assert result["max_dd_trough_date"] == "2024-01-03"

    def test_date_range_rebases_inception_return(
        self,
        tmp_path: Path,
        market_db_path: str,
    ) -> None:
        path = tmp_path / "range-return.db"
        create_tables(str(path))
        with sqlite3.connect(path) as conn:
            conn.executemany(
                "INSERT INTO Portfolio_Daily "
                "(date, total_value, daily_return, cumulative_return, net_inflow) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    ("2024-01-01", 100.0, 0.0, 0.0, 100.0),
                    ("2024-01-02", 110.0, 0.1, 0.1, 0.0),
                    ("2024-01-03", 121.0, 0.1, 0.21, 0.0),
                ],
            )

        result = calculate_metrics(
            str(path),
            db2_path=market_db_path,
            start_date="2024-01-02",
            end_date="2024-01-03",
            risk_free_rate=0.0,
        )

        assert np.isclose(result["total_return"], 0.1)

    def test_empty_database_returns_minimal_result(
        self,
        tmp_path: Path,
        market_db_path: str,
    ) -> None:
        path = tmp_path / "empty.db"
        create_tables(str(path))

        result = calculate_metrics(str(path), db2_path=market_db_path)

        assert result == {"start_date": "", "end_date": "", "base_currency": "EUR"}
