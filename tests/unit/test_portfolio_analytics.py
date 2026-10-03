"""Return and risk analytics: the conventions every Portfolio statistic rests on."""

from __future__ import annotations

import math
import sqlite3

import pytest

from src.portfolio import analytics
from src.portfolio.analytics import (
    RiskFree,
    annualize,
    chain,
    drawdown_statistics,
    flow_adjusted_returns,
    load_inflation,
    period_returns,
    relative_statistics,
    risk_statistics,
    weekday_returns,
    xirr,
)


def test_deposits_and_withdrawals_are_not_returns() -> None:
    # 100 in, +10% on day 2, then 50 deposited (worth 160), then 80 withdrawn.
    values = [100.0, 110.0, 160.0, 80.0]
    flows = [100.0, 0.0, 50.0, -80.0]
    returns = flow_adjusted_returns(values, flows)
    assert returns[0] is None
    assert returns[1] == pytest.approx(0.10)
    assert returns[2] == pytest.approx(0.0)
    assert returns[3] == pytest.approx(0.0)
    assert chain(returns) == pytest.approx(0.10)


def test_nothing_invested_is_not_a_return() -> None:
    returns = flow_adjusted_returns([0.0, 0.0, 100.0, 101.0], [0.0, 0.0, 100.0, 0.0])
    assert returns[:2] == [None, None]
    assert returns[2] == pytest.approx(0.0)
    assert returns[3] == pytest.approx(0.01)


def test_price_return_removes_dividends_received() -> None:
    values = [100.0, 103.0]
    flows = [100.0, 0.0]
    total = flow_adjusted_returns(values, flows)
    price_only = flow_adjusted_returns(values, flows, income=[0.0, 2.0])
    assert total[1] == pytest.approx(0.03)
    assert price_only[1] == pytest.approx(0.01)


def test_weekend_returns_carry_into_monday() -> None:
    # Fri 2024-01-05, Sat, Sun, Mon 2024-01-08.
    dates = ["2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08"]
    days, returns = weekday_returns(dates, [0.01, 0.0, 0.02, 0.01])
    assert days == ["2024-01-05", "2024-01-08"]
    assert returns[0] == pytest.approx(0.01)
    assert returns[1] == pytest.approx(1.02 * 1.01 - 1)


def test_annualize_needs_a_year() -> None:
    assert annualize(0.05, 200) is None
    assert annualize(0.21, 730) == pytest.approx(1.21 ** (365.25 / 730) - 1)
    assert annualize(-1.0, 800) is None


def test_monthly_returns_compound_inside_each_month() -> None:
    dates = ["2024-01-30", "2024-01-31", "2024-02-01"]
    months = dict(period_returns(dates, [0.01, 0.02, -0.01], 7))
    assert months["2024-01"] == pytest.approx(1.01 * 1.02 - 1)
    assert months["2024-02"] == pytest.approx(-0.01)


def test_risk_free_accrues_over_calendar_days() -> None:
    rate = RiskFree("series", "EUR", dates=["2024-01-01"], rates=[0.0365], ticker="RiskFree_EUR")
    assert rate.accrual("2024-01-05", "2024-01-08") == pytest.approx(1.0365 ** (3 / 365.25) - 1)
    assert RiskFree("missing", "CHF").accrual("2024-01-05", "2024-01-08") == 0.0


def test_sharpe_and_sortino_follow_their_definitions() -> None:
    dates = [f"2024-01-{day:02d}" for day in (2, 3, 4, 5, 8, 9, 10, 11, 12, 15)]
    returns = [0.01, -0.005, 0.004, 0.002, -0.01, 0.006, 0.003, -0.002, 0.008, 0.001]
    cash = RiskFree("override", "EUR", constant=0.0)
    stats = risk_statistics(dates, returns, cash, "2024-01-01")
    mean = sum(returns) / len(returns)
    sd = math.sqrt(sum((value - mean) ** 2 for value in returns) / (len(returns) - 1))
    downside = math.sqrt(sum(min(value, 0) ** 2 for value in returns) / len(returns))
    assert stats["volatility"] == pytest.approx(sd * math.sqrt(261))
    assert stats["sharpe_ratio"] == pytest.approx(mean / sd * math.sqrt(261))
    assert stats["sortino_ratio"] == pytest.approx(mean / downside * math.sqrt(261))
    assert stats["win_rate"] == pytest.approx(7 / 10)


def test_a_portfolio_that_never_moves_has_no_sharpe_ratio() -> None:
    dates = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"]
    stats = risk_statistics(dates, [0.0] * 5, RiskFree("override", "EUR", constant=0.03), "2024-01-01")
    assert stats["sharpe_ratio"] is None
    assert stats["sortino_ratio"] is None


def test_drawdown_reports_peak_trough_and_recovery() -> None:
    dates = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"]
    wealth = [1.10, 0.88, 0.99, 1.12, 1.05]
    stats = drawdown_statistics(dates, wealth, "2024-01-01")
    assert stats["max_drawdown"] == pytest.approx(0.88 / 1.10 - 1)
    assert stats["max_dd_peak_date"] == "2024-01-02"
    assert stats["max_dd_trough_date"] == "2024-01-03"
    assert stats["max_dd_recovery_date"] == "2024-01-05"
    assert stats["current_drawdown"] == pytest.approx(1.05 / 1.12 - 1)


def test_xirr_matches_a_simple_annual_rate() -> None:
    rate = xirr([("2023-01-01", -100.0), ("2024-01-01", 110.0)])
    assert rate == pytest.approx(1.10 ** (365.25 / 365) - 1, rel=1e-6)
    assert xirr([("2023-01-01", 100.0)]) is None


def test_a_benchmark_identical_to_the_portfolio_has_beta_one() -> None:
    dates = []
    returns = []
    day = 0
    import datetime

    start = datetime.date(2024, 1, 1)
    while len(dates) < 120:
        current = start + datetime.timedelta(days=day)
        day += 1
        if current.weekday() < 5:
            dates.append(current.isoformat())
            returns.append(0.01 * math.sin(len(dates) / 3))
    stats = relative_statistics(dates, returns, returns, RiskFree("override", "EUR", constant=0.0), "2023-12-29", [])
    assert stats["beta"] == pytest.approx(1.0)
    assert stats["correlation"] == pytest.approx(1.0)
    assert stats["tracking_error"] == pytest.approx(0.0, abs=1e-12)


def test_inflation_extrapolates_unpublished_months() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Currency TEXT, Price REAL)")
    rows = [(f"{2023 + (index // 12)}-{index % 12 + 1:02d}-01", "Inflation_EUR", "EUR", 100.0 * 1.002 ** index) for index in range(18)]
    conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", rows)
    index = load_inflation(conn, "EUR")
    assert index is not None
    result = index.between("2023-01-15", "2024-09-15")
    # Published to 2024-06; July to September grow at the trailing monthly rate.
    assert result["estimated_months"] == 3
    assert result["total"] == pytest.approx(1.002 ** 20 - 1, rel=1e-6)
    assert analytics.inflation_between(conn, "USD", "2023-01-01", "2024-01-01") is None
