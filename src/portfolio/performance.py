"""Portfolio performance metrics: returns, risk, income, inflation, and a benchmark.

``calculate_metrics`` reads the daily ledger (``Portfolio_Daily``, produced by
``portfolio_state.build_portfolio_state``), converts it to the display
currency at each day's ECB rate, and derives every statistic from one
flow-adjusted return series (see :mod:`src.portfolio.analytics` for the
conventions).  The small helpers below remain for callers that hold a plain
list of returns.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date as Date

import numpy as np

from src.orchestrator.common.db_config import get_db2, get_db3
from src.orchestrator.common.sqlite import connect_read
from src.portfolio import analytics
from src.portfolio.market_data import MarketData

logger = logging.getLogger(__name__)

TRADING_DAYS = 252


# ---------------------------------------------------------------------------
# Risk-free rate
# ---------------------------------------------------------------------------

def get_risk_free_rate(db2_path: str | None = None, base_currency: str = "EUR") -> float:
    """The latest short-term rate for *base_currency* as a decimal, or 0.0 without data.

    Rates come from ``RiskFree_{CUR}`` (three-month government yield or the
    overnight rate), stored by the FX and inflation pipeline step.
    """
    db2_path = db2_path or get_db2()
    try:
        conn = connect_read(db2_path)
    except (OSError, sqlite3.Error):
        return 0.0
    try:
        rate = analytics.load_risk_free(conn, base_currency)
    finally:
        conn.close()
    return rate.rates[-1] if rate.kind == "series" and rate.rates else 0.0


# ---------------------------------------------------------------------------
# Individual metrics on a list of daily returns
# ---------------------------------------------------------------------------

def _annualize_return(total_return: float, years: float) -> float:
    """Convert cumulative total return to annualised."""
    if years <= 0:
        return total_return
    if total_return <= -1:
        return -1.0
    return (1 + total_return) ** (1 / years) - 1


def sharpe_ratio(daily_returns: list[float], rf_annual: float) -> float:
    """Annualised Sharpe ratio: mean excess return over its standard deviation."""
    if len(daily_returns) < 2:
        return 0.0
    rf_daily = (1 + rf_annual) ** (1 / TRADING_DAYS) - 1
    excess = np.array(daily_returns) - rf_daily
    std_excess = np.std(excess, ddof=1)
    if std_excess == 0:
        return 0.0
    return float(np.mean(excess) / std_excess * np.sqrt(TRADING_DAYS))


def sortino_ratio(daily_returns: list[float], rf_annual: float) -> float:
    """Annualised Sortino ratio: mean excess return over the downside deviation.

    The downside deviation is the root mean square of returns below the
    risk-free target (shortfalls only; days above it count as zero).
    """
    if len(daily_returns) < 2:
        return 0.0
    rf_daily = (1 + rf_annual) ** (1 / TRADING_DAYS) - 1
    excess = np.array(daily_returns) - rf_daily
    downside = float(np.sqrt(np.mean(np.minimum(excess, 0) ** 2)))
    if downside == 0:
        return 0.0
    return float(np.mean(excess) / downside * np.sqrt(TRADING_DAYS))


def max_drawdown(cumulative: list[float]) -> tuple[float, str | None, str | None]:
    """Maximum drawdown with the indexes of its peak and trough (as strings)."""
    if not cumulative or len(cumulative) < 2:
        return 0.0, None, None

    max_dd = 0.0
    peak_idx = 0
    trough_idx = 0
    current_peak_idx = 0

    for i in range(1, len(cumulative)):
        if cumulative[i] > cumulative[current_peak_idx]:
            current_peak_idx = i
        dd = (cumulative[i] - cumulative[current_peak_idx]) / cumulative[current_peak_idx]
        if dd < max_dd:
            max_dd = dd
            peak_idx = current_peak_idx
            trough_idx = i

    return max_dd, str(peak_idx), str(trough_idx)


def max_drawdown_with_dates(values: list[float], dates: list[str]) -> tuple[float, str | None, str | None]:
    """Compute max drawdown and return actual date strings."""
    dd_val, peak_i_str, trough_i_str = max_drawdown(values)
    peak_date = dates[int(peak_i_str)] if peak_i_str is not None else None
    trough_date = dates[int(trough_i_str)] if trough_i_str is not None else None
    return dd_val, peak_date, trough_date


def calmar_ratio(ann_return: float, max_dd: float) -> float:
    """Annualised return / |max drawdown|."""
    if max_dd == 0 or max_dd >= 0:
        return 0.0
    return ann_return / abs(max_dd)


def win_rate(daily_returns: list[float]) -> float:
    """Fraction of days with positive returns (excluding zero-return days)."""
    if not daily_returns:
        return 0.0
    wins = sum(1 for r in daily_returns if r > 0)
    losses = sum(1 for r in daily_returns if r < 0)
    total = wins + losses
    return wins / total if total > 0 else 0.0


def avg_win(daily_returns: list[float]) -> float:
    """Average positive daily return."""
    wins = [r for r in daily_returns if r > 0]
    return float(np.mean(wins)) if wins else 0.0


def avg_loss(daily_returns: list[float]) -> float:
    """Average negative daily return."""
    losses = [r for r in daily_returns if r < 0]
    return float(np.mean(losses)) if losses else 0.0


def profit_factor(daily_returns: list[float]) -> float:
    """Gross profit / |gross loss|."""
    wins = sum(r for r in daily_returns if r > 0)
    losses = abs(sum(r for r in daily_returns if r < 0))
    return wins / losses if losses > 0 else float("inf") if wins > 0 else 0.0


def var_historical(daily_returns: list[float], confidence: float = 0.95) -> float:
    """Historical VaR at given confidence level (returns negative number)."""
    if len(daily_returns) < 2:
        return 0.0
    return float(np.percentile(daily_returns, (1 - confidence) * 100))


def cvar_historical(daily_returns: list[float], confidence: float = 0.95) -> float:
    """Historical CVaR (expected shortfall beyond VaR)."""
    if len(daily_returns) < 2:
        return 0.0
    var_val = var_historical(daily_returns, confidence)
    tail = [r for r in daily_returns if r <= var_val]
    return float(np.mean(tail)) if tail else var_val


# ---------------------------------------------------------------------------
# Main calculation
# ---------------------------------------------------------------------------

def _dividends(
    conn3: sqlite3.Connection,
    market: MarketData,
    currency: str,
    owner_user_id: str,
    first: str,
    last: str,
    whole_history: bool,
) -> dict:
    """Gross dividends, withholding tax, and net income in *currency*.

    Each payment is converted at the broker's booking rate into the account
    currency (EUR) and then at that day's ECB rate into *currency*.
    """
    params: list = [owner_user_id]
    window = ""
    if not whole_history:
        window = " AND trade_date >= ? AND trade_date <= ?"
        params.extend([first, last])
    rows = conn3.execute(
        "SELECT activity_type, amount, currency, fx_rate_to_base, trade_date FROM Transactions "
        "WHERE owner_user_id = ? AND activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX')" + window,
        params,
    ).fetchall()
    gross = tax = 0.0
    for activity, amount, native, broker_rate, day in rows:
        day = str(day)[:10]
        if broker_rate:
            value = (amount or 0.0) * broker_rate * (market.rate("EUR", currency, day) or 1.0)
        else:
            value = (amount or 0.0) * (market.rate(native or "EUR", currency, day) or 1.0)
        if activity == "WITHHOLDING_TAX":
            tax += value
        else:
            gross += abs(value)
    return {"total_gross": gross, "total_tax": tax, "total_net": gross + tax}


def _benchmark(
    market: MarketData,
    ticker: str,
    currency: str,
    base_date: str,
    dates: list[str],
    portfolio: list[float],
    risk_free: analytics.RiskFree,
) -> tuple[dict, list[float | None]]:
    """Benchmark statistics on the portfolio's weekday grid and its cumulative path."""
    series = market.price_series(ticker, currency)
    empty: list[float | None] = [None] * len(dates)
    if series is None or len(series.dates) < 2:
        return {"ticker": ticker, "available": False, "message": f"No stored prices for {ticker}."}, empty
    grid = [base_date, *dates]
    prices = [series.on(day) for day in grid]
    returns: list[float | None] = []
    for previous, current in zip(prices[:-1], prices[1:], strict=False):
        if previous is None or current is None or previous[0] <= 0:
            returns.append(None)
        else:
            returns.append(current[0] / previous[0] - 1.0)
    covered = [index for index, value in enumerate(returns) if value is not None]
    if len(covered) < 2:
        return {"ticker": ticker, "available": False, "message": f"{ticker} has no prices in this period."}, empty
    first, last = covered[0], covered[-1]
    overlap_dates = dates[first:last + 1]
    bench = [value or 0.0 for value in returns[first:last + 1]]
    port = portfolio[first:last + 1]
    overlap_base = grid[first]
    total = analytics.chain(bench)
    port_total = analytics.chain(port)
    days = (analytics._day(overlap_dates[-1]) - analytics._day(overlap_base)).days
    bench_ann = analytics.annualize(total, days)
    port_ann = analytics.annualize(port_total, days)
    risk = analytics.risk_statistics(overlap_dates, bench, risk_free, overlap_base)
    wealth = analytics.wealth_path(bench)
    drawdown = analytics.drawdown_statistics(overlap_dates, wealth, overlap_base)
    bench_months = analytics.period_returns(overlap_dates, bench, 7)
    port_months = dict(analytics.period_returns(overlap_dates, port, 7))
    pairs = [(port_months[month], value) for month, value in bench_months if month in port_months]
    relative = analytics.relative_statistics(overlap_dates, port, bench, risk_free, overlap_base, pairs)
    stale_days = (analytics._day(dates[-1]) - analytics._day(series.last_date)).days if series.last_date else None
    path: list[float | None] = [None] * len(dates)
    level = 1.0
    for index in range(first, last + 1):
        level *= 1.0 + (returns[index] or 0.0)
        path[index] = level - 1.0
    for index in range(last + 1, len(dates)):
        path[index] = path[last]
    info = {
        "ticker": ticker,
        "available": True,
        "price_ticker": series.ticker,
        "price_currency": series.source_currency,
        "coverage_start": overlap_base,
        "coverage_end": overlap_dates[-1],
        "full_coverage": first == 0 and last == len(dates) - 1,
        "last_price_date": series.last_date,
        "stale_days": stale_days,
        "total_return": total,
        "annualized_return": bench_ann,
        "portfolio_total_return": port_total,
        "portfolio_annualized_return": port_ann,
        "excess_return": (1 + port_ann) / (1 + bench_ann) - 1 if port_ann is not None and bench_ann is not None else None,
        "relative_return": (1 + port_total) / (1 + total) - 1 if total > -1 else None,
        "volatility": risk.get("volatility"),
        "sharpe_ratio": risk.get("sharpe_ratio"),
        "max_drawdown": drawdown["max_drawdown"],
        **{key: relative.get(key) for key in (
            "beta", "alpha", "correlation", "r_squared", "tracking_error",
            "information_ratio", "up_capture", "down_capture", "weeks",
        )},
        "monthly": dict(bench_months),
    }
    return info, path


def calculate_metrics(
    db3_path: str | None = None,
    db2_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    risk_free_rate: float | None = None,
    benchmark_ticker: str | None = None,
    base_currency: str = "EUR",
    owner_user_id: str = "",
) -> dict:
    """Compute performance metrics for the portfolio over a period.

    Args:
        db3_path, db2_path: Database paths (defaults from config).
        start_date, end_date: The period (defaults to the whole history).
        risk_free_rate: Annual decimal rate overriding the stored short-term
            rate series for the currency.
        benchmark_ticker: Ticker to compare against (None = no benchmark).
        base_currency: Currency every value and return is expressed in.

    Returns:
        A dict with the headline statistics (the original keys), plus
        ``period``, ``risk_free``, ``inflation``, ``series`` (weekday path for
        charts), ``monthly_returns``, ``annual_returns``, and ``warnings``.
    """
    db3_path = db3_path or get_db3()
    db2_path = db2_path or get_db2()
    currency = (base_currency or "EUR").upper()
    empty = {"start_date": start_date or "", "end_date": end_date or "", "base_currency": currency}

    conn3 = connect_read(db3_path)
    conn2 = connect_read(db2_path)
    try:
        market = MarketData(conn2)
        daily = analytics.load_daily_series(conn3, market, currency, owner_user_id)
        if not daily.dates:
            return empty
        returns = analytics.flow_adjusted_returns(daily.values, daily.flows)
        price_only = analytics.flow_adjusted_returns(daily.values, daily.flows, daily.income)

        upper = len(daily.dates) - 1
        if end_date:
            while upper >= 0 and daily.dates[upper] > end_date:
                upper -= 1
        # The period runs from the close of *start_date* (its base) to the
        # close of *end_date*; before the first deposit the base moves to the
        # first invested day.
        lower = 0
        if start_date:
            while lower <= upper and daily.dates[lower] < start_date:
                lower += 1
        first = next((index for index in range(lower + 1, upper + 1) if returns[index] is not None), None)
        if first is None:
            return {**empty, "start_date": daily.dates[max(lower, 0)] if daily.dates else "", "end_date": daily.dates[upper] if upper >= 0 else ""}
        base = first - 1
        base_date = daily.dates[base]
        end = daily.dates[upper]
        window_dates = daily.dates[first:upper + 1]
        window = returns[first:upper + 1]
        days = (analytics._day(end) - analytics._day(base_date)).days

        total = analytics.chain(window)
        price_total = analytics.chain(price_only[first:upper + 1])
        annualized = analytics.annualize(total, days)

        weekday_dates, weekday = analytics.weekday_returns(window_dates, window)
        wealth = analytics.wealth_path(weekday)
        rf = analytics.load_risk_free(conn2, currency, risk_free_rate)
        risk = analytics.risk_statistics(weekday_dates, weekday, rf, base_date)
        drawdown = analytics.drawdown_statistics(weekday_dates, wealth, base_date)

        inflation_index = analytics.load_inflation(conn2, currency)
        inflation = inflation_index.between(base_date, end) if inflation_index else None
        inflation_total = inflation["total"] if inflation else None
        real_total = (1 + total) / (1 + inflation_total) - 1 if inflation_total is not None else None

        flows = [(daily.dates[index], -daily.flows[index]) for index in range(first, upper + 1) if daily.flows[index]]
        cashflows = [(base_date, -daily.values[base]), *flows, (end, daily.values[upper])]
        irr = analytics.xirr(cashflows)
        mwr_period = (1 + irr) ** (days / 365.25) - 1 if irr is not None else None

        dividends = _dividends(conn3, market, currency, owner_user_id, window_dates[0], end, whole_history=start_date is None and end_date is None)

        benchmark = None
        benchmark_path: list[float | None] = [None] * len(weekday_dates)
        if benchmark_ticker:
            benchmark, benchmark_path = _benchmark(market, benchmark_ticker.strip(), currency, base_date, weekday_dates, weekday, rf)

        # Weekday path for the charts: growth, drawdown, value against money put in.
        value_by_date = dict(zip(daily.dates, daily.values, strict=False))
        invested = daily.values[base]
        invested_by_date: dict[str, float] = {}
        for index in range(first, upper + 1):
            invested += daily.flows[index]
            invested_by_date[daily.dates[index]] = invested
        series = []
        for index, day in enumerate(weekday_dates):
            point = {
                "date": day,
                "cumulative_return": round(wealth[index] - 1, 6),
                "drawdown": round(drawdown["series"][index], 6),
                "value": round(value_by_date.get(day, 0.0), 2),
                "invested": round(invested_by_date.get(day, 0.0), 2),
            }
            if benchmark_path[index] is not None:
                point["benchmark"] = round(benchmark_path[index], 6)
            if inflation_index:
                level = inflation_index.between(base_date, day)
                if level:
                    point["inflation"] = round(level["total"], 6)
            series.append(point)

        monthly = analytics.period_returns(weekday_dates, weekday, 7)
        annual = analytics.period_returns(weekday_dates, weekday, 4)
        bench_monthly = benchmark.get("monthly", {}) if benchmark else {}
        bench_annual = dict(analytics.period_returns(
            weekday_dates,
            [value or 0.0 for value in _path_returns(benchmark_path)],
            4,
        )) if benchmark and benchmark.get("available") else {}
        if benchmark:
            benchmark.pop("monthly", None)
            if benchmark.get("available") and not benchmark.get("full_coverage"):
                bench_annual = {year: value for year, value in bench_annual.items() if year >= benchmark["coverage_start"][:4]}

        warnings = []
        if annualized is None:
            warnings.append({"code": "short_period", "message": "Under a year of history: returns are not annualized, and the Calmar ratio is omitted."})
        risk_info = rf.describe(base_date, end)
        if rf.kind == "missing":
            warnings.append({"code": "risk_free_missing", "message": f"No short-term interest rate is stored for {currency}, so the Sharpe and Sortino ratios use 0%. Run Update FX Data or enter a rate."})
        elif rf.kind == "series" and risk_info.get("stale"):
            warnings.append({"code": "risk_free_stale", "message": f"The {currency} short-term rate ends {rf.dates[-1]}; later days use that rate."})
        if inflation and inflation["estimated_months"]:
            warnings.append({"code": "inflation_estimated", "message": f"Inflation is published to {inflation['last_observation']}; the last {inflation['estimated_months']} month(s) are extrapolated at the trailing annual rate."})
        if inflation is None:
            warnings.append({"code": "inflation_missing", "message": f"No consumer price index is stored for {currency}, so the real return is not available."})
        if benchmark and not benchmark.get("available"):
            warnings.append({"code": "benchmark_missing", "message": benchmark.get("message", "The benchmark has no prices.")})
        elif benchmark and not benchmark.get("full_coverage"):
            warnings.append({"code": "benchmark_partial", "message": f"{benchmark['ticker']} prices cover {benchmark['coverage_start']} to {benchmark['coverage_end']}; the comparison uses those dates only."})
        if benchmark and benchmark.get("available") and (benchmark.get("stale_days") or 0) > 7:
            warnings.append({"code": "benchmark_stale", "message": f"{benchmark['ticker']}'s last stored price is {benchmark['last_price_date']}; later days repeat it."})
        if daily.fx_missing:
            warnings.append({"code": "fx_missing", "message": f"{len(daily.fx_missing)} day(s) have no {currency} exchange rate and are left out."})

        distribution = risk.get("return_distribution") or {}
        calmar = annualized / abs(drawdown["max_drawdown"]) if annualized is not None and drawdown["max_drawdown"] < 0 else None
        return {
            "start_date": base_date,
            "end_date": end,
            "base_currency": currency,
            "period": {
                "start": base_date,
                "end": end,
                "days": days,
                "years": days / 365.25,
                "observations": len(weekday),
                "annualized": annualized is not None,
                "requested_start": start_date,
            },
            "total_return": total,
            "annualized_return": annualized,
            "price_return": price_total,
            "income_return": total - price_total,
            "money_weighted_return": irr if days >= analytics.MIN_DAYS_TO_ANNUALIZE else None,
            "money_weighted_period_return": mwr_period,
            "volatility": risk.get("volatility"),
            "downside_deviation": risk.get("downside_deviation"),
            "sharpe_ratio": risk.get("sharpe_ratio"),
            "sortino_ratio": risk.get("sortino_ratio"),
            "max_drawdown": drawdown["max_drawdown"],
            "max_dd_peak_date": drawdown["max_dd_peak_date"],
            "max_dd_trough_date": drawdown["max_dd_trough_date"],
            "max_dd_recovery_date": drawdown["max_dd_recovery_date"],
            "max_dd_days": drawdown["max_dd_days"],
            "current_drawdown": drawdown["current_drawdown"],
            "calmar_ratio": calmar,
            "win_rate": risk.get("win_rate"),
            "avg_win": risk.get("avg_win"),
            "avg_loss": risk.get("avg_loss"),
            "profit_factor": risk.get("profit_factor"),
            "var_95": risk.get("var_95"),
            "cvar_95": risk.get("cvar_95"),
            "positive_months": sum(1 for _month, value in monthly if value > 0),
            "months": len(monthly),
            "total_dividend_income": dividends["total_net"],
            "risk_free_rate": risk.get("risk_free_rate", rf.constant),
            "cash_return": risk.get("cash_return"),
            "risk_free": risk_info,
            "inflation": inflation,
            "dividend_breakdown": dividends,
            "return_distribution": distribution or None,
            "return_attribution": {
                "total_return": total,
                "dividend_yield": total - price_total,
                "capital_appreciation": price_total,
                "real_return": real_total,
                "inflation_total": inflation_total,
            },
            "benchmark": benchmark,
            "series": series,
            "monthly_returns": [
                {"month": month, "portfolio": value, "benchmark": bench_monthly.get(month)}
                for month, value in monthly
            ],
            "annual_returns": [
                {
                    "year": int(year),
                    "portfolio": value,
                    "benchmark": bench_annual.get(year),
                    # The first year starts after its 1 January; the last may end before 31 December.
                    "partial": year == base_date[:4] or (year == end[:4] and end[5:] < "12-31"),
                }
                for year, value in annual
            ],
            "inflation_series": [],
            "warnings": warnings,
            "computed_at": Date.today().isoformat(),
        }
    finally:
        conn3.close()
        conn2.close()


def _path_returns(path: list[float | None]) -> list[float | None]:
    """Turn a cumulative path back into period returns (None before it starts)."""
    out: list[float | None] = []
    previous: float | None = None
    for value in path:
        if value is None:
            out.append(None)
            continue
        out.append((1 + value) / (1 + previous) - 1 if previous is not None else value)
        previous = value
    return out
