"""Return and risk analytics for a portfolio, from one daily return series.

Every statistic on the Portfolio page is derived here, from the same
flow-adjusted daily returns, so the headline return, the monthly map, the
drawdown chart, and the ratios cannot disagree.

Conventions
-----------
* Values are end-of-day; deposits and withdrawals arrive at the start of
  the day: ``r_t = (V_t - V_{t-1} - F_t) / (V_{t-1} + F_t)``.  Chaining the
  daily returns gives the time-weighted return (TWR).
* The ledger values every calendar day, but markets do not move at the
  weekend.  Risk statistics use weekday returns (weekend days are carried
  into Monday) and annualize with 261 weekdays a year.
* The risk-free rate is a short-term rate in the display currency (three
  month government yield or overnight rate), accrued over calendar days.
* Benchmark comparisons (beta, correlation, tracking error) use weekly
  returns: Tokyo, Frankfurt, and New York close at different hours, so
  same-day returns of a global portfolio understate co-movement.
"""

from __future__ import annotations

import math
import sqlite3
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date as Date

import numpy as np

from src.portfolio.market_data import MarketData

PERIODS_PER_YEAR = 261
WEEKS_PER_YEAR = 52.18
MIN_DAYS_TO_ANNUALIZE = 365

# Short-term rates stored by the FX/inflation pipeline step (annual percent).
RISK_FREE_SOURCES = {
    "EUR": "Euro area 3-month AAA government bond yield (ECB)",
    "USD": "3-month US Treasury bill (FRED DTB3)",
    "GBP": "Sterling overnight index average, SONIA (FRED IUDSOIA)",
    "JPY": "Japan overnight call rate (FRED IRSTCI01JPM156N, monthly)",
}


def risk_free_ticker(currency: str) -> str:
    return f"RiskFree_{currency.upper()}"


def _day(text: str) -> Date:
    return Date.fromisoformat(text[:10])


def is_weekday(text: str) -> bool:
    return _day(text).weekday() < 5


# ---------------------------------------------------------------------------
# Series
# ---------------------------------------------------------------------------


@dataclass
class DailySeries:
    """The ledger's calendar-day series in the display currency."""

    dates: list[str]
    values: list[float]
    flows: list[float]
    income: list[float]
    currency: str
    fx_missing: list[str] = field(default_factory=list)


def load_daily_series(
    conn3: sqlite3.Connection,
    market: MarketData,
    currency: str,
    owner_user_id: str = "",
) -> DailySeries:
    """Read ``Portfolio_Daily`` (stored in EUR) and convert each day at that day's ECB rate."""
    rows = conn3.execute(
        "SELECT date, total_value, net_inflow, dividend_income FROM Portfolio_Daily "
        "WHERE owner_user_id = ? ORDER BY date",
        (owner_user_id,),
    ).fetchall()
    currency = (currency or "EUR").upper()
    dates: list[str] = []
    values: list[float] = []
    flows: list[float] = []
    income: list[float] = []
    missing: list[str] = []
    for row in rows:
        day = str(row[0])[:10]
        rate = market.rate("EUR", currency, day)
        if rate is None:
            missing.append(day)
            continue
        dates.append(day)
        values.append(float(row[1] or 0.0) * rate)
        flows.append(float(row[2] or 0.0) * rate)
        income.append(float(row[3] or 0.0) * rate)
    return DailySeries(dates, values, flows, income, currency, missing)


def flow_adjusted_returns(
    values: list[float],
    flows: list[float],
    income: list[float] | None = None,
) -> list[float | None]:
    """Daily returns with external flows at the start of the day.

    With *income*, dividends received that day are removed too, which gives
    the price-only return.  ``None`` marks days with nothing invested (before
    the first deposit or after everything was withdrawn).
    """
    returns: list[float | None] = [None] * len(values)
    for index in range(1, len(values)):
        base = values[index - 1] + flows[index]
        if base <= 0.01:
            continue
        received = income[index] if income else 0.0
        change = values[index] - values[index - 1] - flows[index] - received
        # A single-day move beyond ±100% is a pricing error, not a return.
        returns[index] = max(min(change / base, 1.0), -1.0)
    return returns


def chain(returns: list[float | None]) -> float:
    growth = 1.0
    for value in returns:
        if value is not None:
            growth *= 1.0 + value
    return growth - 1.0


def weekday_returns(dates: list[str], returns: list[float | None]) -> tuple[list[str], list[float]]:
    """Compound weekend and holiday-free calendar returns into weekday observations."""
    out_dates: list[str] = []
    out: list[float] = []
    growth = 1.0
    pending = False
    for day, value in zip(dates, returns, strict=False):
        if value is not None:
            growth *= 1.0 + value
            pending = True
        if is_weekday(day) and pending:
            out_dates.append(day)
            out.append(growth - 1.0)
            growth = 1.0
            pending = False
    if pending and out:
        out[-1] = (1.0 + out[-1]) * growth - 1.0
    return out_dates, out


def wealth_path(returns: list[float]) -> list[float]:
    wealth = []
    level = 1.0
    for value in returns:
        level *= 1.0 + value
        wealth.append(level)
    return wealth


def annualize(total: float | None, days: int) -> float | None:
    """Compound annual rate for a return over *days*; None under a year, where it would extrapolate."""
    if total is None or days < MIN_DAYS_TO_ANNUALIZE or total <= -1:
        return None
    return (1.0 + total) ** (365.25 / days) - 1.0


def period_returns(dates: list[str], returns: list[float | None], width: int) -> list[tuple[str, float]]:
    """Chain daily returns into calendar periods (``width`` 7 for months, 4 for years)."""
    out: dict[str, float] = {}
    for day, value in zip(dates, returns, strict=False):
        if value is None:
            continue
        key = day[:width]
        out[key] = (1.0 + out.get(key, 0.0)) * (1.0 + value) - 1.0
    return sorted(out.items())


# ---------------------------------------------------------------------------
# Rates
# ---------------------------------------------------------------------------


@dataclass
class RiskFree:
    """Annual short-term rates (decimal) by date, or a constant."""

    kind: str  # "series", "override", or "missing"
    currency: str
    dates: list[str] = field(default_factory=list)
    rates: list[float] = field(default_factory=list)
    constant: float = 0.0
    ticker: str | None = None

    def annual(self, day: str) -> float:
        if self.kind != "series" or not self.dates:
            return self.constant
        index = bisect_right(self.dates, day) - 1
        return self.rates[max(index, 0)]

    def accrual(self, start: str, end: str) -> float:
        """Interest earned from *start* to *end* at the rate prevailing at *start*."""
        days = (_day(end) - _day(start)).days
        if days <= 0:
            return 0.0
        return (1.0 + self.annual(start)) ** (days / 365.25) - 1.0

    def describe(self, start: str, end: str) -> dict:
        info: dict = {"kind": self.kind, "currency": self.currency, "ticker": self.ticker}
        if self.kind == "series":
            info["source"] = RISK_FREE_SOURCES.get(self.currency, self.ticker)
            info["first_date"] = self.dates[0]
            info["last_date"] = self.dates[-1]
            info["stale"] = (_day(end) - _day(self.dates[-1])).days > 45
        elif self.kind == "override":
            info["source"] = "Your rate"
        else:
            info["source"] = f"No short-term rate data for {self.currency}; 0% assumed"
        return info


def load_risk_free(conn2: sqlite3.Connection, currency: str, override: float | None = None) -> RiskFree:
    currency = (currency or "EUR").upper()
    if override is not None:
        return RiskFree("override", currency, constant=float(override))
    ticker = risk_free_ticker(currency)
    try:
        rows = conn2.execute(
            "SELECT Date, Price FROM Stock_Prices WHERE Ticker = ? AND Price IS NOT NULL ORDER BY Date",
            (ticker,),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    if not rows:
        return RiskFree("missing", currency, ticker=ticker)
    return RiskFree(
        "series", currency,
        dates=[str(row[0])[:10] for row in rows],
        rates=[float(row[1]) / 100.0 for row in rows],
        ticker=ticker,
    )


@dataclass
class InflationIndex:
    """A monthly consumer price index; a date takes its month's level."""

    ticker: str
    months: list[str]
    levels: list[float]
    trailing: float  # monthly rate over the last published year

    def level(self, day: str) -> tuple[float, int] | None:
        """Index level for *day* and how many months past the last release it is extrapolated."""
        month = day[:7]
        index = bisect_right(self.months, month) - 1
        if index < 0:
            return None
        last = self.months[index]
        gap = (int(month[:4]) - int(last[:4])) * 12 + int(month[5:7]) - int(last[5:7])
        return self.levels[index] * (1 + self.trailing) ** gap, gap

    def between(self, start: str, end: str) -> dict | None:
        start_level = self.level(start)
        end_level = self.level(end)
        if not start_level or not end_level:
            return None
        total = end_level[0] / start_level[0] - 1.0
        return {
            "ticker": self.ticker,
            "total": total,
            "annualized": annualize(total, (_day(end) - _day(start)).days),
            "last_observation": self.months[-1],
            "estimated_months": max(end_level[1], 0),
            "trailing_annual_rate": (1 + self.trailing) ** 12 - 1,
        }


def load_inflation(conn2: sqlite3.Connection, currency: str) -> InflationIndex | None:
    """The CPI index for *currency*; months after the last release grow at the trailing rate."""
    ticker = f"Inflation_{(currency or 'EUR').upper()}"
    try:
        rows = conn2.execute(
            "SELECT Date, Price FROM Stock_Prices WHERE Ticker = ? AND Price > 0 ORDER BY Date",
            (ticker,),
        ).fetchall()
    except sqlite3.OperationalError:
        return None
    if len(rows) < 13:
        return None
    months: list[str] = []
    levels: list[float] = []
    for row in rows:
        month = str(row[0])[:7]
        if months and months[-1] == month:
            levels[-1] = float(row[1])
            continue
        months.append(month)
        levels.append(float(row[1]))
    if len(levels) < 13:
        return None
    trailing = (levels[-1] / levels[-13]) ** (1 / 12) - 1 if levels[-13] > 0 else 0.0
    return InflationIndex(ticker, months, levels, trailing)


def inflation_between(conn2: sqlite3.Connection, currency: str, start: str, end: str) -> dict | None:
    """Consumer price inflation from *start* to *end* (see :class:`InflationIndex`)."""
    index = load_inflation(conn2, currency)
    return index.between(start, end) if index else None


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def _clean(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def risk_statistics(dates: list[str], returns: list[float], risk_free: RiskFree, base_date: str) -> dict:
    """Volatility, Sharpe, Sortino, tail risk, and daily distribution for weekday returns."""
    stats: dict = {"observations": len(returns)}
    if len(returns) < 2:
        return stats
    values = np.array(returns, dtype=float)
    previous = [base_date, *dates[:-1]]
    cash = np.array([risk_free.accrual(prev, day) for prev, day in zip(previous, dates, strict=False)])
    excess = values - cash
    sd = float(np.std(values, ddof=1))
    # Sharpe divides the mean excess return by the portfolio's own volatility:
    # cash accrues three days of interest on Mondays, which would otherwise
    # add spurious variance to the excess series.
    downside = float(np.sqrt(np.mean(np.minimum(excess, 0.0) ** 2))) if sd > 1e-12 else 0.0
    var_95 = float(np.percentile(values, 5))
    tail = values[values <= var_95]
    gains = values[values > 0]
    losses = values[values < 0]
    stats.update({
        "volatility": sd * math.sqrt(PERIODS_PER_YEAR),
        "sharpe_ratio": _clean(float(np.mean(excess)) / sd * math.sqrt(PERIODS_PER_YEAR)) if sd > 1e-12 else None,
        "sortino_ratio": _clean(float(np.mean(excess)) / downside * math.sqrt(PERIODS_PER_YEAR)) if downside > 0 else None,
        "downside_deviation": downside * math.sqrt(PERIODS_PER_YEAR),
        "var_95": var_95,
        "cvar_95": float(np.mean(tail)) if len(tail) else var_95,
        "win_rate": len(gains) / (len(gains) + len(losses)) if len(gains) + len(losses) else None,
        "avg_win": float(np.mean(gains)) if len(gains) else None,
        "avg_loss": float(np.mean(losses)) if len(losses) else None,
        "profit_factor": _clean(float(np.sum(gains)) / abs(float(np.sum(losses)))) if len(losses) and np.sum(losses) else None,
        "cash_return": float(np.prod(1.0 + cash) - 1.0),
        "risk_free_rate": float((np.prod(1.0 + cash)) ** (PERIODS_PER_YEAR / len(cash)) - 1.0),
    })
    try:
        from scipy import stats as scipy_stats

        skewness = float(scipy_stats.skew(values)) if len(values) >= 3 else None
        kurtosis = float(scipy_stats.kurtosis(values)) if len(values) >= 4 else None
    except Exception:  # noqa: BLE001 - shape statistics are optional
        skewness = kurtosis = None
    stats["return_distribution"] = {
        "min": float(np.min(values)),
        "p25": float(np.percentile(values, 25)),
        "median": float(np.median(values)),
        "p75": float(np.percentile(values, 75)),
        "max": float(np.max(values)),
        "skewness": _clean(skewness),
        "kurtosis": _clean(kurtosis),
        "positive_days": int(len(gains)),
        "negative_days": int(len(losses)),
        "zero_days": int(len(values) - len(gains) - len(losses)),
        "best_day_date": dates[int(np.argmax(values))],
        "worst_day_date": dates[int(np.argmin(values))],
    }
    return stats


def drawdown_statistics(dates: list[str], wealth: list[float], base_date: str) -> dict:
    """Deepest fall from a previous high, when it started, bottomed, and recovered."""
    path_dates = [base_date, *dates]
    path = [1.0, *wealth]
    peak_level, peak_index = path[0], 0
    worst, worst_peak, worst_trough = 0.0, 0, 0
    drawdowns: list[float] = []
    for index, level in enumerate(path):
        if level > peak_level:
            peak_level, peak_index = level, index
        drawdown = level / peak_level - 1.0 if peak_level > 0 else 0.0
        drawdowns.append(drawdown)
        if drawdown < worst:
            worst, worst_peak, worst_trough = drawdown, peak_index, index
    recovery = None
    if worst < 0:
        target = path[worst_peak]
        for index in range(worst_trough + 1, len(path)):
            if path[index] >= target:
                recovery = path_dates[index]
                break
    end_of_episode = recovery or path_dates[-1]
    return {
        "max_drawdown": worst,
        "max_dd_peak_date": path_dates[worst_peak] if worst < 0 else None,
        "max_dd_trough_date": path_dates[worst_trough] if worst < 0 else None,
        "max_dd_recovery_date": recovery,
        "max_dd_days": (_day(end_of_episode) - _day(path_dates[worst_peak])).days if worst < 0 else 0,
        "current_drawdown": drawdowns[-1],
        "series": drawdowns[1:],
    }


def weekly(dates: list[str], returns: list[float]) -> tuple[list[str], list[float]]:
    """Chain weekday returns into ISO weeks, dated by each week's last observation."""
    keys: list[tuple[int, int]] = []
    out_dates: list[str] = []
    out: list[float] = []
    for day, value in zip(dates, returns, strict=False):
        iso = _day(day).isocalendar()
        key = (iso[0], iso[1])
        if keys and keys[-1] == key:
            out[-1] = (1.0 + out[-1]) * (1.0 + value) - 1.0
            out_dates[-1] = day
        else:
            keys.append(key)
            out_dates.append(day)
            out.append(value)
    return out_dates, out


def relative_statistics(
    dates: list[str],
    portfolio: list[float],
    benchmark: list[float],
    risk_free: RiskFree,
    base_date: str,
    monthly_pairs: list[tuple[float, float]],
) -> dict:
    """Beta, alpha, correlation, tracking error, and capture against a benchmark."""
    week_dates, port_weeks = weekly(dates, portfolio)
    _same_dates, bench_weeks = weekly(dates, benchmark)
    stats: dict = {"weeks": len(port_weeks)}
    if len(port_weeks) < 12:
        return stats
    previous = [base_date, *week_dates[:-1]]
    cash = np.array([risk_free.accrual(prev, day) for prev, day in zip(previous, week_dates, strict=False)])
    rp = np.array(port_weeks)
    rb = np.array(bench_weeks)
    ep = rp - cash
    eb = rb - cash
    variance = float(np.var(eb, ddof=1))
    beta = float(np.cov(ep, eb, ddof=1)[0, 1] / variance) if variance > 0 else None
    active = rp - rb
    tracking = float(np.std(active, ddof=1)) * math.sqrt(WEEKS_PER_YEAR)
    correlation = float(np.corrcoef(rp, rb)[0, 1]) if np.std(rp) > 0 and np.std(rb) > 0 else None
    stats.update({
        "beta": _clean(beta),
        "alpha": _clean((float(np.mean(ep)) - beta * float(np.mean(eb))) * WEEKS_PER_YEAR) if beta is not None else None,
        "correlation": _clean(correlation),
        "r_squared": _clean(correlation ** 2) if correlation is not None else None,
        "tracking_error": tracking,
        "information_ratio": _clean(float(np.mean(active)) * WEEKS_PER_YEAR / tracking) if tracking > 0 else None,
    })
    ups = [(p, b) for p, b in monthly_pairs if b > 0]
    downs = [(p, b) for p, b in monthly_pairs if b < 0]
    if len(ups) >= 3:
        stats["up_capture"] = _clean(float(np.mean([p for p, _ in ups]) / np.mean([b for _, b in ups])))
    if len(downs) >= 3:
        stats["down_capture"] = _clean(float(np.mean([p for p, _ in downs]) / np.mean([b for _, b in downs])))
    return stats


def xirr(cashflows: list[tuple[str, float]]) -> float | None:
    """Annual internal rate of return for dated cash flows (investor's view), or None."""
    flows = [(day, amount) for day, amount in cashflows if abs(amount) > 1e-9]
    if len(flows) < 2 or not any(amount > 0 for _, amount in flows) or not any(amount < 0 for _, amount in flows):
        return None
    origin = _day(flows[0][0])
    times = np.array([(_day(day) - origin).days / 365.25 for day, _ in flows])
    amounts = np.array([amount for _, amount in flows])

    def npv(rate: float) -> float:
        return float(np.sum(amounts / (1.0 + rate) ** times))

    try:
        from scipy.optimize import brentq

        low, high = -0.9999, 10.0
        if npv(low) * npv(high) > 0:
            return None
        return float(brentq(npv, low, high, maxiter=200))
    except Exception:  # noqa: BLE001 - no well-defined rate
        return None
