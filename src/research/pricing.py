"""Market and balance-sheet inputs for the option and bond calculators.

The calculators themselves run in the browser so every assumption can be
changed instantly; this module supplies what a company's own data says: its
latest split-adjusted price, how volatile it has been, its dividend yield, and
the debt, interest, and balance-sheet lines that credit models need.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from itertools import pairwise
from typing import Any

from src.comparison.service import _STATEMENT_LABELS, _normalise_label, _number, _statement_rows

TRADING_DAYS = 252
# Realised-volatility look-back windows, in trading days.
VOLATILITY_WINDOWS: tuple[tuple[str, int], ...] = (("1M", 21), ("3M", 63), ("6M", 126), ("1Y", 252), ("3Y", 756))
# RiskMetrics decay for the exponentially weighted estimate.
EWMA_LAMBDA = 0.94
ROLLING_WINDOW = 63
ROLLING_STEP = 5

# Credit models describe operating companies; a bank's or insurer's balance
# sheet is mostly deposits and policy liabilities, so the results mislead.
FINANCIAL_INDUSTRIES = {"Banks", "Insurance", "Securities & Commodity Futures", "Other Financing Business"}

_BALANCE = ("BalanceSheet", "balance_sheet")
_INCOME = ("IncomeStatement", "income_statement")
# Interest-bearing debt, by the labels companies report it under.
DEBT_LABELS: dict[str, tuple[str, ...]] = {
    "ShortTermBorrowings": ("Short-term borrowings", "Short-term loans payable", "Current liabilities - Short-term loans payable"),
    "CommercialPaper": ("Commercial papers", "Commercial paper"),
    "CurrentBonds": ("Current portion of bonds payable", "Current portion of bonds", "Current portion of corporate bonds"),
    "CurrentLongTermBorrowings": (
        "Current portion of long-term borrowings",
        "Current portion of long-term loans payable",
        "Current liabilities - Current portion of long-term loans payable",
    ),
    "Bonds": ("Bonds payable", "Corporate bonds"),
    "LongTermBorrowings": ("Long-term borrowings", "Long-term loans payable", "Non-current liabilities - Long-term loans payable"),
}
CREDIT_LABELS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    **_STATEMENT_LABELS,
    "RetainedEarnings": (_BALANCE, ("Retained earnings",)),
    "Cash": (_BALANCE, ("Cash and deposits", "Cash and cash equivalents")),
    "InterestExpense": (_INCOME, (
        "Non-operating expenses - Interest expenses",
        "Interest expenses",
        "Financial expenses - Interest expenses",
        "Interest expense",
    )),
    **{key: (_BALANCE, labels) for key, labels in DEBT_LABELS.items()},
}


def log_returns(prices: list[float]) -> list[float]:
    """Daily log returns between consecutive positive prices."""
    return [
        math.log(current / previous)
        for previous, current in pairwise(prices)
        if previous > 0 and current > 0
    ]


def realized_volatility(returns: list[float], window: int) -> float | None:
    """Annualised standard deviation of the last ``window`` daily returns."""
    if window < 2 or len(returns) < window:
        return None
    sample = returns[-window:]
    mean = sum(sample) / window
    variance = sum((value - mean) ** 2 for value in sample) / (window - 1)
    return math.sqrt(variance * TRADING_DAYS)


def ewma_volatility(returns: list[float], decay: float = EWMA_LAMBDA) -> float | None:
    """Exponentially weighted volatility, which reacts faster to recent moves."""
    if len(returns) < 20:
        return None
    variance = sum(value * value for value in returns[:20]) / 20
    for value in returns[20:]:
        variance = decay * variance + (1 - decay) * value * value
    return math.sqrt(variance * TRADING_DAYS)


def rolling_volatility(dates: list[str], returns: list[float], window: int = ROLLING_WINDOW, step: int = ROLLING_STEP) -> list[dict[str, Any]]:
    """Realised volatility over a moving window, sampled every ``step`` days; ``dates[i]`` closes ``returns[i]``."""
    points: list[dict[str, Any]] = []
    for end in range(len(returns), window - 1, -step):
        value = realized_volatility(returns[:end], window)
        if value is not None:
            points.append({"date": dates[end - 1], "value": value})
    return points[::-1]


def _line_values(history: dict[str, Any], sources: tuple[str, ...], labels: tuple[str, ...], count: int) -> list[float | None]:
    wanted = {_normalise_label(label) for label in labels}
    matching = [
        row["values"] for row in _statement_rows(history, sources)
        if _normalise_label(row.get("field") or row.get("record_field") or row.get("metric")) in wanted
        and isinstance(row.get("values"), list)
    ]
    return [
        next((value for values in matching if index < len(values) and (value := _number(values[index])) is not None), None)
        for index in range(count)
    ]


def credit_inputs(history: dict[str, Any]) -> dict[str, Any]:
    """Balance-sheet and income lines from one fiscal year, the latest that reports total assets.

    Every line comes from that same year, so ratios never mix years; a line the
    company did not report is ``None``. Debt is the sum of the interest-bearing
    lines found, and the cost of debt is interest expense over average debt.
    """
    periods = [str(period) for period in history.get("periods", [])]
    count = len(periods)
    series = {key: _line_values(history, sources, labels, count) for key, (sources, labels) in CREDIT_LABELS.items()}
    index = next((i for i in range(count - 1, -1, -1) if series["TotalAssets"][i] is not None), None)
    if index is None:
        return {"period": None, "lines": {}, "debt": {}, "debt_total": None, "previous_debt_total": None, "interest_coverage": None, "cost_of_debt": None}

    def debt_total(at: int) -> float | None:
        values = [series[key][at] for key in DEBT_LABELS]
        found = [value for value in values if value is not None]
        return sum(found) if found else None

    lines = {key: values[index] for key, values in series.items() if key not in DEBT_LABELS}
    total = debt_total(index)
    previous = debt_total(index - 1) if index > 0 else None
    interest = lines.get("InterestExpense")
    operating = lines.get("OperatingIncome")
    average_debt = (total + previous) / 2 if total is not None and previous is not None else total
    return {
        "period": periods[index],
        "lines": lines,
        "debt": {key: series[key][index] for key in DEBT_LABELS},
        "debt_total": total,
        "previous_debt_total": previous,
        "interest_coverage": operating / interest if operating is not None and interest else None,
        "cost_of_debt": interest / average_debt if interest is not None and average_debt else None,
    }


def volatility_inputs(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Realised volatility estimates from daily price rows (``trade_date``, ``price``)."""
    points = [
        (str(row.get("trade_date") or row.get("date"))[:10], price)
        for row in rows
        if (price := _number(row.get("price"))) is not None and price > 0
    ]
    prices = [price for _, price in points]
    returns = log_returns(prices)
    dates = [day for day, _ in points[1:]]
    estimates = [{"window": label, "days": days, "value": realized_volatility(returns, days)} for label, days in VOLATILITY_WINDOWS]
    estimates.append({"window": "EWMA", "days": None, "value": ewma_volatility(returns)})
    history_returns = returns[-(3 * TRADING_DAYS + ROLLING_WINDOW):]
    return {
        "estimates": estimates,
        "history": rolling_volatility(dates[-len(history_returns):], history_returns),
        "observations": len(returns),
    }


def pricing_inputs(db: str, company_code: str, today: date | None = None) -> dict[str, Any] | None:
    """Everything the calculators can prefill for one company, or ``None`` if it is unknown."""
    from src.security_analysis import get_security_overview, get_security_statements
    from src.security_analysis.security_analysis import get_security_price_history
    from src.web_app.api.security_analysis import _compute_metrics

    from .positions import is_edinet_code

    # Research keys a holding without an EDINET company (a US share, an ETF) by its symbol.
    lookup = {"company_code": company_code} if is_edinet_code(company_code) else {"ticker": company_code}
    try:
        overview = get_security_overview(db, **lookup)
    except ValueError:
        return None
    company = overview.get("company", {}) or {}
    market = overview.get("market", {}) or {}
    metadata = overview.get("metadata", {}) or {}
    code = str(company.get("edinet_code") or company.get("company_code") or company_code)
    ticker = str(company.get("ticker") or "")
    metrics = _compute_metrics(db, code, market, company) if ticker else {}
    start = ((today or date.today()) - timedelta(days=5 * 365)).isoformat()
    prices = get_security_price_history(db, ticker, start_date=start, adjusted=True) if ticker else []
    history: dict[str, Any] = {}
    if is_edinet_code(code):
        try:
            history = get_security_statements(
                db, code, periods=4,
                statement_sources={"income": "IncomeStatement", "balance": "BalanceSheet"},
            )
        except ValueError:
            history = {}
    industry = str(company.get("industry") or "")
    return {
        "company": {
            "company_code": code,
            "company_name": company.get("company_name") or code,
            "ticker": ticker,
            "industry": industry,
        },
        "currency": {"price": market.get("price_currency"), "reporting": metadata.get("reporting_currency")},
        "spot": metrics.get("LatestPrice"),
        "price_date": prices[-1].get("trade_date") if prices else market.get("latest_price_date"),
        "dividend_yield": metrics.get("DividendsYield"),
        "market_cap": metrics.get("MarketCap"),
        "volatility": volatility_inputs(prices),
        "credit": credit_inputs(history),
        "financial": industry in FINANCIAL_INDUSTRIES,
    }
