"""Drill-down views of a saved single backtest, built from its stored result.

The saved ``result.json`` keeps the daily tracker (``daily``), the yearly
per-holding rows, and the dividend ledger. This module turns them into what
the workspace and the HTML report show below the portfolio level: each
holding's growth, yearly figures, and dividends; how the allocation drifted;
and how much each holding added to the portfolio's return over time.
"""

from __future__ import annotations

import math
from typing import Any

MAX_POINTS = 260


def _num(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _sample_indices(count: int, limit: int = MAX_POINTS) -> list[int]:
    if count <= limit:
        return list(range(count))
    step = (count - 1) / (limit - 1)
    indices = sorted({round(i * step) for i in range(limit)})
    if indices[-1] != count - 1:
        indices.append(count - 1)
    return indices


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def holding_tickers(result: dict[str, Any]) -> list[str]:
    tickers = [str(row.get("Ticker", "")) for row in result.get("per_company") or [] if row.get("Ticker")]
    if tickers:
        return tickers
    daily = result.get("daily") or []
    return sorted({key[len("shares_"):] for key in (daily[0] if daily else {}) if key.startswith("shares_")})


def build_single_detail(result: dict[str, Any]) -> dict[str, Any]:
    """Per-holding drill-down for one saved single backtest."""
    metrics = result.get("metrics") or {}
    daily = result.get("daily") or []
    tickers = holding_tickers(result)
    initial = _num(metrics.get("initial_capital")) or 0.0
    per_company = {str(row.get("Ticker")): row for row in result.get("per_company") or []}
    names = result.get("names") or {}
    yearly_rows = [row for row in result.get("per_company_per_year") or [] if not str(row.get("Ticker", "")).startswith("BENCH:")]
    payments = result.get("dividend_payments") or []
    indices = _sample_indices(len(daily))
    dates = [str(daily[i].get("Date", ""))[:10] for i in indices]

    allocation: dict[str, list[float | None]] = {}
    contribution: dict[str, list[float | None]] = {}
    holdings: list[dict[str, Any]] = []
    for ticker in tickers:
        company = per_company.get(ticker, {})
        capital = _num(company.get("capital_invested")) or 0.0
        growth: list[float | None] = []
        weights: list[float | None] = []
        added: list[float | None] = []
        entered = False
        for i in indices:
            day = daily[i]
            value = _num(day.get(f"mktval_{ticker}")) or 0.0
            dividends = _num(day.get(f"div_cash_{ticker}")) or 0.0
            total = _num(day.get("portfolio_total")) or 0.0
            entered = entered or value > 0
            worth = value + dividends if entered else capital
            growth.append(_round(worth / capital) if capital else None)
            weights.append(_round(value / total) if total else None)
            added.append(_round((worth - capital) / initial) if initial else None)
        allocation[ticker] = weights
        contribution[ticker] = added
        years = [{
            "year": int(row.get("Year")),
            "start_date": row.get("Start_Date") or None,
            "end_date": row.get("End_Date") or None,
            "start_price": _num(row.get("Start_Price")),
            "end_price": _num(row.get("End_Price")),
            "price_return": _num(row.get("Price_Return_Pct")),
            "dividend_return": _num(row.get("Dividend_Return_Pct")),
            "total_return": _num(row.get("Total_Return_Pct")),
            "contribution": _num(row.get("Weighted_Return")),
            "dividend_per_share": _num(row.get("Dividend_Per_Share")),
            "dividends_received": _num(row.get("Total_Dividends_Received")),
            "start_weight": _num(row.get("Weighted_Value_Start")),
        } for row in yearly_rows if str(row.get("Ticker")) == ticker]
        holdings.append({
            "ticker": ticker,
            "name": names.get(ticker, ""),
            "currency": company.get("Currency") or "",
            "weight": _num(company.get("weight")),
            "start_price": _num(company.get("start_price")),
            "end_price": _num(company.get("end_price")),
            "price_return": _num(company.get("price_return")),
            "dividend_return": _num(company.get("dividend_return")),
            "total_return": _num(company.get("total_return")),
            "contribution": _num(company.get("weighted_total")),
            "capital_invested": capital or None,
            "shares": _num(company.get("shares_purchased")),
            "market_value": _num(company.get("market_value")),
            "dividends_received": _num(company.get("dividends_received")),
            "growth": growth,
            "years": years,
            "dividends": [payment for payment in payments if str(payment.get("ticker")) == ticker],
        })

    cash = []
    for i in indices:
        total = _num(daily[i].get("portfolio_total")) or 0.0
        cash.append(_round((_num(daily[i].get("cash")) or 0.0) / total) if total else None)
    portfolio = [_round((_num(daily[i].get("cumulative_return")) or 0.0) - 1.0) for i in indices]

    year_list = sorted({year["year"] for holding in holdings for year in holding["years"]})
    contribution_by_year = [
        {"year": year, **{holding["ticker"]: next((item["contribution"] for item in holding["years"] if item["year"] == year), None) for holding in holdings}}
        for year in year_list
    ]
    return {
        "start_date": metrics.get("start_date"),
        "end_date": metrics.get("end_date"),
        "base_currency": metrics.get("base_currency") or "",
        "initial_capital": initial,
        "dates": dates,
        "portfolio": portfolio,
        "holdings": sorted(holdings, key=lambda item: -(item["contribution"] or 0.0)),
        "allocation": {"holdings": allocation, "cash": cash},
        "contribution": contribution,
        "contribution_by_year": contribution_by_year,
        "dividend_payments": payments,
    }
