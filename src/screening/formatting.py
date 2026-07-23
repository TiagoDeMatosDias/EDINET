"""Display-only formatting for screening results."""

from __future__ import annotations

import math

import pandas as pd

FORMAT_RULES: dict[str, str] = {
    "Margin": "percent",
    "Yield": "percent",
    "Payout": "percent",
    "Return": "percent",
    "Ratio": "ratio",
    "Turnover": "ratio",
    "Growth": "percent",
    "ZScore": "ratio",
    "MarketCap": "currency",
    "EnterpriseValue": "currency",
    "Price": "currency",
    "SharePrice": "currency",
}


def _format_grouped_number(value: float, decimals: int | None = None) -> str:
    if decimals is None:
        decimals = 0 if math.isclose(value, round(value), abs_tol=1e-9) else 3
    formatted = f"{value:,.{decimals}f}"
    return formatted.rstrip("0").rstrip(".") if decimals > 0 else formatted


def _infer_column_format(column_name: str) -> str | None:
    lowered = str(column_name).casefold()
    return next(
        (
            rule
            for pattern, rule in FORMAT_RULES.items()
            if pattern.casefold() in lowered
        ),
        None,
    )


def format_financial_value(
    value,
    column_name: str,
    formatted: bool = False,
) -> str:
    """Format a raw screening value without changing stored precision."""
    if value is None or pd.isna(value):
        return "—"
    if not formatted or isinstance(value, bool):
        return str(value)
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return str(value)
    if column_name == "ScreeningRank":
        return str(int(round(numeric_value)))
    if column_name == "ScreeningScore":
        return _format_grouped_number(numeric_value, 3)
    column_format = _infer_column_format(column_name)
    if column_format == "percent":
        return f"{numeric_value * 100:,.2f}%"
    if column_format == "currency":
        decimals = 0 if math.isclose(numeric_value, round(numeric_value), abs_tol=1e-9) else 2
        return _format_grouped_number(numeric_value, decimals)
    if column_format == "ratio":
        return _format_grouped_number(numeric_value, 2)
    return _format_grouped_number(numeric_value)
