"""Portfolio scenario calculations with explicit shock assumptions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class ScenarioShock:
    price_shocks: Mapping[str, float]
    fx_shocks: Mapping[str, float] | None = None
    cash_rate_change: float = 0.0


def apply_scenario(
    holdings: Mapping[str, float],
    prices: Mapping[str, float],
    currencies: Mapping[str, str],
    shock: ScenarioShock,
) -> dict[str, float]:
    """Return shocked market values by ticker; inputs are not mutated."""
    values: dict[str, float] = {}
    for ticker, shares in holdings.items():
        price = prices.get(ticker)
        if price is None or price < 0:
            raise ValueError(f"Missing or invalid price for {ticker}")
        price_factor = 1 + shock.price_shocks.get(ticker, 0.0)
        currency = currencies.get(ticker, "")
        fx_factor = 1 + (shock.fx_shocks or {}).get(currency, 0.0)
        values[ticker] = shares * price * price_factor * fx_factor
    return values
