"""Portfolio return attribution: holding contribution, currency, industry, benchmark."""

from __future__ import annotations

from typing import Any


def holding_contribution(
    beginning_weights: dict[str, float],
    local_returns: dict[str, float],
    fx_returns: dict[str, float] | None = None,
) -> dict[str, dict[str, float]]:
    """Allocate portfolio return to individual holdings.

    Uses beginning-of-period weights and local/FX returns.
    Returns per-ticker dicts with contribution, weight, and return components.
    """
    result: dict[str, dict[str, float]] = {}
    for ticker, weight in beginning_weights.items():
        local_ret = local_returns.get(ticker, 0.0)
        fx_ret = fx_returns.get(ticker, 0.0) if fx_returns else 0.0
        total_ret = (1 + local_ret) * (1 + fx_ret) - 1
        contribution = weight * total_ret
        result[ticker] = {
            "weight": weight,
            "local_return": local_ret,
            "fx_return": fx_ret,
            "total_return": total_ret,
            "contribution": contribution,
        }
    return result


def currency_attribution(
    beginning_weights: dict[str, float],
    local_returns: dict[str, float],
    fx_returns: dict[str, float],
) -> dict[str, Any]:
    """Decompose portfolio return into local, FX, and interaction components."""
    contributions = holding_contribution(beginning_weights, local_returns, fx_returns)
    total_local = sum(c["weight"] * c["local_return"] for c in contributions.values())
    total_fx = sum(c["weight"] * c["fx_return"] for c in contributions.values())
    total_interaction = sum(
        c["weight"] * c["local_return"] * c["fx_return"]
        for c in contributions.values()
    )
    total = total_local + total_fx + total_interaction
    return {
        "local_contribution": total_local,
        "fx_contribution": total_fx,
        "interaction": total_interaction,
        "total_return": total,
        "holdings": contributions,
    }


def industry_attribution(
    beginning_weights: dict[str, float],
    returns: dict[str, float],
    industry_map: dict[str, str],
) -> dict[str, dict[str, float]]:
    """Aggregate return contribution by industry classification."""
    industry_weights: dict[str, float] = {}
    industry_returns: dict[str, dict[str, float]] = {}

    for ticker, weight in beginning_weights.items():
        industry = industry_map.get(ticker, "Other")
        ret = returns.get(ticker, 0.0)
        industry_weights[industry] = industry_weights.get(industry, 0.0) + weight
        if industry not in industry_returns:
            industry_returns[industry] = {"weighted_return": 0.0, "weight": 0.0}
        industry_returns[industry]["weighted_return"] += weight * ret
        industry_returns[industry]["weight"] += weight

    result: dict[str, dict[str, float]] = {}
    for industry, data in industry_returns.items():
        total_weight = data["weight"]
        result[industry] = {
            "weight": total_weight,
            "contribution": data["weighted_return"],
            "industry_return": data["weighted_return"] / total_weight if total_weight > 0 else 0.0,
        }
    return result


def multi_period_link(period_returns: list[float]) -> float:
    """Compound a list of periodic returns into a total-period return."""
    cumulative = 1.0
    for r in period_returns:
        cumulative *= 1 + r
    return cumulative - 1


def contribution_reconciliation(
    contributions: dict[str, float],
    total_return: float,
    tolerance: float = 1e-6,
) -> dict[str, Any]:
    """Check that individual contributions sum to the total portfolio return."""
    summed = sum(contributions.values())
    residual = total_return - summed
    return {
        "total_return": total_return,
        "sum_of_contributions": summed,
        "residual": residual,
        "reconciled": abs(residual) < tolerance,
    }
