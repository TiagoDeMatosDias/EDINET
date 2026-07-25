"""Company comparison calculations: common-size, growth bridges, percentiles."""

from __future__ import annotations

from typing import Any


def common_size_income(metrics: dict[str, float | None]) -> dict[str, float | None]:
    """Scale income-statement metrics by revenue where available."""
    revenue = metrics.get("Revenue")
    if not revenue or revenue == 0:
        return {k: None for k in metrics}
    return {k: (v / revenue if v is not None else None) for k, v in metrics.items()}


def common_size_balance(metrics: dict[str, float | None]) -> dict[str, float | None]:
    """Scale balance-sheet metrics by total assets where available."""
    assets = metrics.get("TotalAssets")
    if not assets or assets == 0:
        return {k: None for k in metrics}
    return {k: (v / assets if v is not None else None) for k, v in metrics.items()}


def growth_rate(current: float | None, previous: float | None) -> float | None:
    """Year-over-year growth rate; None when either value is missing or zero."""
    if current is None or previous is None or previous == 0:
        return None
    return (current - previous) / previous


def growth_matrix(
    current_metrics: dict[str, dict[str, float | None]],
    previous_metrics: dict[str, dict[str, float | None]],
) -> dict[str, dict[str, float | None]]:
    """Compute growth rates per company per metric."""
    result: dict[str, dict[str, float | None]] = {}
    all_codes = set(current_metrics.keys()) | set(previous_metrics.keys())
    for code in all_codes:
        current = current_metrics.get(code, {})
        previous = previous_metrics.get(code, {})
        metric_names = set(current.keys()) | set(previous.keys())
        result[code] = {
            metric: growth_rate(current.get(metric), previous.get(metric))
            for metric in metric_names
        }
    return result


def peer_percentile(
    company_code: str,
    metric: str,
    metrics_map: dict[str, dict[str, float | None]],
) -> float | None:
    """Compute the percentile rank of one company within a peer set.

    Uses the 'midpoint' tie method. Returns None when fewer than two
    comparable values exist.
    """
    values = []
    company_value = None
    for code, company_metrics in metrics_map.items():
        val = company_metrics.get(metric)
        if val is not None:
            values.append(val)
            if code == company_code:
                company_value = val
    if company_value is None or len(values) < 2:
        return None
    sorted_values = sorted(values)
    below = sum(1 for v in sorted_values if v < company_value)
    ties = sum(1 for v in sorted_values if v == company_value) - 1
    n = len(sorted_values)
    percentile = (below + 0.5 * ties) / n
    return percentile


def normalize_companies(
    company_codes: list[str],
    overviews: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Produce a consistent comparison matrix with common-size and percentile data."""
    metrics_map: dict[str, dict[str, float | None]] = {}
    for code in company_codes:
        overview = overviews.get(code, {})
        raw_metrics = overview.get("metrics", {})
        metrics_map[code] = {
            k: v for k, v in raw_metrics.items()
            if isinstance(v, (int, float))
        }

    result: dict[str, dict[str, Any]] = {}
    for code in company_codes:
        company_metrics = metrics_map.get(code, {})
        percentile_map: dict[str, float | None] = {}
        for metric in company_metrics:
            percentile_map[metric] = peer_percentile(code, metric, metrics_map)

        result[code] = {
            "metrics": company_metrics,
            "common_size_income": common_size_income(company_metrics),
            "common_size_balance": common_size_balance(company_metrics),
            "percentiles": percentile_map,
            "company_name": overviews.get(code, {}).get("company", {}).get("company_name"),
        }
    return result
