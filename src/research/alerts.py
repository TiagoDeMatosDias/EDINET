"""Deterministic in-app alert evaluation with deduplication and cooldown."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .storage import ResearchStore


def evaluate_expression(expression: dict[str, Any], values: dict[str, Any]) -> bool:
    """Evaluate one bounded comparison against a metric snapshot."""
    metric = str(expression.get("metric", "")).strip()
    operator = expression.get("operator")
    target = expression.get("value")
    actual = values.get(metric)
    if actual is None:
        return False
    try:
        if operator == ">":
            return actual > target
        if operator == ">=":
            return actual >= target
        if operator == "<":
            return actual < target
        if operator == "<=":
            return actual <= target
        if operator == "=":
            return actual == target
        if operator == "!=":
            return actual != target
    except (TypeError, ValueError):
        return False
    return False


def evaluate_price_crossing(
    metric: str,
    threshold: float,
    current_price: float,
    previous_price: float | None,
) -> tuple[bool, str]:
    """Detect a price threshold crossing.

    Only triggers on crossing, not on every observation above/below threshold.
    Returns (triggered, direction).
    """
    if current_price >= threshold and (previous_price is None or previous_price < threshold):
        return True, "crossed_up"
    if current_price <= threshold and (previous_price is None or previous_price > threshold):
        return True, "crossed_down"
    return False, ""


def evaluate_metric_change(
    current_value: float,
    previous_value: float | None,
    threshold_pct: float,
) -> bool:
    """Detect a material change in a metric relative to the prior observation."""
    if previous_value is None or previous_value == 0:
        return False
    change_pct = abs((current_value - previous_value) / previous_value)
    return change_pct >= threshold_pct


def build_dedupe_key(alert_id: str, company_code: str, context: str = "") -> str:
    """Create a stable deduplication key to prevent duplicate alert events."""
    return f"{alert_id}:{company_code}:{context}" if context else f"{alert_id}:{company_code}"


def evaluate_filing_alert(
    store: ResearchStore,
    alert: dict[str, Any],
    new_filing_doc_ids: list[str],
) -> list[dict[str, Any]]:
    """Evaluate a 'new filing' alert given recently discovered filings."""
    target_code = alert.get("edinet_code", "")
    if not target_code:
        return []
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    events: list[dict[str, Any]] = []
    for doc_id in new_filing_doc_ids:
        dedupe = build_dedupe_key(alert["alert_id"], target_code, doc_id)
        event = store.record_alert_event(
            alert["alert_id"],
            f'{{"doc_id":"{doc_id}","company_code":"{target_code}","occurred_at":"{now}"}}',
            dedupe_key=dedupe,
        )
        if event:
            events.append(event)
    return events


def evaluate_all_user_alerts(
    store: ResearchStore,
    user_id: str,
    *,
    price_snapshot: dict[str, float] | None = None,
    previous_prices: dict[str, float] | None = None,
    new_filing_doc_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Run all enabled alerts for a user and record triggered events.

    Returns a summary of evaluated/total/triggered counts.
    """
    alerts = store.list_alerts(user_id)
    triggered = 0
    for alert in alerts:
        if not alert.get("enabled"):
            continue
        import json

        try:
            expression = json.loads(alert.get("expression_json", "{}"))
        except (TypeError, json.JSONDecodeError):
            continue
        alert_code = alert.get("edinet_code", "")
        # Price crossing evaluation
        if expression.get("metric") == "LatestPrice" and price_snapshot:
            price = price_snapshot.get(alert_code, 0)
            previous = previous_prices.get(alert_code) if previous_prices else None
            crossing, _ = evaluate_price_crossing("LatestPrice", expression.get("value", 0), price, previous)
            if crossing:
                store.record_alert_event(
                    alert["alert_id"],
                    f'{{"price":{price},"threshold":{expression.get("value")}}}',
                    dedupe_key=build_dedupe_key(alert["alert_id"], alert_code),
                )
                triggered += 1
        # New filing evaluation
        if expression.get("kind") == "new_filing" and new_filing_doc_ids:
            events = evaluate_filing_alert(store, alert, new_filing_doc_ids)
            triggered += len(events)
    return {
        "total_alerts": len(alerts),
        "triggered": triggered,
    }
