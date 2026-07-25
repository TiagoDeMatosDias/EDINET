"""Explicit data-quality checks for normalized XBRL facts."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterable


def assess_facts(doc_id: str, facts: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return explainable issues without changing source values."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    issues: list[dict[str, Any]] = []
    for fact in facts:
        fact_id = fact.get("fact_id", "")
        # Nil facts
        if fact.get("is_nil"):
            issues.append(_issue(doc_id, "info", "nil_fact", "Fact is explicitly nil", fact_id, now))
            continue
        # Missing context
        if not fact.get("context_id"):
            issues.append(_issue(doc_id, "warning", "missing_context", "Fact has no context reference", fact_id, now))
        # Non-numeric narrative value
        value = fact.get("value_text")
        if fact.get("numeric_value") is None and value not in (None, ""):
            issues.append(_issue(doc_id, "info", "non_numeric_value", "Fact value is narrative text", fact_id, now))
        # Negative values for normally positive concepts
        numeric = fact.get("numeric_value")
        concept = fact.get("concept", "")
        if numeric is not None and numeric < 0 and concept.lower() in {
            "revenue", "assets", "equity", "netincome",
        }:
            issues.append(_issue(doc_id, "warning", "negative_value",
                                 f"Unexpected negative value for {concept}: {numeric}", fact_id, now))
        # Extreme scale
        if numeric is not None and abs(numeric) > 1e15:
            issues.append(_issue(doc_id, "warning", "extreme_scale",
                                 f"Extreme numeric value: {numeric:.2e}", fact_id, now))
        # Missing unit for numeric facts
        if numeric is not None and not fact.get("unit_id"):
            issues.append(_issue(doc_id, "warning", "missing_unit",
                                 "Numeric fact lacks a unit reference", fact_id, now))
    return issues


def assess_filing(doc_id: str, filing_info: dict[str, Any]) -> list[dict[str, Any]]:
    """Filing-level quality checks beyond per-fact assessment."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    issues: list[dict[str, Any]] = []
    if not filing_info.get("edinet_code"):
        issues.append(_issue(doc_id, "warning", "missing_company_code",
                             "Filing has no EDINET code", None, now))
    if not filing_info.get("period_end"):
        issues.append(_issue(doc_id, "warning", "missing_period",
                             "Filing has no period end date", None, now))
    status = filing_info.get("status", "")
    if status == "failed":
        issues.append(_issue(doc_id, "error", "parse_failed",
                             filing_info.get("parse_error", "Filing parse failed"), None, now))
    return issues


def _issue(doc_id: str, severity: str, code: str, message: str, fact_id: str | None, created_at: str) -> dict[str, Any]:
    return {
        "issue_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}:{code}:{fact_id}")),
        "doc_id": doc_id,
        "severity": severity,
        "code": code,
        "message": message,
        "fact_id": fact_id,
        "created_at": created_at,
    }
