"""Provenance resolution: trace a displayed value to its source facts and quality evidence."""

from __future__ import annotations

import hashlib
from typing import Any

from .catalog import FilingCatalog


def _observation_hash(company_code: str, metric_id: str, period_end: str, value_numeric: float | None) -> str:
    payload = f"{company_code}|{metric_id}|{period_end}|{value_numeric}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def resolve_provenance(
    catalog: FilingCatalog,
    company_code: str,
    metric_id: str,
    period_end: str | None = None,
    observation_id: str | None = None,
) -> dict[str, Any] | None:
    """Return a full provenance trace for the best matching observation.

    Accepts either an explicit observation_id or a company/metric/period tuple.
    """
    if observation_id:
        obs = catalog.get_observation(observation_id)
    else:
        rows = catalog.find_observations(company_code, metric_id, period_end, limit=1)
        obs = rows[0] if rows else None

    if obs is None:
        return None

    sources = catalog.get_observation_sources(dict(obs)["observation_id"])
    metric = catalog.get_metric(metric_id)
    filing = catalog.get_filing(dict(obs).get("doc_id", "")) if dict(obs).get("doc_id") else None
    quality_issues = catalog.list_quality_issues(dict(filing)["doc_id"]) if filing else []

    result: dict[str, Any] = {
        "observation": dict(obs),
        "metric": dict(metric) if metric else None,
        "sources": [dict(s) for s in sources],
        "filing": {
            "doc_id": dict(filing).get("doc_id"),
            "submitted_at": dict(filing).get("submitted_at"),
            "archive_sha256": dict(filing).get("archive_sha256"),
        } if filing else None,
        "quality_issues": [dict(i) for i in quality_issues],
    }
    return result


def provenance_batch(
    catalog: FilingCatalog,
    requests: list[dict[str, Any]],
) -> list[dict[str, Any] | None]:
    """Resolve provenance for multiple observation requests in one call."""
    return [
        resolve_provenance(
            catalog,
            company_code=req.get("company_code", ""),
            metric_id=req.get("metric_id", ""),
            period_end=req.get("period_end"),
            observation_id=req.get("observation_id"),
        )
        for req in requests
    ]
