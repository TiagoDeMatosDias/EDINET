"""Server side of screening jobs: persist a finished screen as the user's last result."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from src.jobs.service import JobRecord
from src.screening.job_runner import (
    computed_column_specs,
    criteria_to_dicts,
    ranking_rules_to_dicts,
)

logger = logging.getLogger(__name__)


def screening_definition(request: dict[str, Any]) -> dict[str, Any]:
    """The serialisable screen definition stored with a result (from a request dict)."""
    return {
        "criteria": criteria_to_dicts(request.get("criteria") or []),
        "criteria_match": request.get("criteria_match") or "all",
        "columns": list(request.get("columns") or []),
        "computed_columns": computed_column_specs(request.get("computed_columns") or []),
        "screening_date": request.get("screening_date"),
        "period": request.get("period"),
        "ranking_algorithm": request.get("ranking_algorithm") or "none",
        "ranking_rules": ranking_rules_to_dicts(request.get("ranking_rules") or []),
        "sort_by": request.get("sort_by"),
        "sort_order": request.get("sort_order") or "DESC",
    }


def persist_screening_result(user_id: str, request: dict[str, Any], result: dict[str, Any]) -> dict[str, Any] | None:
    """Persist a user's last result and add it to the recent-work stream."""
    try:
        from src.portfolio.screening_results import replace_last_screening_result
        from src.research.runtime import store as research_store

        definition = screening_definition(request)
        stored = replace_last_screening_result(user_id, result, definition)
        fingerprint = hashlib.sha256(
            json.dumps(definition, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
        ).hexdigest()[:24]
        research_store.record_recent_work(
            user_id,
            "screen",
            f"definition:{fingerprint}",
            f"Screen · {result.get('row_count', 0):,} matches",
            f"As of {request.get('screening_date') or 'latest available data'}",
            "/screen",
            {"row_count": result.get("row_count", 0), "definition": definition},
        )
        return stored
    except Exception as exc:  # noqa: BLE001 - a cache failure must not discard a completed screen
        logger.warning("Could not persist latest screening result for %s: %s", user_id, exc, exc_info=True)
        return None


def finalize(job: JobRecord, result: dict[str, Any], artifacts: dict[str, Any]) -> dict[str, Any]:
    """Store the screen as the owner's last result and return it with ``last_run_at``."""
    final = dict(result)
    if job.owner_user_id:
        stored = persist_screening_result(job.owner_user_id, job.payload.get("request") or {}, final)
        if stored:
            final["last_run_at"] = stored["updated_at"]
    return final
