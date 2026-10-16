"""Queue a validated pipeline as a ``pipeline`` job in jobs.db."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.paths import jobs_dir

if TYPE_CHECKING:
    from src.jobs.service import JobRecord

# ``src.jobs`` imports ``src.pipeline_jobs.redaction``, so the job service is
# imported inside the functions to keep this package importable first.


def pipeline_workspace(job_id: str) -> Path:
    """The folder holding one pipeline job's uploads (created by the job service on enqueue)."""
    return jobs_dir() / job_id


def enqueue_pipeline(
    config: dict[str, Any],
    steps: list[dict[str, Any]],
    *,
    job_id: str,
    owner_user_id: str | None,
    setup_name: str | None = None,
    schedule_id: str | None = None,
) -> "JobRecord":
    """Persist the job and its steps; a worker picks it up when one is free."""
    from src.jobs.service import get_job_service

    if not steps:
        raise ValueError("At least one pipeline step is required")
    normalized = [dict(step) for step in steps]
    payload = {
        "config": dict(config),
        "steps": normalized,
        "workspace": str(pipeline_workspace(job_id)),
        "setup_name": setup_name,
        "schedule_id": schedule_id,
    }
    title = f"Pipeline: {setup_name}" if setup_name else "Pipeline"
    return get_job_service().enqueue(
        "pipeline",
        payload,
        owner_user_id=owner_user_id,
        title=title,
        steps=normalized,
        job_id=job_id,
    )


def active_pipeline_count() -> int:
    """Pipeline jobs that are waiting or running."""
    from src.jobs.service import get_job_service

    return len(
        get_job_service().list_jobs(kinds=["pipeline"], active_only=True, limit=1)
    )
