"""Pipeline job query and cancellation endpoints, backed by jobs.db.

All routes require operator or admin permission. The response shapes are the
original pipeline ones; ``open`` jobs are reported as ``pending``.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from src.auth.models import AuthenticatedUser
from src.jobs.errors import InvalidJobState
from src.jobs.protocol import PUBLIC_PIPELINE_STATUS, TERMINAL_STATUSES
from src.jobs.service import JobRecord, get_job_service

from .models import CancelJobRequest, JobOutputResponse, JobStatus

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs", tags=["pipeline jobs"])

_NOT_FOUND = "Pipeline job not found"


def _require_operator(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser) or user.role not in ("admin", "operator"):
        raise HTTPException(status_code=403, detail="Operator permission required")
    return user


def _status_of(record: JobRecord) -> str:
    return PUBLIC_PIPELINE_STATUS.get(record.status, record.status)


def _job_status(record: JobRecord, steps: list[dict[str, Any]]) -> JobStatus:
    current = next((s["step_name"] for s in steps if s["status"] == "running"), None)
    return JobStatus(
        job_id=record.job_id,
        status=_status_of(record),
        current_step=current,
        progress_percent=record.progress_percent,
        step_count=len(steps),
        completed_step_count=sum(1 for s in steps if s["status"] == "completed"),
        created_at=record.created_at,
        started_at=record.started_at,
        completed_at=record.completed_at,
        error_message=record.error_message,
        status_message=record.status_message,
        steps=steps,
    )


def _load(job_id: str) -> tuple[JobRecord, list[dict[str, Any]]]:
    service = get_job_service()
    try:
        record = service.get(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=_NOT_FOUND) from exc
    if record.kind != "pipeline":
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return record, service.steps(job_id)


@router.get("/{job_id}", response_model=JobStatus)
def get_job_status(request: Request, job_id: str) -> JobStatus:
    """Return the current persisted status of a pipeline job."""
    _require_operator(request)
    return _job_status(*_load(job_id))


@router.post("/{job_id}/cancel", response_model=JobStatus)
def cancel_job(request: Request, job_id: str, payload: CancelJobRequest) -> JobStatus:
    """Cancel a waiting job at once, or ask the worker running it to stop."""
    _require_operator(request)
    record, _steps = _load(job_id)
    if payload.force and record.status in ("open", *TERMINAL_STATUSES):
        # Hard cancellation only means something while a worker holds the job.
        detail = (
            "Cannot cancel job with status '%s'" % _status_of(record)
            if record.status in TERMINAL_STATUSES
            else "Forced cancellation is available only while the pipeline is running"
        )
        raise HTTPException(status_code=409, detail=detail)
    service = get_job_service()
    try:
        service.cancel(job_id, force=payload.force)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=_NOT_FOUND) from exc
    except InvalidJobState as exc:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel job with status '{_status_of(record)}'",
        ) from exc
    logger.info("Cancellation requested for job %s (force=%s)", job_id, payload.force)
    return _job_status(*_load(job_id))


@router.get("", response_model=list[JobStatus])
def list_jobs(
    request: Request,
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[JobStatus]:
    """Return the most recently created pipeline jobs."""
    _require_operator(request)
    service = get_job_service()
    records = service.list_jobs(kinds=["pipeline"], limit=limit, offset=offset)
    return [_job_status(record, service.steps(record.job_id)) for record in records]


@router.get("/{job_id}/output", response_model=JobOutputResponse)
def get_job_output(request: Request, job_id: str) -> JobOutputResponse:
    """Return bounded, redacted output for a terminal pipeline job."""
    _require_operator(request)
    record, steps = _load(job_id)
    if record.status not in TERMINAL_STATUSES:
        raise HTTPException(
            status_code=409,
            detail="Job output is available only after termination",
        )
    output: dict[str, Any] = {}
    for step in steps:
        if step["status"] == "completed" and step["result_json"] is not None:
            output[str(step["step_name"])] = json.loads(step["result_json"])
    return JobOutputResponse(
        job_id=record.job_id,
        status=_status_of(record),
        output=output,
    )
