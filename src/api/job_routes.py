"""Durable pipeline job query and cancellation endpoints.

All routes require operator or admin permission.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request

from src.auth.models import AuthenticatedUser
from src.pipeline_jobs import ForceCancellationUnsupported, InvalidJobState

from . import runtime
from .models import CancelJobRequest, JobOutputResponse, JobStatus

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs", tags=["pipeline jobs"])


def _require_operator(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser) or user.role not in ("admin", "operator"):
        raise HTTPException(status_code=403, detail="Operator permission required")
    return user


@router.get("/{job_id}", response_model=JobStatus)
def get_job_status(request: Request, job_id: str) -> JobStatus:
    """Return the current persisted status of a pipeline job."""
    _require_operator(request)
    try:
        return JobStatus(**runtime.job_manager.get_job(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Pipeline job not found") from exc


@router.post("/{job_id}/cancel", response_model=JobStatus)
def cancel_job(request: Request, job_id: str, payload: CancelJobRequest) -> JobStatus:
    """Request cooperative cancellation of a pipeline job."""
    _require_operator(request)
    try:
        job = runtime.job_manager.cancel(job_id, force=payload.force)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Pipeline job not found") from exc
    except (ForceCancellationUnsupported, InvalidJobState) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    logger.info("Cancellation requested for job %s", job_id)
    return JobStatus(**job)


@router.get("", response_model=list[JobStatus])
def list_jobs(
    request: Request,
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[JobStatus]:
    """Return the most recently created pipeline jobs."""
    _require_operator(request)
    return [
        JobStatus(**job)
        for job in runtime.job_manager.list_jobs(limit, offset)
    ]


@router.get("/{job_id}/output", response_model=JobOutputResponse)
def get_job_output(request: Request, job_id: str) -> JobOutputResponse:
    """Return bounded, redacted output for a terminal pipeline job."""
    _require_operator(request)
    try:
        return JobOutputResponse(**runtime.job_manager.get_output(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Pipeline job not found") from exc
    except InvalidJobState as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
