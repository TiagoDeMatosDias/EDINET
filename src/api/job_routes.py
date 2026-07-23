"""Durable pipeline job query and cancellation endpoints."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from src.pipeline_jobs import ForceCancellationUnsupported, InvalidJobState

from . import runtime
from .models import CancelJobRequest, JobOutputResponse, JobStatus

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs", tags=["pipeline jobs"])


@router.get("/{job_id}", response_model=JobStatus)
def get_job_status(job_id: str) -> JobStatus:
    """Return the current persisted status of a pipeline job."""
    try:
        return JobStatus(**runtime.job_manager.get_job(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Pipeline job not found") from exc


@router.post("/{job_id}/cancel", response_model=JobStatus)
def cancel_job(job_id: str, request: CancelJobRequest) -> JobStatus:
    """Request cooperative cancellation of a pipeline job."""
    try:
        job = runtime.job_manager.cancel(job_id, force=request.force)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Pipeline job not found") from exc
    except (ForceCancellationUnsupported, InvalidJobState) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    logger.info("Cancellation requested for job %s", job_id)
    return JobStatus(**job)


@router.get("", response_model=list[JobStatus])
def list_jobs(
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[JobStatus]:
    """Return the most recently created pipeline jobs."""
    return [
        JobStatus(**job)
        for job in runtime.job_manager.list_jobs(limit, offset)
    ]


@router.get("/{job_id}/output", response_model=JobOutputResponse)
def get_job_output(job_id: str) -> JobOutputResponse:
    """Return bounded, redacted output for a terminal pipeline job."""
    try:
        return JobOutputResponse(**runtime.job_manager.get_output(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Pipeline job not found") from exc
    except InvalidJobState as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
