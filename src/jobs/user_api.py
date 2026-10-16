"""Job tray API for signed-in users: ``/api/my-jobs*`` and ``/api/workers/status``.

A user sees the jobs they own. Administrators and operators also see pipeline
jobs and system jobs (no owner). Also holds the exception handlers that turn
``JobPending`` / ``JobFailed`` / ``JobCancelled`` into HTTP responses.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from src.auth.dependencies import CurrentUser
from src.auth.models import AuthenticatedUser

from .errors import InvalidJobState, JobCancelled, JobFailed, JobPending
from .kinds import KINDS
from .service import JobRecord, JobService, get_job_service

router = APIRouter(tags=["jobs"])

_PRIVILEGED_ROLES = frozenset({"admin", "operator"})
_PENDING_WAITING = "No worker is online; the job will start when one connects."
_PENDING_RUNNING = "Still running; it will appear in the job tray when it finishes."


class JobSummary(BaseModel):
    job_id: str
    kind: str
    kind_label: str
    title: str
    status: str
    progress_percent: float
    status_message: str | None
    progress: dict[str, Any] | None
    created_at: str
    started_at: str | None
    completed_at: str | None
    updated_at: str
    error_message: str | None
    notify: bool
    link: str | None
    cancellable: bool
    waiting_for_worker: bool


class JobDetail(JobSummary):
    result: Any = None


class WorkerCapacity(BaseModel):
    online: int
    interactive_slots: int
    batch_slots: int


class MyJobsResponse(BaseModel):
    jobs: list[JobSummary]
    server_time: str
    workers: WorkerCapacity


class CancelBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    force: bool = False


def is_privileged(user: AuthenticatedUser) -> bool:
    return user.role in _PRIVILEGED_ROLES


def can_see(user: AuthenticatedUser, record: JobRecord) -> bool:
    if record.owner_user_id == user.user_id:
        return True
    return is_privileged(user) and (record.owner_user_id is None or record.kind == "pipeline")


def job_link(record: JobRecord) -> str | None:
    if record.kind.startswith("backtest_"):
        result_id = (record.result or {}).get("result_id") if record.status == "completed" else None
        return f"/backtest?result={quote(str(result_id), safe='')}" if result_id else None
    if record.kind == "screening":
        return "/screening"
    if record.kind == "pipeline":
        return "/pipeline"
    return None


def summarize(
    record: JobRecord, user: AuthenticatedUser, served: dict[str, bool]
) -> JobSummary:
    spec = KINDS.get(record.kind)
    return JobSummary(
        job_id=record.job_id,
        kind=record.kind,
        kind_label=spec.label if spec else record.kind,
        title=record.title,
        status=record.status,
        progress_percent=record.progress_percent,
        status_message=record.status_message,
        progress=record.progress,
        created_at=record.created_at,
        started_at=record.started_at,
        completed_at=record.completed_at,
        updated_at=record.updated_at,
        error_message=record.error_message,
        notify=record.notify,
        link=job_link(record),
        cancellable=record.is_active and can_see(user, record),
        waiting_for_worker=record.status == "open" and not served.get(record.lane, False),
    )


def detail(record: JobRecord, user: AuthenticatedUser, service: JobService) -> JobDetail:
    served = {record.lane: service.lane_served(record.lane)}
    return JobDetail(**summarize(record, user, served).model_dump(), result=record.result)


def _lanes_served(service: JobService) -> dict[str, bool]:
    return {lane: service.lane_served(lane) for lane in ("interactive", "batch")}


@router.get("/api/my-jobs", response_model=MyJobsResponse)
def list_my_jobs(
    user: CurrentUser,
    active: bool = False,
    limit: int = Query(30, ge=1, le=100),
    updated_since: str | None = None,
) -> MyJobsResponse:
    service = get_job_service()
    privileged = is_privileged(user)
    records = service.list_jobs(
        owner_user_id=user.user_id,
        include_system=privileged,
        also_kinds=["pipeline"] if privileged else None,
        active_only=active,
        updated_since=updated_since,
        limit=limit,
    )
    served = _lanes_served(service)
    return MyJobsResponse(
        jobs=[summarize(record, user, served) for record in records],
        server_time=datetime.now(timezone.utc).isoformat(),
        workers=WorkerCapacity(**service.capacity()),
    )


def _visible_job(service: JobService, user: AuthenticatedUser, job_id: str) -> JobRecord:
    try:
        record = service.get(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found") from None
    if not can_see(user, record):
        raise HTTPException(status_code=404, detail="Job not found")
    return record


@router.get("/api/my-jobs/{job_id}", response_model=JobDetail)
def get_my_job(job_id: str, user: CurrentUser) -> JobDetail:
    service = get_job_service()
    return detail(_visible_job(service, user, job_id), user, service)


@router.post("/api/my-jobs/{job_id}/cancel", response_model=JobSummary)
def cancel_my_job(job_id: str, user: CurrentUser, body: CancelBody | None = None) -> JobSummary:
    service = get_job_service()
    _visible_job(service, user, job_id)
    force = bool(body and body.force)
    if force and not is_privileged(user):
        raise HTTPException(status_code=403, detail="Only administrators and operators can force a cancel")
    try:
        record = service.cancel(job_id, force=force)
    except InvalidJobState as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    return summarize(record, user, _lanes_served(service))


@router.get("/api/workers/status", response_model=WorkerCapacity)
def workers_status(user: CurrentUser) -> WorkerCapacity:
    return WorkerCapacity(**get_job_service().capacity())


def _correlation_id(request: Request) -> str:
    return getattr(request.state, "correlation_id", None) or str(uuid4())


def install_job_error_handlers(app: FastAPI) -> None:
    """Map the job exceptions raised by ``JobService.run`` to responses (plan section 6.2)."""

    @app.exception_handler(JobPending)
    async def job_pending(request: Request, exc: JobPending) -> JSONResponse:
        return JSONResponse(
            status_code=202,
            content={
                "code": "job_pending",
                "job_id": exc.job_id,
                "waiting_for_worker": exc.waiting_for_worker,
                "detail": _PENDING_WAITING if exc.waiting_for_worker else _PENDING_RUNNING,
                "correlation_id": _correlation_id(request),
            },
        )

    @app.exception_handler(JobFailed)
    async def job_failed(request: Request, exc: JobFailed) -> JSONResponse:
        return JSONResponse(
            status_code=502,
            content={
                "code": "job_failed",
                "detail": exc.message,
                "job_id": exc.job_id,
                "correlation_id": _correlation_id(request),
            },
        )

    @app.exception_handler(JobCancelled)
    async def job_cancelled(request: Request, exc: JobCancelled) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={
                "code": "job_cancelled",
                "detail": "The job was cancelled",
                "job_id": exc.job_id,
                "correlation_id": _correlation_id(request),
            },
        )


__all__ = [
    "JobDetail",
    "JobSummary",
    "WorkerCapacity",
    "can_see",
    "detail",
    "install_job_error_handlers",
    "is_privileged",
    "router",
    "summarize",
]
