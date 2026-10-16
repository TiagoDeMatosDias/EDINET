"""Administrator API for workers: ``/api/admin/workers*`` (plan section 8.5)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from src.auth.dependencies import CurrentUser, require_admin

from .launcher import default_server_url
from .protocol import LOCAL_WORKER_ID
from .registry import WorkerRecord
from .service import get_job_service
from .user_api import JobDetail, JobSummary, detail, summarize

router = APIRouter(
    prefix="/api/admin/workers",
    tags=["workers"],
    dependencies=[Depends(require_admin)],
)

_NO_LAUNCHER = {
    "state": "stopped",
    "pid": None,
    "restarts": 0,
    "last_exit_code": None,
    "command": None,
}


class RunningJobView(BaseModel):
    job_id: str
    kind: str
    title: str
    started_at: str | None
    progress_percent: float


class WorkerView(BaseModel):
    worker_id: str
    name: str
    kind: str
    enabled: bool
    interactive_slots: int
    batch_slots: int
    created_at: str
    online: bool
    last_seen_at: str | None
    version: str | None
    hostname: str | None
    pid: int | None
    platform: str | None
    data_access: bool
    data_access_detail: str | None
    interactive_busy: int
    batch_busy: int
    running_jobs: list[RunningJobView]


class WorkersAdminResponse(BaseModel):
    workers: list[WorkerView]
    launcher: dict[str, Any]
    settings: dict[str, str]


class WorkerPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=120)
    interactive_slots: int | None = Field(default=None, ge=0, le=8)
    batch_slots: int | None = Field(default=None, ge=0, le=8)
    enabled: bool | None = None


class EnrollmentBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(default="", max_length=120)


class EnrollmentResult(BaseModel):
    enrollment: str
    command: str
    expires_at: str


class EchoBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sleep_seconds: float = Field(default=0, ge=0, le=3600)
    result: Any = None
    fail: str | None = Field(default=None, max_length=500)
    artifact_text: str | None = Field(default=None, max_length=100_000)
    progress_steps: int = Field(default=0, ge=0, le=100)
    wait: bool = True


def _view(
    worker: WorkerRecord,
    session: dict[str, Any] | None,
    running: list[dict[str, Any]],
    online_cutoff: str,
) -> WorkerView:
    online = bool(session and session["last_seen_at"] >= online_cutoff)
    return WorkerView(
        worker_id=worker.worker_id,
        name=worker.name,
        kind=worker.kind,
        enabled=worker.enabled,
        interactive_slots=worker.interactive_slots,
        batch_slots=worker.batch_slots,
        created_at=worker.created_at,
        online=online,
        last_seen_at=session["last_seen_at"] if session else None,
        version=session["version"] if session else None,
        hostname=session["hostname"] if session else None,
        pid=session["pid"] if session else None,
        platform=session["platform"] if session else None,
        data_access=bool(session and session["data_access"]),
        data_access_detail=session["data_access_detail"] if session else None,
        interactive_busy=int(session["interactive_busy"]) if session else 0,
        batch_busy=int(session["batch_busy"]) if session else 0,
        running_jobs=[RunningJobView(**{k: job[k] for k in RunningJobView.model_fields}) for job in running],
    )


def _cutoff(heartbeat_seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=3 * heartbeat_seconds)).isoformat(
        timespec="microseconds"
    )


def _launcher_status(request: Request) -> dict[str, Any]:
    launcher = getattr(request.app.state, "launcher", None)
    return launcher.status() if launcher is not None else dict(_NO_LAUNCHER)


def _view_of(worker_id: str) -> WorkerView:
    service = get_job_service()
    worker = service.registry.get(worker_id)
    return _view(
        worker,
        service.store.latest_sessions().get(worker_id),
        service.store.running_jobs_by_worker().get(worker_id, []),
        _cutoff(service.heartbeat_seconds),
    )


@router.get("", response_model=WorkersAdminResponse)
def list_workers(request: Request) -> WorkersAdminResponse:
    from src.settings import get_setting

    service = get_job_service()
    sessions = service.store.latest_sessions()
    running = service.store.running_jobs_by_worker()
    cutoff = _cutoff(service.heartbeat_seconds)
    return WorkersAdminResponse(
        workers=[
            _view(worker, sessions.get(worker.worker_id), running.get(worker.worker_id, []), cutoff)
            for worker in service.registry.list()
        ],
        launcher=_launcher_status(request),
        settings={"launch_local": str(get_setting("workers.launch_local"))},
    )


@router.patch("/{worker_id}", response_model=WorkerView)
def update_worker(worker_id: str, body: WorkerPatch) -> WorkerView:
    changes = body.model_dump(exclude_unset=True, exclude_none=True)
    service = get_job_service()
    try:
        service.registry.update(worker_id, **changes)
    except KeyError:
        raise HTTPException(status_code=404, detail="Worker not found") from None
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    service.notifier.bump()
    return _view_of(worker_id)


@router.delete("/{worker_id}")
def revoke_worker(worker_id: str) -> dict[str, Any]:
    if worker_id == LOCAL_WORKER_ID:
        raise HTTPException(
            status_code=409, detail="The local worker cannot be revoked; disable it instead"
        )
    service = get_job_service()
    try:
        service.registry.revoke(worker_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Worker not found") from None
    return {"worker_id": worker_id, "revoked": True}


@router.post("/enrollments", response_model=EnrollmentResult)
def create_enrollment(
    body: EnrollmentBody, request: Request, user: CurrentUser
) -> EnrollmentResult:
    from src.web_app.security import get_settings
    from src.web_app.tls import certificate_fingerprint, server_certificate_path

    certificate = server_certificate_path()
    if certificate is None:
        raise HTTPException(
            status_code=409,
            detail="The server has no TLS certificate yet; start it once with HTTPS first",
        )
    settings = getattr(request.app.state, "settings", None) or get_settings()
    enrollment, expires_at = get_job_service().registry.create_enrollment(
        body.name or "Worker",
        user.user_id,
        default_server_url(settings),
        certificate_fingerprint(certificate),
    )
    return EnrollmentResult(
        enrollment=enrollment,
        command=f"ShadeWorker enroll {enrollment}",
        expires_at=expires_at,
    )


@router.post("/local/restart")
def restart_local_worker(request: Request) -> dict[str, Any]:
    launcher = getattr(request.app.state, "launcher", None)
    if launcher is None:
        raise HTTPException(status_code=409, detail="The local worker launcher is not running")
    return launcher.restart()


@router.post("/echo", response_model=JobDetail, responses={202: {"model": JobSummary}})
async def echo(body: EchoBody, user: CurrentUser) -> Any:
    """Run the diagnostic ``echo`` job (tests and the packaging smoke test)."""
    from starlette.concurrency import run_in_threadpool

    service = get_job_service()
    payload = body.model_dump(exclude={"wait"})
    if not body.wait:
        record = await run_in_threadpool(
            lambda: service.enqueue("echo", payload, owner_user_id=user.user_id, title="Diagnostic echo")
        )
        return _accepted(summarize(record, user, {"interactive": True}))
    record = await service.run("echo", payload, owner_user_id=user.user_id, title="Diagnostic echo")
    return await run_in_threadpool(detail, record, user, service)


def _accepted(summary: JobSummary):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=202, content=summary.model_dump(mode="json"))


__all__ = ["router"]
