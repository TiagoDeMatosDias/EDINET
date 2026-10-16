"""HTTP API used by job workers: ``/api/worker/*`` (plan section 5).

Workers authenticate with ``Authorization: Bearer srw_...`` checked here; the
user-token middleware skips this prefix. Errors use the application's error
envelope ``{"code", "detail", "correlation_id"}``.
"""

from __future__ import annotations

import ipaddress
import threading
import time
from typing import Any, Callable, Coroutine
from uuid import uuid4

from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.concurrency import run_in_threadpool

from src.paths import jobs_dir
from src.version import __version__

from .errors import ArtifactTooLarge, InvalidArtifactName, WorkerProtocolError
from .protocol import (
    ArtifactResponse,
    ClaimRequest,
    ClaimResponse,
    CompleteRequest,
    EnrollRequest,
    EnrollResponse,
    FailRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    ProgressRequest,
    ProgressResponse,
    RegisterRequest,
    RegisterResponse,
    ReleaseRequest,
    TerminalResponse,
    UnregisterRequest,
)
from .registry import WorkerRecord
from .service import ARTIFACT_NAME, JobService, get_job_service

_TUNNEL_HEADERS = ("cf-connecting-ip", "cf-ray", "x-forwarded-for", "forwarded", "x-real-ip")
_WILDCARD_HOSTS = frozenset({"", "0.0.0.0", "::"})
_STREAM_FLUSH_BYTES = 1024 * 1024

ENROLL_FAILURE_LIMIT = 5
ENROLL_WINDOW_SECONDS = 600.0
ENROLL_BLOCK_SECONDS = 600.0


class WorkerNetworkForbidden(WorkerProtocolError):
    code = "worker_network_forbidden"
    status_code = 403
    default_message = "Workers may only connect from this machine"


class WorkerUnauthorized(WorkerProtocolError):
    code = "worker_unauthorized"
    status_code = 401
    default_message = "A valid worker token is required"


class JobNotFound(WorkerProtocolError):
    code = "job_not_found"
    status_code = 404
    default_message = "The job was not found"


class EnrollmentRefused(WorkerProtocolError):
    code = "enrollment_invalid"
    status_code = 403
    default_message = "The enrollment code is invalid, expired, or already used"


class EnrollmentThrottled(WorkerProtocolError):
    code = "enrollment_throttled"
    status_code = 429
    default_message = "Too many failed enrollment attempts; try again later"


class _EnrollThrottle:
    """Five failed attempts within 10 minutes block enrollment for 10 minutes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._failures: list[float] = []
        self._blocked_until = 0.0

    def check(self) -> None:
        with self._lock:
            if time.monotonic() < self._blocked_until:
                raise EnrollmentThrottled()

    def failed(self) -> None:
        with self._lock:
            now = time.monotonic()
            self._failures = [t for t in self._failures if now - t < ENROLL_WINDOW_SECONDS]
            self._failures.append(now)
            if len(self._failures) >= ENROLL_FAILURE_LIMIT:
                self._blocked_until = now + ENROLL_BLOCK_SECONDS
                self._failures = []

    def reset(self) -> None:
        with self._lock:
            self._failures = []
            self._blocked_until = 0.0


_throttle = _EnrollThrottle()


def reset_enroll_throttle() -> None:
    _throttle.reset()


def _envelope(request: Request, status_code: int, code: str, detail: str) -> JSONResponse:
    correlation_id = getattr(request.state, "correlation_id", None) or str(uuid4())
    headers = {"X-Correlation-ID": correlation_id}
    if status_code == 401:
        headers["WWW-Authenticate"] = "Bearer"
    return JSONResponse(
        status_code=status_code,
        content={"code": code, "detail": detail, "correlation_id": correlation_id},
        headers=headers,
    )


class _WorkerRoute(APIRoute):
    """Turn worker-protocol errors into the error envelope."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original = super().get_route_handler()

        async def handler(request: Request) -> Response:
            try:
                return await original(request)
            except WorkerProtocolError as exc:
                return _envelope(request, exc.status_code, exc.code, str(exc))
            except KeyError:
                return _envelope(request, 404, JobNotFound.code, JobNotFound.default_message)

        return handler


def _is_loopback(host: str) -> bool:
    text = host.strip().strip("[]").casefold()
    if text == "localhost":
        return True
    try:
        address = ipaddress.ip_address(text)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_loopback


def network_guard(request: Request) -> None:
    """Allow loopback clients (or the specific bind address) that did not come via a proxy."""
    if any(header in request.headers for header in _TUNNEL_HEADERS):
        raise WorkerNetworkForbidden()
    client = request.client.host if request.client else ""
    if _is_loopback(client):
        return
    bind = str(getattr(getattr(request.app.state, "settings", None), "host", "")).strip().strip("[]")
    if bind not in _WILDCARD_HOSTS and not _is_loopback(bind) and client == bind:
        return
    raise WorkerNetworkForbidden()


def current_worker(request: Request) -> WorkerRecord:
    scheme, _, token = request.headers.get("Authorization", "").partition(" ")
    worker = None
    if scheme.casefold() == "bearer" and token:
        worker = get_job_service().registry.authenticate(token.strip())
    if worker is None:
        raise WorkerUnauthorized()
    return worker


def _service() -> JobService:
    return get_job_service()


router = APIRouter(
    prefix="/api/worker",
    tags=["workers"],
    route_class=_WorkerRoute,
    dependencies=[Depends(network_guard)],
)


@router.post("/enroll", response_model=EnrollResponse)
def enroll(body: EnrollRequest) -> EnrollResponse:
    _throttle.check()
    try:
        worker, token = _service().registry.redeem_enrollment(body.code, body.name)
    except ValueError:
        _throttle.failed()
        raise EnrollmentRefused() from None
    return EnrollResponse(worker_id=worker.worker_id, token=token, server_version=__version__)


@router.post("/register", response_model=RegisterResponse)
def register(body: RegisterRequest, worker: WorkerRecord = Depends(current_worker)) -> RegisterResponse:
    return _service().register(worker.worker_id, body)


@router.post("/claim", response_model=None)
async def claim(
    body: ClaimRequest, request: Request, worker: WorkerRecord = Depends(current_worker)
) -> Response:
    assignment = await _service().claim(
        worker.worker_id, body, should_stop=request.is_disconnected
    )
    if assignment is None:
        return Response(status_code=204)
    return JSONResponse(ClaimResponse(job=assignment).model_dump(mode="json"))


@router.post("/heartbeat", response_model=HeartbeatResponse)
def heartbeat(body: HeartbeatRequest, worker: WorkerRecord = Depends(current_worker)) -> HeartbeatResponse:
    return _service().heartbeat(worker.worker_id, body)


@router.post("/jobs/{job_id}/progress", response_model=ProgressResponse)
def progress(
    job_id: str, body: ProgressRequest, worker: WorkerRecord = Depends(current_worker)
) -> ProgressResponse:
    return _service().progress(worker.worker_id, job_id, body)


@router.put("/jobs/{job_id}/artifacts/{name}", response_model=ArtifactResponse)
async def upload_artifact(
    job_id: str, name: str, request: Request, worker: WorkerRecord = Depends(current_worker)
) -> ArtifactResponse:
    from src.web_app.security import get_settings

    service = _service()
    if not ARTIFACT_NAME.fullmatch(name):
        raise InvalidArtifactName()
    lease_token = request.headers.get("X-Lease-Token", "")
    await run_in_threadpool(service.check_lease, worker.worker_id, job_id, lease_token)
    limit = getattr(getattr(request.app.state, "settings", None), "max_backtest_artifact_bytes", None)
    if limit is None:
        limit = get_settings().max_backtest_artifact_bytes
    declared = request.headers.get("Content-Length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise ArtifactTooLarge()

    folder = jobs_dir() / job_id / "artifacts"
    await run_in_threadpool(lambda: folder.mkdir(parents=True, exist_ok=True))
    part = folder / f"{name}.part"
    received = 0
    handle = await run_in_threadpool(part.open, "wb")
    try:
        async for chunk in request.stream():
            received += len(chunk)
            if received > limit:
                raise ArtifactTooLarge()
            await run_in_threadpool(handle.write, chunk)
    except BaseException:
        await run_in_threadpool(handle.close)
        part.unlink(missing_ok=True)
        raise
    await run_in_threadpool(handle.close)
    return await run_in_threadpool(
        service.store_artifact,
        worker.worker_id,
        job_id,
        lease_token,
        name,
        part,
        request.headers.get("Content-Type"),
        max_bytes=limit,
    )


@router.post("/jobs/{job_id}/complete", response_model=TerminalResponse)
def complete(
    job_id: str, body: CompleteRequest, worker: WorkerRecord = Depends(current_worker)
) -> TerminalResponse:
    return _service().complete(worker.worker_id, job_id, body)


@router.post("/jobs/{job_id}/fail", response_model=TerminalResponse)
def fail(job_id: str, body: FailRequest, worker: WorkerRecord = Depends(current_worker)) -> TerminalResponse:
    return _service().fail(worker.worker_id, job_id, body)


@router.post("/jobs/{job_id}/release", response_model=TerminalResponse)
def release(
    job_id: str, body: ReleaseRequest, worker: WorkerRecord = Depends(current_worker)
) -> TerminalResponse:
    return _service().release(worker.worker_id, job_id, body)


@router.post("/unregister", status_code=204)
def unregister(body: UnregisterRequest, worker: WorkerRecord = Depends(current_worker)) -> Response:
    _service().unregister(worker.worker_id, body)
    return Response(status_code=204)


__all__ = ["current_worker", "network_guard", "reset_enroll_throttle", "router"]
