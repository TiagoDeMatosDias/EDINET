"""Test doubles for the worker: a fake ``/api/worker/*`` HTTPS server and a fake JobService."""

from __future__ import annotations

import asyncio
import hashlib
import secrets
import socket
import sqlite3
import threading
import time
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO, Callable

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from src.jobs.kinds import KINDS
from src.jobs.protocol import (
    ArtifactResponse,
    ClaimRequest,
    ClaimResponse,
    CompleteRequest,
    EnrollRequest,
    EnrollResponse,
    FailRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    JobAssignment,
    JobLeaseState,
    ProgressRequest,
    ProgressResponse,
    RegisterRequest,
    RegisterResponse,
    ReleaseRequest,
    TerminalResponse,
    UnregisterRequest,
    WorkerPaths,
)
from src.version import __version__
from src.web_app.tls import certificate_fingerprint, provision_tls

TOKEN = "srw_test-token"


def wait_until(predicate: Callable[[], Any], timeout: float = 20.0, interval: float = 0.05) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition not met in time")


def make_sqlite(path: Path) -> Path:
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS t (x)")
        conn.commit()
    return path


@dataclass
class FakeJob:
    job_id: str
    kind: str
    lane: str
    payload: dict[str, Any]
    workspace: str
    status: str = "open"
    attempt: int = 0
    lease_token: str = ""
    lease_valid: bool = True
    cancel: str = "none"
    result: dict[str, Any] | None = None
    error: str | None = None
    retryable: bool | None = None
    released: bool = False
    artifacts: dict[str, bytes] = field(default_factory=dict)
    progress: list[dict[str, Any]] = field(default_factory=list)


class FakeServerState:
    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.jobs: dict[str, FakeJob] = {}
        self.sessions: set[str] = set()
        self.unregistered = False
        self.data_access: bool | None = None
        self.heartbeats = 0
        self.config_version = 1
        self.slots = (1, 1)
        self.shutdown = False
        self.registrations = 0
        self.register_error: tuple[int, str] | None = None
        self.claim_wait = 0.4
        self.market_db = make_sqlite(root / "market.db")
        self.filings_db = make_sqlite(root / "filings.db")
        self.enrolled: list[EnrollRequest] = []
        (root / "jobs").mkdir(exist_ok=True)

    def enqueue(self, kind: str, payload: dict[str, Any] | None = None) -> FakeJob:
        job_id = secrets.token_hex(8)
        workspace = self.root / "jobs" / job_id
        workspace.mkdir(parents=True, exist_ok=True)
        job = FakeJob(job_id, kind, KINDS[kind].lane, payload or {}, str(workspace))
        with self.lock:
            self.jobs[job_id] = job
        return job

    def request_cancel(self, job_id: str, mode: str) -> None:
        self.jobs[job_id].cancel = mode

    def revoke_lease(self, job_id: str) -> None:
        self.jobs[job_id].lease_valid = False

    def register_response(self, session_id: str) -> RegisterResponse:
        return RegisterResponse(
            worker_id="w-test",
            name="Test worker",
            session_id=session_id,
            server_version=__version__,
            lease_seconds=5,
            heartbeat_seconds=1,
            claim_wait_seconds=1,
            interactive_slots=self.slots[0],
            batch_slots=self.slots[1],
            config_version=self.config_version,
            paths=WorkerPaths(
                market_db=str(self.market_db),
                filings_db=str(self.filings_db),
                data_dir=str(self.root),
                jobs_dir=str(self.root / "jobs"),
            ),
            settings={
                "edinet.api_key": "test-key",
                "limits.max_backtest_artifact_bytes": 1048576,
                "limits.max_upload_bytes": 1048576,
                "limits.max_export_bytes": 1048576,
            },
        )


def _error(status: int, code: str, detail: str = "") -> JSONResponse:
    return JSONResponse({"code": code, "detail": detail or code, "correlation_id": "test"}, status_code=status)


def build_app(state: FakeServerState) -> FastAPI:
    app = FastAPI()

    def unauthorized(request: Request) -> JSONResponse | None:
        if request.headers.get("authorization") != f"Bearer {TOKEN}":
            return _error(401, "worker_unauthorized", "Bad token")
        return None

    def running(job_id: str, token: str) -> FakeJob | None:
        job = state.jobs.get(job_id)
        if job is None or job.status not in ("in_progress", "cancelling"):
            return None
        if job.lease_token != token or not job.lease_valid:
            return None
        return job

    @app.post("/api/worker/enroll")
    async def enroll(body: EnrollRequest) -> Any:
        if not body.code.startswith("good"):
            return _error(403, "worker_unauthorized", "Invalid or expired enrollment")
        state.enrolled.append(body)
        return EnrollResponse(worker_id="w-enrolled", token=TOKEN, server_version=__version__)

    @app.post("/api/worker/register")
    async def register(request: Request, body: RegisterRequest) -> Any:
        if (denied := unauthorized(request)) is not None:
            return denied
        if state.register_error:
            return _error(*state.register_error)
        if body.version != __version__:
            return _error(409, "version_mismatch", "Version mismatch")
        state.sessions.add(body.session_id)
        state.registrations += 1
        state.unregistered = False
        return state.register_response(body.session_id)

    @app.post("/api/worker/claim")
    async def claim(request: Request, body: ClaimRequest) -> Any:
        if (denied := unauthorized(request)) is not None:
            return denied
        if body.session_id not in state.sessions:
            return _error(409, "unknown_session")
        if not state.data_access:
            return _error(409, "no_data_access")
        deadline = time.monotonic() + state.claim_wait
        while True:
            with state.lock:
                for lane in body.lanes:
                    for job in state.jobs.values():
                        if job.status == "open" and job.lane == lane and (not body.kinds or job.kind in body.kinds):
                            job.status = "in_progress"
                            job.attempt += 1
                            job.lease_token = secrets.token_urlsafe(12)
                            job.lease_valid = True
                            return ClaimResponse(
                                job=JobAssignment(
                                    job_id=job.job_id,
                                    kind=job.kind,
                                    lane=job.lane,  # type: ignore[arg-type]
                                    title=job.kind,
                                    payload=job.payload,
                                    attempt=job.attempt,
                                    lease_token=job.lease_token,
                                    lease_expires_at="2099-01-01T00:00:00+00:00",
                                    workspace=job.workspace,
                                )
                            )
            if time.monotonic() >= deadline:
                return Response(status_code=204)
            await asyncio.sleep(0.02)

    @app.post("/api/worker/heartbeat")
    async def heartbeat(request: Request, body: HeartbeatRequest) -> Any:
        if (denied := unauthorized(request)) is not None:
            return denied
        if body.session_id not in state.sessions:
            return _error(409, "unknown_session")
        state.heartbeats += 1
        state.data_access = body.data_access
        states = []
        for lease in body.jobs:
            job = running(lease.job_id, lease.lease_token)
            if job is None:
                states.append(JobLeaseState(job_id=lease.job_id, lease_ok=False))
            else:
                states.append(
                    JobLeaseState(job_id=lease.job_id, lease_ok=True, cancel=job.cancel)  # type: ignore[arg-type]
                )
        return HeartbeatResponse(jobs=states, config_version=state.config_version, shutdown=state.shutdown)

    @app.post("/api/worker/jobs/{job_id}/progress")
    async def progress(request: Request, job_id: str, body: ProgressRequest) -> Any:
        if (denied := unauthorized(request)) is not None:
            return denied
        job = running(job_id, body.lease_token)
        if job is None:
            return _error(409, "lease_lost")
        job.progress.append(body.model_dump(exclude={"lease_token"}, exclude_none=True))
        return ProgressResponse(cancel=job.cancel, lease_expires_at="2099-01-01T00:00:00+00:00")  # type: ignore[arg-type]

    @app.put("/api/worker/jobs/{job_id}/artifacts/{name}")
    async def artifact(request: Request, job_id: str, name: str) -> Any:
        if (denied := unauthorized(request)) is not None:
            return denied
        job = running(job_id, request.headers.get("x-lease-token", ""))
        if job is None:
            return _error(409, "lease_lost")
        data = await request.body()
        job.artifacts[name] = data
        job.progress.append({"artifact": name, "content_type": request.headers.get("content-type")})
        return ArtifactResponse(name=name, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    @app.post("/api/worker/jobs/{job_id}/complete")
    async def complete(request: Request, job_id: str, body: CompleteRequest) -> Any:
        if (denied := unauthorized(request)) is not None:
            return denied
        job = running(job_id, body.lease_token)
        if job is None:
            return _error(409, "lease_lost")
        job.status, job.result = "completed", body.result
        return TerminalResponse(status="completed")

    @app.post("/api/worker/jobs/{job_id}/fail")
    async def fail(request: Request, job_id: str, body: FailRequest) -> Any:
        if (denied := unauthorized(request)) is not None:
            return denied
        job = running(job_id, body.lease_token)
        if job is None:
            return _error(409, "lease_lost")
        job.status = "cancelled" if body.cancelled else "failed"
        job.error, job.retryable = body.error, body.retryable
        return TerminalResponse(status=job.status)  # type: ignore[arg-type]

    @app.post("/api/worker/jobs/{job_id}/release")
    async def release(request: Request, job_id: str, body: ReleaseRequest) -> Any:
        if (denied := unauthorized(request)) is not None:
            return denied
        job = running(job_id, body.lease_token)
        if job is None:
            return _error(409, "lease_lost")
        job.status, job.released = "open", True
        return TerminalResponse(status="open")

    @app.post("/api/worker/unregister")
    async def unregister(request: Request, body: UnregisterRequest) -> Any:
        if (denied := unauthorized(request)) is not None:
            return denied
        state.sessions.discard(body.session_id)
        state.unregistered = True
        return Response(status_code=204)

    @app.get("/boom")
    async def boom() -> Any:
        return _error(503, "unavailable")

    return app


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class FakeServer:
    """The fake protocol app served over HTTPS on loopback with a generated certificate."""

    def __init__(self, root: Path) -> None:
        self.state = FakeServerState(root)
        cert, key = provision_tls(cert_dir=root / "certs", host="127.0.0.1")
        self.fingerprint = certificate_fingerprint(cert)
        self.port = free_port()
        self.url = f"https://127.0.0.1:{self.port}"
        config = uvicorn.Config(
            build_app(self.state),
            host="127.0.0.1",
            port=self.port,
            ssl_certfile=str(cert),
            ssl_keyfile=str(key),
            log_level="warning",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, name="fake-worker-server", daemon=True)

    def start(self) -> FakeServer:
        self._thread.start()
        wait_until(lambda: self._server.started, timeout=15)
        return self

    def stop(self) -> None:
        # uvicorn waits for open keep-alive connections before it exits; drop them.
        for connection in list(self._server.server_state.connections):
            connection.loop.call_soon_threadsafe(connection.transport.abort)
        self._server.should_exit = True
        self._thread.join(10)


# --- fake JobService (implements the pinned method signatures of plan 6.1) ------


class FakeLeaseLost(Exception):
    pass


class _FakeRegistry:
    def __init__(self) -> None:
        self.workers: dict[str, str] = {}

    def ensure_local_worker(self, worker_id: str = "local", name: str = "Local worker") -> str:
        self.workers[worker_id] = name
        return "srw_fake"


class FakeJobService:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.registry = _FakeRegistry()
        self.lock = threading.Lock()
        self.jobs: dict[str, FakeJob] = {}
        self.registered: list[RegisterRequest] = []
        self.heartbeats: list[HeartbeatRequest] = []
        self.unregistered = False

    def enqueue(self, kind: str, payload: dict[str, Any] | None = None) -> FakeJob:
        job_id = secrets.token_hex(8)
        workspace = self.root / job_id
        workspace.mkdir(parents=True, exist_ok=True)
        job = FakeJob(job_id, kind, KINDS[kind].lane, payload or {}, str(workspace))
        with self.lock:
            self.jobs[job_id] = job
        return job

    def register(self, worker_id: str, request: RegisterRequest) -> None:
        self.registered.append(request)

    def claim_now(self, worker_id: str, request: ClaimRequest) -> JobAssignment | None:
        with self.lock:
            for lane in request.lanes:
                for job in self.jobs.values():
                    if job.status == "open" and job.lane == lane and (not request.kinds or job.kind in request.kinds):
                        job.status = "in_progress"
                        job.attempt += 1
                        job.lease_token = secrets.token_urlsafe(8)
                        return JobAssignment(
                            job_id=job.job_id,
                            kind=job.kind,
                            lane=job.lane,  # type: ignore[arg-type]
                            payload=job.payload,
                            attempt=job.attempt,
                            lease_token=job.lease_token,
                            lease_expires_at="2099-01-01T00:00:00+00:00",
                            workspace=job.workspace,
                        )
        return None

    def heartbeat(self, worker_id: str, request: HeartbeatRequest) -> HeartbeatResponse:
        self.heartbeats.append(request)
        states = []
        for lease in request.jobs:
            job = self.jobs[lease.job_id]
            states.append(JobLeaseState(job_id=job.job_id, lease_ok=True, cancel=job.cancel))  # type: ignore[arg-type]
        return HeartbeatResponse(jobs=states, config_version=1)

    def progress(self, worker_id: str, job_id: str, request: ProgressRequest) -> ProgressResponse:
        job = self.jobs[job_id]
        job.progress.append(request.model_dump(exclude={"lease_token"}, exclude_none=True))
        return ProgressResponse(cancel=job.cancel, lease_expires_at="2099-01-01T00:00:00+00:00")  # type: ignore[arg-type]

    def store_artifact(
        self, worker_id: str, job_id: str, lease_token: str, name: str, source: BinaryIO | Path,
        content_type: str | None = None,
    ) -> ArtifactResponse:
        data = Path(source).read_bytes() if isinstance(source, Path) else source.read()
        self.jobs[job_id].artifacts[name] = data
        return ArtifactResponse(name=name, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def complete(self, worker_id: str, job_id: str, request: CompleteRequest) -> TerminalResponse:
        job = self.jobs[job_id]
        job.status, job.result = "completed", request.result
        return TerminalResponse(status="completed")

    def fail(self, worker_id: str, job_id: str, request: FailRequest) -> TerminalResponse:
        job = self.jobs[job_id]
        job.status = "cancelled" if request.cancelled else "failed"
        job.error, job.retryable = request.error, request.retryable
        return TerminalResponse(status=job.status)  # type: ignore[arg-type]

    def release(self, worker_id: str, job_id: str, request: ReleaseRequest) -> TerminalResponse:
        raise NotImplementedError

    def unregister(self, worker_id: str, request: UnregisterRequest) -> None:
        self.unregistered = True
