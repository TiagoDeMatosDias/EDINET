"""The job service: how the server queues work and how workers take it.

Producers (API routes, the scheduler) call ``enqueue`` / ``run``; workers
reach the ``register`` ... ``release`` methods through ``worker_api``. All
state lives in ``jobs.db`` (see ``store``), so a ``JobService`` holds nothing
but configuration and the in-process notifier.
"""

from __future__ import annotations

import asyncio
import errno
import hashlib
import json
import logging
import os
import re
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, BinaryIO, Callable
from uuid import UUID, uuid4

from starlette.concurrency import run_in_threadpool

from src.paths import jobs_db_path, jobs_dir
from src.pipeline_jobs.redaction import safe_public_text
from src.version import __version__

from . import kinds as kind_registry
from .errors import (
    ArtifactTooLarge,
    FinalizeError,
    InvalidArtifactName,
    JobCancelled,
    JobFailed,
    JobPending,
    LeaseLost,
    VersionMismatch,
    WorkerDisabled,
)
from .notifier import JobNotifier
from .protocol import (
    ACTIVE_STATUSES,
    CLAIM_WAIT_SECONDS,
    HEARTBEAT_SECONDS,
    INTERACTIVE_WAIT_SECONDS,
    LEASE_SECONDS,
    MAX_RESULT_BYTES,
    PUBLIC_PIPELINE_STATUS,
    SWEEP_INTERVAL_SECONDS,
    TERMINAL_STATUSES,
    ArtifactResponse,
    ClaimRequest,
    CompleteRequest,
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
from .registry import WorkerRegistry
from .store import JobsStore, utc_now

logger = logging.getLogger(__name__)

ARTIFACT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
CLEANUP_INTERVAL_SECONDS = 3600
_MAX_STATUS_CHARS = 500
_MAX_ERROR_CHARS = 1000
_COPY_CHUNK = 1024 * 1024
# Placeholder meaning "any owner" in ``list_jobs``.
ANY_OWNER: Any = ...


def _loads(text: str | None) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return None


@dataclass(frozen=True)
class JobRecord:
    """One row of ``jobs`` with its JSON columns parsed."""

    job_id: str
    kind: str
    lane: str
    status: str
    title: str
    owner_user_id: str | None
    priority: int
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error_message: str | None
    status_message: str | None
    progress_percent: float
    progress: dict[str, Any] | None
    exclusive: str
    attempt: int
    max_attempts: int
    worker_id: str | None
    session_id: str | None
    lease_token: str | None
    lease_expires_at: str | None
    cancel_requested: str
    notify: bool
    dedupe_key: str | None
    created_at: str
    claimed_at: str | None
    started_at: str | None
    completed_at: str | None
    updated_at: str

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "JobRecord":
        values = dict(row)
        payload = _loads(values.pop("payload_json", None))
        values["payload"] = payload if isinstance(payload, dict) else {}
        values["result"] = _loads(values.pop("result_json", None))
        values["progress"] = _loads(values.pop("progress_json", None))
        values["notify"] = bool(values["notify"])
        return cls(**values)

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def is_active(self) -> bool:
        return self.status in ACTIVE_STATUSES

    @property
    def public_status(self) -> str:
        """The status in the pipeline UI's vocabulary (``open`` is ``pending``)."""
        return PUBLIC_PIPELINE_STATUS.get(self.status, self.status)

    @property
    def exclusive_resources(self) -> frozenset[str]:
        return frozenset(part for part in self.exclusive.split(",") if part)


def _copy_into(source: BinaryIO, part: Path, final: Path, max_bytes: int) -> None:
    """Copy ``source`` to ``final`` through ``part``, refusing more than ``max_bytes``."""
    written = 0
    try:
        with part.open("wb") as out:
            while chunk := source.read(_COPY_CHUNK):
                written += len(chunk)
                if written > max_bytes:
                    raise ArtifactTooLarge()
                out.write(chunk)
        os.replace(part, final)
    finally:
        part.unlink(missing_ok=True)


def _canonical_job_id(job_id: str) -> str:
    """The job id when it is a canonical UUID, else ``ValueError`` (paths are built from it)."""
    if str(UUID(job_id)) != job_id:
        raise ValueError("job id is not canonical")
    return job_id


class JobService:
    def __init__(
        self,
        store: JobsStore,
        registry: WorkerRegistry,
        *,
        lease_seconds: int = LEASE_SECONDS,
        heartbeat_seconds: int = HEARTBEAT_SECONDS,
        claim_wait_seconds: int = CLAIM_WAIT_SECONDS,
    ) -> None:
        self.store = store
        self.registry = registry
        self.lease_seconds = lease_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.claim_wait_seconds = claim_wait_seconds
        self.notifier = JobNotifier()
        # Set when the server begins to stop: no more jobs are handed out and
        # waiting claims return at once instead of holding the shutdown open.
        self.stopping = False

    # -- producers -----------------------------------------------------------

    def enqueue(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        owner_user_id: str | None,
        title: str,
        priority: int = 0,
        notify: bool | None = None,
        dedupe_key: str | None = None,
        steps: list[dict[str, Any]] | None = None,
        job_id: str | None = None,
    ) -> JobRecord:
        """Queue a job. With an active job under the same ``dedupe_key``, that job is returned."""
        spec = kind_registry.kind_spec(kind)
        new_id = _canonical_job_id(job_id) if job_id else str(uuid4())
        row, created = self.store.insert_job(
            job_id=new_id,
            kind=kind,
            lane=spec.lane,
            title=safe_public_text(title, 200) or spec.label,
            owner_user_id=owner_user_id,
            priority=priority,
            payload=payload,
            exclusive=",".join(sorted(spec.exclusive)),
            max_attempts=spec.max_attempts,
            notify=spec.notify if notify is None else notify,
            dedupe_key=dedupe_key,
            steps=steps,
        )
        if created:
            (jobs_dir() / new_id).mkdir(parents=True, exist_ok=True)
            self.notifier.bump()
        return JobRecord.from_row(row)

    def set_notify(self, job_id: str, notify: bool = True) -> None:
        self.store.set_notify(job_id, notify)
        self.notifier.bump()

    async def wait(self, job_id: str, timeout: float) -> JobRecord:
        """The job once it is finished, or as it stands when ``timeout`` passes."""
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            version = self.notifier.version
            record = await run_in_threadpool(self.get, job_id)
            remaining = deadline - asyncio.get_running_loop().time()
            if record.is_terminal or remaining <= 0:
                return record
            await self.notifier.wait_for_change(version, min(1.0, remaining))

    def wait_sync(self, job_id: str, timeout: float) -> JobRecord:
        import time

        deadline = time.monotonic() + timeout
        while True:
            version = self.notifier.version
            record = self.get(job_id)
            remaining = deadline - time.monotonic()
            if record.is_terminal or remaining <= 0:
                return record
            self.notifier.wait_for_change_sync(version, min(1.0, remaining))

    def _result_of(self, record: JobRecord, *, served: bool) -> JobRecord | JobPending:
        """Map a waited-for job to the record, or the exception to raise."""
        if record.status == "completed":
            return record
        if record.status in ("failed", "interrupted"):
            raise JobFailed(record.error_message or "The job failed", record.job_id)
        if record.status == "cancelled":
            raise JobCancelled(record.job_id)
        return JobPending(record.job_id, waiting_for_worker=record.status == "open" and not served)

    async def run(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        owner_user_id: str | None,
        title: str,
        timeout: float = INTERACTIVE_WAIT_SECONDS,
        **options: Any,
    ) -> JobRecord:
        """Queue a job and wait for it (``JobPending`` when it takes longer than ``timeout``)."""
        record = await run_in_threadpool(
            lambda: self.enqueue(kind, payload, owner_user_id=owner_user_id, title=title, **options)
        )
        if not await run_in_threadpool(self.lane_served, record.lane):
            await run_in_threadpool(self.set_notify, record.job_id)
            raise JobPending(record.job_id, waiting_for_worker=True)
        record = await self.wait(record.job_id, timeout)
        served = record.status != "open" or await run_in_threadpool(self.lane_served, record.lane)
        outcome = self._result_of(record, served=served)
        if isinstance(outcome, JobPending):
            await run_in_threadpool(self.set_notify, record.job_id)
            raise outcome
        return outcome

    def run_sync(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        owner_user_id: str | None,
        title: str,
        timeout: float = INTERACTIVE_WAIT_SECONDS,
        **options: Any,
    ) -> JobRecord:
        record = self.enqueue(kind, payload, owner_user_id=owner_user_id, title=title, **options)
        if not self.lane_served(record.lane):
            self.set_notify(record.job_id)
            raise JobPending(record.job_id, waiting_for_worker=True)
        record = self.wait_sync(record.job_id, timeout)
        served = record.status != "open" or self.lane_served(record.lane)
        outcome = self._result_of(record, served=served)
        if isinstance(outcome, JobPending):
            self.set_notify(record.job_id)
            raise outcome
        return outcome

    # -- readers -------------------------------------------------------------

    def get(self, job_id: str) -> JobRecord:
        return JobRecord.from_row(self.store.get(job_id))

    def list_jobs(
        self,
        *,
        kinds: list[str] | None = None,
        owner_user_id: Any = ANY_OWNER,
        include_system: bool = False,
        also_kinds: list[str] | None = None,
        active_only: bool = False,
        updated_since: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[JobRecord]:
        rows = self.store.list_jobs(
            kinds=kinds,
            owner_user_id=owner_user_id,
            include_system=include_system,
            also_kinds=also_kinds,
            active_only=active_only,
            updated_since=updated_since,
            limit=limit,
            offset=offset,
        )
        return [JobRecord.from_row(row) for row in rows]

    def steps(self, job_id: str) -> list[dict[str, Any]]:
        return self.store.steps(job_id)

    def artifacts(self, job_id: str) -> dict[str, Path]:
        return {row["name"]: Path(row["stored_path"]) for row in self.store.artifacts(job_id)}

    def _online_sessions(self) -> list[dict[str, Any]]:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=3 * self.heartbeat_seconds)
        ).isoformat(timespec="microseconds")
        usable = {worker.worker_id for worker in self.registry.list() if worker.enabled}
        return [row for row in self.store.online_sessions(cutoff) if row["worker_id"] in usable]

    def capacity(self) -> dict[str, int]:
        """Online workers and the slots they offer per lane."""
        sessions = self._online_sessions()
        return {
            "online": len({row["worker_id"] for row in sessions}),
            "interactive_slots": sum(int(row["interactive_slots"]) for row in sessions),
            "batch_slots": sum(int(row["batch_slots"]) for row in sessions),
        }

    def lane_served(self, lane: str) -> bool:
        """Whether an online worker could take a job of this lane (batch slots serve both)."""
        for row in self._online_sessions():
            if row["batch_slots"] > 0 or (lane == "interactive" and row["interactive_slots"] > 0):
                return True
        return False

    def health_summary(self) -> dict[str, Any]:
        counts = self.store.counts_by_status()
        return {
            "queue_depth": counts.get("open", 0),
            "active": counts.get("in_progress", 0) + counts.get("cancelling", 0),
            "counts_by_status": counts,
            "workers_online": self.capacity()["online"],
        }

    # -- control -------------------------------------------------------------

    def cancel(self, job_id: str, *, force: bool = False) -> JobRecord:
        row = self.store.request_cancel(job_id, force=force)
        self.notifier.bump()
        return JobRecord.from_row(row)

    # -- worker side ---------------------------------------------------------

    def _enabled_worker(self, worker_id: str):
        try:
            worker = self.registry.get(worker_id)
        except KeyError:
            raise WorkerDisabled() from None
        if worker.revoked or not worker.enabled:
            raise WorkerDisabled()
        return worker

    def register(self, worker_id: str, request: RegisterRequest) -> RegisterResponse:
        from src.orchestrator.common.db_config import get_filings_db, get_market_db
        from src.paths import data_dir
        from src.settings import edinet_api_key
        from src.web_app.security import get_settings

        if request.version != __version__:
            raise VersionMismatch(
                f"The worker is version {request.version} but the server is {__version__}"
            )
        worker = self._enabled_worker(worker_id)
        self.store.upsert_session(
            worker_id=worker_id,
            session_id=request.session_id,
            hostname=request.hostname,
            pid=request.pid,
            platform=request.platform,
            version=request.version,
            interactive_slots=worker.interactive_slots,
            batch_slots=worker.batch_slots,
        )
        self.notifier.bump()
        limits = get_settings()
        return RegisterResponse(
            worker_id=worker.worker_id,
            name=worker.name,
            session_id=request.session_id,
            server_version=__version__,
            lease_seconds=self.lease_seconds,
            heartbeat_seconds=self.heartbeat_seconds,
            claim_wait_seconds=self.claim_wait_seconds,
            interactive_slots=worker.interactive_slots,
            batch_slots=worker.batch_slots,
            config_version=worker.config_version,
            paths=WorkerPaths(
                market_db=str(get_market_db()),
                filings_db=str(get_filings_db()),
                data_dir=str(data_dir()),
                jobs_dir=str(jobs_dir()),
            ),
            settings={
                "edinet.api_key": edinet_api_key(),
                "limits.max_backtest_artifact_bytes": limits.max_backtest_artifact_bytes,
                "limits.max_upload_bytes": limits.max_upload_bytes,
                "limits.max_export_bytes": limits.max_export_bytes,
                "limits.max_report_artifact_bytes": limits.max_report_artifact_bytes,
            },
        )

    def unregister(self, worker_id: str, request: UnregisterRequest) -> None:
        self.store.end_session(worker_id, request.session_id)
        self.notifier.bump()

    def _assignment(self, row: dict[str, Any]) -> JobAssignment:
        job_id = row["job_id"]
        workspace = jobs_dir() / job_id
        workspace.mkdir(parents=True, exist_ok=True)
        record = JobRecord.from_row(row)
        return JobAssignment(
            job_id=job_id,
            kind=record.kind,
            lane=record.lane,
            title=record.title,
            payload=record.payload,
            attempt=record.attempt,
            lease_token=str(record.lease_token),
            lease_expires_at=str(record.lease_expires_at),
            owner_user_id=record.owner_user_id,
            workspace=str(workspace),
        )

    def begin_shutdown(self) -> None:
        """Stop handing out jobs. Only sets a flag, so a signal handler may call it."""
        self.stopping = True

    def release_worker_jobs(self, worker_id: str, reason: str = "The server was stopped") -> int:
        """Settle the jobs of a worker whose process is gone (see ``JobsStore.release_worker``)."""
        changed = self.store.release_worker(worker_id, reason=reason)
        self.notifier.bump()
        return changed

    def claim_now(self, worker_id: str, request: ClaimRequest) -> JobAssignment | None:
        if self.stopping:
            return None
        self._enabled_worker(worker_id)
        row = self.store.claim(
            worker_id=worker_id,
            session_id=request.session_id,
            lanes=list(request.lanes),
            kinds=request.kinds,
            lease_seconds=self.lease_seconds,
        )
        if row is None:
            return None
        self.notifier.bump()  # a claim can hold an exclusive resource others wait on
        return self._assignment(row)

    async def claim(
        self,
        worker_id: str,
        request: ClaimRequest,
        *,
        wait_seconds: float | None = None,
        should_stop: Callable[[], Awaitable[bool]] | None = None,
    ) -> JobAssignment | None:
        """Long poll: a job as soon as one is claimable, else ``None`` after the wait.

        ``should_stop`` lets the caller abandon the wait (the worker went away) so a job
        is not handed to a connection nobody reads.
        """
        wait = self.claim_wait_seconds if wait_seconds is None else wait_seconds
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait
        while True:
            version = self.notifier.version
            assignment = await run_in_threadpool(self.claim_now, worker_id, request)
            if assignment is not None:
                return assignment
            remaining = deadline - loop.time()
            if remaining <= 0 or self.stopping or (should_stop is not None and await should_stop()):
                return None
            await self.notifier.wait_for_change(version, min(1.0, remaining))

    def heartbeat(self, worker_id: str, request: HeartbeatRequest) -> HeartbeatResponse:
        try:
            worker = self.registry.get(worker_id)
        except KeyError:
            raise WorkerDisabled() from None
        states = self.store.heartbeat(
            worker_id=worker_id,
            session_id=request.session_id,
            data_access=request.data_access,
            data_access_detail=request.data_access_detail,
            interactive_busy=request.interactive_busy,
            batch_busy=request.batch_busy,
            leases=[(lease.job_id, lease.lease_token) for lease in request.jobs],
            lease_seconds=self.lease_seconds,
        )
        if any(state["cancel"] != "none" for state in states):
            self.notifier.bump()
        return HeartbeatResponse(
            jobs=[JobLeaseState(**state) for state in states],
            config_version=worker.config_version,
            shutdown=worker.revoked or not worker.enabled,
        )

    def check_lease(self, worker_id: str, job_id: str, lease_token: str) -> None:
        """Raise ``LeaseLost`` / ``KeyError`` unless this worker holds the job's live lease."""
        self.store.check_lease(job_id, worker_id, lease_token)

    def progress(self, worker_id: str, job_id: str, request: ProgressRequest) -> ProgressResponse:
        row = self.store.progress(
            job_id,
            worker_id,
            request.lease_token,
            percent=request.progress_percent,
            message=request.status_message,
            progress=request.progress,
            step=request.step,
            lease_seconds=self.lease_seconds,
        )
        self.notifier.bump()
        return ProgressResponse(cancel=row["cancel_requested"], lease_expires_at=row["lease_expires_at"])

    def store_artifact(
        self,
        worker_id: str,
        job_id: str,
        lease_token: str,
        name: str,
        source: BinaryIO | Path,
        content_type: str | None = None,
        *,
        max_bytes: int | None = None,
    ) -> ArtifactResponse:
        """Keep an uploaded file as ``jobs_dir()/<job_id>/artifacts/<name>``.

        A ``Path`` source is moved into place (the caller streamed it beside the
        destination); a file object is copied. Re-uploading a name replaces it.
        """
        if not ARTIFACT_NAME.fullmatch(name):
            raise InvalidArtifactName()
        if max_bytes is None:
            from src.web_app.security import get_settings

            max_bytes = get_settings().max_backtest_artifact_bytes
        try:
            self.store.check_lease(job_id, worker_id, lease_token)
            _canonical_job_id(job_id)
        except (LeaseLost, KeyError, ValueError):
            if isinstance(source, Path):
                source.unlink(missing_ok=True)
            raise
        folder = jobs_dir() / job_id / "artifacts"
        folder.mkdir(parents=True, exist_ok=True)
        final = folder / name
        if isinstance(source, Path):
            try:
                if source.stat().st_size > max_bytes:
                    raise ArtifactTooLarge()
                try:
                    os.replace(source, final)
                except OSError as exc:
                    if exc.errno != errno.EXDEV:
                        raise
                    # On another file system: copy it across instead.
                    with source.open("rb") as handle:
                        _copy_into(handle, folder / f"{name}.part", final, max_bytes)
            finally:
                source.unlink(missing_ok=True)
        else:
            _copy_into(source, folder / f"{name}.part", final, max_bytes)
        digest = hashlib.sha256()
        size = 0
        with final.open("rb") as handle:
            while chunk := handle.read(_COPY_CHUNK):
                digest.update(chunk)
                size += len(chunk)
        self.store.record_artifact(
            job_id,
            name=name,
            size_bytes=size,
            sha256=digest.hexdigest(),
            content_type=content_type,
            stored_path=str(final),
        )
        return ArtifactResponse(name=name, size_bytes=size, sha256=digest.hexdigest())

    def complete(self, worker_id: str, job_id: str, request: CompleteRequest) -> TerminalResponse:
        """Finish a job, running its kind's finalizer first (call from a thread)."""
        current = self.store.get(job_id)
        if current["status"] in TERMINAL_STATUSES and current["worker_id"] == worker_id:
            # An earlier call by this lease already ended the job; the store settles retries.
            row = self.store.complete(job_id, worker_id, request.lease_token, "null")
            return TerminalResponse(status=row["status"])
        row = self.store.check_lease(
            job_id, worker_id, request.lease_token, extend_seconds=self.lease_seconds
        )
        record = JobRecord.from_row(row)
        result = request.result
        encoded = json.dumps(result, default=str, ensure_ascii=False, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > MAX_RESULT_BYTES:
            return self._fail_terminal(worker_id, job_id, request.lease_token, "The result was too large")
        try:
            finalizer = kind_registry.finalizer_for(record.kind)
            if finalizer is not None:
                final = finalizer(record, result, self.artifacts(job_id))
                encoded = json.dumps(final, default=str, ensure_ascii=False, separators=(",", ":"))
        except FinalizeError as exc:
            return self._fail_terminal(
                worker_id, job_id, request.lease_token,
                safe_public_text(exc.message, _MAX_ERROR_CHARS) or "The result could not be saved",
            )
        except Exception:
            logger.exception("Finalizer for job %s (%s) crashed", job_id, record.kind)
            return self._fail_terminal(
                worker_id, job_id, request.lease_token, "The result could not be saved"
            )
        row = self.store.complete(job_id, worker_id, request.lease_token, encoded)
        self.notifier.bump()
        return TerminalResponse(status=row["status"])

    def _fail_terminal(self, worker_id: str, job_id: str, lease_token: str, message: str) -> TerminalResponse:
        row = self.store.fail(job_id, worker_id, lease_token, error=message)
        self.notifier.bump()
        return TerminalResponse(status=row["status"])

    def fail(self, worker_id: str, job_id: str, request: FailRequest) -> TerminalResponse:
        row = self.store.fail(
            job_id,
            worker_id,
            request.lease_token,
            error=safe_public_text(request.error, _MAX_ERROR_CHARS) or "The job failed",
            retryable=request.retryable,
            cancelled=request.cancelled,
        )
        self.notifier.bump()
        return TerminalResponse(status=row["status"])

    def release(self, worker_id: str, job_id: str, request: ReleaseRequest) -> TerminalResponse:
        row = self.store.release(job_id, worker_id, request.lease_token, reason=request.reason)
        self.notifier.bump()
        return TerminalResponse(status=row["status"])

    # -- maintenance ---------------------------------------------------------

    def sweep(self) -> int:
        """Resolve jobs whose worker stopped responding (plan section 5.8)."""
        changed = self.store.sweep()
        if changed:
            self.notifier.bump()
        return changed

    def cleanup(self, retention_hours: int) -> int:
        """Delete finished jobs older than the retention and their workspace folders."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=retention_hours)
        ).isoformat(timespec="microseconds")
        deleted = self.store.delete_terminal_before(cutoff)
        root = jobs_dir().resolve(strict=False)
        for job_id in deleted:
            try:
                folder = (jobs_dir() / _canonical_job_id(job_id)).resolve(strict=False)
            except ValueError:
                continue  # legacy ids that are not canonical UUIDs have no workspace here
            if folder.parent == root and folder.exists():
                shutil.rmtree(folder, ignore_errors=True)
        return len(deleted)


class JobMaintenance:
    """Background thread that sweeps expired leases and trims old jobs."""

    def __init__(
        self,
        service: JobService,
        retention_hours: Callable[[], int] | int,
        *,
        sweep_interval: float = SWEEP_INTERVAL_SECONDS,
        cleanup_interval: float = CLEANUP_INTERVAL_SECONDS,
    ) -> None:
        self.service = service
        self._retention = retention_hours if callable(retention_hours) else (lambda: retention_hours)
        self.sweep_interval = sweep_interval
        self.cleanup_interval = cleanup_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="job-maintenance", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None

    def _run(self) -> None:
        import time

        next_cleanup = 0.0
        while not self._stop.is_set():
            try:
                self.service.sweep()
                if time.monotonic() >= next_cleanup:
                    next_cleanup = time.monotonic() + self.cleanup_interval
                    self.service.cleanup(self._retention())
            except Exception:
                logger.exception("Job maintenance failed")
            self._stop.wait(self.sweep_interval)


_service: JobService | None = None
_service_lock = threading.Lock()


def get_job_service() -> JobService:
    """The process's job service, built on first use from the configured databases."""
    global _service
    with _service_lock:
        if _service is None:
            from src.orchestrator.common.db_config import get_app_db

            _service = JobService(JobsStore(jobs_db_path()), WorkerRegistry(get_app_db()))
        return _service


def set_job_service(service: JobService | None) -> None:
    """Replace the singleton (tests); ``None`` makes the next ``get_job_service`` rebuild it."""
    global _service
    with _service_lock:
        _service = service


def start_job_service(
    service: JobService | None = None, *, retention_hours: Callable[[], int] | int | None = None
) -> JobMaintenance:
    """Start the lease sweeper and hourly retention cleanup."""
    target = service or get_job_service()
    if retention_hours is None:
        from src.web_app.security import get_settings

        retention_hours = get_settings().job_retention_hours
    maintenance = JobMaintenance(target, retention_hours)
    maintenance.start()
    return maintenance


__all__ = [
    "ANY_OWNER",
    "ARTIFACT_NAME",
    "JobMaintenance",
    "JobRecord",
    "JobService",
    "get_job_service",
    "set_job_service",
    "start_job_service",
    "utc_now",
]
