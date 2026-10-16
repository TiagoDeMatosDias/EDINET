"""``InlineWorker``: run jobs inside the current process, straight from a ``JobService``.

No HTTP and no subprocess: it claims through ``service.claim_now``, runs the real
handler in a thread with a real ``JobContext``, stores artifacts and completes or
fails through the service. Feature tests use it as
``with InlineWorker(get_job_service()): client.post(...)``.
"""

from __future__ import annotations

import logging
import threading
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable

from src.jobs.kinds import KINDS, handler_for
from src.jobs.protocol import (
    ClaimRequest,
    CompleteRequest,
    FailRequest,
    HeartbeatRequest,
    JobAssignment,
    JobLease,
    ProgressRequest,
    RegisterRequest,
    StepEvent,
    UnregisterRequest,
)
from src.version import __version__
from src.worker.context import JobCancelledError, JobContext, JobError

logger = logging.getLogger(__name__)

POLL_SECONDS = 0.02
MONITOR_SECONDS = 0.1
IDLE_HEARTBEAT_SECONDS = 1.0
_SETTING_KEYS = (
    "edinet.api_key",
    "limits.max_backtest_artifact_bytes",
    "limits.max_upload_bytes",
    "limits.max_export_bytes",
    "limits.max_report_artifact_bytes",
)


def _lease_lost_types() -> tuple[type[BaseException], ...]:
    try:
        from src.jobs.errors import LeaseLost
    except ImportError:
        return ()
    return (LeaseLost,)


class InlineWorker:
    def __init__(
        self,
        service: Any,
        *,
        lanes: Iterable[str] = ("batch", "interactive"),
        kinds: Iterable[str] | None = None,
        worker_id: str = "inline",
        handlers: dict[str, Callable[[JobContext], dict]] | None = None,
    ) -> None:
        self.service = service
        self.lanes = list(lanes)
        self.kinds = list(kinds) if kinds is not None else list(KINDS)
        self.worker_id = worker_id
        self.handlers = dict(handlers or {})
        self._session_id = uuid.uuid4().hex
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()
        self._current: tuple[JobAssignment, threading.Event] | None = None

    # --- lifecycle -----------------------------------------------------------

    def __enter__(self) -> InlineWorker:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    def start(self) -> None:
        self.service.registry.ensure_local_worker(self.worker_id, name="Inline worker")
        self.service.register(
            self.worker_id,
            RegisterRequest(
                session_id=self._session_id,
                version=__version__,
                hostname="inline",
                platform="inline",
                kinds=self.kinds,
            ),
        )
        self._heartbeat()
        for target, name in ((self._run_loop, "inline-worker"), (self._monitor_loop, "inline-worker-monitor")):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        with self._lock:
            if self._current is not None:
                self._current[1].set()  # let a long handler end
        for thread in self._threads:
            thread.join(timeout)
        self._threads.clear()
        try:
            self.service.unregister(self.worker_id, UnregisterRequest(session_id=self._session_id))
        except Exception:  # noqa: BLE001 - the service may be torn down already
            logger.debug("Inline worker unregister failed", exc_info=True)

    # --- threads -------------------------------------------------------------

    def _heartbeat(self) -> Any:
        with self._lock:
            current = self._current
        request = HeartbeatRequest(
            session_id=self._session_id,
            data_access=True,
            jobs=[JobLease(job_id=current[0].job_id, lease_token=current[0].lease_token)] if current else [],
            interactive_busy=1 if current and current[0].lane == "interactive" else 0,
            batch_busy=1 if current and current[0].lane == "batch" else 0,
        )
        return self.service.heartbeat(self.worker_id, request)

    def _monitor_loop(self) -> None:
        idle_since_beat = 0.0
        while not self._stop.wait(MONITOR_SECONDS):
            with self._lock:
                current = self._current
            idle_since_beat += MONITOR_SECONDS
            if current is None and idle_since_beat < IDLE_HEARTBEAT_SECONDS:
                continue
            idle_since_beat = 0.0
            try:
                response = self._heartbeat()
            except Exception:  # noqa: BLE001 - keep monitoring
                logger.debug("Inline heartbeat failed", exc_info=True)
                continue
            if current is None:
                continue
            for state in response.jobs:
                if state.job_id == current[0].job_id and (not state.lease_ok or state.cancel != "none"):
                    current[1].set()

    def _run_loop(self) -> None:
        request = ClaimRequest(session_id=self._session_id, lanes=self.lanes, kinds=self.kinds)
        while not self._stop.is_set():
            try:
                assignment = self.service.claim_now(self.worker_id, request)
            except Exception:  # noqa: BLE001
                logger.exception("Inline claim failed")
                self._stop.wait(0.5)
                continue
            if assignment is None:
                self._stop.wait(POLL_SECONDS)
                continue
            try:
                self._run_job(assignment)
            except Exception:  # noqa: BLE001
                logger.exception("Inline job %s handling failed", assignment.job_id)

    # --- running one job -----------------------------------------------------

    def _settings(self) -> dict[str, Any]:
        try:
            from src.settings import get_setting

            return {key: get_setting(key) for key in _SETTING_KEYS}
        except Exception:  # noqa: BLE001 - settings are optional for handlers under test
            return {}

    def _run_job(self, assignment: JobAssignment) -> None:
        cancel_event = threading.Event()
        with self._lock:
            self._current = (assignment, cancel_event)
        lease_lost = _lease_lost_types()
        job_id, token = assignment.job_id, assignment.lease_token
        output_dir = Path(assignment.workspace) / f"out-{assignment.attempt}"
        output_dir.mkdir(parents=True, exist_ok=True)
        spec = KINDS.get(assignment.kind)
        label = spec.label if spec else assignment.kind

        def sink(event: str, data: dict[str, Any]) -> None:
            if event == "progress":
                request = ProgressRequest(
                    lease_token=token,
                    progress_percent=None if data.get("percent") is None else max(0.0, min(100.0, float(data["percent"]))),
                    status_message=(data.get("message") or None) and str(data["message"])[:500],
                    progress=data.get("progress"),
                )
            else:
                request = ProgressRequest(lease_token=token, step=StepEvent.model_validate(data))
            try:
                response = self.service.progress(self.worker_id, job_id, request)
            except lease_lost:
                cancel_event.set()
                return
            if response.cancel != "none":
                cancel_event.set()

        context = JobContext(
            job_id=job_id,
            kind=assignment.kind,
            attempt=assignment.attempt,
            payload=assignment.payload,
            workspace=Path(assignment.workspace),
            output_dir=output_dir,
            cancel_event=cancel_event,
            settings_values=self._settings(),
            sink=sink,
        )
        try:
            try:
                handler = self.handlers.get(assignment.kind) or handler_for(assignment.kind)
                context.checkpoint()
                result = handler(context) or {}
                for path in sorted(output_dir.iterdir()):
                    if path.is_file():
                        self.service.store_artifact(self.worker_id, job_id, token, path.name, path)
                self.service.complete(self.worker_id, job_id, CompleteRequest(lease_token=token, result=result))
            except JobCancelledError:
                self._fail(job_id, token, "The job was cancelled", False, True)
            except JobError as exc:
                self._fail(job_id, token, exc.message, exc.retryable, False)
            except lease_lost:
                logger.info("Inline job %s lost its lease", job_id)
            except Exception as exc:  # noqa: BLE001 - mirror the child: generic public message
                logger.exception("Inline job %s (%s) failed", job_id, assignment.kind)
                self._fail(job_id, token, f"{label} failed ({type(exc).__name__})", False, False)
        finally:
            with self._lock:
                self._current = None

    def _fail(self, job_id: str, token: str, error: str, retryable: bool, cancelled: bool) -> None:
        try:
            self.service.fail(
                self.worker_id,
                job_id,
                FailRequest(lease_token=token, error=error, retryable=retryable, cancelled=cancelled),
            )
        except _lease_lost_types():
            logger.info("Inline job %s lost its lease before failing", job_id)
