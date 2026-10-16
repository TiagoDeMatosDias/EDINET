"""The worker supervisor: slots, claim loops, heartbeats and child lifecycle.

The supervisor holds no job data. Each slot owns one spawned child process and a
thread that claims a job (long poll), hands it to the child, relays progress,
reacts to cancellation and lease loss, uploads artifacts and reports the outcome.
One extra thread heartbeats for all running jobs. Heavy imports stay in children.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import platform
import signal
import socket
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, Sequence

from src.jobs.protocol import (
    ClaimRequest,
    CompleteRequest,
    FailRequest,
    HeartbeatRequest,
    JobAssignment,
    JobLease,
    ProgressRequest,
    RegisterRequest,
    RegisterResponse,
    ReleaseRequest,
    StepEvent,
    UnregisterRequest,
    WorkerPaths,
)
from src.version import __version__
from src.worker.client import (
    BACKOFF_SECONDS,
    CertificateMismatchError,
    LeaseLostError,
    NoDataAccessError,
    ServerUnavailableError,
    UnknownSessionError,
    WorkerApiError,
    WorkerClient,
    WorkerRejectedError,
    retry_unavailable,
)

logger = logging.getLogger(__name__)

SOFT_CANCEL_GRACE_SECONDS = 60.0
SHUTDOWN_WAIT_SECONDS = 10.0
PROGRESS_INTERVAL_SECONDS = 1.0
DATA_ACCESS_RECHECK_SECONDS = 30.0
PARENT_CHECK_SECONDS = 5.0
INTERACTIVE_RECYCLE_JOBS = 50
FINISH_RETRY_SECONDS = 300.0
STARTUP_TIMEOUT_SECONDS = 3600.0
LANES_BY_KIND: dict[str, list[str]] = {"interactive": ["interactive"], "batch": ["batch", "interactive"]}

EXIT_OK = 0
EXIT_CONFIGURATION = 2
EXIT_REJECTED = 3


class _Stopping(Exception):
    """Raised inside start-up when a stop was requested."""


def check_data_access(paths: WorkerPaths) -> tuple[bool, str | None]:
    """Whether market.db and filings.db exist and open read-only from this machine."""
    problems: list[str] = []
    for label, path in (("market.db", paths.market_db), ("filings.db", paths.filings_db)):
        target = Path(path)
        if not target.is_file():
            problems.append(f"{label} not found at {path}")
            continue
        try:
            with closing(sqlite3.connect(f"{target.resolve().as_uri()}?mode=ro", uri=True, timeout=5)) as conn:
                conn.execute("PRAGMA schema_version").fetchone()
        except sqlite3.Error as exc:
            problems.append(f"{label} cannot be opened: {exc}")
    return (not problems), ("; ".join(problems)[:500] or None)


def process_is_alive(pid: int) -> bool:
    """Whether ``pid`` is a running process (used to follow the server that launched us)."""
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        synchronize, query_limited, wait_timeout = 0x00100000, 0x1000, 0x102
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(synchronize | query_limited, False, pid)
        if not handle:
            return False
        try:
            return bool(kernel32.WaitForSingleObject(handle, 0) == wait_timeout)
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@dataclass
class _Running:
    assignment: JobAssignment
    lost: bool = False  # the server no longer recognises our lease
    hard: bool = False  # hard cancel (or an ignored soft cancel)
    released: bool = False  # given back during shutdown
    shutdown_cancel: bool = False  # soft-cancelled because the worker is stopping
    cancel_sent_at: float | None = None
    last_progress_sent: float = 0.0
    pending: dict[str, Any] | None = None

    @property
    def job_id(self) -> str:
        return self.assignment.job_id


@dataclass
class _Slot:
    slot_id: int
    kind: str  # "interactive" or "batch"
    process: Any = None
    conn: Connection | None = None
    job: _Running | None = None
    jobs_run: int = 0
    retired: bool = False
    thread: threading.Thread | None = None
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def lanes(self) -> list[str]:
        return LANES_BY_KIND[self.kind]

    def alive(self) -> bool:
        return self.process is not None and self.process.is_alive()


class Supervisor:
    def __init__(
        self,
        client: WorkerClient,
        *,
        interactive_slots: int | None = None,
        batch_slots: int | None = None,
        parent_pid: int | None = None,
        preload_modules: Sequence[str] | None = None,
        run_startup: bool = True,
        handler_overrides: dict[str, str] | None = None,
    ) -> None:
        self.client = client
        self._slot_override = (interactive_slots, batch_slots)
        self._parent_pid = parent_pid
        self._preload = preload_modules  # None = the child's default heavy imports (tests pass ())
        self._run_startup = run_startup
        self._handler_overrides = handler_overrides  # tests: kind -> "module:function"
        self._ctx = multiprocessing.get_context("spawn")
        self._stop = threading.Event()
        self._lock = threading.RLock()
        self._slots: list[_Slot] = []
        self._next_slot_id = 0
        self._running: dict[str, _Running] = {}
        self._session_id = uuid.uuid4().hex
        self._response: RegisterResponse | None = None
        self._config_version = 0
        self._data_access = False
        self._data_access_detail: str | None = None
        self._last_access_check = 0.0
        self._startup_ok = False
        self._threads: list[threading.Thread] = []
        self._shutdown_done = False
        self.soft_cancel_grace = SOFT_CANCEL_GRACE_SECONDS
        self.exit_code = EXIT_OK
        self._kinds: list[str] = []

    # --- public ------------------------------------------------------------

    @property
    def worker_id(self) -> str:
        return self._response.worker_id if self._response else ""

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def request_stop(self, reason: str = "requested", exit_code: int | None = None) -> None:
        if exit_code is not None and self.exit_code == EXIT_OK:
            self.exit_code = exit_code
        if not self._stop.is_set():
            logger.info("Worker stopping: %s", reason)
            self._stop.set()

    def run(self, *, handle_signals: bool = True) -> int:
        """Run until stopped; returns the exit code."""
        restore = self._install_signal_handlers() if handle_signals else None
        try:
            try:
                self.start()
            except _Stopping:
                pass
            while not self._stop.wait(0.5):
                pass
        finally:
            self.shutdown()
            if restore:
                restore()
        return self.exit_code

    def start(self) -> None:
        from src.jobs.kinds import KINDS
        from src.worker.runtime import install_worker_runtime

        self._kinds = list(KINDS)
        response = self._register()
        install_worker_runtime(response)
        logger.info(
            "Worker %s registered (session %s, %s interactive and %s batch slots)",
            response.worker_id, self._session_id, response.interactive_slots, response.batch_slots,
        )
        if self._run_startup:
            self._startup_ok = self._run_startup_child()
        self._refresh_data_access(force=True)
        self._heartbeat_once()  # the server refuses claims until it has seen data_access=true
        self._apply_slot_counts()
        self._spawn_thread(self._heartbeat_loop, "worker-heartbeat")
        if self._parent_pid:
            self._spawn_thread(self._parent_loop, "worker-parent-watch")

    def shutdown(self) -> None:
        """Stop claiming, soft-cancel, wait, release what is left, kill children, unregister."""
        if self._shutdown_done:
            return
        self._shutdown_done = True
        self._stop.set()
        with self._lock:
            slots = list(self._slots)
        for slot in slots:
            running = slot.job
            if running is not None:
                running.shutdown_cancel = True
                self._send_cancel(slot, running)
        deadline = time.monotonic() + SHUTDOWN_WAIT_SECONDS
        while time.monotonic() < deadline and any(slot.job is not None for slot in slots):
            time.sleep(0.05)
        for slot in slots:
            running = slot.job
            if running is not None and not running.released:
                running.released = True
                self._release(running, "The worker was stopped")
        for slot in slots:
            self._kill_child(slot)
        for slot in slots:
            if slot.thread is not None:
                slot.thread.join(timeout=1.0)
        if self._response is not None:
            try:
                self.client.unregister(UnregisterRequest(session_id=self._session_id))
            except Exception as exc:  # noqa: BLE001 - the server may already be gone
                logger.info("Unregister failed: %s", exc)
        self.client.close()
        logger.info("Worker stopped")

    # --- registration, startup, data access ---------------------------------

    def _register(self) -> RegisterResponse:
        attempt = 0
        while True:
            try:
                response = self.client.register(
                    RegisterRequest(
                        session_id=self._session_id,
                        version=__version__,
                        hostname=socket.gethostname(),
                        pid=os.getpid(),
                        platform=platform.platform(),
                        kinds=self._kinds,
                    )
                )
                break
            except ServerUnavailableError as exc:
                delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
                attempt += 1
                logger.warning("Cannot register (%s); retrying in %ss", exc, delay)
                if self._stop.wait(delay):
                    raise _Stopping() from exc
        self._response = response
        self._config_version = response.config_version
        self.client.claim_wait_seconds = response.claim_wait_seconds
        return response

    def _run_startup_child(self) -> bool:
        from src.worker.child import startup_main

        assert self._response is not None
        process = self._ctx.Process(
            target=startup_main, args=(self._response.model_dump(mode="json"),), name="shade-startup"
        )
        process.start()
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        while process.is_alive() and time.monotonic() < deadline and not self._stop.is_set():
            process.join(0.2)
        if process.is_alive():
            process.kill()
            process.join(5)
        ok = process.exitcode == 0
        logger.info("Startup maintenance %s (exit code %s)", "finished" if ok else "failed", process.exitcode)
        return ok

    def _refresh_data_access(self, *, force: bool = False) -> None:
        """Check data access; while it is false, re-check (and re-run start-up) every 30 s."""
        assert self._response is not None
        now = time.monotonic()
        if not force and self._data_access is False and now - self._last_access_check < DATA_ACCESS_RECHECK_SECONDS:
            return
        self._last_access_check = now
        ok, detail = check_data_access(self._response.paths)
        if not ok and not self._startup_ok and self._run_startup and not force:
            self._startup_ok = self._run_startup_child()
            ok, detail = check_data_access(self._response.paths)
        if ok != self._data_access:
            logger.info("Data access is now %s%s", ok, f" ({detail})" if detail else "")
        self._data_access, self._data_access_detail = ok, detail

    # --- threads -------------------------------------------------------------

    def _spawn_thread(self, target: Any, name: str, *args: Any) -> threading.Thread:
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        thread.start()
        self._threads.append(thread)
        return thread

    def _parent_loop(self) -> None:
        assert self._parent_pid is not None
        while not self._stop.wait(PARENT_CHECK_SECONDS):
            if not process_is_alive(self._parent_pid):
                self.request_stop("the server process is gone")
                return

    def _install_signal_handlers(self) -> Any:
        if threading.current_thread() is not threading.main_thread():
            return None
        names = ["SIGTERM", "SIGINT", "SIGBREAK"]  # SIGBREAK is CTRL_BREAK_EVENT on Windows
        previous: dict[int, Any] = {}

        def handler(signum: int, _frame: Any) -> None:
            self.request_stop(f"signal {signum}")

        for name in names:
            number = getattr(signal, name, None)
            if number is not None:
                previous[number] = signal.signal(number, handler)

        def restore() -> None:
            for number, old in previous.items():
                signal.signal(number, old)

        return restore

    # --- slots ---------------------------------------------------------------

    def _desired_slots(self) -> tuple[int, int]:
        assert self._response is not None
        interactive, batch = self._slot_override
        return (
            self._response.interactive_slots if interactive is None else interactive,
            self._response.batch_slots if batch is None else batch,
        )

    def _apply_slot_counts(self) -> None:
        """Start missing slots and retire surplus ones (a busy slot retires when its job ends)."""
        wanted = dict(zip(("interactive", "batch"), self._desired_slots(), strict=True))
        with self._lock:
            for kind, count in wanted.items():
                active = [slot for slot in self._slots if slot.kind == kind and not slot.retired]
                for _ in range(count - len(active)):
                    slot = _Slot(self._next_slot_id, kind)
                    self._next_slot_id += 1
                    self._slots.append(slot)
                    slot.thread = self._spawn_thread(self._slot_loop, f"worker-slot-{slot.slot_id}", slot)
                surplus = sorted(active, key=lambda s: (s.job is not None, -s.slot_id))[: max(0, len(active) - count)]
                for slot in surplus:
                    slot.retired = True

    def _busy(self, kind: str) -> int:
        with self._lock:
            return sum(1 for slot in self._slots if slot.kind == kind and slot.job is not None)

    def _start_child(self, slot: _Slot) -> None:
        from src.worker.child import child_main

        assert self._response is not None
        parent_conn, child_conn = self._ctx.Pipe(duplex=True)
        process = self._ctx.Process(
            target=child_main,
            args=(
                child_conn,
                slot.slot_id,
                slot.lanes,
                self._response.model_dump(mode="json"),
                self._preload,
                self._handler_overrides,
            ),
            name=f"shade-slot-{slot.slot_id}",
        )
        process.start()
        child_conn.close()
        slot.process, slot.conn, slot.jobs_run = process, parent_conn, 0

    def _kill_child(self, slot: _Slot) -> None:
        process, conn = slot.process, slot.conn
        slot.process, slot.conn = None, None
        if process is not None:
            try:
                if process.is_alive():
                    process.kill()
                process.join(5)
            except (OSError, ValueError, AssertionError):
                pass
        if conn is not None:
            try:
                conn.close()
            except OSError:
                pass

    def _stop_child_gracefully(self, slot: _Slot) -> None:
        process, conn = slot.process, slot.conn
        if process is not None and conn is not None and process.is_alive():
            try:
                with slot.send_lock:
                    conn.send(("stop",))
                process.join(3)
            except (OSError, ValueError):
                pass
        self._kill_child(slot)

    def _send_cancel(self, slot: _Slot, running: _Running) -> None:
        conn = slot.conn
        if conn is None:
            return
        try:
            with slot.send_lock:
                conn.send(("cancel", running.job_id))
        except (OSError, ValueError):
            return
        if running.cancel_sent_at is None:
            running.cancel_sent_at = time.monotonic()

    # --- the slot loop -------------------------------------------------------

    def _slot_loop(self, slot: _Slot) -> None:
        assert self._response is not None
        try:
            while not self._stop.is_set() and not slot.retired:
                try:
                    if not slot.alive():
                        self._kill_child(slot)
                        self._start_child(slot)
                    if not self._data_access:
                        self._stop.wait(1.0)
                        continue
                    assignment = self.client.claim(
                        ClaimRequest(session_id=self._session_id, lanes=slot.lanes, kinds=self._kinds)
                    )
                except ServerUnavailableError as exc:
                    logger.warning("Claim failed (%s)", exc)
                    self._stop.wait(2.0)
                    continue
                except UnknownSessionError:
                    self._reregister()
                    continue
                except NoDataAccessError:
                    self._stop.wait(2.0)
                    continue
                except (WorkerRejectedError, CertificateMismatchError) as exc:
                    self._fatal(exc)
                    return
                except Exception:  # noqa: BLE001 - a slot must not die
                    logger.exception("Slot %s failed", slot.slot_id)
                    self._stop.wait(2.0)
                    continue
                if assignment is None:
                    continue
                running = _Running(assignment)
                if self._stop.is_set():
                    self._release(running, "The worker was stopped")
                    break
                try:
                    self._run_assignment(slot, running)
                except Exception:  # noqa: BLE001
                    logger.exception("Job %s handling failed", assignment.job_id)
        finally:
            if slot.job is None:
                self._stop_child_gracefully(slot)
            with self._lock:
                if slot in self._slots and slot.retired:
                    self._slots.remove(slot)

    def _run_assignment(self, slot: _Slot, running: _Running) -> None:
        assignment = running.assignment
        with self._lock:
            slot.job = running
            self._running[assignment.job_id] = running
        logger.info("Running job %s (%s, attempt %s) in slot %s", assignment.job_id, assignment.kind, assignment.attempt, slot.slot_id)
        try:
            outcome = self._watch(slot, running)
            kind = outcome[0]
            if kind == "done":
                self._finish_done(running, outcome[1], outcome[2])
            elif kind == "error":
                message, retryable, cancelled = outcome[1:]
                if cancelled and running.shutdown_cancel:
                    running.released = True
                    self._release(running, "The worker was stopped")
                else:
                    self._fail(running, message, retryable, cancelled)
            elif kind == "crashed":
                logger.error("Job child for slot %s died during job %s", slot.slot_id, assignment.job_id)
                self._fail(running, "The worker process stopped unexpectedly", True, False)
            elif kind == "hard":
                self._fail(running, "The job was cancelled", False, True)
            # "lost" and "released": nothing to report
            slot.jobs_run += 1
            if kind not in ("done", "error"):
                self._kill_child(slot)
            elif assignment.lane == "batch" or slot.jobs_run >= INTERACTIVE_RECYCLE_JOBS:
                self._stop_child_gracefully(slot)
        finally:
            self._cleanup_output(assignment)
            with self._lock:
                self._running.pop(assignment.job_id, None)
                slot.job = None

    @staticmethod
    def _cleanup_output(assignment: JobAssignment) -> None:
        import shutil

        shutil.rmtree(Path(assignment.workspace) / f"out-{assignment.attempt}", ignore_errors=True)

    def _watch(self, slot: _Slot, running: _Running) -> tuple[Any, ...]:
        """Relay child messages until the job ends; returns the outcome."""
        conn = slot.conn
        assert conn is not None
        try:
            with slot.send_lock:
                conn.send(("job", running.assignment.model_dump(mode="json")))
        except (OSError, ValueError):
            return self._crash_outcome(running)
        while True:
            if running.lost or running.hard or running.released:
                self._kill_child(slot)
                return self._crash_outcome(running)
            if (
                running.cancel_sent_at is not None
                and not running.shutdown_cancel
                and time.monotonic() - running.cancel_sent_at > self.soft_cancel_grace
            ):
                logger.warning("Job %s ignored the cancel request; killing it", running.job_id)
                running.hard = True
                continue
            try:
                message = conn.recv() if conn.poll(0.2) else None
            except (EOFError, OSError):
                return self._crash_outcome(running)
            if message is None:
                if not slot.alive():
                    return self._crash_outcome(running)
                self._flush_progress(slot, running)
                continue
            kind = message[0]
            if kind == "progress":
                self._queue_progress(running, message[2], message[3], message[4])
                self._flush_progress(slot, running)
            elif kind == "step":
                self._send_step(slot, running, message[2])
            elif kind == "done":
                return ("done", message[2], message[3])
            elif kind == "error":
                return ("error", message[2], message[3], message[4])

    @staticmethod
    def _crash_outcome(running: _Running) -> tuple[Any, ...]:
        if running.lost:
            return ("lost",)
        if running.released:
            return ("released",)
        if running.hard:
            return ("hard",)
        return ("crashed",)

    # --- progress ------------------------------------------------------------

    @staticmethod
    def _queue_progress(running: _Running, percent: Any, message: Any, progress: Any) -> None:
        pending = running.pending or {}
        for key, value in (("progress_percent", percent), ("status_message", message), ("progress", progress)):
            if value is not None:
                pending[key] = value
        running.pending = pending

    def _flush_progress(self, slot: _Slot, running: _Running, *, force: bool = False) -> None:
        if not running.pending:
            return
        now = time.monotonic()
        if not force and now - running.last_progress_sent < PROGRESS_INTERVAL_SECONDS:
            return
        body, running.pending = dict(running.pending), None
        running.last_progress_sent = now
        if "progress_percent" in body:
            body["progress_percent"] = max(0.0, min(100.0, float(body["progress_percent"])))
        if "status_message" in body:
            body["status_message"] = str(body["status_message"])[:500]
        self._send_progress(slot, running, ProgressRequest(lease_token=running.assignment.lease_token, **body))

    def _send_step(self, slot: _Slot, running: _Running, event: dict[str, Any]) -> None:
        try:
            step = StepEvent.model_validate(event)
        except ValueError:
            logger.warning("Dropping invalid step event for job %s: %s", running.job_id, event)
            return
        request = ProgressRequest(lease_token=running.assignment.lease_token, step=step)
        self._send_progress(slot, running, request, attempts=3)

    def _send_progress(self, slot: _Slot, running: _Running, request: ProgressRequest, attempts: int = 1) -> None:
        for attempt in range(attempts):
            try:
                response = self.client.progress(running.job_id, request)
            except LeaseLostError:
                running.lost = True
                return
            except ServerUnavailableError as exc:
                logger.warning("Progress for %s not delivered: %s", running.job_id, exc)
                if attempt + 1 < attempts and not self._stop.wait(1.0 + attempt):
                    continue
                return
            except WorkerApiError as exc:
                logger.warning("Progress for %s rejected: %s", running.job_id, exc)
                return
            self._apply_cancel(slot, running, response.cancel)
            return

    def _apply_cancel(self, slot: _Slot, running: _Running, mode: str) -> None:
        if mode == "soft":
            self._send_cancel(slot, running)
        elif mode == "hard":
            running.hard = True

    # --- reporting results ---------------------------------------------------

    def _call(self, call: Any, what: str) -> Any:
        """A terminal call, retried through outages; ``None`` when the lease is gone."""
        try:
            return retry_unavailable(
                call, stop=self._stop, deadline=time.monotonic() + FINISH_RETRY_SECONDS, what=what
            )
        except LeaseLostError:
            logger.warning("%s: the lease was lost", what)
        except ServerUnavailableError as exc:
            logger.error("%s: giving up (%s); the lease will expire", what, exc)
        return None

    def _finish_done(self, running: _Running, result: dict[str, Any], artifacts: list[str]) -> None:
        a = running.assignment
        if running.pending:
            body, running.pending = dict(running.pending), None
            try:
                self.client.progress(a.job_id, ProgressRequest(lease_token=a.lease_token, **body))
            except Exception as exc:  # noqa: BLE001 - the final progress is advisory
                logger.info("Final progress for %s not delivered: %s", a.job_id, exc)
        for path in artifacts:
            try:
                uploaded = self._call(lambda p=path: self.client.upload_artifact(a.job_id, a.lease_token, p), f"upload {path}")
            except WorkerApiError as exc:
                self._fail(running, f"Could not upload {Path(path).name}: {exc}", False, False)
                return
            if uploaded is None:
                return
        try:
            self._call(
                lambda: self.client.complete(a.job_id, CompleteRequest(lease_token=a.lease_token, result=result)),
                f"complete {a.job_id}",
            )
        except WorkerApiError as exc:
            self._fail(running, f"The result was rejected: {exc}", False, False)

    def _fail(self, running: _Running, error: str, retryable: bool, cancelled: bool) -> None:
        a = running.assignment
        request = FailRequest(
            lease_token=a.lease_token, error=(error or "The job failed")[:1000], retryable=retryable, cancelled=cancelled
        )
        try:
            self._call(lambda: self.client.fail(a.job_id, request), f"fail {a.job_id}")
        except WorkerApiError as exc:
            logger.warning("fail %s rejected: %s", a.job_id, exc)

    def _release(self, running: _Running, reason: str) -> None:
        a = running.assignment
        try:
            self.client.release(a.job_id, ReleaseRequest(lease_token=a.lease_token, reason=reason))
        except Exception as exc:  # noqa: BLE001 - shutting down; the lease expires on its own
            logger.warning("Release of %s failed: %s", a.job_id, exc)

    # --- heartbeat -----------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        assert self._response is not None
        while not self._stop.wait(max(1, self._response.heartbeat_seconds)):
            try:
                self._heartbeat_once()
            except Exception:  # noqa: BLE001 - keep heartbeating
                logger.exception("Heartbeat failed")

    def _reregister(self) -> None:
        with self._lock:
            response = self._register()
            from src.worker.runtime import install_worker_runtime

            install_worker_runtime(response)
            self._apply_slot_counts()

    def _fatal(self, exc: Exception) -> None:
        code = EXIT_REJECTED if isinstance(exc, WorkerRejectedError) else EXIT_CONFIGURATION
        logger.error("The server rejected this worker: %s", exc)
        self.request_stop(str(exc), exit_code=code)

    def _heartbeat_once(self) -> None:
        self._refresh_data_access()
        with self._lock:
            leases = [
                JobLease(job_id=r.job_id, lease_token=r.assignment.lease_token)
                for r in self._running.values()
                if not (r.lost or r.released)
            ]
        request = HeartbeatRequest(
            session_id=self._session_id,
            data_access=self._data_access,
            data_access_detail=self._data_access_detail,
            interactive_busy=self._busy("interactive"),
            batch_busy=self._busy("batch"),
            jobs=leases,
        )
        try:
            response = self.client.heartbeat(request)
        except ServerUnavailableError as exc:
            logger.warning("Heartbeat failed: %s", exc)
            return
        except UnknownSessionError:
            self._reregister()
            return
        except (WorkerRejectedError, CertificateMismatchError) as exc:
            self._fatal(exc)
            return
        for state in response.jobs:
            self._apply_lease_state(state.job_id, state.lease_ok, state.cancel)
        if response.shutdown:
            self.request_stop("the server asked this worker to shut down")
        elif response.config_version != self._config_version:
            logger.info("Configuration changed (version %s); re-registering", response.config_version)
            self._reregister()

    def _apply_lease_state(self, job_id: str, lease_ok: bool, cancel: str) -> None:
        with self._lock:
            running = self._running.get(job_id)
            slot = next((s for s in self._slots if s.job is running), None) if running else None
        if running is None or slot is None:
            return
        if not lease_ok:
            logger.warning("Lease for job %s was lost; killing its process", job_id)
            running.lost = True
            self._kill_child(slot)
        elif cancel == "soft":
            self._send_cancel(slot, running)
        elif cancel == "hard":
            running.hard = True
            self._kill_child(slot)
