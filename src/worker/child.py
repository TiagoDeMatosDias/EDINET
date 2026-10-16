"""Job child process: one per slot, started by the supervisor with the ``spawn`` method.

The supervisor talks to the child over a duplex pipe. Messages to the child:
``("job", assignment_dict)``, ``("cancel", job_id)``, ``("stop",)``. Messages from
the child: ``("ready", slot_id)``, ``("progress", job_id, percent, message, progress)``,
``("step", job_id, event)``, and finally per job either
``("done", job_id, result, [artifact paths])`` or
``("error", job_id, public_message, retryable, cancelled)``.

A listener thread owns the receiving side so a cancel request can reach a
handler that is busy in the main thread.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import shutil
import signal
import threading
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, Sequence

from src.jobs.protocol import JobAssignment, RegisterResponse
from src.worker.context import JobCancelledError, JobContext, JobError

logger = logging.getLogger(__name__)

DEFAULT_PRELOAD: tuple[str, ...] = ("pandas", "src.orchestrator", "src.backtesting", "src.screening")
ORPHAN_EXIT_SECONDS = 20.0


def _preload(modules: Sequence[str]) -> None:
    """Import heavy modules ahead of the first job so it starts quickly; failures only log."""
    import importlib

    for name in modules:
        try:
            importlib.import_module(name)
        except Exception:  # noqa: BLE001 - best effort; the handler imports what it needs
            logger.warning("Could not pre-import %s", name, exc_info=True)


class _Inbox:
    """Receives supervisor messages on a thread; jobs queue up, cancels act immediately."""

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self.jobs: queue.Queue[tuple[dict[str, Any], threading.Event] | None] = queue.Queue()
        self._events: dict[str, threading.Event] = {}
        self._early_cancels: set[str] = set()
        self._lock = threading.Lock()

    def start(self) -> None:
        threading.Thread(target=self._listen, name="child-inbox", daemon=True).start()

    def _listen(self) -> None:
        while True:
            try:
                message = self._conn.recv()
            except (EOFError, OSError):
                self._supervisor_gone()
                return
            kind = message[0]
            if kind == "job":
                assignment = message[1]
                event = threading.Event()
                with self._lock:
                    if assignment["job_id"] in self._early_cancels:
                        event.set()
                    self._events[assignment["job_id"]] = event
                self.jobs.put((assignment, event))
            elif kind == "cancel":
                with self._lock:
                    known = self._events.get(message[1])
                    if known is None:
                        self._early_cancels.add(message[1])
                if known is not None:
                    known.set()
            elif kind == "stop":
                self.jobs.put(None)
                return

    def _supervisor_gone(self) -> None:
        """The pipe closed: cancel what runs, then exit even if the handler ignores it."""
        with self._lock:
            for event in self._events.values():
                event.set()
        self.jobs.put(None)
        timer = threading.Timer(ORPHAN_EXIT_SECONDS, os._exit, args=(1,))
        timer.daemon = True
        timer.start()

    def forget(self, job_id: str) -> None:
        with self._lock:
            self._events.pop(job_id, None)
            self._early_cancels.discard(job_id)


def _artifact_files(output_dir: Path) -> list[str]:
    return sorted(str(path) for path in output_dir.iterdir() if path.is_file())


def run_assignment(
    assignment: dict[str, Any],
    cancel_event: threading.Event,
    settings: dict[str, Any],
    send: Any,
    handler_overrides: dict[str, str] | None = None,
) -> tuple[Any, ...]:
    """Run one job's handler and return the final message (``done`` or ``error``)."""
    from src.jobs.kinds import KINDS, handler_for, resolve

    parsed = JobAssignment.model_validate(assignment)
    job_id = parsed.job_id
    spec = KINDS.get(parsed.kind)
    label = spec.label if spec else parsed.kind
    output_dir = Path(parsed.workspace) / f"out-{parsed.attempt}"
    shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    def sink(event: str, data: dict[str, Any]) -> None:
        if event == "progress":
            send(("progress", job_id, data.get("percent"), data.get("message"), data.get("progress")))
        else:
            send(("step", job_id, data))

    context = JobContext(
        job_id=job_id,
        kind=parsed.kind,
        attempt=parsed.attempt,
        payload=parsed.payload,
        workspace=Path(parsed.workspace),
        output_dir=output_dir,
        cancel_event=cancel_event,
        settings_values=settings,
        sink=sink,
    )
    outcome: tuple[Any, ...]
    try:
        override = (handler_overrides or {}).get(parsed.kind)
        handler = resolve(override) if override else handler_for(parsed.kind)
        context.checkpoint()
        result = handler(context)
        if result is None:
            result = {}
        if not isinstance(result, dict):
            raise TypeError("a handler must return a dict")
        json.dumps(result)  # fail here, with a clear cause, rather than while completing
        outcome = ("done", job_id, result, _artifact_files(output_dir))
    except JobCancelledError:
        outcome = ("error", job_id, "The job was cancelled", False, True)
    except JobError as exc:
        outcome = ("error", job_id, exc.message, exc.retryable, False)
    except BaseException as exc:  # noqa: BLE001 - report every failure to the supervisor
        # A cancelled job may also end on its next report to a supervisor that is gone.
        if isinstance(exc, (KeyboardInterrupt, SystemExit, OSError, EOFError)) and cancel_event.is_set():
            outcome = ("error", job_id, "The job was cancelled", False, True)
        else:
            logger.exception("Job %s (%s) failed", job_id, parsed.kind)
            outcome = ("error", job_id, f"{label} failed ({type(exc).__name__})", False, False)

    if spec is not None and spec.writes and not (outcome[0] == "error" and outcome[-1]):
        try:
            from src.worker import maintenance

            maintenance.after_write(spec.writes)
        except Exception:  # noqa: BLE001 - maintenance never fails a job
            logger.warning("Maintenance after job %s failed", job_id, exc_info=True)
    return outcome


def child_main(
    conn: Connection,
    slot_id: int,
    lanes: list[str],
    register_response: dict[str, Any],
    preload: Sequence[str] | None = None,
    handler_overrides: dict[str, str] | None = None,
) -> None:
    """Entry point of a job child. ``preload`` and ``handler_overrides`` exist for tests."""
    for name in ("SIGINT", "SIGBREAK"):  # the supervisor decides when children stop
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), signal.SIG_IGN)
    from src.worker.runtime import install_worker_runtime

    response = RegisterResponse.model_validate(register_response)
    install_worker_runtime(response, child=True)
    logger.info("Job child %s started (lanes %s, pid %s)", slot_id, lanes, os.getpid())
    _preload(DEFAULT_PRELOAD if preload is None else preload)

    inbox = _Inbox(conn)
    inbox.start()
    send_lock = threading.Lock()

    def send(message: tuple[Any, ...]) -> None:
        with send_lock:
            conn.send(message)

    send(("ready", slot_id))
    while True:
        item = inbox.jobs.get()
        if item is None:
            break
        assignment, cancel_event = item
        try:
            outcome = run_assignment(assignment, cancel_event, dict(response.settings), send, handler_overrides)
            send(outcome)
        except (BrokenPipeError, OSError):
            break
        finally:
            inbox.forget(assignment["job_id"])
    logger.info("Job child %s stopping", slot_id)


def startup_main(register_response: dict[str, Any]) -> None:
    """Entry point of the one-off child that runs ``maintenance.startup`` (exit code 1 on failure)."""
    from src.worker.runtime import install_worker_runtime

    install_worker_runtime(register_response, child=True)
    from src.worker import maintenance

    results = maintenance.startup()
    raise SystemExit(0 if results.get("schemas") == "ok" else 1)
