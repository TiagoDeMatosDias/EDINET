"""Rolling backtests as background jobs the browser polls.

A rolling run can take many minutes. Running it in a worker thread and
polling for progress keeps it going when the page is left or reloaded, and
works through proxies that buffer streamed responses (Cloudflare quick tunnels
do not pass Server-Sent Events). At most ``slots`` runs compute at once; the
rest wait their turn.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)

_KEEP_FINISHED_SECONDS = 3600


@dataclass
class RollingJob:
    job_id: str
    owner: str | None
    status: str = "queued"  # queued, running, saving, complete, failed, cancelled
    progress: dict[str, Any] = field(default_factory=dict)
    result_id: str | None = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    cancel: threading.Event = field(default_factory=threading.Event)

    def view(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "progress": self.progress,
            "result_id": self.result_id,
            "error": self.error,
            "created_at": self.created_at,
        }


class _Progress:
    """The ``progress_queue`` the runner writes to: keeps only the latest message."""

    def __init__(self, job: RollingJob) -> None:
        self.job = job

    def put(self, message: dict[str, Any]) -> None:
        if message.get("type") == "result":
            return
        self.job.progress = {key: value for key, value in message.items() if key != "type"}


class RollingJobs:
    def __init__(self, slots: int = 2) -> None:
        self._jobs: dict[str, RollingJob] = {}
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(slots)

    def _prune(self) -> None:
        cutoff = time.time() - _KEEP_FINISHED_SECONDS
        for job_id, job in list(self._jobs.items()):
            if job.finished_at is not None and job.finished_at < cutoff:
                del self._jobs[job_id]

    def start(
        self,
        owner: str | None,
        run: Callable[[Any, threading.Event], dict],
        save: Callable[[dict], str],
    ) -> RollingJob:
        """Start ``run(progress_queue, cancel_event)``; ``save(result)`` stores it and returns its id."""
        job = RollingJob(job_id=uuid.uuid4().hex, owner=owner)
        with self._lock:
            self._prune()
            self._jobs[job.job_id] = job
        threading.Thread(target=self._work, args=(job, run, save), name=f"rolling-{job.job_id[:8]}", daemon=True).start()
        return job

    def _work(self, job: RollingJob, run: Callable[[Any, threading.Event], dict], save: Callable[[dict], str]) -> None:
        try:
            while not self._slots.acquire(timeout=1.0):
                if job.cancel.is_set():
                    job.status = "cancelled"
                    return
            try:
                if job.cancel.is_set():
                    job.status = "cancelled"
                    return
                job.status = "running"
                result = run(_Progress(job), job.cancel)
                job.status = "saving"
                job.result_id = save(result)
                job.status = "complete"
            finally:
                self._slots.release()
        except Exception as exc:  # noqa: BLE001 - the job reports, never raises
            if "cancelled" in str(exc).lower():
                job.status = "cancelled"
            else:
                logger.exception("Rolling backtest job %s failed", job.job_id)
                job.status = "failed"
                job.error = str(exc) if isinstance(exc, ValueError) else "The backtest failed; see the server log."
        finally:
            job.finished_at = time.time()

    def get(self, job_id: str, owner: str | None) -> RollingJob | None:
        with self._lock:
            job = self._jobs.get(job_id)
        return job if job is not None and job.owner == owner else None

    def for_owner(self, owner: str | None) -> list[RollingJob]:
        with self._lock:
            return [job for job in self._jobs.values() if job.owner == owner]


jobs = RollingJobs()
