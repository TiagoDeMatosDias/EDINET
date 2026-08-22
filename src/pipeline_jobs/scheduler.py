"""Background scheduler for persisted administrator pipeline schedules."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable

from config import Config
from src.orchestrator import validate_input
from src.orchestrator.orchestrator import (
    constrain_pipeline_paths,
    resolve_file_uploads,
)

from .manager import PipelineJobManager
from .store import JobStore

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 5 * 60


class PipelineScheduler:
    """Submit at most one due schedule when the pipeline queue is idle."""

    def __init__(
        self,
        store: JobStore,
        manager: PipelineJobManager,
        *,
        input_roots: tuple[str, ...] = (),
        max_upload_bytes: int = 10 * 1024 * 1024,
        check_interval_seconds: float = CHECK_INTERVAL_SECONDS,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = store
        self.manager = manager
        self.input_roots = input_roots
        self.max_upload_bytes = max_upload_bytes
        self.check_interval_seconds = check_interval_seconds
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._next_check_at: datetime | None = None
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._check_lock = threading.Lock()
        self._stopping = False

    def start(self) -> None:
        with self._state_lock:
            if self._thread and self._thread.is_alive():
                return
            self._stopping = False
            self._wake_event.clear()
            self._next_check_at = self._now() + timedelta(seconds=self.check_interval_seconds)
            self._thread = threading.Thread(
                target=self._run,
                name="pipeline-scheduler",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._state_lock:
            self._stopping = True
            self._wake_event.set()
            thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self.check_interval_seconds + 1.0))
        with self._state_lock:
            self._thread = None

    def _run(self) -> None:
        while True:
            with self._state_lock:
                if self._stopping:
                    return
                next_check_at = self._next_check_at or self._now()
            delay = max(0.0, (next_check_at - self._now()).total_seconds())
            if self._wake_event.wait(delay):
                self._wake_event.clear()
                continue
            try:
                self.run_once()
            finally:
                with self._state_lock:
                    self._next_check_at = self._now() + timedelta(seconds=self.check_interval_seconds)

    def run_once(self) -> list[str]:
        """Trigger one due schedule, returning the created job IDs."""
        with self._check_lock:
            return self._run_once()

    def check_now(self) -> list[str]:
        """Run a check immediately and reset the next automatic check window."""
        triggered = self.run_once()
        with self._state_lock:
            self._next_check_at = self._now() + timedelta(seconds=self.check_interval_seconds)
            self._wake_event.set()
        return triggered

    def next_check_at(self) -> datetime:
        with self._state_lock:
            return self._next_check_at or (
                self._now() + timedelta(seconds=self.check_interval_seconds)
            )

    def status(self, triggered_job_ids: list[str] | None = None) -> dict[str, object]:
        return {
            "checked_at": self._now(),
            "next_check_at": self.next_check_at(),
            "active_pipeline": self.manager.active_count() > 0,
            "triggered_job_ids": triggered_job_ids or [],
        }

    def _run_once(self) -> list[str]:
        if self.manager.active_count() > 0:
            return []

        current = self._now()
        triggered: list[str] = []
        for schedule in self.store.list_schedules(enabled_only=True):
            if not self._is_due(schedule, current):
                continue
            try:
                job_id = self._submit(schedule)
            except Exception:
                logger.exception(
                    "Scheduled pipeline '%s' could not be queued",
                    schedule.get("name", schedule.get("schedule_id")),
                )
                continue
            run_at = current.isoformat()
            self.store.mark_schedule_run(schedule["schedule_id"], run_at)
            triggered.append(job_id)
            # The queue has one worker; do not let one scheduler pass enqueue
            # another run behind a pipeline that was just submitted.
            break
        return triggered

    def _submit(self, schedule: dict) -> str:
        job_id = self.manager.new_job_id()
        workspace = self.manager.workspace_for(job_id, create=True)
        try:
            steps = validate_input(schedule["config"], schedule["steps"])
            config = resolve_file_uploads(
                dict(schedule["config"]),
                steps,
                workspace=workspace,
                max_bytes=self.max_upload_bytes,
            )
            config = constrain_pipeline_paths(
                config,
                steps,
                workspace=workspace,
                allowed_input_roots=self.input_roots,
            )
            job = self.manager.submit(Config.from_dict(config), steps, job_id=job_id)
        except Exception:
            self.manager.discard_workspace(job_id)
            raise
        return str(job["job_id"])

    @staticmethod
    def _is_due(schedule: dict, now: datetime) -> bool:
        last_run = schedule.get("last_run_at")
        if not last_run:
            return True
        try:
            previous = datetime.fromisoformat(str(last_run))
        except ValueError:
            return True
        if previous.tzinfo is None:
            previous = previous.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        frequency = schedule.get("frequency")
        if frequency == "daily":
            return now - previous >= timedelta(days=1)
        if frequency == "weekly":
            return now - previous >= timedelta(days=7)
        if frequency == "monthly":
            return (now.year * 12 + now.month) - (previous.year * 12 + previous.month) >= 1
        return False


__all__ = ["CHECK_INTERVAL_SECONDS", "PipelineScheduler"]
