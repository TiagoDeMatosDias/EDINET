"""Single-worker pipeline queue with truthful cancellation and results."""

from __future__ import annotations

import inspect
import json
import logging
import shutil
import threading
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from pathlib import Path
from time import perf_counter
from typing import Any, Callable
from uuid import UUID, uuid4

from src.orchestrator import execute_step

from .context import PipelineCancelled, StepExecutionContext
from .redaction import safe_public_text, serialize_bounded
from .store import TERMINAL_STATUSES, JobStore

logger = logging.getLogger(__name__)

StepExecutor = Callable[..., Any]


class InvalidJobState(RuntimeError):
    """Raised when an operation is invalid for the job's current state."""


class ForceCancellationUnsupported(InvalidJobState):
    """Raised when hard termination of a running Python thread is requested."""


class PipelineJobManager:
    """Queue pipeline work and persist every externally visible transition."""

    def __init__(
        self,
        store: JobStore,
        *,
        step_executor: StepExecutor | None = None,
        max_workers: int = 1,
        workspace_root: str | Path | None = None,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self.store = store
        self.workspace_root = Path(
            workspace_root or (store.path.parent / "jobs")
        ).resolve(strict=False)
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        self._step_executor = step_executor or execute_step
        self._executor_accepts_context = self._accepts_context(
            self._step_executor
        )
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="pipeline-job",
        )
        self._lock = threading.Lock()
        self._cancel_events: dict[str, threading.Event] = {}
        self._futures: dict[str, Future[None]] = {}
        interrupted = self.store.interrupt_incomplete()
        if interrupted:
            logger.warning(
                "Marked %d unfinished pipeline job(s) interrupted",
                interrupted,
            )

    @staticmethod
    def new_job_id() -> str:
        return str(uuid4())

    def workspace_for(self, job_id: str, *, create: bool = False) -> Path:
        """Return the isolated workspace for a canonical UUID job ID."""
        try:
            canonical = str(UUID(job_id))
        except (ValueError, AttributeError) as exc:
            raise ValueError("job_id must be a UUID") from exc
        if canonical != job_id.casefold():
            raise ValueError("job_id must use canonical UUID form")
        workspace = self.workspace_root / canonical
        if create:
            workspace.mkdir(parents=True, exist_ok=True)
        return workspace

    def discard_workspace(self, job_id: str) -> None:
        """Remove only the verified workspace belonging to one job."""
        workspace = self.workspace_for(job_id)
        if workspace.exists():
            shutil.rmtree(workspace)

    @staticmethod
    def _accepts_context(executor: StepExecutor) -> bool:
        try:
            parameters = inspect.signature(executor).parameters.values()
        except (TypeError, ValueError):
            return False
        return any(
            parameter.name == "context"
            or parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )

    def _invoke_step(
        self,
        step_name: str,
        config: Any,
        overwrite: bool,
        context: StepExecutionContext,
    ) -> Any:
        kwargs: dict[str, Any] = {"overwrite": overwrite}
        if self._executor_accepts_context:
            kwargs["context"] = context
        return self._step_executor(step_name, config, **kwargs)

    def submit(
        self,
        config: Any,
        steps: list[dict[str, Any]],
        *,
        job_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist and enqueue a validated execution specification."""
        normalized_steps = [dict(step) for step in steps]
        if not normalized_steps:
            raise ValueError("At least one pipeline step is required")

        selected_job_id = job_id or self.new_job_id()
        self.workspace_for(selected_job_id, create=True)
        try:
            job = self.store.create_job(selected_job_id, normalized_steps)
        except Exception:
            self.discard_workspace(selected_job_id)
            raise
        cancel_event = threading.Event()
        with self._lock:
            self._cancel_events[selected_job_id] = cancel_event
            future = self._pool.submit(
                self._run_job,
                selected_job_id,
                config,
                normalized_steps,
                cancel_event,
            )
            self._futures[selected_job_id] = future
        future.add_done_callback(
            lambda completed: self._forget(selected_job_id, completed)
        )
        return job

    def _forget(self, job_id: str, future: Future[None]) -> None:
        try:
            future.result()
        except CancelledError:
            pass
        except Exception:
            logger.exception("Pipeline worker crashed for job %s", job_id)
            try:
                self.store.finish_job(
                    job_id,
                    "failed",
                    "Pipeline worker stopped unexpectedly",
                )
            except Exception:
                logger.exception(
                    "Could not persist worker failure for job %s",
                    job_id,
                )
        finally:
            with self._lock:
                self._futures.pop(job_id, None)
                self._cancel_events.pop(job_id, None)

    def _run_job(
        self,
        job_id: str,
        config: Any,
        steps: list[dict[str, Any]],
        cancel_event: threading.Event,
    ) -> None:
        if cancel_event.is_set() or not self.store.start_job(job_id):
            current = self.store.get_job(job_id)
            if current["status"] in {"pending", "cancelling"}:
                self.store.finish_job(
                    job_id,
                    "cancelled",
                    "Cancelled before execution",
                )
            return

        step_count = len(steps)
        for ordinal, step in enumerate(steps):
            if cancel_event.is_set():
                self.store.finish_job(job_id, "cancelled", "Cancelled by user")
                return

            step_name = str(step["name"])
            progress_before = (ordinal / step_count) * 100
            self.store.start_step(job_id, ordinal, step_name, progress_before)
            started = perf_counter()
            step_progress = 0.0

            def report_progress(
                percent: float,
                message: str | None,
                step_ordinal: int = ordinal,
            ) -> None:
                nonlocal step_progress
                step_progress = max(step_progress, min(100.0, percent))
                job_progress = (
                    (step_ordinal + (step_progress / 100.0)) / step_count
                ) * 100
                self.store.update_step_progress(
                    job_id,
                    step_ordinal,
                    step_progress,
                    job_progress,
                    safe_public_text(message),
                )

            context = StepExecutionContext(
                job_id=job_id,
                step_name=step_name,
                workspace=self.workspace_for(job_id),
                cancel_event=cancel_event,
                progress_callback=report_progress,
            )
            try:
                context.checkpoint()
                result = self._invoke_step(
                    step_name,
                    config,
                    bool(step.get("overwrite", False)),
                    context,
                )
            except PipelineCancelled:
                duration_ms = max(
                    0,
                    round((perf_counter() - started) * 1000),
                )
                progress = ((ordinal + step_progress / 100.0) / step_count) * 100
                self.store.finish_step(
                    job_id,
                    ordinal,
                    status="cancelled",
                    duration_ms=duration_ms,
                    progress_percent=progress,
                    step_progress_percent=step_progress,
                    error_message="Cancelled by user",
                )
                self.store.finish_job(job_id, "cancelled", "Cancelled by user")
                logger.info(
                    "Pipeline job %s cancelled in step %s",
                    job_id,
                    step_name,
                )
                return
            except Exception as exc:
                duration_ms = max(
                    0,
                    round((perf_counter() - started) * 1000),
                )
                public_error = (
                    f"Step '{step_name}' failed ({type(exc).__name__})"
                )
                self.store.finish_step(
                    job_id,
                    ordinal,
                    status="failed",
                    duration_ms=duration_ms,
                    progress_percent=progress_before,
                    step_progress_percent=step_progress,
                    error_message=public_error,
                )
                self.store.finish_job(job_id, "failed", public_error)
                logger.exception(
                    "Pipeline job %s failed in step %s",
                    job_id,
                    step_name,
                )
                return

            duration_ms = max(0, round((perf_counter() - started) * 1000))
            progress_after = ((ordinal + 1) / step_count) * 100
            self.store.finish_step(
                job_id,
                ordinal,
                status="completed",
                duration_ms=duration_ms,
                progress_percent=progress_after,
                result_json=serialize_bounded(result),
            )

            if cancel_event.is_set():
                self.store.finish_job(job_id, "cancelled", "Cancelled by user")
                return

        self.store.finish_job(job_id, "completed")

    def get_job(self, job_id: str) -> dict[str, Any]:
        """Return a job with its ordered step state."""
        return self.store.get_job(job_id, include_steps=True)

    def list_jobs(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return self.store.list_jobs(limit, offset)

    def cancel(self, job_id: str, *, force: bool = False) -> dict[str, Any]:
        """Request cancellation without claiming a running thread has stopped."""
        job = self.store.get_job(job_id)
        status = str(job["status"])
        if status in TERMINAL_STATUSES:
            raise InvalidJobState(
                f"Cannot cancel job with status '{status}'"
            )
        if force and status in {"running", "cancelling"}:
            raise ForceCancellationUnsupported(
                "Forced cancellation of a running pipeline is not supported"
            )

        with self._lock:
            cancel_event = self._cancel_events.get(job_id)
            future = self._futures.get(job_id)
            if cancel_event is not None:
                cancel_event.set()

        self.store.mark_cancelling(job_id)
        if status == "pending" and future is not None and future.cancel():
            self.store.finish_job(
                job_id,
                "cancelled",
                "Cancelled before execution",
            )

        return self.get_job(job_id)

    def get_output(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id, include_steps=True)
        if job["status"] not in TERMINAL_STATUSES:
            raise InvalidJobState(
                "Job output is available only after termination"
            )

        output: dict[str, Any] = {}
        for step in job["steps"]:
            if (
                step["status"] != "completed"
                or step["result_json"] is None
            ):
                continue
            output[str(step["step_name"])] = json.loads(step["result_json"])
        return {
            "job_id": job_id,
            "status": job["status"],
            "output": output,
        }

    def active_count(self) -> int:
        return self.store.active_count()

    def health_summary(self) -> dict[str, Any]:
        """Return aggregate queue state suitable for the health endpoint."""
        counts = self.store.status_counts()
        return {
            "queue_depth": counts.get("pending", 0),
            "active": sum(
                counts.get(status, 0)
                for status in ("pending", "running", "cancelling")
            ),
            "counts_by_status": counts,
        }

    def cleanup(self, max_age_hours: int = 24) -> int:
        expired = self.store.expired_job_ids(max_age_hours)
        deleted = self.store.delete_jobs(expired)
        for job_id in expired:
            try:
                self.discard_workspace(job_id)
            except (OSError, ValueError):
                logger.warning(
                    "Could not remove workspace for expired job %s",
                    job_id,
                    exc_info=True,
                )
        return deleted

    def wait_for_terminal(
        self,
        job_id: str,
        timeout: float = 10,
    ) -> dict[str, Any]:
        """Wait for the local worker; intended for tests and shutdown support."""
        with self._lock:
            future = self._futures.get(job_id)
        if future is not None:
            try:
                future.result(timeout=timeout)
            except CancelledError:
                pass
        return self.get_job(job_id)

    def shutdown(self, *, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait, cancel_futures=not wait)
