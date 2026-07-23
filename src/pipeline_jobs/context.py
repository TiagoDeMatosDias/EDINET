"""Cooperative execution context passed to cancellation-aware steps."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ProgressCallback = Callable[[float, str | None], None]


class PipelineCancelled(RuntimeError):
    """Raised at a cooperative checkpoint after cancellation is requested."""


@dataclass(frozen=True)
class StepExecutionContext:
    """Per-step cancellation, progress, and workspace interface."""

    job_id: str
    step_name: str
    workspace: Path
    cancel_event: threading.Event
    progress_callback: ProgressCallback

    @property
    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def checkpoint(self) -> None:
        if self.cancel_event.is_set():
            raise PipelineCancelled("Pipeline cancellation requested")

    def report_progress(
        self,
        completed: int | float,
        total: int | float,
        message: str | None = None,
    ) -> None:
        """Persist bounded step progress and then honor cancellation."""
        if total <= 0:
            raise ValueError("Progress total must be positive")
        self.checkpoint()
        fraction = max(0.0, min(1.0, float(completed) / float(total)))
        self.progress_callback(fraction * 100.0, message)
        self.checkpoint()
