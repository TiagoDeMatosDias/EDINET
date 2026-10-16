"""The object a job handler receives, and the errors it may raise."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

# sink(event, data): event "progress" carries {"percent", "message", "progress"},
# event "step" carries a pipeline StepEvent dict. Whoever runs the handler supplies it
# (the child process pipes it to the supervisor; InlineWorker calls the job service).
EventSink = Callable[[str, dict[str, Any]], None]


class JobError(Exception):
    """An expected failure; ``message`` is shown to users as the job's error."""

    def __init__(self, message: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.message = message
        self.retryable = retryable


class JobCancelledError(Exception):
    """Raised by ``JobContext.checkpoint`` after a cancel request."""

    def __init__(self, message: str = "The job was cancelled") -> None:
        super().__init__(message)


@dataclass
class JobContext:
    job_id: str
    kind: str
    attempt: int
    payload: dict[str, Any]
    workspace: Path  # jobs_dir()/<job_id> (same machine as the server)
    output_dir: Path  # files written here are uploaded as artifacts
    cancel_event: threading.Event
    settings_values: dict[str, Any] = field(default_factory=dict, repr=False)
    sink: EventSink | None = field(default=None, repr=False)

    def checkpoint(self) -> None:
        if self.cancel_event.is_set():
            raise JobCancelledError()

    def report_progress(
        self,
        percent: float | None = None,
        message: str | None = None,
        progress: dict[str, Any] | None = None,
    ) -> None:
        if self.sink is not None:
            self.sink("progress", {"percent": percent, "message": message, "progress": progress})

    def step_event(self, event: dict[str, Any]) -> None:
        if self.sink is not None:
            self.sink("step", dict(event))

    @property
    def settings(self) -> dict[str, Any]:
        return self.settings_values
