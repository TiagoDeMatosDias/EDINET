"""Pipeline handler: runs the selected orchestrator steps in order inside a worker.

The server validated the configuration and resolved its paths; this module only
executes. Progress reaches the server as step events plus an overall percent, with
the same arithmetic the old in-process pipeline manager used.
"""

from __future__ import annotations

import inspect
import json
import logging
from time import perf_counter
from typing import Any

from config import Config
from src.orchestrator import execute_step
from src.pipeline_jobs.context import PipelineCancelled, StepExecutionContext
from src.pipeline_jobs.redaction import safe_public_text, serialize_bounded
from src.worker.context import JobCancelledError, JobContext, JobError

logger = logging.getLogger(__name__)

CANCELLED_MESSAGE = "Cancelled by user"


def _accepts_context(executor: Any) -> bool:
    try:
        parameters = inspect.signature(executor).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == "context" or parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _bounded(value: Any) -> Any:
    """The redacted, size-bounded form of a step result (JSON-compatible)."""
    return json.loads(serialize_bounded(value))


def _elapsed_ms(started: float) -> int:
    return max(0, round((perf_counter() - started) * 1000))


def run(context: JobContext) -> dict:
    """Run ``payload["steps"]`` against ``payload["config"]``; result ``{"steps": {name: output}}``."""
    payload = context.payload
    steps: list[dict[str, Any]] = [dict(step) for step in payload.get("steps") or []]
    if not steps:
        raise JobError("The pipeline has no steps to run")
    config = Config.from_dict(dict(payload.get("config") or {}))
    step_count = len(steps)
    accepts_context = _accepts_context(execute_step)
    results: dict[str, Any] = {}

    for ordinal, step in enumerate(steps):
        context.checkpoint()
        step_name = str(step["name"])
        context.step_event({"ordinal": ordinal, "status": "running", "progress_percent": 0.0})
        context.report_progress((ordinal / step_count) * 100, f"Running {step_name}")
        started = perf_counter()
        step_progress = 0.0

        def on_progress(
            percent: float, message: str | None, step_ordinal: int = ordinal
        ) -> None:
            nonlocal step_progress
            step_progress = max(step_progress, min(100.0, percent))
            text = safe_public_text(message)
            context.step_event(
                {
                    "ordinal": step_ordinal,
                    "status": "running",
                    "progress_percent": step_progress,
                    "status_message": text,
                }
            )
            context.report_progress(
                ((step_ordinal + step_progress / 100.0) / step_count) * 100, text
            )

        step_context = StepExecutionContext(
            job_id=context.job_id,
            step_name=step_name,
            workspace=context.workspace,
            cancel_event=context.cancel_event,
            progress_callback=on_progress,
        )
        kwargs: dict[str, Any] = {"overwrite": bool(step.get("overwrite", False))}
        if accepts_context:
            kwargs["context"] = step_context
        try:
            step_context.checkpoint()
            result = execute_step(step_name, config, **kwargs)
        except PipelineCancelled:
            context.step_event(
                {
                    "ordinal": ordinal,
                    "status": "cancelled",
                    "progress_percent": step_progress,
                    "error_message": CANCELLED_MESSAGE,
                    "duration_ms": _elapsed_ms(started),
                }
            )
            logger.info("Pipeline job %s cancelled in step %s", context.job_id, step_name)
            raise JobCancelledError(CANCELLED_MESSAGE) from None
        except Exception as exc:
            public_error = f"Step '{step_name}' failed ({type(exc).__name__})"
            context.step_event(
                {
                    "ordinal": ordinal,
                    "status": "failed",
                    "progress_percent": step_progress,
                    "error_message": public_error,
                    "duration_ms": _elapsed_ms(started),
                }
            )
            logger.exception("Pipeline job %s failed in step %s", context.job_id, step_name)
            raise JobError(public_error) from None

        bounded = _bounded(result)
        results[step_name] = bounded
        context.step_event(
            {
                "ordinal": ordinal,
                "status": "completed",
                "progress_percent": 100.0,
                "result": bounded,
                "duration_ms": _elapsed_ms(started),
            }
        )
        context.report_progress(((ordinal + 1) / step_count) * 100, None)

    context.checkpoint()
    return {"steps": results}
