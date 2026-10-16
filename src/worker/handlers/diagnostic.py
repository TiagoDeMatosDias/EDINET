"""Diagnostic handler: ``echo`` exercises the whole job path without touching data."""

from __future__ import annotations

import time

from src.worker.context import JobContext, JobError

_SLEEP_SLICE_SECONDS = 0.05


def echo(context: JobContext) -> dict:
    """Sleep, report progress, optionally fail or write an artifact, then echo ``result``."""
    payload = context.payload
    steps = max(0, int(payload.get("progress_steps") or 0))
    sleep_seconds = max(0.0, float(payload.get("sleep_seconds") or 0))
    failure = payload.get("fail")

    deadline = time.monotonic() + sleep_seconds
    reported = 0
    while True:
        context.checkpoint()
        elapsed_share = 1.0 if sleep_seconds <= 0 else min(1.0, 1 - (deadline - time.monotonic()) / sleep_seconds)
        while reported < steps and elapsed_share * steps >= reported + 1:
            reported += 1
            context.report_progress(100.0 * reported / steps, f"Step {reported} of {steps}")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(_SLEEP_SLICE_SECONDS, remaining))
    while reported < steps:
        reported += 1
        context.report_progress(100.0 * reported / steps, f"Step {reported} of {steps}")

    if failure:
        raise JobError(str(failure))
    text = payload.get("artifact_text")
    if text is not None:
        (context.output_dir / "echo.txt").write_text(str(text), encoding="utf-8")
    return {"echo": payload.get("result")}
