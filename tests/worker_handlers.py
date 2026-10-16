"""Handlers used by worker tests; spawned children import them by dotted path."""

from __future__ import annotations

import os
import time

from src.worker.context import JobContext


def probe(context: JobContext) -> dict:
    """``mode`` selects: ``pid`` (default), ``raise``, ``crash``, ``hang`` (ignores cancel), ``wait`` (honours cancel)."""
    mode = context.payload.get("mode", "pid")
    context.report_progress(10.0, f"pid {os.getpid()}")
    if mode == "raise":
        raise ValueError("secret internal detail")
    if mode == "crash":
        os._exit(1)
    if mode == "hang":
        while True:
            time.sleep(0.1)
    if mode == "wait":
        while True:
            context.checkpoint()
            time.sleep(0.05)
    return {"pid": os.getpid()}
