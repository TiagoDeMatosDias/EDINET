"""InlineWorker against an in-memory stand-in for the JobService."""

from __future__ import annotations

import pytest

from src.worker.context import JobCancelledError, JobContext, JobError
from src.worker.inline import InlineWorker
from tests.worker_fakes import FakeJobService, wait_until


@pytest.fixture
def service(tmp_path):
    return FakeJobService(tmp_path)


def test_runs_echo_with_progress_and_artifacts(service):
    with InlineWorker(service):
        job = service.enqueue("echo", {"result": {"a": 1}, "artifact_text": "hi", "progress_steps": 2})
        wait_until(lambda: job.status == "completed", timeout=10)

    assert job.result == {"echo": {"a": 1}}
    assert job.artifacts == {"echo.txt": b"hi"}
    assert [p["progress_percent"] for p in job.progress] == [50.0, 100.0]
    assert service.registered[0].kinds and service.unregistered
    assert service.heartbeats[0].data_access is True
    assert service.registry.workers == {"inline": "Inline worker"}


def test_error_mapping(service):
    def boom(context: JobContext) -> dict:
        raise RuntimeError("internal detail")

    def retry(context: JobContext) -> dict:
        raise JobError("Try again later", retryable=True)

    with InlineWorker(service, handlers={"backtest_single": boom, "backtest_csv": retry}):
        failed = service.enqueue("echo", {"fail": "Nope"})
        crashed = service.enqueue("backtest_single")
        retryable = service.enqueue("backtest_csv")
        wait_until(lambda: all(j.status == "failed" for j in (failed, crashed, retryable)), timeout=10)

    assert (failed.error, failed.retryable) == ("Nope", False)
    assert (crashed.error, crashed.retryable) == ("Backtest failed (RuntimeError)", False)
    assert (retryable.error, retryable.retryable) == ("Try again later", True)


def test_cancel_request_reaches_a_running_handler(service):
    with InlineWorker(service):
        job = service.enqueue("echo", {"sleep_seconds": 30})
        wait_until(lambda: job.status == "in_progress", timeout=10)
        job.cancel = "soft"
        wait_until(lambda: job.status == "cancelled", timeout=10)

    assert job.error == "The job was cancelled"


def test_lanes_and_kinds_filter_what_is_claimed(service):
    with InlineWorker(service, lanes=("batch",)):
        interactive = service.enqueue("echo")
        batch = service.enqueue("backtest_rolling")
        wait_until(lambda: batch.status != "open", timeout=10)
        wait_until(lambda: batch.status in ("completed", "failed"), timeout=10)

    assert interactive.status == "open"
    assert batch.status == "failed"  # no backtest handler module yet / handler error: reported, not raised

    with InlineWorker(service, kinds=["echo"]):
        wait_until(lambda: interactive.status == "completed", timeout=10)


def test_checkpoint_raises_after_cancel(tmp_path):
    import threading

    event = threading.Event()
    context = JobContext("j", "echo", 1, {}, tmp_path, tmp_path, event)
    context.checkpoint()
    event.set()
    with pytest.raises(JobCancelledError):
        context.checkpoint()
    context.report_progress(5)  # no sink: ignored
    assert context.settings == {}
