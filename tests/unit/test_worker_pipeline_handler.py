"""The pipeline handler: step events, progress arithmetic, failure and cancellation."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from src.worker.context import JobCancelledError, JobContext, JobError
from src.worker.handlers import pipeline as handler


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def __call__(self, event: str, data: dict) -> None:
        self.events.append((event, data))

    def steps(self, ordinal: int | None = None) -> list[dict]:
        return [
            data for event, data in self.events
            if event == "step" and (ordinal is None or data["ordinal"] == ordinal)
        ]

    def percents(self) -> list[float]:
        return [data["percent"] for event, data in self.events if event == "progress"]


def make_context(tmp_path: Path, steps: list[dict], sink: Recorder, cancel: threading.Event | None = None) -> JobContext:
    return JobContext(
        job_id="00000000-0000-4000-8000-000000000001",
        kind="pipeline",
        attempt=1,
        payload={"config": {"key": "value"}, "steps": steps, "workspace": str(tmp_path)},
        workspace=tmp_path,
        output_dir=tmp_path / "out",
        cancel_event=cancel or threading.Event(),
        sink=sink,
    )


def test_success_reports_step_events_progress_and_bounded_results(tmp_path, monkeypatch):
    calls = []

    def execute(name, config, *, overwrite=False, context=None):
        calls.append((name, overwrite, config.get("key")))
        if name == "first":
            context.report_progress(1, 2, "half way")
        return {"step": name, "api_key": "must-not-persist"}

    monkeypatch.setattr(handler, "execute_step", execute)
    sink = Recorder()
    result = handler.run(
        make_context(tmp_path, [{"name": "first", "overwrite": True}, {"name": "second"}], sink)
    )

    assert calls == [("first", True, "value"), ("second", False, "value")]
    assert result == {
        "steps": {
            "first": {"step": "first", "api_key": "[REDACTED]"},
            "second": {"step": "second", "api_key": "[REDACTED]"},
        }
    }
    assert [e["status"] for e in sink.steps(0)] == ["running", "running", "completed"]
    assert sink.steps(0)[1]["progress_percent"] == 50.0
    assert sink.steps(0)[1]["status_message"] == "half way"
    assert sink.steps(0)[-1]["result"]["api_key"] == "[REDACTED]"
    assert sink.steps(1)[-1]["progress_percent"] == 100.0
    # Overall percent: step 0 starts at 0, reaches 25 half way through, 50 after it.
    assert sink.percents() == [0.0, 25.0, 50.0, 50.0, 100.0]


def test_failed_step_reports_public_error_and_stops(tmp_path, monkeypatch):
    ran = []

    def execute(name, config, *, overwrite=False, context=None):
        ran.append(name)
        raise ValueError(r"secret=C:\private\operator.db")

    monkeypatch.setattr(handler, "execute_step", execute)
    sink = Recorder()
    with pytest.raises(JobError) as caught:
        handler.run(make_context(tmp_path, [{"name": "fails"}, {"name": "never"}], sink))

    assert caught.value.message == "Step 'fails' failed (ValueError)"
    assert ran == ["fails"]
    failed = sink.steps(0)[-1]
    assert failed["status"] == "failed"
    assert failed["error_message"] == "Step 'fails' failed (ValueError)"
    assert "private" not in str(sink.events)
    assert sink.steps(1) == []


def test_cancel_inside_step_reports_cancelled_event(tmp_path, monkeypatch):
    cancel = threading.Event()

    def execute(name, config, *, overwrite=False, context=None):
        context.report_progress(1, 4, "started")
        cancel.set()
        context.checkpoint()
        raise AssertionError("checkpoint must raise after cancellation")

    monkeypatch.setattr(handler, "execute_step", execute)
    sink = Recorder()
    with pytest.raises(JobCancelledError):
        handler.run(make_context(tmp_path, [{"name": "long"}, {"name": "never"}], sink, cancel))

    last = sink.steps(0)[-1]
    assert last["status"] == "cancelled"
    assert last["progress_percent"] == 25.0
    assert sink.steps(1) == []


def test_cancel_between_steps_skips_the_rest(tmp_path, monkeypatch):
    cancel = threading.Event()
    ran = []

    def execute(name, config, *, overwrite=False, context=None):
        ran.append(name)
        cancel.set()
        return None

    monkeypatch.setattr(handler, "execute_step", execute)
    sink = Recorder()
    with pytest.raises(JobCancelledError):
        handler.run(make_context(tmp_path, [{"name": "a"}, {"name": "b"}], sink, cancel))

    assert ran == ["a"]
    assert sink.steps(0)[-1]["status"] == "completed"
    assert sink.steps(1) == []


def test_empty_step_list_is_an_error(tmp_path):
    with pytest.raises(JobError):
        handler.run(make_context(tmp_path, [], Recorder()))
