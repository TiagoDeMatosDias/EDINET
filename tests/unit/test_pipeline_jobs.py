"""Tests for the durable pipeline job lifecycle."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable

import pytest

from src.pipeline_jobs import InvalidJobState, JobStore, PipelineJobManager
from src.pipeline_jobs.redaction import serialize_bounded


@pytest.fixture
def manager_factory(tmp_path):
    managers: list[PipelineJobManager] = []

    def create(executor: Callable[..., object]) -> PipelineJobManager:
        manager = PipelineJobManager(
            JobStore(tmp_path / f"jobs-{len(managers)}.db"),
            step_executor=executor,
        )
        managers.append(manager)
        return manager

    yield create

    for manager in managers:
        manager.shutdown(wait=True)


def test_success_persists_ordered_steps_and_redacted_output(manager_factory):
    calls: list[tuple[str, bool]] = []

    def execute(name, _config, *, overwrite=False):
        calls.append((name, overwrite))
        return {
            "step": name,
            "api_key": "must-not-persist",
            "nested": {"token": "also-secret"},
        }

    manager = manager_factory(execute)
    submitted = manager.submit(
        object(),
        [
            {"name": "first", "overwrite": False},
            {"name": "second", "overwrite": True},
        ],
    )

    job = manager.wait_for_terminal(submitted["job_id"], timeout=2)

    assert job["status"] == "completed"
    assert job["progress_percent"] == 100
    assert job["completed_step_count"] == 2
    assert [step["step_name"] for step in job["steps"]] == ["first", "second"]
    assert [step["status"] for step in job["steps"]] == ["completed", "completed"]
    assert all(step["duration_ms"] >= 0 for step in job["steps"])
    assert calls == [("first", False), ("second", True)]

    output = manager.get_output(submitted["job_id"])["output"]
    assert output["first"]["api_key"] == "[REDACTED]"
    assert output["second"]["nested"]["token"] == "[REDACTED]"
    assert "must-not-persist" not in json.dumps(output)


def test_first_failure_stops_later_steps_and_exposes_safe_error(manager_factory):
    calls: list[str] = []

    def execute(name, _config, *, overwrite=False):
        calls.append(name)
        raise ValueError(r"secret=C:\private\operator.db")

    manager = manager_factory(execute)
    submitted = manager.submit(
        object(),
        [{"name": "fails"}, {"name": "must-not-run"}],
    )

    job = manager.wait_for_terminal(submitted["job_id"], timeout=2)

    assert job["status"] == "failed"
    assert job["progress_percent"] < 100
    assert job["current_step"] is None
    assert calls == ["fails"]
    assert [step["status"] for step in job["steps"]] == ["failed", "pending"]
    assert "operator.db" not in job["error_message"]
    assert "secret=" not in job["error_message"]


def test_running_cancel_waits_for_step_boundary_and_skips_later_steps(
    manager_factory,
):
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def execute(name, _config, *, overwrite=False):
        calls.append(name)
        started.set()
        assert release.wait(2), "test-controlled step was not released"
        return {"finished": name}

    manager = manager_factory(execute)
    submitted = manager.submit(
        object(),
        [{"name": "active"}, {"name": "must-not-run"}],
    )
    assert started.wait(2), "worker did not start"

    cancelling = manager.cancel(submitted["job_id"])
    assert cancelling["status"] == "cancelling"
    release.set()
    job = manager.wait_for_terminal(submitted["job_id"], timeout=2)

    assert job["status"] == "cancelled"
    assert job["progress_percent"] < 100
    assert calls == ["active"]
    assert [step["status"] for step in job["steps"]] == ["completed", "pending"]


def test_context_reports_progress_and_cancels_inside_active_step(manager_factory):
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def execute(name, _config, *, overwrite=False, context=None):
        calls.append(name)
        context.report_progress(
            1,
            4,
            r"Reading C:\private\operator.csv token=do-not-expose",
        )
        started.set()
        assert release.wait(2), "test-controlled checkpoint was not released"
        context.checkpoint()
        return name

    manager = manager_factory(execute)
    submitted = manager.submit(
        object(),
        [{"name": "active"}, {"name": "must-not-run"}],
    )
    assert started.wait(2), "worker did not report progress"

    running = manager.get_job(submitted["job_id"])
    assert running["progress_percent"] == pytest.approx(12.5)
    assert running["steps"][0]["progress_percent"] == pytest.approx(25)
    assert "operator.csv" not in running["status_message"]
    assert "do-not-expose" not in running["status_message"]

    manager.cancel(submitted["job_id"])
    release.set()
    terminal = manager.wait_for_terminal(submitted["job_id"], timeout=2)

    assert terminal["status"] == "cancelled"
    assert terminal["completed_step_count"] == 0
    assert calls == ["active"]
    assert [step["status"] for step in terminal["steps"]] == [
        "cancelled",
        "pending",
    ]


def test_queued_job_can_be_cancelled_before_execution(manager_factory):
    first_started = threading.Event()
    release_first = threading.Event()
    calls: list[str] = []

    def execute(name, _config, *, overwrite=False):
        calls.append(name)
        if name == "blocking":
            first_started.set()
            assert release_first.wait(2), "test-controlled step was not released"
        return name

    manager = manager_factory(execute)
    first = manager.submit(object(), [{"name": "blocking"}])
    assert first_started.wait(2), "first worker did not start"
    queued = manager.submit(object(), [{"name": "queued"}])

    cancelled = manager.cancel(queued["job_id"])
    release_first.set()
    manager.wait_for_terminal(first["job_id"], timeout=2)

    assert cancelled["status"] == "cancelled"
    assert manager.get_job(queued["job_id"])["status"] == "cancelled"
    assert calls == ["blocking"]


def test_output_is_rejected_before_job_terminates(manager_factory):
    started = threading.Event()
    release = threading.Event()

    def execute(name, _config, *, overwrite=False):
        started.set()
        assert release.wait(2), "test-controlled step was not released"
        return name

    manager = manager_factory(execute)
    submitted = manager.submit(object(), [{"name": "blocking"}])
    assert started.wait(2), "worker did not start"

    with pytest.raises(InvalidJobState):
        manager.get_output(submitted["job_id"])

    release.set()
    manager.wait_for_terminal(submitted["job_id"], timeout=2)


def test_manager_startup_marks_stale_work_interrupted(tmp_path):
    store = JobStore(tmp_path / "stale.db")
    store.create_job("stale", [{"name": "unfinished"}])
    assert store.start_job("stale")
    store.start_step("stale", 0, "unfinished", 0)

    manager = PipelineJobManager(store, step_executor=lambda *_args, **_kwargs: None)
    try:
        job = manager.get_job("stale")
        assert job["status"] == "interrupted"
        assert job["current_step"] is None
        assert job["steps"][0]["status"] == "interrupted"
    finally:
        manager.shutdown(wait=True)


def test_bounded_serialization_is_valid_redacted_json():
    result = serialize_bounded(
        {"api_key": "secret", "payload": '"' * 1000},
        max_bytes=200,
    )

    assert len(result.encode("utf-8")) <= 200
    decoded = json.loads(result)
    assert decoded["truncated"] is True
    assert "secret" not in result


def test_cleanup_removes_expired_job_and_its_workspace(tmp_path):
    store = JobStore(tmp_path / "cleanup.db")
    manager = PipelineJobManager(
        store,
        step_executor=lambda *_args, **_kwargs: None,
        workspace_root=tmp_path / "workspaces",
    )
    try:
        submitted = manager.submit(object(), [{"name": "done"}])
        job_id = submitted["job_id"]
        manager.wait_for_terminal(job_id, timeout=2)
        workspace = manager.workspace_for(job_id)
        (workspace / "artifact.txt").write_text("owned", encoding="utf-8")

        with store._connect() as conn:
            conn.execute(
                "UPDATE pipeline_jobs SET completed_at = ? WHERE job_id = ?",
                ("2000-01-01T00:00:00+00:00", job_id),
            )

        assert manager.cleanup(max_age_hours=1) == 1
        assert not workspace.exists()
        with pytest.raises(KeyError):
            manager.get_job(job_id)
    finally:
        manager.shutdown(wait=True)
