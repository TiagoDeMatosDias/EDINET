"""API tests for pipeline jobs: enqueue into jobs.db, run on a worker, same HTTP shapes."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import src.api.job_routes as job_routes
import src.api.pipeline_routes as pipeline_routes
import src.api.router as pipeline_api
import src.api.system_routes as system_routes
import src.jobs.service as job_service_module
from src.auth.models import AuthenticatedUser
from src.jobs.registry import WorkerRegistry
from src.jobs.service import JobService, set_job_service
from src.jobs.store import JobsStore
from src.orchestrator.orchestrator import UploadTooLargeError
from src.paths import jobs_dir
from src.worker.handlers import pipeline as pipeline_handler
from src.worker.inline import InlineWorker

OPERATOR = AuthenticatedUser(
    user_id="operator-1", username="op", email=None, role="operator", status="active"
)


@pytest.fixture
def service(tmp_path):
    registry = WorkerRegistry(tmp_path / "workers-app.db")
    registry.ensure_local_worker()
    isolated = JobService(JobsStore(tmp_path / "jobs.db"), registry)
    previous = job_service_module._service
    set_job_service(isolated)
    yield isolated
    set_job_service(previous)


@pytest.fixture
def client(service, monkeypatch):
    monkeypatch.setattr(pipeline_routes, "_require_operator", lambda _request: OPERATOR)
    monkeypatch.setattr(job_routes, "_require_operator", lambda _request: OPERATOR)
    monkeypatch.setattr(system_routes, "_require_operator", lambda _request: OPERATOR)
    monkeypatch.setattr(
        pipeline_routes,
        "resolve_file_uploads",
        lambda config, _steps, **_kwargs: dict(config),
    )
    monkeypatch.setattr(
        pipeline_routes,
        "validate_input",
        lambda config, steps: list(steps or []),
    )
    return TestClient(pipeline_api.app)


def _run(client, steps, config=None):
    response = client.post(
        "/api/pipeline/run",
        json={"steps": steps, "config": config or {}},
    )
    assert response.status_code == 202, response.text
    return response.json()


def _wait_status(client, job_id, wanted, timeout=5.0):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/api/jobs/{job_id}").json()
        if body["status"] in wanted:
            return body
        time.sleep(0.02)
    raise AssertionError(f"job did not reach {wanted}: {body['status']}")


def test_submit_creates_a_pending_job_with_steps_and_workspace(client, service):
    created = _run(
        client,
        [{"name": "first", "overwrite": False}, {"name": "second", "overwrite": True}],
        {"api_key": "kept-in-payload-only"},
    )

    assert created["status"] == "pending"
    job_id = created["job_id"]
    record = service.get(job_id)
    assert record.kind == "pipeline"
    assert record.owner_user_id == "operator-1"
    assert record.payload["workspace"] == str(jobs_dir() / job_id)
    assert (jobs_dir() / job_id).is_dir()
    assert [(s["step_name"], s["overwrite"]) for s in service.steps(job_id)] == [
        ("first", 0),
        ("second", 1),
    ]

    body = client.get(f"/api/jobs/{job_id}").json()
    assert body["status"] == "pending"
    assert body["step_count"] == 2
    assert body["completed_step_count"] == 0
    assert [step["status"] for step in body["steps"]] == ["pending", "pending"]
    assert "api_key" not in json.dumps(body)
    assert [job["job_id"] for job in client.get("/api/jobs").json()] == [job_id]


def test_job_runs_on_a_worker_and_keeps_the_old_shapes(client, service, monkeypatch):
    def execute(name, config, *, overwrite=False, context=None):
        return {"name": name, "overwrite": overwrite, "api_key": "never-persist-this"}

    monkeypatch.setattr(pipeline_handler, "execute_step", execute)
    with InlineWorker(service):
        created = _run(client, [{"name": "first"}, {"name": "second", "overwrite": True}])
        body = _wait_status(client, created["job_id"], {"completed", "failed"})

        assert body["status"] == "completed"
        assert body["progress_percent"] == 100
        assert body["completed_step_count"] == 2
        assert body["current_step"] is None
        assert [step["step_name"] for step in body["steps"]] == ["first", "second"]
        assert all(step["duration_ms"] is not None for step in body["steps"])

        output = client.get(f"/api/jobs/{created['job_id']}/output").json()
        assert output["status"] == "completed"
        assert output["output"]["first"]["api_key"] == "[REDACTED]"
        assert output["output"]["second"]["overwrite"] is True

        assert [j["job_id"] for j in client.get("/api/jobs?limit=1&offset=0").json()] == [
            created["job_id"]
        ]
        assert client.get("/api/jobs?limit=1&offset=1").json() == []

        status = client.get("/api/system/status")
        assert status.status_code == 200
        jobs = status.json()["jobs"]
        assert jobs["active"] == 0
        assert jobs["counts_by_status"]["completed"] == 1
        assert jobs["workers_online"] == 1
        assert "jobs" not in client.get("/health").json()


def test_failed_step_surfaces_the_public_error(client, service, monkeypatch):
    def execute(name, config, *, overwrite=False, context=None):
        raise ValueError(r"secret=C:\private\operator.db")

    monkeypatch.setattr(pipeline_handler, "execute_step", execute)
    with InlineWorker(service):
        created = _run(client, [{"name": "fails"}, {"name": "never"}])
        body = _wait_status(client, created["job_id"], {"failed"})

    assert body["error_message"] == "Step 'fails' failed (ValueError)"
    assert [step["status"] for step in body["steps"]] == ["failed", "pending"]
    assert "private" not in json.dumps(body)


def test_pending_job_cancels_at_once_and_force_needs_a_running_job(client, service):
    job_id = _run(client, [{"name": "waiting"}])["job_id"]

    forced = client.post(f"/api/jobs/{job_id}/cancel", json={"force": True})
    assert forced.status_code == 409

    cancelled = client.post(f"/api/jobs/{job_id}/cancel", json={"force": False})
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"

    again = client.post(f"/api/jobs/{job_id}/cancel", json={"force": False})
    assert again.status_code == 409
    assert client.post("/api/jobs/missing/cancel", json={}).status_code == 404


def test_running_job_soft_then_force_cancel(client, service, monkeypatch):
    started = threading.Event()
    release = threading.Event()

    def execute(name, config, *, overwrite=False, context=None):
        started.set()
        # The worker raises the cancel flag once it sees the request on a heartbeat.
        assert context.cancel_event.wait(5) or release.is_set(), "cancel never reached the step"
        context.checkpoint()
        return name

    monkeypatch.setattr(pipeline_handler, "execute_step", execute)
    try:
        with InlineWorker(service):
            job_id = _run(client, [{"name": "blocking"}, {"name": "must-not-run"}])["job_id"]
            assert started.wait(5), "worker did not start"
            assert client.get(f"/api/jobs/{job_id}").json()["status"] == "running"

            soft = client.post(f"/api/jobs/{job_id}/cancel", json={"force": False})
            assert soft.status_code == 200
            assert soft.json()["status"] == "cancelling"

            force = client.post(f"/api/jobs/{job_id}/cancel", json={"force": True})
            assert force.status_code == 200
            assert service.get(job_id).cancel_requested == "hard"

            body = _wait_status(client, job_id, {"cancelled"})
        assert [step["status"] for step in body["steps"]] == ["cancelled", "pending"]
    finally:
        release.set()


def test_output_is_rejected_before_the_job_finishes(client):
    job_id = _run(client, [{"name": "waiting"}])["job_id"]

    assert client.get(f"/api/jobs/{job_id}/output").status_code == 409


def test_other_job_kinds_are_not_pipeline_jobs(client, service):
    echo = service.enqueue("echo", {}, owner_user_id=None, title="Echo")

    assert client.get(f"/api/jobs/{echo.job_id}").status_code == 404
    assert client.get("/api/jobs").json() == []


def test_oversized_embedded_upload_returns_413_without_workspace(client, monkeypatch, service):
    captured = {}

    def reject_upload(_config, _steps, **kwargs):
        captured["workspace"] = kwargs["workspace"]
        raise UploadTooLargeError("Embedded file exceeds the configured size limit")

    monkeypatch.setattr(pipeline_routes, "resolve_file_uploads", reject_upload)
    response = client.post(
        "/api/pipeline/run",
        json={"steps": [{"name": "upload"}], "config": {}},
    )

    assert response.status_code == 413
    assert not captured["workspace"].exists()
    assert service.list_jobs(kinds=["pipeline"]) == []


def test_multipart_upload_is_streamed_into_the_job_workspace(client, service, monkeypatch):
    captured = {}

    def execute(_name, config, *, overwrite=False, context=None):
        captured["config"] = config.settings
        return None

    monkeypatch.setattr(pipeline_handler, "execute_step", execute)
    payload = {
        "steps": [{"name": "import_stock_prices_csv"}],
        "config": {
            "import_stock_prices_csv_config": {
                "csv_file": {
                    "__pipeline_upload__": "import_stock_prices_csv_config.csv_file",
                },
                "date_column": "Date",
                "price_column": "Price",
                "default_currency": "JPY",
            },
        },
    }
    with InlineWorker(service):
        response = client.post(
            "/api/pipeline/run",
            data={"config": json.dumps(payload)},
            files={
                "upload:import_stock_prices_csv_config.csv_file": (
                    "prices.csv",
                    b"Date,Price\n2025-01-01,100\n",
                    "text/csv",
                ),
            },
        )
        assert response.status_code == 202
        job_id = response.json()["job_id"]
        _wait_status(client, job_id, {"completed"})

    csv_path = Path(captured["config"]["import_stock_prices_csv_config"]["csv_file"])
    assert csv_path.name.endswith("-prices.csv")
    assert (jobs_dir() / job_id) in csv_path.parents
    assert csv_path.read_bytes() == b"Date,Price\n2025-01-01,100\n"


def test_pipeline_request_rejects_unknown_fields(client):
    response = client.post(
        "/api/pipeline/run",
        json={"steps": [], "config": {}, "unexpected": True},
    )

    assert response.status_code == 422
