"""API integration tests for persisted pipeline jobs."""

from __future__ import annotations

import threading

from fastapi.testclient import TestClient

import src.api.pipeline_routes as pipeline_routes
import src.api.router as pipeline_api
import src.api.runtime as api_runtime
from src.orchestrator.orchestrator import UploadTooLargeError
from src.pipeline_jobs import JobStore, PipelineJobManager


def _install_manager(monkeypatch, tmp_path, executor):
    manager = PipelineJobManager(
        JobStore(tmp_path / "api-jobs.db"),
        step_executor=executor,
    )
    monkeypatch.setattr(api_runtime, "job_manager", manager)
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
    monkeypatch.setattr(
        pipeline_routes.Config,
        "from_dict",
        staticmethod(lambda config: dict(config)),
    )
    return manager


def test_submit_returns_202_and_terminal_output_is_persisted(
    monkeypatch,
    tmp_path,
):
    def execute(name, config, *, overwrite=False):
        return {
            "name": name,
            "overwrite": overwrite,
            "api_key": config.get("api_key"),
        }

    manager = _install_manager(monkeypatch, tmp_path, execute)
    try:
        with TestClient(pipeline_api.app) as client:
            response = client.post(
                "/api/pipeline/run",
                json={
                    "steps": [
                        {"name": "first", "overwrite": False},
                        {"name": "second", "overwrite": True},
                    ],
                    "config": {"api_key": "never-persist-this"},
                },
            )
            assert response.status_code == 202
            created = response.json()

            terminal = manager.wait_for_terminal(created["job_id"], timeout=2)
            assert terminal["status"] == "completed"

            status = client.get(f"/api/jobs/{created['job_id']}")
            assert status.status_code == 200
            status_body = status.json()
            assert status_body["status"] == "completed"
            assert [step["step_name"] for step in status_body["steps"]] == [
                "first",
                "second",
            ]

            output = client.get(f"/api/jobs/{created['job_id']}/output")
            assert output.status_code == 200
            assert (
                output.json()["output"]["first"]["api_key"]
                == "[REDACTED]"
            )

            first_page = client.get("/api/jobs?limit=1&offset=0")
            assert first_page.status_code == 200
            assert [job["job_id"] for job in first_page.json()] == [
                created["job_id"]
            ]
            assert client.get("/api/jobs?limit=1&offset=1").json() == []

            health = client.get("/health")
            assert health.status_code == 200
            assert health.json()["jobs"]["active"] == 0
            assert health.json()["jobs"]["counts_by_status"]["completed"] == 1
    finally:
        manager.shutdown(wait=True)


def test_running_job_returns_before_step_finishes_and_cancels_truthfully(
    monkeypatch,
    tmp_path,
):
    started = threading.Event()
    release = threading.Event()

    def execute(name, _config, *, overwrite=False):
        started.set()
        assert release.wait(2), "test-controlled step was not released"
        return name

    manager = _install_manager(monkeypatch, tmp_path, execute)
    try:
        with TestClient(pipeline_api.app) as client:
            response = client.post(
                "/api/pipeline/run",
                json={
                    "steps": [
                        {"name": "blocking"},
                        {"name": "must-not-run"},
                    ],
                    "config": {},
                },
            )
            assert response.status_code == 202
            job_id = response.json()["job_id"]
            assert started.wait(2), "worker did not start"

            force = client.post(
                f"/api/jobs/{job_id}/cancel",
                json={"force": True},
            )
            assert force.status_code == 409

            cancel = client.post(
                f"/api/jobs/{job_id}/cancel",
                json={"force": False},
            )
            assert cancel.status_code == 200
            assert cancel.json()["status"] == "cancelling"

            release.set()
            terminal = manager.wait_for_terminal(job_id, timeout=2)
            assert terminal["status"] == "cancelled"
            assert [step["status"] for step in terminal["steps"]] == [
                "completed",
                "pending",
            ]
    finally:
        release.set()
        manager.shutdown(wait=True)


def test_oversized_embedded_upload_returns_413_without_workspace(
    monkeypatch,
    tmp_path,
):
    manager = _install_manager(
        monkeypatch,
        tmp_path,
        lambda *_args, **_kwargs: None,
    )
    captured = {}

    def reject_upload(_config, _steps, **kwargs):
        captured["workspace"] = kwargs["workspace"]
        raise UploadTooLargeError("Embedded file exceeds the configured size limit")

    monkeypatch.setattr(pipeline_routes, "resolve_file_uploads", reject_upload)
    try:
        with TestClient(pipeline_api.app) as client:
            response = client.post(
                "/api/pipeline/run",
                json={"steps": [{"name": "upload"}], "config": {}},
            )
        assert response.status_code == 413
        assert not captured["workspace"].exists()
    finally:
        manager.shutdown(wait=True)


def test_pipeline_request_rejects_unknown_fields(monkeypatch, tmp_path):
    manager = _install_manager(
        monkeypatch,
        tmp_path,
        lambda *_args, **_kwargs: None,
    )
    try:
        with TestClient(pipeline_api.app) as client:
            response = client.post(
                "/api/pipeline/run",
                json={"steps": [], "config": {}, "unexpected": True},
            )
        assert response.status_code == 422
    finally:
        manager.shutdown(wait=True)
