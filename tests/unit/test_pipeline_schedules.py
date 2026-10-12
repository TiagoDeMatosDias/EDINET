from datetime import datetime, timezone

from fastapi.testclient import TestClient

import src.api.router as pipeline_api
import src.api.runtime as api_runtime
import src.api.schedule_routes as schedule_routes
import src.api.setup_routes as setup_routes
from src.pipeline_jobs import JobStore, PipelineScheduler


class FakeManager:
    def __init__(self, active=0):
        self.active = active

    def active_count(self):
        return self.active


class CapturingManager:
    """Records what the scheduler submits so tests can inspect the run config."""

    def __init__(self, workspace_root):
        self.workspace_root = workspace_root
        self.submitted = []

    def active_count(self):
        return 0

    def new_job_id(self):
        return "job-1"

    def workspace_for(self, job_id, create=False):
        path = self.workspace_root / job_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def submit(self, config, steps, job_id=None):
        self.submitted.append({"config": config, "steps": steps})
        return {"job_id": job_id}

    def discard_workspace(self, job_id):
        pass


def test_setup_round_trip(tmp_path):
    store = JobStore(tmp_path / "setups.db")
    created = store.save_setup(
        "Daily refresh",
        [{"name": "update_stock_prices", "overwrite": False}],
        {"update_stock_prices_config.start": "2024-01-01"},
    )

    assert created["name"] == "Daily refresh"
    assert created["steps"][0]["name"] == "update_stock_prices"
    assert created["config"] == {"update_stock_prices_config.start": "2024-01-01"}
    assert [setup["name"] for setup in store.list_setups()] == ["Daily refresh"]

    updated = store.save_setup(
        "Daily refresh",
        [{"name": "download_documents"}],
        {},
    )
    assert updated["steps"] == [{"name": "download_documents"}]
    assert updated["config"] == {}
    assert updated["created_at"] == created["created_at"]
    assert updated["updated_at"] >= created["updated_at"]

    assert store.delete_setup("Daily refresh") is True
    assert store.list_setups() == []
    assert store.delete_setup("Daily refresh") is False


def test_setup_delete_blocked_while_referenced(tmp_path):
    store = JobStore(tmp_path / "referenced.db")
    store.save_setup("Daily refresh", [{"name": "update_stock_prices"}], {})
    store.create_schedule(
        name="Daily",
        frequency="daily",
        enabled=True,
        setup_name="Daily refresh",
    )

    try:
        store.delete_setup("Daily refresh")
        raise AssertionError("expected deleting a referenced setup to fail")
    except ValueError:
        pass

    assert store.list_setups() != []


def test_schedule_round_trip_and_last_run(tmp_path):
    store = JobStore(tmp_path / "schedules.db")
    store.save_setup("Daily refresh", [{"name": "update_stock_prices"}], {})
    created = store.create_schedule(
        name="Daily refresh",
        frequency="daily",
        enabled=True,
        setup_name="Daily refresh",
    )

    assert created["enabled"] is True
    assert created["setup_name"] == "Daily refresh"
    assert created["last_run_at"] is None

    store.mark_schedule_run(created["schedule_id"], "2026-01-01T00:00:00+00:00")
    assert store.get_schedule(created["schedule_id"])["last_run_at"] == (
        "2026-01-01T00:00:00+00:00"
    )
    reset = store.reset_schedule_run(created["schedule_id"])
    assert reset["last_run_at"] is None


def test_scheduler_checks_without_uptime_gate(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")
    store.save_setup("Daily refresh", [{"name": "update_stock_prices"}], {})
    store.create_schedule(
        name="Daily refresh",
        frequency="daily",
        enabled=True,
        setup_name="Daily refresh",
    )
    scheduler = PipelineScheduler(
        store,
        FakeManager(),
        check_interval_seconds=300,
        now=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    scheduler._submit = lambda schedule: "job-id"

    assert scheduler.run_once() == ["job-id"]
    assert scheduler.check_interval_seconds == 300


def test_scheduler_does_not_queue_behind_active_pipeline(tmp_path):
    store = JobStore(tmp_path / "active.db")
    store.save_setup("Daily refresh", [{"name": "update_stock_prices"}], {})
    store.create_schedule(
        name="Daily refresh",
        frequency="daily",
        enabled=True,
        setup_name="Daily refresh",
    )
    scheduler = PipelineScheduler(
        store,
        FakeManager(active=1),
        check_interval_seconds=300,
        now=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    scheduler._submit = lambda schedule: "job-id"

    assert scheduler.run_once() == []


def test_scheduler_submits_the_current_setup_version(tmp_path):
    store = JobStore(tmp_path / "latest.db")
    store.save_setup(
        "Daily refresh",
        [{"name": "update_stock_prices"}],
        {"update_stock_prices_config.start": "2024-01-01"},
    )
    store.create_schedule(
        name="Daily",
        frequency="daily",
        enabled=True,
        setup_name="Daily refresh",
    )
    manager = CapturingManager(tmp_path / "workspaces")
    scheduler = PipelineScheduler(
        store,
        manager,
        check_interval_seconds=300,
        now=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert scheduler.run_once() == ["job-1"]
    first = manager.submitted[0]["config"].settings
    assert first["update_stock_prices_config"]["start"] == "2024-01-01"

    # Editing the setup must be picked up by the next run, not a stale copy.
    store.save_setup(
        "Daily refresh",
        [{"name": "update_stock_prices"}],
        {"update_stock_prices_config.start": "2025-06-01"},
    )
    store.reset_schedule_run(store.list_schedules()[0]["schedule_id"])

    assert scheduler.run_once() == ["job-1"]
    second = manager.submitted[1]["config"].settings
    assert second["update_stock_prices_config"]["start"] == "2025-06-01"


def test_schedule_api_persists_enabled_control(monkeypatch, tmp_path):
    store = JobStore(tmp_path / "api.db")
    store.save_setup("Daily refresh", [{"name": "update_stock_prices"}], {})
    monkeypatch.setattr(api_runtime, "job_store", store)
    monkeypatch.setattr(schedule_routes, "_require_admin", lambda _request: None)
    monkeypatch.setattr(schedule_routes, "validate_input", lambda config, steps: steps)

    client = TestClient(pipeline_api.app)
    response = client.post(
        "/api/admin/pipeline-schedules",
        json={
            "name": "Daily refresh",
            "frequency": "daily",
            "enabled": True,
            "setup_name": "Daily refresh",
        },
    )
    assert response.status_code == 201
    schedule_id = response.json()["schedule_id"]
    assert response.json()["setup_name"] == "Daily refresh"

    updated = client.patch(
        f"/api/admin/pipeline-schedules/{schedule_id}",
        json={"enabled": False},
    )
    assert updated.status_code == 200
    assert updated.json()["enabled"] is False
    assert store.get_schedule(schedule_id)["enabled"] is False

    store.mark_schedule_run(schedule_id, "2026-01-01T00:00:00+00:00")
    reset = client.post(
        f"/api/admin/pipeline-schedules/{schedule_id}/reset-last-run"
    )
    assert reset.status_code == 200
    assert reset.json()["last_run_at"] is None


def test_schedule_api_requires_existing_setup(monkeypatch, tmp_path):
    store = JobStore(tmp_path / "missing.db")
    monkeypatch.setattr(api_runtime, "job_store", store)
    monkeypatch.setattr(schedule_routes, "_require_admin", lambda _request: None)

    client = TestClient(pipeline_api.app)
    response = client.post(
        "/api/admin/pipeline-schedules",
        json={
            "name": "Daily refresh",
            "frequency": "daily",
            "enabled": True,
            "setup_name": "Does not exist",
        },
    )
    assert response.status_code == 404


def test_setup_api_round_trip(monkeypatch, tmp_path):
    store = JobStore(tmp_path / "setups-api.db")
    monkeypatch.setattr(api_runtime, "job_store", store)
    monkeypatch.setattr(setup_routes, "_require_admin", lambda _request: None)
    monkeypatch.setattr(schedule_routes, "validate_input", lambda config, steps: steps)

    client = TestClient(pipeline_api.app)
    created = client.put(
        "/api/admin/pipeline-setups/Daily refresh",
        json={
            "steps": [{"name": "update_stock_prices", "overwrite": False}],
            "config": {"update_stock_prices_config.start": "2024-01-01"},
        },
    )
    assert created.status_code == 200
    assert created.json()["name"] == "Daily refresh"
    assert created.json()["config"] == {
        "update_stock_prices_config.start": "2024-01-01"
    }

    listed = client.get("/api/admin/pipeline-setups")
    assert [setup["name"] for setup in listed.json()] == ["Daily refresh"]

    # Saving the same name again updates the shared setup in place.
    updated = client.put(
        "/api/admin/pipeline-setups/Daily refresh",
        json={"steps": [{"name": "download_documents"}], "config": {}},
    )
    assert updated.status_code == 200
    assert updated.json()["steps"] == [{"name": "download_documents"}]
    assert [setup["name"] for setup in client.get("/api/admin/pipeline-setups").json()] == ["Daily refresh"]

    deleted = client.delete("/api/admin/pipeline-setups/Daily refresh")
    assert deleted.status_code == 204
    assert client.get("/api/admin/pipeline-setups").json() == []

    missing = client.delete("/api/admin/pipeline-setups/Daily refresh")
    assert missing.status_code == 404


def test_setup_api_blocks_delete_while_referenced(monkeypatch, tmp_path):
    store = JobStore(tmp_path / "referenced-api.db")
    store.save_setup("Daily refresh", [{"name": "update_stock_prices"}], {})
    store.create_schedule(
        name="Daily",
        frequency="daily",
        enabled=True,
        setup_name="Daily refresh",
    )
    monkeypatch.setattr(api_runtime, "job_store", store)
    monkeypatch.setattr(setup_routes, "_require_admin", lambda _request: None)

    client = TestClient(pipeline_api.app)
    response = client.delete("/api/admin/pipeline-setups/Daily refresh")
    assert response.status_code == 409


def test_schedule_api_can_trigger_immediate_check(monkeypatch):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    class FakeScheduler:
        def check_now(self):
            return ["job-id"]

        def status(self, triggered_job_ids=None):
            return {
                "checked_at": now,
                "next_check_at": datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc),
                "active_pipeline": True,
                "triggered_job_ids": triggered_job_ids or [],
            }

    monkeypatch.setattr(api_runtime, "scheduler", FakeScheduler())
    monkeypatch.setattr(schedule_routes, "_require_admin", lambda _request: None)
    response = TestClient(pipeline_api.app).post("/api/admin/pipeline-schedules/check")

    assert response.status_code == 200
    assert response.json()["triggered_job_ids"] == ["job-id"]
    assert response.json()["active_pipeline"] is True
