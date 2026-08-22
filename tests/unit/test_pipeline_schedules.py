from datetime import datetime, timezone

from fastapi.testclient import TestClient

import src.api.router as pipeline_api
import src.api.runtime as api_runtime
import src.api.schedule_routes as schedule_routes
from src.pipeline_jobs import JobStore, PipelineScheduler


class FakeManager:
    def __init__(self, active=0):
        self.active = active

    def active_count(self):
        return self.active


def test_schedule_round_trip_and_last_run(tmp_path):
    store = JobStore(tmp_path / "schedules.db")
    created = store.create_schedule(
        name="Daily refresh",
        frequency="daily",
        enabled=True,
        steps=[{"name": "update_stock_prices", "overwrite": False}],
        config={"update_stock_prices_config": {}},
    )

    assert created["enabled"] is True
    assert created["steps"][0]["name"] == "update_stock_prices"
    assert created["last_run_at"] is None

    store.mark_schedule_run(created["schedule_id"], "2026-01-01T00:00:00+00:00")
    assert store.get_schedule(created["schedule_id"])["last_run_at"] == (
        "2026-01-01T00:00:00+00:00"
    )
    reset = store.reset_schedule_run(created["schedule_id"])
    assert reset["last_run_at"] is None


def test_scheduler_checks_without_uptime_gate(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")
    store.create_schedule(
        name="Daily refresh",
        frequency="daily",
        enabled=True,
        steps=[{"name": "update_stock_prices"}],
        config={},
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
    store.create_schedule(
        name="Daily refresh",
        frequency="daily",
        enabled=True,
        steps=[{"name": "update_stock_prices"}],
        config={},
    )
    scheduler = PipelineScheduler(
        store,
        FakeManager(active=1),
        check_interval_seconds=300,
        now=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    scheduler._submit = lambda schedule: "job-id"

    assert scheduler.run_once() == []

def test_schedule_api_persists_enabled_control(monkeypatch, tmp_path):
    store = JobStore(tmp_path / "api.db")
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
            "steps": [{"name": "update_stock_prices", "overwrite": False}],
            "config": {},
        },
    )
    assert response.status_code == 201
    schedule_id = response.json()["schedule_id"]

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
