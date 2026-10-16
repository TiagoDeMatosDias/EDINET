"""/api/my-jobs and /api/workers/status: visibility, links, cancelling."""

from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.api import router as auth_router
from src.jobs.protocol import ClaimRequest, CompleteRequest, HeartbeatRequest, RegisterRequest
from src.jobs.registry import WorkerRegistry
from src.jobs.service import JobService, set_job_service
from src.jobs.store import JobsStore
from src.jobs.user_api import install_job_error_handlers
from src.jobs.user_api import router as my_jobs_router
from src.version import __version__
from src.web_app.security import AppSettings, install_security

PASSWORD = "correct horse battery staple"


@pytest.fixture
def service(tmp_path):
    registry = WorkerRegistry(tmp_path / "app.db")
    registry.ensure_local_worker()
    svc = JobService(JobsStore(tmp_path / "jobs.db"), registry)
    set_job_service(svc)
    yield svc
    set_job_service(None)


@pytest.fixture
def people(tmp_path, service):
    """An accounts-mode app with an administrator (first account) and two members."""
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(my_jobs_router)
    install_job_error_handlers(app)
    install_security(app, AppSettings(auth_mode="accounts", auth_db_path=tmp_path / "auth.db"))
    client = TestClient(app)
    result = {"client": client}
    for name in ("admin", "alice", "bob"):
        client.post("/api/auth/register", json={"username": name, "password": PASSWORD})
        login = client.post("/api/auth/login", json={"login": name, "password": PASSWORD}).json()
        headers = {"Authorization": f"Bearer {login['access_token']}"}
        result[name] = {"headers": headers, "id": client.get("/api/auth/me", headers=headers).json()["user_id"]}
    return result


def online(service):
    session = str(uuid4())
    service.register("local", RegisterRequest(session_id=session, version=__version__))
    service.heartbeat("local", HeartbeatRequest(session_id=session, data_access=True))
    return session


def test_users_see_only_their_own_jobs(people, service):
    client = people["client"]
    mine = service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="Alice job")
    service.enqueue("echo", {}, owner_user_id=people["bob"]["id"], title="Bob job")
    service.enqueue("echo", {}, owner_user_id=None, title="System job")
    service.enqueue("pipeline", {}, owner_user_id=people["admin"]["id"], title="Pipeline")

    body = client.get("/api/my-jobs", headers=people["alice"]["headers"]).json()

    assert [job["job_id"] for job in body["jobs"]] == [mine.job_id]
    assert set(body) == {"jobs", "server_time", "workers"}
    assert body["workers"] == {"online": 0, "interactive_slots": 0, "batch_slots": 0}


def test_administrators_also_see_pipeline_and_system_jobs(people, service):
    client = people["client"]
    own = service.enqueue("echo", {}, owner_user_id=people["admin"]["id"], title="Mine")
    pipeline = service.enqueue("pipeline", {}, owner_user_id=people["bob"]["id"], title="Pipeline")
    system = service.enqueue("echo", {}, owner_user_id=None, title="System")
    service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="Alice")

    jobs = client.get("/api/my-jobs", headers=people["admin"]["headers"]).json()["jobs"]

    assert {job["job_id"] for job in jobs} == {own.job_id, pipeline.job_id, system.job_id}
    assert [job["job_id"] for job in jobs] == [system.job_id, pipeline.job_id, own.job_id]  # newest first


def test_job_summary_fields(people, service):
    client = people["client"]
    queued = service.enqueue("echo", {"secret": "payload"}, owner_user_id=people["alice"]["id"], title="Echo test")

    job = client.get("/api/my-jobs", headers=people["alice"]["headers"]).json()["jobs"][0]

    assert set(job) == {
        "job_id", "kind", "kind_label", "title", "status", "progress_percent", "status_message", "progress",
        "created_at", "started_at", "completed_at", "updated_at", "error_message", "notify", "link",
        "cancellable", "waiting_for_worker",
    }
    assert (job["job_id"], job["kind"], job["kind_label"], job["status"]) == (queued.job_id, "echo", "Diagnostic", "open")
    assert job["cancellable"] is True and job["link"] is None
    assert job["waiting_for_worker"] is True  # nobody is online


def test_waiting_for_worker_clears_when_a_worker_serves_the_lane(people, service):
    client = people["client"]
    service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="t")
    online(service)

    body = client.get("/api/my-jobs", headers=people["alice"]["headers"]).json()

    assert body["jobs"][0]["waiting_for_worker"] is False
    assert body["workers"] == {"online": 1, "interactive_slots": 1, "batch_slots": 2}
    assert client.get("/api/workers/status", headers=people["bob"]["headers"]).json() == body["workers"]


def test_links_per_kind(people, service):
    client = people["client"]
    admin = people["admin"]["id"]
    session = online(service)
    jobs = {
        kind: service.enqueue(kind, {}, owner_user_id=admin, title=kind)
        for kind in ("backtest_single", "backtest_rolling", "screening", "pipeline", "translation")
    }

    def links():
        rows = client.get("/api/my-jobs", headers=people["admin"]["headers"]).json()["jobs"]
        return {row["kind"]: row["link"] for row in rows}

    assert links() == {"backtest_single": None, "backtest_rolling": None, "screening": "/screening",
                       "pipeline": "/pipeline", "translation": None}
    for kind in ("backtest_single", "backtest_rolling"):
        job = service.claim_now("local", ClaimRequest(session_id=session, lanes=["batch", "interactive"], kinds=[kind]))
        service.store.complete(job.job_id, "local", job.lease_token, '{"result_id": "bt 1/x"}')
    assert links()["backtest_single"] == "/backtest?result=bt%201%2Fx"
    assert links()["backtest_rolling"] == "/backtest?result=bt%201%2Fx"
    assert jobs


def test_detail_includes_the_result(people, service):
    client = people["client"]
    session = online(service)
    queued = service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="t")
    job = service.claim_now("local", ClaimRequest(session_id=session, lanes=["interactive"]))
    service.complete("local", job.job_id, CompleteRequest(lease_token=job.lease_token, result={"echo": 7}))

    response = client.get(f"/api/my-jobs/{queued.job_id}", headers=people["alice"]["headers"])

    body = response.json()
    assert response.status_code == 200 and body["result"] == {"echo": 7} and body["status"] == "completed"
    assert body["cancellable"] is False and body["completed_at"]


def test_other_users_jobs_are_404(people, service):
    client = people["client"]
    queued = service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="t")

    assert client.get(f"/api/my-jobs/{queued.job_id}", headers=people["bob"]["headers"]).status_code == 404
    assert client.post(f"/api/my-jobs/{queued.job_id}/cancel", headers=people["bob"]["headers"]).status_code == 404
    assert client.get("/api/my-jobs/missing", headers=people["bob"]["headers"]).status_code == 404
    # administrators see only system and pipeline jobs of others
    assert client.get(f"/api/my-jobs/{queued.job_id}", headers=people["admin"]["headers"]).status_code == 404
    pipeline = service.enqueue("pipeline", {}, owner_user_id=people["alice"]["id"], title="p")
    assert client.get(f"/api/my-jobs/{pipeline.job_id}", headers=people["admin"]["headers"]).status_code == 200
    assert client.get(f"/api/my-jobs/{pipeline.job_id}", headers=people["bob"]["headers"]).status_code == 404


def test_authentication_is_required(people):
    client = people["client"]
    assert client.get("/api/my-jobs").status_code == 401
    assert client.get("/api/workers/status").status_code == 401


def test_cancel(people, service):
    client = people["client"]
    queued = service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="t")

    response = client.post(f"/api/my-jobs/{queued.job_id}/cancel", headers=people["alice"]["headers"],
                           json={"force": False})

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled" and response.json()["cancellable"] is False
    again = client.post(f"/api/my-jobs/{queued.job_id}/cancel", headers=people["alice"]["headers"])
    assert again.status_code == 409


def test_cancelling_a_running_job_asks_the_worker_to_stop(people, service):
    client = people["client"]
    session = online(service)
    queued = service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="t")
    service.claim_now("local", ClaimRequest(session_id=session, lanes=["interactive"]))

    response = client.post(f"/api/my-jobs/{queued.job_id}/cancel", headers=people["alice"]["headers"])

    assert response.json()["status"] == "cancelling" and response.json()["cancellable"] is True
    assert service.get(queued.job_id).cancel_requested == "soft"


def test_only_privileged_users_may_force(people, service):
    client = people["client"]
    session = online(service)
    queued = service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="t")
    service.claim_now("local", ClaimRequest(session_id=session, lanes=["interactive"]))
    pipeline = service.enqueue("pipeline", {}, owner_user_id=people["bob"]["id"], title="p")

    assert client.post(f"/api/my-jobs/{queued.job_id}/cancel", headers=people["alice"]["headers"],
                       json={"force": True}).status_code == 403
    assert service.get(queued.job_id).status == "in_progress"
    # an administrator cannot see Alice's echo job, but can cancel (and force) pipeline jobs
    forced = client.post(f"/api/my-jobs/{pipeline.job_id}/cancel", headers=people["admin"]["headers"], json={"force": True})
    assert forced.status_code == 200 and forced.json()["status"] == "cancelled"


def test_list_filters(people, service):
    client = people["client"]
    headers = people["alice"]["headers"]
    first = service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="first")
    second = service.enqueue("echo", {}, owner_user_id=people["alice"]["id"], title="second")
    service.cancel(first.job_id)

    active = client.get("/api/my-jobs?active=true", headers=headers).json()["jobs"]
    limited = client.get("/api/my-jobs?limit=1", headers=headers).json()["jobs"]
    since = client.get("/api/my-jobs", params={"updated_since": service.get(first.job_id).updated_at}, headers=headers)

    assert [job["job_id"] for job in active] == [second.job_id]
    assert len(limited) == 1
    assert since.status_code == 200
    assert client.get("/api/my-jobs?limit=0", headers=headers).status_code == 422
    assert client.get("/api/my-jobs?limit=101", headers=headers).status_code == 422
