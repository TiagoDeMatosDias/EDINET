"""The /api/worker/* routes: authentication, network rules, claim, progress, artifacts, finishing."""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.datastructures import Headers

from src.auth.api import router as auth_router
from src.jobs import kinds as kind_registry
from src.jobs import worker_api
from src.jobs.admin_api import router as admin_router
from src.jobs.errors import FinalizeError
from src.jobs.registry import WorkerRegistry, parse_enrollment
from src.jobs.service import JobService, set_job_service
from src.jobs.store import JobsStore
from src.jobs.user_api import install_job_error_handlers
from src.jobs.user_api import router as my_jobs_router
from src.paths import jobs_dir
from src.version import __version__
from src.web_app.security import AppSettings, _max_request_bytes, install_security

LOOPBACK = ("127.0.0.1", 50000)
ARTIFACT_LIMIT = 2_000


@pytest.fixture(autouse=True)
def fresh_throttle():
    worker_api.reset_enroll_throttle()
    yield
    worker_api.reset_enroll_throttle()


@pytest.fixture
def service(tmp_path):
    registry = WorkerRegistry(tmp_path / "app.db")
    svc = JobService(JobsStore(tmp_path / "jobs.db"), registry, claim_wait_seconds=1)
    set_job_service(svc)
    yield svc
    set_job_service(None)


def make_app(tmp_path, auth_mode="disabled") -> FastAPI:
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(worker_api.router)
    app.include_router(my_jobs_router)
    app.include_router(admin_router)
    install_job_error_handlers(app)
    install_security(
        app,
        AppSettings(auth_mode=auth_mode, auth_db_path=tmp_path / "auth.db",
                    max_backtest_artifact_bytes=ARTIFACT_LIMIT),
    )
    return app


@pytest.fixture
def client(tmp_path, service):
    return TestClient(make_app(tmp_path), client=LOOPBACK)


@pytest.fixture
def token(service):
    return service.registry.ensure_local_worker()


def auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def register(client, token, session="session-0001") -> dict:
    response = client.post(
        "/api/worker/register",
        headers=auth(token),
        json={"session_id": session, "version": __version__, "hostname": "box", "pid": 7, "platform": "test"},
    )
    assert response.status_code == 200, response.text
    return response.json()


def heartbeat(client, token, session="session-0001", jobs=(), data_access=True):
    return client.post(
        "/api/worker/heartbeat",
        headers=auth(token),
        json={"session_id": session, "data_access": data_access, "jobs": list(jobs)},
    )


def ready(client, token, session="session-0001"):
    client.headers.update(auth(token))  # later requests of the test act as this worker
    register(client, token, session)
    assert heartbeat(client, token, session).status_code == 200


def claim(client, token, session="session-0001", lanes=("interactive",)):
    return client.post(
        "/api/worker/claim", headers=auth(token), json={"session_id": session, "lanes": list(lanes)}
    )


def claimed_job(client, service, token, kind="echo", **enqueue):
    ready(client, token)
    queued = service.enqueue(kind, {"p": 1}, owner_user_id="u", title="Job", **enqueue)
    response = claim(client, token, lanes=("batch", "interactive"))
    assert response.status_code == 200, response.text
    job = response.json()["job"]
    assert job["job_id"] == queued.job_id
    return job


def lease(job) -> dict[str, str]:
    return {"X-Lease-Token": job["lease_token"]}


# -- authentication and network --------------------------------------------------------


def test_requests_need_a_worker_token(client, token):
    missing = client.post("/api/worker/register", json={"session_id": "session-0001", "version": __version__})
    wrong = client.post("/api/worker/heartbeat", headers=auth("srw_wrong"), json={"session_id": "s"})
    scheme = client.post("/api/worker/claim", headers={"Authorization": f"Basic {token}"},
                         json={"session_id": "s", "lanes": ["batch"]})

    for response in (missing, wrong, scheme):
        assert response.status_code == 401
        body = response.json()
        assert body["code"] == "worker_unauthorized" and body["correlation_id"]
        assert response.headers["X-Correlation-ID"] == body["correlation_id"]


def test_revoked_tokens_stop_working(client, service, token):
    register(client, token)
    service.registry.revoke("local")

    assert heartbeat(client, token).status_code == 401


def test_user_tokens_and_worker_tokens_are_not_interchangeable(tmp_path, service, token):
    app = make_app(tmp_path, "accounts")
    client = TestClient(app, client=LOOPBACK)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "admin", "password": password})
    user_token = client.post("/api/auth/login", json={"login": "admin", "password": password}).json()["access_token"]

    # A user's session token is not a worker credential.
    refused = client.post("/api/worker/register", headers=auth(user_token),
                          json={"session_id": "session-0001", "version": __version__})
    assert refused.status_code == 401 and refused.json()["code"] == "worker_unauthorized"
    # A worker token is not a user credential, on user routes or admin routes.
    for path in ("/api/my-jobs", "/api/workers/status", "/api/admin/workers", "/api/auth/me"):
        response = client.get(path, headers=auth(token))
        assert response.status_code == 401, path
        assert response.json()["code"] == "unauthorized"
    # The worker routes need no user session in accounts mode, and the user token still works elsewhere.
    assert register(client, token)["worker_id"] == "local"
    assert client.get("/api/my-jobs", headers=auth(user_token)).status_code == 200


def test_worker_routes_do_not_treat_the_caller_as_the_local_user(client, token):
    # auth.mode=disabled makes every other caller the local administrator; workers still need a token.
    assert client.get("/api/my-jobs").status_code == 200
    assert client.post("/api/worker/unregister", json={"session_id": "session-0001"}).status_code == 401


@pytest.mark.parametrize(
    "header", ["CF-Connecting-IP", "CF-Ray", "X-Forwarded-For", "Forwarded", "X-Real-IP"]
)
def test_tunnel_headers_are_refused(client, token, header):
    response = client.post(
        "/api/worker/register",
        headers={**auth(token), header: "203.0.113.9"},
        json={"session_id": "session-0001", "version": __version__},
    )

    assert response.status_code == 403
    assert response.json()["code"] == "worker_network_forbidden"


def test_clients_from_other_machines_are_refused(tmp_path, service, token):
    remote = TestClient(make_app(tmp_path), client=("203.0.113.9", 4000))
    response = remote.post("/api/worker/enroll", json={"code": "x" * 12, "version": __version__})

    assert response.status_code == 403 and response.json()["code"] == "worker_network_forbidden"


@pytest.mark.parametrize("host", ["127.0.0.1", "127.5.5.5", "::1", "::ffff:127.0.0.1", "localhost"])
def test_loopback_clients_are_allowed(host):
    worker_api.network_guard(_request(host))


def _request(client_host, bind="0.0.0.0", headers=None):
    return SimpleNamespace(
        headers=Headers(headers or {}),
        client=SimpleNamespace(host=client_host),
        app=SimpleNamespace(state=SimpleNamespace(settings=SimpleNamespace(host=bind))),
    )


def test_a_specific_bind_address_is_allowed_as_a_client():
    worker_api.network_guard(_request("192.168.1.20", bind="192.168.1.20"))
    for bind in ("0.0.0.0", "::", "192.168.1.99", "127.0.0.1"):
        with pytest.raises(worker_api.WorkerNetworkForbidden) as raised:
            worker_api.network_guard(_request("192.168.1.20", bind=bind))
        assert raised.value.code == "worker_network_forbidden"
    with pytest.raises(worker_api.WorkerNetworkForbidden):
        worker_api.network_guard(_request("192.168.1.20", bind="192.168.1.20", headers={"X-Forwarded-For": "1"}))
    with pytest.raises(worker_api.WorkerNetworkForbidden):
        worker_api.network_guard(SimpleNamespace(headers=Headers({}), client=None,
                                                 app=SimpleNamespace(state=SimpleNamespace())))


# -- enrollment -----------------------------------------------------------------------------


def test_enrollment_gives_a_working_worker(client, service):
    text, _ = service.registry.create_enrollment("Laptop", "admin", "https://127.0.0.1:8000", "ab" * 32)
    code = parse_enrollment(text)[2]

    response = client.post("/api/worker/enroll", json={"code": code, "name": "ignored", "version": __version__})

    assert response.status_code == 200
    body = response.json()
    assert body["server_version"] == __version__ and body["token"].startswith("srw_")
    assert service.registry.get(body["worker_id"]).name == "Laptop"
    assert register(client, body["token"])["worker_id"] == body["worker_id"]
    again = client.post("/api/worker/enroll", json={"code": code, "version": __version__})
    assert again.status_code == 403 and again.json()["code"] == "enrollment_invalid"


def test_failed_enrollments_are_throttled(client, service, monkeypatch):
    bad = {"code": "not-a-valid-code", "version": __version__}
    statuses = [client.post("/api/worker/enroll", json=bad).status_code for _ in range(6)]

    assert statuses == [403, 403, 403, 403, 403, 429]
    assert client.post("/api/worker/enroll", json=bad).json()["code"] == "enrollment_throttled"
    # even a valid code is refused while blocked
    text, _ = service.registry.create_enrollment("W", None, "u", "f")
    assert client.post("/api/worker/enroll", json={"code": parse_enrollment(text)[2], "version": __version__}).status_code == 429
    # the block ends after ten minutes
    real = time.monotonic
    monkeypatch.setattr(worker_api.time, "monotonic", lambda: real() + 601)
    assert client.post("/api/worker/enroll", json={"code": parse_enrollment(text)[2], "version": __version__}).status_code == 200


# -- register and heartbeat ---------------------------------------------------------------------


def test_register_response(client, token):
    body = register(client, token)

    assert body["worker_id"] == "local" and body["session_id"] == "session-0001"
    assert body["server_version"] == __version__
    assert {"market_db", "filings_db", "data_dir", "jobs_dir"} == set(body["paths"])
    assert body["settings"]["limits.max_backtest_artifact_bytes"] > 0
    assert body["heartbeat_seconds"] == 15 and body["lease_seconds"] == 60


def test_version_mismatch_is_refused(client, token):
    response = client.post("/api/worker/register", headers=auth(token),
                           json={"session_id": "session-0001", "version": "0.0.1"})

    assert response.status_code == 409 and response.json()["code"] == "version_mismatch"


def test_disabled_workers_cannot_register_and_are_told_to_shut_down(client, service, token):
    ready(client, token)
    service.registry.update("local", enabled=False)

    assert heartbeat(client, token).json()["shutdown"] is True
    response = client.post("/api/worker/register", headers=auth(token),
                           json={"session_id": "session-0002", "version": __version__})
    assert response.status_code == 403 and response.json()["code"] == "worker_disabled"


def test_unknown_sessions_must_register_again(client, token):
    response = heartbeat(client, token, session="never-registered")

    assert response.status_code == 409 and response.json()["code"] == "unknown_session"
    assert claim(client, token, session="never-registered").json()["code"] == "unknown_session"


def test_claim_is_refused_without_data_access(client, service, token):
    register(client, token)
    service.enqueue("echo", {}, owner_user_id=None, title="t")

    response = claim(client, token)

    assert response.status_code == 409 and response.json()["code"] == "no_data_access"


def test_heartbeat_reports_each_lease_and_cancel(client, service, token):
    job = claimed_job(client, service, token)
    service.cancel(job["job_id"])

    response = heartbeat(client, token, jobs=[
        {"job_id": job["job_id"], "lease_token": job["lease_token"]},
        {"job_id": job["job_id"], "lease_token": "stale"},
    ])

    body = response.json()
    assert [(s["lease_ok"], s["cancel"]) for s in body["jobs"]] == [(True, "soft"), (False, "none")]
    assert body["shutdown"] is False and body["config_version"] >= 1


# -- claim -------------------------------------------------------------------------------------


def test_claim_returns_204_when_nothing_is_waiting(client, token):
    ready(client, token)
    started = time.monotonic()

    response = claim(client, token)

    assert response.status_code == 204 and response.content == b""
    assert 0.8 < time.monotonic() - started < 3


def test_claim_returns_the_assignment(client, service, token):
    job = claimed_job(client, service, token)

    assert set(job) == {"job_id", "kind", "lane", "title", "payload", "attempt", "lease_token",
                        "lease_expires_at", "owner_user_id", "workspace"}
    assert job["payload"] == {"p": 1} and job["attempt"] == 1 and job["owner_user_id"] == "u"
    assert job["workspace"] == str(jobs_dir() / job["job_id"])


def test_claim_long_poll_returns_promptly_when_a_job_is_enqueued(client, service, token):
    ready(client, token)
    service.claim_wait_seconds = 10
    result: dict = {}

    def poll():
        started = time.monotonic()
        result["response"] = claim(client, token)
        result["done"] = time.monotonic()
        result["started"] = started

    thread = threading.Thread(target=poll)
    thread.start()
    time.sleep(0.5)
    enqueued = time.monotonic()
    service.enqueue("echo", {}, owner_user_id=None, title="late")
    thread.join(timeout=10)

    assert result["response"].status_code == 200
    assert result["done"] - enqueued < 1.0
    assert result["done"] - result["started"] < 5


# -- progress and finishing -------------------------------------------------------------------------


def test_progress_updates_the_job_and_pipeline_steps(client, service, token):
    job = claimed_job(client, service, token, kind="pipeline",
                      steps=[{"name": "a", "overwrite": True}, {"name": "b"}])
    url = f"/api/worker/jobs/{job['job_id']}/progress"

    def post(**body):
        return client.post(url, json={"lease_token": job["lease_token"], **body})

    first = post(progress_percent=10, status_message="Starting", step={"ordinal": 0, "status": "running"})
    assert first.status_code == 200 and first.json()["cancel"] == "none"
    assert first.json()["lease_expires_at"]
    post(progress_percent=50, step={"ordinal": 0, "status": "completed", "result": {"rows": 2}, "duration_ms": 5})
    post(step={"ordinal": 1, "status": "running", "progress_percent": 25, "status_message": "Half"})

    record = service.get(job["job_id"])
    assert record.progress_percent == 50
    steps = service.steps(job["job_id"])
    assert [s["status"] for s in steps] == ["completed", "running"]
    assert steps[0]["duration_ms"] == 5 and steps[1]["progress_percent"] == 25
    service.cancel(job["job_id"], force=True)
    assert post().json()["cancel"] == "hard"


def test_progress_with_a_stale_lease_is_refused(client, service, token):
    job = claimed_job(client, service, token)

    response = client.post(f"/api/worker/jobs/{job['job_id']}/progress", json={"lease_token": "stale"})

    assert response.status_code == 409 and response.json()["code"] == "lease_lost"


def test_unknown_jobs_are_404(client, token):
    ready(client, token)

    for method, suffix, body in (
        ("post", "progress", {"lease_token": "x"}),
        ("post", "complete", {"lease_token": "x"}),
        ("post", "fail", {"lease_token": "x"}),
        ("post", "release", {"lease_token": "x"}),
    ):
        response = getattr(client, method)(f"/api/worker/jobs/{uuid4()}/{suffix}", headers=auth(token), json=body)
        assert response.status_code == 404 and response.json()["code"] == "job_not_found", suffix


def test_complete_is_idempotent_and_then_rejects_other_leases(client, service, token):
    job = claimed_job(client, service, token)
    url = f"/api/worker/jobs/{job['job_id']}/complete"

    first = client.post(url, json={"lease_token": job["lease_token"], "result": {"echo": 1}})
    second = client.post(url, json={"lease_token": job["lease_token"], "result": {"echo": 1}})
    other = client.post(url, json={"lease_token": "other", "result": {}})

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json() == {"status": "completed"}
    assert other.status_code == 409 and other.json()["code"] == "lease_lost"
    assert service.get(job["job_id"]).result == {"echo": 1}


def test_complete_runs_the_finalizer(client, service, token, monkeypatch):
    calls = []

    def finalize(job, result, artifacts):
        calls.append((job.kind, result, sorted(artifacts)))
        return {"public": result["raw"]}

    monkeypatch.setattr(kind_registry, "finalizer_for", lambda kind: finalize)
    job = claimed_job(client, service, token)
    client.put(f"/api/worker/jobs/{job['job_id']}/artifacts/result.json", headers=lease(job), content=b"{}")

    response = client.post(f"/api/worker/jobs/{job['job_id']}/complete",
                           json={"lease_token": job["lease_token"], "result": {"raw": 5}})

    assert response.json() == {"status": "completed"}
    assert calls == [("echo", {"raw": 5}, ["result.json"])]
    assert service.get(job["job_id"]).result == {"public": 5}


def test_a_finalizer_error_fails_the_job(client, service, token, monkeypatch):
    def finalize(job, result, artifacts):
        raise FinalizeError("Could not store the result")

    monkeypatch.setattr(kind_registry, "finalizer_for", lambda kind: finalize)
    job = claimed_job(client, service, token)

    response = client.post(f"/api/worker/jobs/{job['job_id']}/complete", json={"lease_token": job["lease_token"]})

    assert response.json() == {"status": "failed"}
    assert service.get(job["job_id"]).error_message == "Could not store the result"


def test_fail_and_release(client, service, token):
    job = claimed_job(client, service, token)
    body = {"lease_token": job["lease_token"], "error": "It broke", "retryable": True}

    retried = client.post(f"/api/worker/jobs/{job['job_id']}/fail", json=body)
    assert retried.json() == {"status": "open"}

    second = claim(client, token).json()["job"]
    released = client.post(f"/api/worker/jobs/{second['job_id']}/release",
                           json={"lease_token": second["lease_token"], "reason": "Stopping"})
    assert released.json() == {"status": "open"}

    third = claim(client, token).json()["job"]
    failed = client.post(f"/api/worker/jobs/{third['job_id']}/fail",
                         json={"lease_token": third["lease_token"], "error": "Final"})
    assert failed.json() == {"status": "failed"}
    assert service.get(third["job_id"]).error_message == "Final"


def test_a_cancelled_job_is_acknowledged_by_the_worker(client, service, token):
    job = claimed_job(client, service, token)
    service.cancel(job["job_id"])

    response = client.post(f"/api/worker/jobs/{job['job_id']}/fail",
                           json={"lease_token": job["lease_token"], "cancelled": True})

    assert response.json() == {"status": "cancelled"}


def test_unregister_ends_the_session(client, service, token):
    ready(client, token)

    response = client.post("/api/worker/unregister", headers=auth(token), json={"session_id": "session-0001"})

    assert response.status_code == 204
    assert heartbeat(client, token).status_code == 409


# -- artifacts --------------------------------------------------------------------------------------------


def test_artifact_upload(client, service, token):
    job = claimed_job(client, service, token)
    url = f"/api/worker/jobs/{job['job_id']}/artifacts"
    payload = b"hello artifact"

    response = client.put(f"{url}/echo.txt", headers={**lease(job), "Content-Type": "text/plain"}, content=payload)

    import hashlib

    assert response.status_code == 200
    assert response.json() == {"name": "echo.txt", "size_bytes": len(payload),
                               "sha256": hashlib.sha256(payload).hexdigest()}
    folder = jobs_dir() / job["job_id"] / "artifacts"
    assert (folder / "echo.txt").read_bytes() == payload and not list(folder.glob("*.part"))
    row = service.store.artifacts(job["job_id"])[0]
    assert row["content_type"] == "text/plain" and row["size_bytes"] == len(payload)
    # uploading the same name again replaces it
    client.put(f"{url}/echo.txt", headers=lease(job), content=b"new")
    assert (folder / "echo.txt").read_bytes() == b"new"
    assert len(service.store.artifacts(job["job_id"])) == 1


@pytest.mark.parametrize("name", [".hidden", "-dash", "a b", "x" * 101, "semi;colon", "..", "a%2Fb"])
def test_invalid_artifact_names(client, service, token, name):
    job = claimed_job(client, service, token)

    response = client.put(f"/api/worker/jobs/{job['job_id']}/artifacts/{name}", headers=lease(job), content=b"x")

    assert response.status_code in (404, 422)
    if response.status_code == 422:
        assert response.json().get("code") == "invalid_artifact_name"
    folder = jobs_dir() / job["job_id"] / "artifacts"
    assert not folder.exists() or not list(folder.iterdir())


def test_artifact_size_limit(client, service, token):
    job = claimed_job(client, service, token)
    url = f"/api/worker/jobs/{job['job_id']}/artifacts"

    exact = client.put(f"{url}/exact.bin", headers=lease(job), content=b"x" * ARTIFACT_LIMIT)
    declared = client.put(f"{url}/big.bin", headers=lease(job), content=b"x" * (ARTIFACT_LIMIT + 1))
    chunked = client.put(f"{url}/chunked.bin", headers=lease(job),
                         content=(b"x" * 500 for _ in range(10)))

    assert exact.status_code == 200
    for response in (declared, chunked):
        assert response.status_code == 413 and response.json()["code"] == "artifact_too_large"
    folder = jobs_dir() / job["job_id"] / "artifacts"
    assert sorted(path.name for path in folder.iterdir()) == ["exact.bin"]


def test_artifact_upload_needs_the_lease(client, service, token):
    job = claimed_job(client, service, token)
    url = f"/api/worker/jobs/{job['job_id']}/artifacts/a.bin"

    for headers in ({}, {"X-Lease-Token": "stale"}):
        response = client.put(url, headers=headers, content=b"x")
        assert response.status_code == 409 and response.json()["code"] == "lease_lost"
    assert client.put(f"/api/worker/jobs/{uuid4()}/artifacts/a.bin", headers=lease(job), content=b"x").status_code == 404
    folder = jobs_dir() / job["job_id"] / "artifacts"
    assert not folder.exists() or not list(folder.iterdir())


def test_body_limits_for_worker_routes(tmp_path):
    settings = AppSettings(auth_mode="disabled", auth_db_path=tmp_path / "auth.db",
                           max_backtest_artifact_bytes=300 * 1024 * 1024)
    artifact = _max_request_bytes(settings, "/api/worker/jobs/abc/artifacts/result.json")
    other_worker = _max_request_bytes(settings, "/api/worker/jobs/abc/complete")

    assert artifact == 300 * 1024 * 1024 + 1024 * 1024
    assert other_worker == 33 * 1024 * 1024
    assert _max_request_bytes(settings, "/api/screening/run") < artifact


def test_artifact_bodies_far_over_the_limit_are_stopped_by_the_middleware(client, service, token):
    job = claimed_job(client, service, token)

    response = client.put(
        f"/api/worker/jobs/{job['job_id']}/artifacts/huge.bin", headers=lease(job),
        content=b"x" * (ARTIFACT_LIMIT + 1024 * 1024 + 10),
    )

    assert response.status_code == 413
    folder = jobs_dir() / job["job_id"] / "artifacts"
    assert not folder.exists() or not list(folder.iterdir())
