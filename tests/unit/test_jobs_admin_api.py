"""/api/admin/workers: listing, editing, revoking, enrolling, local restart, echo."""

from __future__ import annotations

import hashlib
import threading
import time
from uuid import uuid4

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.api import router as auth_router
from src.jobs import worker_api
from src.jobs.admin_api import router as admin_router
from src.jobs.protocol import (
    ClaimRequest,
    CompleteRequest,
    FailRequest,
    HeartbeatRequest,
    RegisterRequest,
)
from src.jobs.registry import WorkerRegistry, parse_enrollment
from src.jobs.service import JobService, set_job_service
from src.jobs.store import JobsStore
from src.jobs.user_api import install_job_error_handlers
from src.jobs.user_api import router as my_jobs_router
from src.version import __version__
from src.web_app import tls
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
def api(tmp_path, service):
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(admin_router)
    app.include_router(my_jobs_router)
    install_job_error_handlers(app)
    install_security(app, AppSettings(auth_mode="accounts", auth_db_path=tmp_path / "auth.db", port=8443))
    client = TestClient(app)
    headers = {}
    for name in ("admin", "member"):
        client.post("/api/auth/register", json={"username": name, "password": PASSWORD})
        login = client.post("/api/auth/login", json={"login": name, "password": PASSWORD}).json()
        headers[name] = {"Authorization": f"Bearer {login['access_token']}"}
    client.headers.update(headers["admin"])
    return SimpleApi(app, client, headers)


class SimpleApi:
    def __init__(self, app, client, headers):
        self.app, self.client, self.headers = app, client, headers


def connect(service, *, data_access=True):
    session = str(uuid4())
    service.register("local", RegisterRequest(session_id=session, version=__version__, hostname="box", pid=42, platform="linux"))
    service.heartbeat("local", HeartbeatRequest(session_id=session, data_access=data_access,
                                                data_access_detail="ok", interactive_busy=1))
    return session


def test_only_administrators_may_use_it(api):
    member = api.headers["member"]
    for method, path in (("get", "/api/admin/workers"), ("patch", "/api/admin/workers/local"),
                         ("delete", "/api/admin/workers/local"), ("post", "/api/admin/workers/enrollments"),
                         ("post", "/api/admin/workers/local/restart"), ("post", "/api/admin/workers/echo")):
        response = api.client.request(method.upper(), path, headers=member, json={})
        assert response.status_code == 403, path
    bare = TestClient(api.app)
    assert bare.get("/api/admin/workers").status_code == 401


def test_list_joins_the_registry_with_the_live_session(api, service):
    before = api.client.get("/api/admin/workers").json()
    local = before["workers"][0]
    assert local["worker_id"] == "local" and local["kind"] == "local" and local["online"] is False
    assert local["last_seen_at"] is None and local["running_jobs"] == []
    assert before["settings"] == {"launch_local": "off"}  # tests/conftest.py turns the launcher off
    assert before["launcher"]["state"] == "stopped"

    session = connect(service)
    service.enqueue("echo", {}, owner_user_id=None, title="Running one")
    job = service.claim_now("local", ClaimRequest(session_id=session, lanes=["interactive"]))

    local = api.client.get("/api/admin/workers").json()["workers"][0]
    assert local["online"] is True
    assert (local["hostname"], local["pid"], local["platform"], local["version"]) == ("box", 42, "linux", __version__)
    assert local["data_access"] is True and local["data_access_detail"] == "ok"
    assert (local["interactive_busy"], local["batch_busy"]) == (1, 0)
    assert local["running_jobs"] == [{
        "job_id": job.job_id, "kind": "echo", "title": "Running one",
        "started_at": service.get(job.job_id).started_at, "progress_percent": 0.0,
    }]
    assert set(local) == {
        "worker_id", "name", "kind", "enabled", "interactive_slots", "batch_slots", "created_at", "online",
        "last_seen_at", "version", "hostname", "pid", "platform", "data_access", "data_access_detail",
        "interactive_busy", "batch_busy", "running_jobs",
    }


def test_a_silent_session_is_offline(api, service):
    connect(service)
    service.heartbeat_seconds = 0
    time.sleep(0.01)

    local = api.client.get("/api/admin/workers").json()["workers"][0]
    assert local["online"] is False and local["last_seen_at"]


def test_launcher_status_comes_from_the_launcher(api):
    class Fake:
        restarted = 0

        def status(self):
            return {"state": "running", "pid": 5, "restarts": 1, "last_exit_code": 3, "command": ["x"]}

        def restart(self):
            self.restarted += 1
            return {**self.status(), "restarts": 2}

    api.app.state.launcher = fake = Fake()

    assert api.client.get("/api/admin/workers").json()["launcher"]["pid"] == 5
    response = api.client.post("/api/admin/workers/local/restart")
    assert response.status_code == 200 and response.json()["restarts"] == 2 and fake.restarted == 1


def test_restart_without_a_launcher_is_a_conflict(api):
    assert api.client.post("/api/admin/workers/local/restart").status_code == 409


def test_patch_updates_and_bumps_the_config_version(api, service):
    before = service.registry.get("local").config_version

    response = api.client.patch("/api/admin/workers/local",
                                json={"name": "Main", "interactive_slots": 2, "batch_slots": 0, "enabled": False})

    body = response.json()
    assert response.status_code == 200
    assert (body["name"], body["interactive_slots"], body["batch_slots"], body["enabled"]) == ("Main", 2, 0, False)
    assert service.registry.get("local").config_version == before + 1
    only_name = api.client.patch("/api/admin/workers/local", json={"name": "Again"}).json()
    assert only_name["enabled"] is False and only_name["interactive_slots"] == 2


def test_patch_validation(api):
    assert api.client.patch("/api/admin/workers/local", json={"batch_slots": 9}).status_code == 422
    assert api.client.patch("/api/admin/workers/local", json={"unknown": 1}).status_code == 422
    assert api.client.patch("/api/admin/workers/local", json={"name": ""}).status_code == 422
    assert api.client.patch("/api/admin/workers/missing", json={"name": "x"}).status_code == 404


def test_revoke(api, service):
    text, _ = service.registry.create_enrollment("Laptop", None, "https://x", "f")
    worker, token = service.registry.redeem_enrollment(parse_enrollment(text)[2], "x")

    assert api.client.delete("/api/admin/workers/local").status_code == 409
    assert service.registry.get("local").revoked_at is None
    assert api.client.delete(f"/api/admin/workers/{worker.worker_id}").status_code == 200
    assert service.registry.authenticate(token) is None
    ids = [w["worker_id"] for w in api.client.get("/api/admin/workers").json()["workers"]]
    assert ids == ["local"]
    assert api.client.delete(f"/api/admin/workers/{worker.worker_id}").status_code == 404


@pytest.fixture
def certificate(tmp_path, monkeypatch):
    directory = tmp_path / "certs"
    cert_path, _ = tls.provision_tls(cert_dir=directory)
    monkeypatch.setattr(tls, "certs_dir", lambda: directory)
    return cert_path


def test_enrollment_embeds_the_server_address_and_certificate_fingerprint(api, service, certificate):
    response = api.client.post("/api/admin/workers/enrollments", json={"name": "Laptop"})

    assert response.status_code == 200
    body = response.json()
    assert body["command"] == f"ShadeWorker enroll {body['enrollment']}"
    url, fingerprint, code = parse_enrollment(body["enrollment"])
    assert url == "https://127.0.0.1:8443"
    der = x509.load_pem_x509_certificate(certificate.read_bytes()).public_bytes(serialization.Encoding.DER)
    assert fingerprint == hashlib.sha256(der).hexdigest() == tls.certificate_fingerprint(certificate)
    assert fingerprint == fingerprint.lower() and ":" not in fingerprint and len(fingerprint) == 64
    assert body["expires_at"]
    worker, _ = service.registry.redeem_enrollment(code, "")
    assert worker.name == "Laptop"


def test_enrollment_needs_a_certificate(api, tmp_path, monkeypatch):
    monkeypatch.setattr(tls, "certs_dir", lambda: tmp_path / "no-certs")

    assert api.client.post("/api/admin/workers/enrollments", json={"name": "x"}).status_code == 409


def test_enrolled_worker_can_register_over_http(api, service, certificate):
    app = api.app
    app.include_router(worker_api.router)
    code = parse_enrollment(
        api.client.post("/api/admin/workers/enrollments", json={"name": "Laptop"}).json()["enrollment"])[2]
    worker_client = TestClient(app, client=("127.0.0.1", 5000))
    worker_api.reset_enroll_throttle()

    enrolled = worker_client.post("/api/worker/enroll", json={"code": code, "version": __version__}).json()

    assert service.registry.authenticate(enrolled["token"]).worker_id == enrolled["worker_id"]


def _complete_later(service, *, how="complete", delay=0.2):
    session = connect(service)

    def work():
        time.sleep(delay)
        deadline = time.monotonic() + 5
        job = None
        while job is None and time.monotonic() < deadline:
            job = service.claim_now("local", ClaimRequest(session_id=session, lanes=["interactive"]))
            time.sleep(0.01)
        assert job is not None
        if how == "complete":
            service.complete("local", job.job_id, CompleteRequest(
                lease_token=job.lease_token, result={"echo": job.payload["result"]}))
        else:
            service.fail("local", job.job_id, FailRequest(lease_token=job.lease_token, error="Echo failed on purpose"))

    thread = threading.Thread(target=work)
    thread.start()
    return thread


def test_echo_without_a_worker_is_pending(api):
    response = api.client.post("/api/admin/workers/echo", json={"result": 1})

    assert response.status_code == 202
    body = response.json()
    assert body["code"] == "job_pending" and body["waiting_for_worker"] is True and body["job_id"]
    assert body["detail"] == "No worker is online; the job will start when one connects."


def test_echo_returns_the_completed_job(api, service):
    thread = _complete_later(service)

    response = api.client.post("/api/admin/workers/echo", json={"result": {"n": 1}, "sleep_seconds": 0})
    thread.join()

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed" and body["kind"] == "echo" and body["result"] == {"echo": {"n": 1}}
    assert service.get(body["job_id"]).payload == {
        "sleep_seconds": 0.0, "result": {"n": 1}, "fail": None, "artifact_text": None, "progress_steps": 0}


def test_echo_failure_is_a_502(api, service):
    thread = _complete_later(service, how="fail")

    response = api.client.post("/api/admin/workers/echo", json={"fail": "boom"})
    thread.join()

    assert response.status_code == 502
    assert response.json()["code"] == "job_failed" and response.json()["detail"] == "Echo failed on purpose"


def test_echo_without_waiting_returns_the_queued_job(api, service):
    response = api.client.post("/api/admin/workers/echo", json={"wait": False, "artifact_text": "hi"})

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "open" and body["kind"] == "echo" and "code" not in body
    assert service.get(body["job_id"]).payload["artifact_text"] == "hi"


def test_echo_validates_its_input(api):
    assert api.client.post("/api/admin/workers/echo", json={"sleep_seconds": -1}).status_code == 422
    assert api.client.post("/api/admin/workers/echo", json={"wat": 1}).status_code == 422
