"""Report manifest determinism tests."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.api import router as auth_router
from src.reports import api as report_api
from src.reports.manifest import build_manifest, canonical_json, content_sha256
from src.research.storage import ResearchStore
from src.web_app.security import AppSettings, install_security


def test_canonical_json_and_manifest_hashes_are_stable():
    assert canonical_json({"b": 1, "a": 2}) == b'{"a":2,"b":1}'
    recipe = {"sections": ["overview", "filings"]}
    inputs = {"companies": ["E1"], "as_of": "2025-01-01"}
    manifest = build_manifest(report_id="r1", owner_id="u1", recipe=recipe, inputs=inputs, application_version="1.0")
    assert manifest["recipe_sha256"] == content_sha256(recipe)
    assert manifest["inputs_sha256"] == content_sha256(inputs)


def test_report_run_is_owner_scoped_and_downloadable(monkeypatch, tmp_path):
    monkeypatch.setattr(report_api, "store", ResearchStore(tmp_path / "research.db"))
    monkeypatch.setattr(report_api, "REPORT_ROOT", tmp_path / "reports")
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(report_api.router)
    install_security(app, AppSettings(auth_mode="accounts", registration_mode="open", auth_db_path=tmp_path / "auth.db"))
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "report-user", "password": password})
    login = client.post("/api/auth/login", json={"login": "report-user", "password": password})
    client.headers["Authorization"] = f"Bearer {login.json()['access_token']}"
    created = client.post("/api/reports/runs", json={"name": "Smoke", "definition": {"companies": ["E1"]}})
    assert created.status_code == 201
    run_id = created.json()["run_id"]
    assert client.get(f"/api/reports/runs/{run_id}/manifest").json()["report_id"] == run_id
    download = client.get(f"/api/reports/runs/{run_id}/download")
    assert download.status_code == 200
    assert download.content.startswith(b"PK")

    other = TestClient(app)
    other.post("/api/auth/register", json={"username": "other-user", "password": password})
    other_login = other.post("/api/auth/login", json={"login": "other-user", "password": password})
    other.headers["Authorization"] = f"Bearer {other_login.json()['access_token']}"
    assert other.get(f"/api/reports/runs/{run_id}").status_code == 404
