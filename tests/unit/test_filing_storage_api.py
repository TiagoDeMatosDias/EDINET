"""/api/admin/filing-storage, and how filing endpoints answer a missing year or the old layout."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.api import router as auth_router
from src.filings import api as filings_api
from src.filings import split as splitting
from src.filings import storage_api
from src.filings.catalog import FilingCatalog
from src.jobs.registry import WorkerRegistry
from src.jobs.service import JobService, set_job_service
from src.jobs.store import JobsStore
from src.web_app.security import AppSettings, install_security
from tests.filing_fixtures import create_legacy_filings_db, filing_row

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
def client(tmp_path, service):
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(storage_api.router)
    app.include_router(filings_api.router)
    storage_api.install_filing_storage_error_handlers(app)
    install_security(app, AppSettings(auth_mode="accounts", auth_db_path=tmp_path / "auth.db", port=8443))
    client = TestClient(app)
    headers = {}
    for name in ("admin", "member"):
        client.post("/api/auth/register", json={"username": name, "password": PASSWORD})
        login = client.post("/api/auth/login", json={"login": name, "password": PASSWORD}).json()
        headers[name] = {"Authorization": f"Bearer {login['access_token']}"}
    client.headers.update(headers["admin"])
    client.member = headers["member"]  # type: ignore[attr-defined]
    return client


def _use(monkeypatch, index) -> FilingCatalog:
    catalog = FilingCatalog(index)
    monkeypatch.setattr(storage_api, "get_filings_db", lambda: str(index))
    monkeypatch.setattr(filings_api, "catalog", catalog)
    return catalog


@pytest.fixture
def legacy(tmp_path, monkeypatch):
    index = create_legacy_filings_db(
        tmp_path / "data" / "filings.db",
        [filing_row("S100A", submitted_at="2019-06-20T09:00"), filing_row("S100B", submitted_at="2020-06-20T09:00")],
    )
    _use(monkeypatch, index)
    return index


@pytest.fixture
def split_catalog(tmp_path, monkeypatch) -> FilingCatalog:
    catalog = _use(monkeypatch, tmp_path / "data" / "filings.db")
    catalog.upsert_filing(filing_row("S100A", submitted_at="2019-06-20T09:00"))
    catalog.upsert_filing(filing_row("S100B", submitted_at="2020-06-20T09:00"))
    return catalog


def test_only_administrators_see_filing_storage(client, legacy):
    assert client.get("/api/admin/filing-storage", headers=client.member).status_code == 403
    assert client.post("/api/admin/filing-storage/split", json={}, headers=client.member).status_code == 403


def test_the_old_layout_shows_the_preflight_and_queues_the_split(client, legacy):
    body = client.get("/api/admin/filing-storage").json()
    assert body["layout"] == "legacy" and body["state"] == "legacy" and body["shards"] == []
    assert [year["shard"] for year in body["preflight"]["years"]] == ["2019", "2020"]
    assert body["preflight"]["needed_bytes"] > 0 and body["jobs"] == []

    # Checks and compaction wait for the split.
    assert client.post("/api/admin/filing-storage/check", json={}).status_code == 409
    assert client.post("/api/admin/filing-storage/compact", json={"shard": "2019"}).status_code == 409

    queued = client.post("/api/admin/filing-storage/split", json={})
    assert queued.status_code == 202 and queued.json()["kind"] == "filings_split"
    again = client.post("/api/admin/filing-storage/split", json={})
    assert again.json()["job_id"] == queued.json()["job_id"]
    jobs = client.get("/api/admin/filing-storage").json()["jobs"]
    assert [job["kind"] for job in jobs] == ["filings_split"]

    assert client.post("/api/admin/filing-storage/split", json={"target_folder": "relative/folder"}).status_code == 400
    assert client.post("/api/admin/filing-storage/split", json={"unknown": 1}).status_code == 422


def test_a_finished_split_waits_for_the_restart(client, legacy):
    splitting.split(legacy)
    body = client.get("/api/admin/filing-storage").json()
    assert body["state"] == "ready" and [year["shard"] for year in body["next"]["years"]] == ["2019", "2020"]
    response = client.post("/api/admin/filing-storage/split", json={})
    assert response.status_code == 409 and "restart the server" in response.json()["detail"]


def test_the_split_layout_lists_years_and_queues_checks_and_compaction(client, split_catalog):
    body = client.get("/api/admin/filing-storage").json()
    assert body["layout"] == "split-v1" and body["state"] == "split" and "preflight" not in body
    assert [(year["shard"], year["filings"], year["state"]) for year in body["shards"]] == [("2019", 1, "active"), ("2020", 1, "active")]
    assert all(year["file_bytes"] > 0 and year["archive_bytes"] > 0 for year in body["shards"])

    check = client.post("/api/admin/filing-storage/check", json={"repair": True, "shard": "2019"})
    assert check.status_code == 202 and check.json()["title"] == "Check and repair filing storage (2019)"
    compact = client.post("/api/admin/filing-storage/compact", json={"shard": "2019"})
    assert compact.status_code == 202 and compact.json()["kind"] == "filing_storage_compact"
    assert client.post("/api/admin/filing-storage/compact", json={"shard": "1999"}).status_code == 404
    assert client.post("/api/admin/filing-storage/compact", json={"shard": "../x"}).status_code == 422
    assert client.post("/api/admin/filing-storage/split", json={}).status_code == 409
    assert client.delete("/api/admin/filing-storage/retired").status_code == 404


def test_the_retired_file_is_shown_and_deleted_by_a_job(client, legacy, monkeypatch):
    splitting.split(legacy, switch=True)
    _use(monkeypatch, legacy)

    body = client.get("/api/admin/filing-storage").json()
    assert body["retired"]["path"].endswith("filings.db.migrated") and body["retired"]["bytes"] > 0
    response = client.delete("/api/admin/filing-storage/retired")
    assert response.status_code == 202 and response.json()["kind"] == "filing_storage_cleanup"


def test_a_missing_year_answers_503_with_its_name(client, split_catalog):
    path = split_catalog.storage.shard_path("2019")
    path.rename(path.with_suffix(".away"))

    response = client.get("/api/filings/S100A/facts")
    assert response.status_code == 503
    assert response.json()["detail"] == "This filing's archive is unavailable: filings_2019.db is missing"
    assert client.get("/api/filings/S100B/facts").status_code == 200
    # The viewer still gets the filing's details from the index, with the reason its contents are missing.
    detail = client.get("/api/filings/S100A").json()
    assert detail["filing"]["doc_id"] == "S100A" and detail["artifacts"] == []
    assert detail["archive_unavailable"] == "This filing's archive is unavailable: filings_2019.db is missing"
    assert client.get("/api/filings/S100B").json()["archive_unavailable"] is None
    body = client.get("/api/admin/filing-storage").json()
    assert {year["shard"]: year["state"] for year in body["shards"]} == {"2019": "missing", "2020": "active"}


def test_downloads_wait_for_the_split(client, legacy):
    response = client.post("/api/filings/acquire", json={"doc_id": "S100NEW"})
    assert response.status_code == 409
    assert response.json()["detail"] == "Filing storage needs to be split first (Admin → Filing storage)"
    # Reads keep working.
    assert client.get("/api/filings/S100A").status_code == 200
