"""Filing downloads and XBRL backfills run as batch jobs on workers."""

from __future__ import annotations

import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.orchestrator.common.db_config as db_config
from src.auth.models import AuthenticatedUser
from src.filings import api as filings_api
from src.filings.acquisition import EdinetAcquisitionError, EdinetDownloadClient
from src.filings.catalog import FilingCatalog
from src.jobs.registry import WorkerRegistry
from src.jobs.service import JobService, get_job_service, set_job_service
from src.jobs.store import JobsStore
from src.jobs.user_api import install_job_error_handlers
from src.worker.context import JobError
from src.worker.handlers import filings as filing_handlers
from src.worker.inline import InlineWorker


def _filing(doc_id: str) -> dict:
    keys = (
        "edinet_code submitter_name period_start period_end submitted_at form_code doc_type_code xbrl_flag csv_flag "
        "archive_path archive_content archive_sha256 archive_size parse_error"
    ).split()
    return {"doc_id": doc_id, "status": "parsed", "created_at": "x", "updated_at": "x", **dict.fromkeys(keys)}


class FakeClient:
    instances: list[FakeClient] = []

    def __init__(self, failing: set[str] | None = None) -> None:
        self.failing = failing or set()
        self.acquired: list[tuple[str, dict]] = []
        self.closed = False
        FakeClient.instances.append(self)

    def acquire_type1(self, doc_id, catalog, metadata=None, **_kwargs):
        if doc_id in self.failing:
            raise EdinetAcquisitionError("EDINET returned HTTP 404")
        self.acquired.append((doc_id, dict(metadata or {})))
        catalog.upsert_filing(_filing(doc_id))
        return 7

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def setup(tmp_path, monkeypatch):
    catalog = FilingCatalog(tmp_path / "Filings.db")
    market = tmp_path / "market.db"
    with sqlite3.connect(market) as conn:
        conn.execute(
            "CREATE TABLE DocumentList (docID TEXT, edinetCode TEXT, submitDateTime TEXT, periodStart TEXT, periodEnd TEXT,"
            " formCode TEXT, docDescription TEXT, filerName TEXT, xbrlFlag TEXT, Downloaded TEXT, legalStatus TEXT, docTypeCode TEXT)"
        )
        rows = [
            ("S100AAAA", "E00001", "2026-03-01", "2025-04-01", "2026-03-31", "030000", "", "A", "1", "True", "1", "120"),
            ("S100BBBB", "E00002", "2026-02-01", "2025-04-01", "2026-03-31", "030000", "", "B", "1", "True", "1", "120"),
            ("S100CCCC", "E00003", "2026-01-01", "2025-04-01", "2026-03-31", "030000", "", "C", "1", "True", "1", "120"),
            ("S100DDDD", "E00004", "2026-01-01", "2025-04-01", "2026-03-31", "043000", "", "D", "1", "True", "1", "140"),
        ]
        conn.executemany("INSERT INTO DocumentList VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    catalog.upsert_filing(_filing("S100BBBB"))
    FakeClient.instances = []
    monkeypatch.setattr(filings_api, "catalog", catalog)
    monkeypatch.setattr(db_config, "get_filings_db", lambda: str(catalog.path))
    monkeypatch.setattr(db_config, "get_market_db", lambda: str(market))
    monkeypatch.setattr(filings_api, "get_market_db", lambda: str(market))
    return catalog


@pytest.fixture
def service(tmp_path):
    registry = WorkerRegistry(tmp_path / "app.db")
    registry.ensure_local_worker()
    svc = JobService(JobsStore(tmp_path / "jobs.db"), registry)
    set_job_service(svc)
    yield svc
    set_job_service(None)


def _app(role: str) -> FastAPI:
    app = FastAPI()
    app.include_router(filings_api.router)
    install_job_error_handlers(app)

    @app.middleware("http")
    async def sign_in(request, call_next):
        request.state.user = AuthenticatedUser("u1", "tester", None, role, "active")
        return await call_next(request)

    return app


@pytest.fixture
def http(setup, service):
    return TestClient(_app("admin"))


def test_acquire_returns_202_at_once_and_the_job_completes(http, setup, service, monkeypatch):
    monkeypatch.setattr(EdinetDownloadClient, "from_settings", classmethod(lambda cls: FakeClient()))
    response = http.post("/api/filings/acquire", json={"doc_id": "S100NEW1", "edinet_code": "E00009"})

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "open" and body["job_id"]
    assert service.get(body["job_id"]).kind == "filing_acquire"
    assert service.get(body["job_id"]).owner_user_id == "u1"
    with InlineWorker(get_job_service()):
        done = service.wait_sync(body["job_id"], 20)
    assert done.status == "completed"
    assert done.result == {"doc_id": "S100NEW1", "status": "parsed", "fact_count": 7}
    assert FakeClient.instances[0].acquired[0][1]["edinet_code"] == "E00009"
    assert FakeClient.instances[0].closed
    assert setup.get_filing("S100NEW1") is not None


def test_acquire_failure_is_the_jobs_error(http, service, monkeypatch):
    monkeypatch.setattr(EdinetDownloadClient, "from_settings", classmethod(lambda cls: FakeClient({"S100NEW1"})))
    job_id = http.post("/api/filings/acquire", json={"doc_id": "S100NEW1"}).json()["job_id"]
    with InlineWorker(get_job_service()):
        done = service.wait_sync(job_id, 20)
    assert done.status == "failed" and "HTTP 404" in (done.error_message or "")


def test_acquire_needs_an_operator_and_valid_input(setup, service):
    member = TestClient(_app("member"))
    assert member.post("/api/filings/acquire", json={"doc_id": "S100NEW1"}).status_code == 403
    admin = TestClient(_app("admin"))
    assert admin.post("/api/filings/acquire", json={"doc_id": "bad id!"}).status_code == 422
    assert service.list_jobs(kinds=["filing_acquire"], include_system=True) == []


def test_backfill_queues_a_job_with_the_summary_as_result(http, service, monkeypatch):
    monkeypatch.setattr(EdinetDownloadClient, "from_settings", classmethod(lambda cls: FakeClient({"S100CCCC"})))
    response = http.post("/api/filings/xbrl-backfill", params={"limit": 10, "doc_type_code": "120"})

    assert response.status_code == 202
    job_id = response.json()["job_id"]
    assert service.get(job_id).kind == "xbrl_backfill"
    assert service.get(job_id).payload == {"limit": 10, "doc_type_code": "120"}
    with InlineWorker(get_job_service()):
        done = service.wait_sync(job_id, 20)
    assert done.status == "completed"
    assert done.result == {"downloaded": 1, "skipped": 1, "failed": 1, "total": 3}
    assert [doc for doc, _ in FakeClient.instances[0].acquired] == ["S100AAAA"]


def test_backfill_without_market_data_answers_the_empty_summary(http, tmp_path, monkeypatch):
    monkeypatch.setattr(filings_api, "get_market_db", lambda: str(tmp_path / "missing.db"))
    response = http.post("/api/filings/xbrl-backfill")
    assert response.status_code == 202 and response.json() == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}


def test_backfill_handler_reports_progress_and_checks_for_cancel(setup, monkeypatch):
    monkeypatch.setattr(EdinetDownloadClient, "from_settings", classmethod(lambda cls: FakeClient()))

    class Context:
        payload = {"limit": 50, "doc_type_code": ""}
        checkpoints = 0
        progress: list = []

        def checkpoint(self):
            Context.checkpoints += 1

        def report_progress(self, percent=None, message=None, progress=None):
            Context.progress.append(percent)

    result = filing_handlers.xbrl_backfill(Context())

    assert result["total"] == 4 and Context.checkpoints == 4 and Context.progress[-1] == 100.0


def test_missing_api_key_is_a_job_error(setup, monkeypatch):
    def refuse(cls):
        raise ValueError("EDINET provider token is required for acquisition")

    monkeypatch.setattr(EdinetDownloadClient, "from_settings", classmethod(refuse))

    class Context:
        payload = {"doc_id": "S100NEW1"}

        def report_progress(self, *args, **kwargs):
            pass

    with pytest.raises(JobError, match="token is required"):
        filing_handlers.acquire(Context())
