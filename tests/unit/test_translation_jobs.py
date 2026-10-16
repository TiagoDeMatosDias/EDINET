"""Translation on job workers: the server reads the cache, a worker runs the engine."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.orchestrator.common.db_config as db_config
from src.filings import api as filings_api
from src.filings import translate as translation
from src.filings.catalog import FilingCatalog
from src.jobs.registry import WorkerRegistry
from src.jobs.service import JobService, get_job_service, set_job_service
from src.jobs.store import JobsStore
from src.jobs.user_api import install_job_error_handlers
from src.orchestrator.common.sqlite import clear_protected_database_paths, protect_database_paths
from src.ownership import api as ownership_api
from src.worker.context import JobError
from src.worker.handlers import translation as translation_handler
from src.worker.inline import InlineWorker

SENTENCE = "当社は成長しています。"
HTML = f"<html><body><p>{SENTENCE}</p><p>円</p></body></html>"


class FakeEngine:
    def __init__(self, monkeypatch) -> None:
        self.calls: list[str] = []
        monkeypatch.setattr(translation, "_try_load_argos", lambda **_kwargs: True)
        monkeypatch.setattr(translation, "_argos_translate", self.translate)

    def translate(self, text: str) -> str:
        self.calls.append(text)
        return "The company is growing."


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    cat = FilingCatalog(tmp_path / "Filings.db")
    monkeypatch.setattr(filings_api, "catalog", cat)
    monkeypatch.setattr("src.filings.runtime.catalog", cat)
    monkeypatch.setattr(db_config, "get_filings_db", lambda: str(cat.path))
    yield cat
    clear_protected_database_paths()


@pytest.fixture
def service(tmp_path):
    registry = WorkerRegistry(tmp_path / "app.db")
    registry.ensure_local_worker()
    svc = JobService(JobsStore(tmp_path / "jobs.db"), registry)
    set_job_service(svc)
    yield svc
    set_job_service(None)


@pytest.fixture
def http(catalog, service):
    app = FastAPI()
    app.include_router(filings_api.router)
    app.include_router(ownership_api.router)
    install_job_error_handlers(app)
    return TestClient(app)


def _job_count(service) -> int:
    return len(service.list_jobs(kinds=["translation"], include_system=True))


class _Context:
    def __init__(self, payload):
        self.payload = payload
        self.progress: list = []
        self.checkpoints = 0

    def checkpoint(self):
        self.checkpoints += 1

    def report_progress(self, percent=None, message=None, progress=None):
        self.progress.append((percent, message))


def test_handler_translates_stores_and_returns(catalog, monkeypatch):
    engine = FakeEngine(monkeypatch)
    context = _Context({"texts": [SENTENCE], "force": False})

    result = translation_handler.run(context)

    assert result == {"translations": {SENTENCE: "The company is growing."}}
    assert catalog.lookup_translations([SENTENCE], version=translation.TRANSLATOR_VERSION) == {SENTENCE: "The company is growing."}
    assert context.checkpoints >= 1 and context.progress[-1][0] == 100.0
    assert engine.calls


def test_handler_reports_an_unavailable_engine(catalog, monkeypatch):
    monkeypatch.setattr(translation, "_try_load_argos", lambda **_kwargs: False)

    with pytest.raises(JobError) as error:
        translation_handler.run(_Context({"texts": [SENTENCE]}))

    assert "unavailable" in error.value.message and error.value.retryable is False


def test_translate_endpoint_runs_a_job_then_answers_from_the_cache(http, service, monkeypatch):
    engine = FakeEngine(monkeypatch)
    with InlineWorker(get_job_service()):
        first = http.post("/api/filings/translate", json={"texts": [SENTENCE, "円", "English"]})
        assert first.status_code == 200
        body = {item["source"]: item["translated"] for item in first.json()["translations"]}
        assert body == {SENTENCE: "The company is growing.", "円": "yen", "English": "English"}
        assert _job_count(service) == 1 and len(engine.calls) == 1

        again = http.post("/api/filings/translate", json={"texts": [SENTENCE, "円"]})
        assert again.status_code == 200
        assert _job_count(service) == 1  # fully cached: no new job


def test_translate_force_retranslates_through_a_job(http, service, monkeypatch):
    engine = FakeEngine(monkeypatch)
    with InlineWorker(get_job_service()):
        http.post("/api/filings/translate", json={"texts": [SENTENCE]})
        http.post("/api/filings/translate", json={"texts": [SENTENCE], "force": True})
    assert _job_count(service) == 2 and len(engine.calls) == 2


def test_translate_without_a_worker_answers_202(http, service):
    response = http.post("/api/filings/translate", json={"texts": [SENTENCE]})

    assert response.status_code == 202
    assert response.json()["code"] == "job_pending" and response.json()["waiting_for_worker"] is True


def test_glossary_labels_need_no_job(http, service):
    response = http.post("/api/filings/translate", json={"texts": ["円", "資産"]})

    assert response.status_code == 200 and _job_count(service) == 0


def test_engine_failure_is_a_502(http, service, monkeypatch):
    monkeypatch.setattr(translation, "_try_load_argos", lambda **_kwargs: False)
    with InlineWorker(get_job_service()):
        response = http.post("/api/filings/translate", json={"texts": [SENTENCE]})
    assert response.status_code == 502 and "unavailable" in response.json()["detail"]


def test_sections_and_body_and_html_translation(http, catalog, service, monkeypatch):
    FakeEngine(monkeypatch)
    sections = [{"section_id": "s1", "doc_id": "S100TEST", "title": "概要", "text": SENTENCE, "ordinal": 1}]
    monkeypatch.setattr(catalog, "get_filing", lambda _doc: {"doc_id": "S100TEST"})
    monkeypatch.setattr(catalog, "list_sections", lambda _doc, query=None, limit=200: sections)
    monkeypatch.setattr(catalog, "get_section", lambda _doc, _sid: sections[0])
    monkeypatch.setattr(catalog, "list_artifacts", lambda _doc: [{"artifact_id": "a1", "member_path": "PublicDoc/r.htm"}])
    monkeypatch.setattr(catalog, "get_artifact_content", lambda _id, _doc_id=None: {"content": HTML.encode()})
    with InlineWorker(get_job_service()):
        listed = http.get("/api/filings/S100TEST/sections-translated", params={"bodies": "true"}).json()
        body = http.get("/api/filings/S100TEST/translate-body", params={"section_id": "s1"}).json()
        html = http.get("/api/filings/S100TEST/html/a1", params={"translate": "true"}).json()

    assert listed["sections"][0]["text_en"] == "The company is growing."
    assert listed["sections"][0]["title_en"] == "Overview"
    assert body["section"]["text_en"] == "The company is growing."
    assert "The company is growing." in html["html_en"] and "yen" in html["html_en"]
    assert html["translation"]["status"] == "complete"


def test_career_translation_uses_the_worker(http, service, monkeypatch, tmp_path):
    FakeEngine(monkeypatch)
    market = tmp_path / "market.db"
    market.write_bytes(b"")
    monkeypatch.setattr(ownership_api, "_database", lambda: str(market))
    monkeypatch.setattr(ownership_api.service, "career_english", lambda _path, lines, company, on, translate: translate(lines))
    with InlineWorker(get_job_service()):
        done = http.post("/api/ownership/careers/translate", json={"lines": [SENTENCE]})
    assert done.status_code == 200
    pending = http.post("/api/ownership/careers/translate", json={"lines": ["別の文章です。"]})
    assert pending.status_code == 202


def test_read_endpoints_work_with_filings_db_protected(http, catalog, monkeypatch):
    catalog.store_translations({SENTENCE: "The company is growing.", "円": "yen"}, version=translation.TRANSLATOR_VERSION)
    protect_database_paths([catalog.path])
    monkeypatch.setattr(catalog, "get_filing", lambda _doc: {"doc_id": "S100TEST"})
    monkeypatch.setattr(catalog, "list_artifacts", lambda _doc: [{"artifact_id": "a1", "member_path": "PublicDoc/r.htm"}])
    monkeypatch.setattr(catalog, "get_artifact_content", lambda _id, _doc_id=None: {"content": HTML.encode()})

    assert http.get("/api/filings").status_code == 200
    assert http.get("/api/filings/coverage").status_code == 200
    plain = http.get("/api/filings/S100TEST/html/a1").json()
    assert SENTENCE in plain["html"]
    translated = http.get("/api/filings/S100TEST/html/a1", params={"translate": "true"})
    assert translated.status_code == 200 and "The company is growing." in translated.json()["html_en"]
    listing = http.post("/api/filings/translate", json={"texts": [SENTENCE]})
    assert listing.status_code == 200
    assert FilingCatalog(catalog.path).lookup_translations([SENTENCE], version=translation.TRANSLATOR_VERSION)


def test_server_imports_no_translation_engine(tmp_path):
    script = textwrap.dedent(
        """
        import sys
        import src.web_app.server  # noqa: F401
        heavy = ("argostranslate", "torch", "ctranslate2", "sentencepiece", "stanza", "spacy")
        loaded = sorted(name for name in heavy if name in sys.modules)
        print("LOADED:" + ",".join(loaded))
        """
    )
    # A fresh interpreter with a clean environment, so earlier tests (which may import the
    # engine or leave server settings in os.environ) cannot affect the result.
    env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "SYSTEMROOT", "LANG"}}
    env["EDINET_DATA_DIR"] = str(tmp_path / "data")
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=300, check=False, env=env)
    assert "LOADED:" in result.stdout, result.stderr[-2000:]
    assert result.stdout.strip().splitlines()[-1] == "LOADED:", result.stdout
