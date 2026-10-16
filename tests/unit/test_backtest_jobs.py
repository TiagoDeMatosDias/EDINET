"""Backtests as jobs: the worker handlers, the job routes, and the finalizers."""

from __future__ import annotations

import json
import threading
import time
import zipfile
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.backtesting.api as backtesting_api
from src.backtesting import store as bt_store
from src.jobs.service import get_job_service
from src.jobs.user_api import install_job_error_handlers
from src.web_app.security import AppSettings, install_security
from src.worker.context import JobCancelledError, JobContext, JobError
from src.worker.handlers import backtest as handlers
from src.worker.inline import InlineWorker

PASSWORD = "correct horse battery staple"
SINGLE_REQUEST = {
    "portfolio": {"AAA": {"mode": "weight", "value": 0.6}, "BBB": {"mode": "weight", "value": 0.4}},
    "start_date": "2020-01-01",
    "end_date": "2023-01-01",
    "initial_capital": 50_000,
}
CSV_REQUEST = {
    "csv_content": "# Benchmark: BENCH\nYear,Tickers,Type,Amount\n2020,AAA,weight,0.6\n2020,BBB,weight,0.4",
    "durations": ["1yr", "2yr"],
}
ROLLING_REQUEST = {"criteria": [{"column": "x"}], "columns": ["x"], "cadence": "yearly", "durations": ["1yr"]}


def _context(tmp_path: Path, payload: dict[str, Any], events: list | None = None, kind: str = "backtest_single") -> JobContext:
    output = tmp_path / "out"
    output.mkdir(exist_ok=True)
    sink = (lambda event, data: events.append((event, data))) if events is not None else None
    return JobContext(
        job_id="job", kind=kind, attempt=1, payload=payload, workspace=tmp_path, output_dir=output,
        cancel_event=threading.Event(), sink=sink,
    )


def _single_payload(**overrides: Any) -> dict[str, Any]:
    payload = {
        "request": SINGLE_REQUEST, "base_currency": "", "risk_free_rate": 0.02,
        "portfolio_owner": "", "portfolio_daily": None,
    }
    payload.update(overrides)
    return payload


# --- handlers ---------------------------------------------------------------


def test_single_handler_writes_the_artifacts_the_route_used_to_save(tmp_path):
    context = _context(tmp_path, _single_payload())
    result = handlers.run_single(context)

    files = {path.name for path in context.output_dir.iterdir()}
    assert {"result.json", "backtest.zip"} <= files
    stored = json.loads((context.output_dir / "result.json").read_text(encoding="utf-8"))
    assert stored["metrics"]["initial_capital"] == 50_000
    assert set(result["metrics"]) == {"total_return", "annualized_return", "excess_return"}
    assert result["metrics"]["total_return"] == stored["metrics"]["total_return"]
    with zipfile.ZipFile(context.output_dir / "backtest.zip") as archive:
        assert "report.txt" in archive.namelist()


def test_single_handler_reads_the_portfolio_benchmark_from_the_payload(tmp_path):
    rows = [
        {"date": f"{year}-{month:02d}-15", "total_value": 1000.0 + 10 * index, "net_inflow": 0.0}
        for index, (year, month) in enumerate((y, m) for y in (2020, 2021, 2022) for m in range(1, 13))
    ]
    payload = _single_payload(
        request={**SINGLE_REQUEST, "benchmark_mode": "portfolio"},
        base_currency="EUR", portfolio_owner="owner-1", portfolio_daily=rows,
    )
    context = _context(tmp_path, payload)
    handlers.run_single(context)
    stored = json.loads((context.output_dir / "result.json").read_text(encoding="utf-8"))
    assert stored["metrics"]["benchmark_total_return"] is not None
    assert not list(tmp_path.glob("portfolio-*"))  # the temporary database is removed


def test_single_handler_reports_a_bad_request_as_a_job_error(tmp_path):
    payload = _single_payload(request={**SINGLE_REQUEST, "portfolio": {}})
    with pytest.raises(JobError):
        handlers.run_single(_context(tmp_path, payload))


def test_single_handler_refuses_an_oversized_artifact(tmp_path, monkeypatch):
    monkeypatch.setattr(handlers, "_artifact_limit", lambda _context: 10)
    with pytest.raises(JobError, match="size limit"):
        handlers.run_single(_context(tmp_path, _single_payload()))


def test_csv_handler_writes_result_and_archive(tmp_path):
    payload = {"request": CSV_REQUEST, "base_currency": "", "risk_free_rate": 0.02, "portfolio_owner": "",
               "portfolio_daily": None, "backtest_id": "20260101_000000_abcd1234"}
    context = _context(tmp_path, payload, kind="backtest_csv")
    result = handlers.run_csv(context)
    stored = json.loads((context.output_dir / "result.json").read_text(encoding="utf-8"))
    assert stored["aggregate"]["total_runs"] == 2
    assert result["aggregate"]["total_runs"] == 2
    with zipfile.ZipFile(context.output_dir / "backtest.zip") as archive:
        assert {"result.json", "summary.json", "report.html"} <= set(archive.namelist())
        assert "20260101_000000_abcd1234" in archive.read("report.html").decode("utf-8")


def _fake_rolling(monkeypatch, seen: dict[str, Any], *, periods: int = 3):
    def run(**kwargs):
        seen.update(kwargs)
        for index in range(periods):
            if kwargs["cancel_event"].is_set():
                raise RuntimeError("cancelled")
            kwargs["progress_queue"].put({
                "type": "progress", "period": f"2020-0{index + 1}-01", "period_index": index,
                "total_periods": periods, "status": "complete", "completed_backtests": index + 1,
                "total_backtests": periods, "phase": f"Completed {index + 1}/{periods}",
            })
            kwargs["on_period"](index) if "on_period" in kwargs else None
        return {
            "aggregate": {"complete_backtests": periods, "stats": {"total_return": {"mean": 0.07}},
                          "benchmark_comparison": {"win_rate": 0.5}},
            "config": {"cadence": "yearly"},
            "runs": [{"period": "2020-01-01", "weighting": "equal", "duration": "1yr"}],
            "paths": {}, "period_holdings": {"2020-01-01": ["AAA"]}, "results": [],
        }

    monkeypatch.setattr(backtesting_api._bt, "run_screening_backtest_rolling", run)
    monkeypatch.setattr(
        "src.backtesting.zip_export.save_rolling_backtest_zip",
        lambda _result, base_dir, _limit: _fake_archive(base_dir),
    )


def _fake_archive(base_dir: str) -> str:
    saved = Path(base_dir) / "20260723_120000_1234abcd"
    saved.mkdir()
    (saved / "backtest.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)
    return str(saved)


def _rolling_payload(**overrides: Any) -> dict[str, Any]:
    payload = {"request": ROLLING_REQUEST, "base_currency": "", "risk_free_rate": 0.02, "portfolio_owner": "u1",
               "portfolio_daily": None, "company_tags": [["E00001", "Core"], ["E00002", "Core"]]}
    payload.update(overrides)
    return payload


def test_rolling_handler_forwards_progress_tags_and_builds_the_archive(tmp_path, monkeypatch):
    seen: dict[str, Any] = {}
    _fake_rolling(monkeypatch, seen)
    events: list = []
    context = _context(tmp_path, _rolling_payload(), events, kind="backtest_rolling")
    result = handlers.run_rolling(context)

    assert seen["company_tags"] == [("E00001", "Core"), ("E00002", "Core")]
    assert seen["portfolio_owner"] == "u1"
    progress = [data for event, data in events if event == "progress" and data["progress"].get("period")]
    assert [item["progress"]["completed_backtests"] for item in progress] == [1, 2, 3]
    assert progress[-1]["percent"] == 100.0
    assert "type" not in progress[0]["progress"]
    assert {path.name for path in context.output_dir.iterdir()} == {"result.json", "backtest.zip"}
    assert result["aggregate"]["complete_backtests"] == 3
    assert result["config"] == {"cadence": "yearly"}


def test_rolling_handler_maps_a_cancel_to_the_runners_cancel(tmp_path, monkeypatch):
    seen: dict[str, Any] = {}
    _fake_rolling(monkeypatch, seen)
    context = _context(tmp_path, _rolling_payload(), kind="backtest_rolling")
    context.cancel_event.set()
    with pytest.raises(JobCancelledError):
        handlers.run_rolling(context)
    assert seen["cancel_event"] is context.cancel_event


def test_rolling_handler_fails_with_the_archive_limit_message(tmp_path, monkeypatch):
    from src.backtesting.zip_export import ExportSizeLimitExceeded

    _fake_rolling(monkeypatch, {})

    def reject(_result, _base_dir, max_bytes):
        raise ExportSizeLimitExceeded(max_bytes, max_bytes + 1)

    monkeypatch.setattr("src.backtesting.zip_export.save_rolling_backtest_zip", reject)
    monkeypatch.setattr(handlers, "_artifact_limit", lambda _context: 3 * 1024 * 1024)
    with pytest.raises(JobError, match="exceeded the 3 MiB limit"):
        handlers.run_rolling(_context(tmp_path, _rolling_payload(), kind="backtest_rolling"))


# --- routes -----------------------------------------------------------------


@pytest.fixture
def worker():
    with InlineWorker(get_job_service()) as inline:
        yield inline


@pytest.fixture
def client() -> TestClient:
    from src.web_app.server import app

    return TestClient(app)


def _wait_view(client: TestClient, job_id: str, headers: dict[str, str] | None = None) -> dict:
    for _ in range(300):
        view = client.get(f"/api/backtesting/jobs/{job_id}", headers=headers).json()
        if view["status"] in ("complete", "failed", "cancelled"):
            return view
        time.sleep(0.02)
    raise AssertionError("job did not finish")


@pytest.mark.parametrize(("kind", "request_body"), [("single", SINGLE_REQUEST), ("csv", CSV_REQUEST)])
def test_jobs_run_and_their_result_opens_through_the_result_route(worker, client, kind, request_body):
    started = client.post("/api/backtesting/jobs", json={"kind": kind, "request": request_body})
    assert started.status_code == 202
    assert started.json()["kind"] == kind
    view = _wait_view(client, started.json()["job_id"])
    assert view["status"] == "complete", view
    assert view["error"] is None and view["waiting_for_worker"] is False
    assert isinstance(view["created_at"], float)

    result = client.get(f"/api/backtesting/result/{view['result_id']}")
    assert result.status_code == 200
    if kind == "single":
        assert result.json()["chart_data"]["cumulative"]
    else:
        assert result.json()["aggregate"]["total_runs"] == 2
    listed = {item["id"]: item for item in client.get("/api/backtesting/list").json()["backtests"]}
    assert listed[view["result_id"]]["kind"] == kind
    assert client.get(f"/api/backtesting/download/{view['result_id']}").status_code == 200


def test_run_returns_the_body_it_always_did(worker, client):
    body = client.post("/api/backtesting/run", json=SINGLE_REQUEST).json()
    assert set(body) == {"id", "status", "path", "summary", "chart_data", "per_company", "yearly_returns",
                         "dividends_by_year", "names", "warnings"}
    assert body["path"] == body["id"] and body["status"] == "complete"
    stored = client.get(f"/api/backtesting/result/{body['id']}").json()
    assert stored["summary"] == body["summary"]
    assert (bt_store.BACKTEST_ROOT / body["id"] / "per_company_per_day.csv").is_file()


def test_run_from_csv_returns_the_body_it_always_did(worker, client):
    body = client.post("/api/backtesting/run-from-csv", json=CSV_REQUEST).json()
    assert set(body) == {"id", "status", "path", "kind", "aggregate", "runs", "paths", "period_holdings",
                         "run_holdings", "names"}
    assert body["kind"] == "csv"


def test_a_bad_request_still_answers_400(worker, client):
    response = client.post("/api/backtesting/run", json={**SINGLE_REQUEST, "portfolio": {}})
    assert response.status_code == 400


def test_run_answers_202_job_pending_when_no_worker_is_running(client):
    response = client.post("/api/backtesting/run", json=SINGLE_REQUEST)
    assert response.status_code == 202
    body = response.json()
    assert body["code"] == "job_pending" and body["waiting_for_worker"] is True
    view = client.get(f"/api/backtesting/jobs/{body['job_id']}").json()
    assert view["status"] == "queued" and view["waiting_for_worker"] is True
    assert client.post(f"/api/backtesting/jobs/{body['job_id']}/cancel").json()["status"] == "cancelled"


def test_several_jobs_can_be_queued_and_all_complete(client):
    ids = [
        client.post("/api/backtesting/jobs", json={"kind": "single", "request": SINGLE_REQUEST}).json()["job_id"]
        for _ in range(2)
    ]
    listed = client.get("/api/backtesting/jobs").json()["jobs"]
    assert {item["job_id"] for item in listed} >= set(ids)
    assert all(item["status"] == "queued" for item in listed if item["job_id"] in ids)
    with InlineWorker(get_job_service()):
        views = [_wait_view(client, job_id) for job_id in ids]
    assert [view["status"] for view in views] == ["complete", "complete"]
    assert views[0]["result_id"] != views[1]["result_id"]


def test_a_rolling_job_reports_progress_and_can_be_cancelled(monkeypatch, client):
    started = threading.Event()

    def slow(**kwargs):
        kwargs["progress_queue"].put({"type": "progress", "period": "2020-01-01", "period_index": 0, "total_periods": 4,
                                      "status": "screening", "completed_backtests": 0, "total_backtests": 4,
                                      "phase": "Screening at 2020-01-01"})
        started.set()
        for _ in range(500):
            if kwargs["cancel_event"].is_set():
                raise RuntimeError("cancelled")
            time.sleep(0.01)
        raise AssertionError("not cancelled")

    monkeypatch.setattr(backtesting_api._bt, "run_screening_backtest_rolling", slow)
    with InlineWorker(get_job_service()):
        job_id = client.post("/api/backtesting/jobs", json={"kind": "rolling", "request": ROLLING_REQUEST}).json()["job_id"]
        assert started.wait(5)
        view = client.get(f"/api/backtesting/rolling-jobs/{job_id}").json()
        assert view["status"] == "running"
        assert view["progress"]["total_backtests"] == 4 and view["progress"]["phase"] == "Screening at 2020-01-01"
        client.post(f"/api/backtesting/rolling-jobs/{job_id}/cancel")
        assert _wait_view(client, job_id)["status"] == "cancelled"
    # The alias is restricted to rolling jobs.
    single = client.post("/api/backtesting/jobs", json={"kind": "single", "request": SINGLE_REQUEST}).json()["job_id"]
    assert client.get(f"/api/backtesting/rolling-jobs/{single}").status_code == 404
    client.post(f"/api/backtesting/jobs/{single}/cancel")


def test_run_rolling_streams_the_same_events_as_before(monkeypatch, worker, client):
    _fake_rolling(monkeypatch, {})
    response = client.post("/api/backtesting/run-rolling", json=ROLLING_REQUEST)
    events = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]
    assert events[-1]["type"] == "result" and events[-1]["id"] == events[-1]["path"]
    assert events[-1]["config"] == {"cadence": "yearly"}


def test_job_validation_happens_before_queueing(client):
    assert client.post("/api/backtesting/jobs", json={"kind": "single", "request": {"portfolio": {}}}).status_code == 422
    assert client.post("/api/backtesting/jobs", json={"kind": "bogus", "request": {}}).status_code == 422
    assert client.post("/api/backtesting/jobs", json={"kind": "csv", "request": {"csv_content": " "}}).status_code == 400
    bad_currency = {"kind": "single", "request": {**SINGLE_REQUEST, "base_currency": "ZZZ"}}
    assert client.post("/api/backtesting/jobs", json=bad_currency).status_code == 400


@pytest.fixture
def accounts(tmp_path):
    app = FastAPI()
    app.include_router(backtesting_api.router)
    install_security(app, AppSettings(auth_mode="accounts", registration_mode="open", auth_db_path=tmp_path / "auth.db"))
    install_job_error_handlers(app)
    service = app.state.auth_service
    service.register("admin-user", PASSWORD)
    service.register("alice", PASSWORD)
    service.register("bob", PASSWORD)

    def headers(username: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {service.login(username, PASSWORD).tokens.access_token}"}

    return TestClient(app), headers


def test_an_account_sees_and_cancels_only_its_own_jobs(accounts):
    client, headers = accounts
    started = client.post("/api/backtesting/jobs", json={"kind": "single", "request": SINGLE_REQUEST}, headers=headers("alice"))
    job_id = started.json()["job_id"]
    assert [job["job_id"] for job in client.get("/api/backtesting/jobs", headers=headers("alice")).json()["jobs"]] == [job_id]
    assert client.get("/api/backtesting/jobs", headers=headers("bob")).json() == {"jobs": []}
    assert client.get(f"/api/backtesting/jobs/{job_id}", headers=headers("bob")).status_code == 404
    assert client.get(f"/api/backtesting/jobs/{job_id}", headers=headers("admin-user")).status_code == 404
    assert client.post(f"/api/backtesting/jobs/{job_id}/cancel", headers=headers("bob")).status_code == 404

    with InlineWorker(get_job_service()):
        view = _wait_view(client, job_id, headers("alice"))
    assert view["status"] == "complete"
    assert client.get(f"/api/backtesting/result/{view['result_id']}", headers=headers("alice")).status_code == 200
    assert client.get(f"/api/backtesting/result/{view['result_id']}", headers=headers("bob")).status_code == 404


def test_a_portfolio_benchmark_job_carries_the_accounts_daily_values(accounts, monkeypatch):
    client, headers = accounts
    monkeypatch.setattr(
        backtesting_api, "_portfolio_daily",
        lambda owner: [{"date": "2020-01-02", "total_value": 10.0, "net_inflow": 0.0}] if owner else [],
    )
    request = {**SINGLE_REQUEST, "benchmark_mode": "portfolio"}
    job_id = client.post("/api/backtesting/jobs", json={"kind": "single", "request": request}, headers=headers("alice")).json()["job_id"]
    payload = get_job_service().get(job_id).payload
    assert payload["portfolio_daily"] == [{"date": "2020-01-02", "total_value": 10.0, "net_inflow": 0.0}]
    assert payload["base_currency"] == "EUR"
    assert payload["portfolio_owner"]
    client.post(f"/api/backtesting/jobs/{job_id}/cancel", headers=headers("alice"))


def test_a_rolling_job_carries_the_accounts_tags(accounts, monkeypatch):
    client, headers = accounts
    from src.research.runtime import store as research_store

    monkeypatch.setattr(
        research_store, "list_all_company_tags",
        lambda _owner: [{"company_code": "E00001", "tag": "Core"}, {"edinet_code": "E00002", "tag": "Core"}, {"tag": "x"}],
    )
    job_id = client.post("/api/backtesting/jobs", json={"kind": "rolling", "request": ROLLING_REQUEST}, headers=headers("alice")).json()["job_id"]
    assert get_job_service().get(job_id).payload["company_tags"] == [["E00001", "Core"], ["E00002", "Core"]]
    client.post(f"/api/backtesting/jobs/{job_id}/cancel", headers=headers("alice"))
