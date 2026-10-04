"""Rolling backtests run as background jobs that only their account can see."""

from __future__ import annotations

import sqlite3
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.backtesting.api as backtesting_api
from src.backtesting.jobs import RollingJobs
from src.web_app.security import AppSettings, install_security

PASSWORD = "correct horse battery staple"


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    root = tmp_path / "backtests"
    monkeypatch.setattr(backtesting_api, "_BACKTEST_ROOT", root)
    monkeypatch.setattr(backtesting_api, "_rolling_jobs", RollingJobs(slots=1))
    market = tmp_path / "market.db"
    conn = sqlite3.connect(market)
    conn.execute("CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Currency TEXT, Price REAL)")
    conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", [
        ("2015-01-05", "TPX", "JPY", 1400.0), ("2026-05-15", "TPX", "JPY", 2900.0), ("2020-01-06", "SPY", "USD", 0.0),
    ])
    conn.commit()
    conn.close()
    monkeypatch.setattr(backtesting_api, "_resolve_db", lambda: str(market))
    monkeypatch.setattr(backtesting_api, "_validate_base_currency", lambda currency: currency)
    monkeypatch.setattr(backtesting_api, "_resolve_risk_free_rate", lambda _rate, _currency: 0.02)

    seen: dict = {}

    def run_rolling(**kwargs):
        seen.update(kwargs)
        kwargs["progress_queue"].put({"type": "progress", "completed": 1, "total": 2, "period": "2020-01-01"})
        return {
            "aggregate": {"complete_backtests": 2, "stats": {"total_return": {"mean": 0.07}}, "benchmark_comparison": {"win_rate": 0.5}},
            "config": {"cadence": "yearly"},
            "runs": [{"period": "2020-01-01", "weighting": "equal", "duration": "1yr", "status": "ok", "annualized_return": 0.07}],
            "paths": {"2020-01-01|equal|1yr": [["2020-01-31", 0.01, 0.0]]},
            "period_holdings": {"2020-01-01": ["7203"]},
            "results": [],
        }

    monkeypatch.setattr(backtesting_api._bt, "run_screening_backtest_rolling", run_rolling)
    monkeypatch.setattr(backtesting_api, "save_rolling_backtest_zip", lambda _result, base_dir, _limit: _new_dir(base_dir))

    app = FastAPI()
    app.include_router(backtesting_api.router)
    install_security(app, AppSettings(auth_mode="accounts", registration_mode="open", auth_db_path=tmp_path / "auth.db"))
    service = app.state.auth_service
    service.register("admin-user", PASSWORD)
    alice = service.register("alice", PASSWORD)
    service.register("bob", PASSWORD)

    def headers(username: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {service.login(username, PASSWORD).tokens.access_token}"}

    return TestClient(app), headers, seen, alice


def _new_dir(base_dir: str) -> str:
    from pathlib import Path

    directory = Path(base_dir) / backtesting_api._new_backtest_id()
    directory.mkdir(parents=True)
    (directory / "backtest.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)
    return str(directory)


def _wait(client, job_id, headers):
    for _ in range(100):
        job = client.get(f"/api/backtesting/rolling-jobs/{job_id}", headers=headers).json()
        if job["status"] in ("complete", "failed", "cancelled"):
            return job
        time.sleep(0.02)
    raise AssertionError("job did not finish")


def test_a_rolling_job_runs_in_the_background_and_its_result_carries_the_run_rows(app_client):
    client, headers, seen, alice = app_client
    alice_headers = headers("alice")
    started = client.post("/api/backtesting/rolling-jobs", json={"criteria": [], "columns": [], "benchmark_mode": "ticker", "benchmark_ticker": "TPX"}, headers=alice_headers)
    assert started.status_code == 202
    job = _wait(client, started.json()["job_id"], alice_headers)
    assert job["status"] == "complete", job
    assert job["progress"] == {"completed": 1, "total": 2, "period": "2020-01-01"}
    assert seen["portfolio_owner"] == alice.user_id

    result = client.get(f"/api/backtesting/result/{job['result_id']}", headers=alice_headers).json()
    assert result["kind"] == "rolling"
    assert result["runs"][0]["annualized_return"] == 0.07
    assert result["paths"]["2020-01-01|equal|1yr"] == [["2020-01-31", 0.01, 0.0]]
    assert result["period_holdings"] == {"2020-01-01": ["7203"]}

    listed = client.get("/api/backtesting/list", headers=alice_headers).json()["backtests"]
    assert listed[0]["kind"] == "rolling"
    assert listed[0]["title"] == "Rolling screen · yearly"
    assert listed[0]["headline"] == {"mean_annualized_return": 0.07, "win_rate": 0.5, "runs": 2}


def test_another_account_cannot_see_or_cancel_a_job(app_client):
    client, headers, _seen, _alice = app_client
    job_id = client.post("/api/backtesting/rolling-jobs", json={"criteria": [], "columns": []}, headers=headers("alice")).json()["job_id"]
    assert client.get(f"/api/backtesting/rolling-jobs/{job_id}", headers=headers("bob")).status_code == 404
    assert client.post(f"/api/backtesting/rolling-jobs/{job_id}/cancel", headers=headers("bob")).status_code == 404
    assert client.get("/api/backtesting/rolling-jobs", headers=headers("bob")).json() == {"jobs": []}
    _wait(client, job_id, headers("alice"))


def test_benchmarks_report_which_have_prices(app_client):
    client, headers, _seen, _alice = app_client
    benchmarks = {item["ticker"]: item for item in client.get("/api/backtesting/benchmarks", headers=headers("alice")).json()["benchmarks"]}
    assert benchmarks["TPX"] == {"ticker": "TPX", "label": "TOPIX", "detail": "TOPIX index, price only", "available": True,
                                 "first_date": "2015-01-05", "last_date": "2026-05-15", "currency": "JPY"}
    assert benchmarks["SPY"]["available"] is False  # only a zero price
    assert benchmarks["IWDA"]["available"] is False


def test_a_job_waiting_for_a_slot_can_be_cancelled():
    import threading

    registry = RollingJobs(slots=1)
    release = threading.Event()
    first = registry.start("alice", lambda _progress, _cancel: (release.wait(2), {})[1], lambda _result: "first")
    second = registry.start("alice", lambda _progress, _cancel: {}, lambda _result: "second")
    time.sleep(0.05)
    assert second.status == "queued"
    second.cancel.set()
    release.set()
    for _ in range(100):
        if first.status == "complete" and second.status == "cancelled":
            break
        time.sleep(0.02)
    assert (first.status, first.result_id, second.status, second.result_id) == ("complete", "first", "cancelled", None)
