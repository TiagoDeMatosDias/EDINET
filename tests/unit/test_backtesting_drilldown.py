"""Drill-down detail, per-run holdings, and the shareable HTML report."""

from __future__ import annotations

import json
import zipfile
from io import BytesIO
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.backtesting.api as backtesting_api
from src.backtesting import run_backtest_set_web, run_backtest_web
from src.backtesting.catalog import BacktestCatalog
from src.backtesting.detail import build_single_detail
from src.backtesting.html_report import render_report
from src.backtesting.zip_export import build_single_backtest_zip
from src.web_app.security import AppSettings, install_security
from tests.unit.test_backtesting_web import _minimal_db_path

PASSWORD = "correct horse battery staple"


@pytest.fixture(scope="module")
def db_path():
    path = _minimal_db_path()
    yield path
    Path(path).unlink(missing_ok=True)


@pytest.fixture(scope="module")
def single(db_path):
    result = run_backtest_web(db_path, {"A": {"mode": "weight", "value": 0.6}, "B": {"mode": "weight", "value": 0.4}}, "2023-01-01", "2023-12-31", benchmark_ticker="BENCH", initial_capital=1_000_000)
    return json.loads(json.dumps(result, default=str))


def test_detail_breaks_the_portfolio_down_by_holding(single):
    detail = build_single_detail(single)
    holdings = {item["ticker"]: item for item in detail["holdings"]}
    assert set(holdings) == {"A", "B"}
    a = holdings["A"]
    # Growth of A ends at its total return; contributions add up to the portfolio's.
    assert a["growth"][-1] - 1 == pytest.approx(a["total_return"], rel=1e-6)
    total_contribution = sum(item["contribution"] for item in detail["holdings"])
    assert total_contribution == pytest.approx(single["metrics"]["total_return"], rel=1e-6)
    assert sum(series[-1] for series in detail["contribution"].values()) == pytest.approx(single["metrics"]["total_return"], rel=1e-6)
    # Weights and cash fill the portfolio on every day shown.
    last = len(detail["dates"]) - 1
    assert sum(series[last] for series in detail["allocation"]["holdings"].values()) + detail["allocation"]["cash"][last] == pytest.approx(1.0)
    assert a["years"] and a["years"][0]["year"] == 2023
    # A's dividend is in its ledger with the as-paid amount and the shares held.
    assert a["dividends"] and a["dividends"][0]["cash"] == pytest.approx(a["dividends"][0]["shares"] * a["dividends"][0]["per_share"])


def test_report_is_one_self_contained_page(single):
    html = render_report(single, meta={"title": "Backtest · A, B"}, backtest_id="demo")
    assert html.startswith("<!doctype html>")
    assert "<script src" not in html and "<link" not in html and "http://" not in html.replace("http://www.w3.org", "")
    for text in ("Cumulative return vs the benchmark", "Contribution to the total return", "Allocation over time", "Dividend ledger", "How these numbers are calculated"):
        assert text in html
    assert html.count("<details") >= 3  # methodology plus one per holding


def test_sets_keep_each_runs_holdings_and_report_them(db_path):
    csv = "Year,Tickers,Type,Amount\n2023,A,weight,0.5\n2023,B,weight,0.5\n"
    result = run_backtest_set_web(db_path, csv, durations=["1yr"], benchmark_ticker="BENCH")
    result = json.loads(json.dumps(result, default=str))
    key = "2023-01-01|csv|1yr"
    assert {item["ticker"] for item in result["run_holdings"][key]} == {"A", "B"}
    html = render_report(result, meta={"kind": "csv"})
    assert "Companies held most often" in html and "Each run" in html


def test_archive_carries_the_report_and_dividend_ledger(single):
    with zipfile.ZipFile(BytesIO(build_single_backtest_zip(single))) as archive:
        names = set(archive.namelist())
        assert {"report.html", "dividend_payments.csv", "report.txt"} <= names
        assert archive.read("report.html").startswith(b"<!doctype html>")


@pytest.fixture
def client(tmp_path, monkeypatch, single):
    root = tmp_path / "backtests"
    monkeypatch.setattr(backtesting_api, "_BACKTEST_ROOT", root)
    monkeypatch.setattr(backtesting_api, "catalog", BacktestCatalog(tmp_path / "app.db"))
    app = FastAPI()
    app.include_router(backtesting_api.router)
    install_security(app, AppSettings(auth_mode="accounts", registration_mode="open", auth_db_path=tmp_path / "auth.db"))
    service = app.state.auth_service
    alice = service.register("alice", PASSWORD)
    service.register("bobby", PASSWORD)
    for backtest_id, payload in (("20260101_090000_aaaaaaaa", single), ("20260101_090000_bbbbbbbb", {"config": {}, "runs": [], "aggregate": {}})):
        directory = root / backtest_id
        directory.mkdir(parents=True)
        (directory / "result.json").write_text(json.dumps(payload), encoding="utf-8")
        backtesting_api.catalog.record_owner(backtest_id, alice.user_id)

    def headers(username: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {service.login(username, PASSWORD).tokens.access_token}"}

    return TestClient(app), headers


def test_detail_and_report_endpoints_follow_ownership(client):
    http, headers = client
    detail = http.get("/api/backtesting/result/20260101_090000_aaaaaaaa/detail", headers=headers("alice"))
    assert detail.status_code == 200 and {item["ticker"] for item in detail.json()["holdings"]} == {"A", "B"}
    assert http.get("/api/backtesting/result/20260101_090000_bbbbbbbb/detail", headers=headers("alice")).status_code == 400
    assert http.get("/api/backtesting/result/20260101_090000_aaaaaaaa/detail", headers=headers("bobby")).status_code == 404

    report = http.get("/api/backtesting/report/20260101_090000_aaaaaaaa", headers=headers("alice"))
    assert report.status_code == 200
    assert report.headers["content-type"].startswith("text/html")
    assert report.headers["content-disposition"].startswith("attachment")
    assert "Backtest report" in report.text
    inline = http.get("/api/backtesting/report/20260101_090000_bbbbbbbb?inline=true", headers=headers("alice"))
    assert inline.headers["content-disposition"].startswith("inline")
    assert http.get("/api/backtesting/report/20260101_090000_aaaaaaaa", headers=headers("bobby")).status_code == 404
