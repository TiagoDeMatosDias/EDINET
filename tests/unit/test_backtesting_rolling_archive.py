"""Rolling backtest SSE archive integration tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import src.backtesting.api as backtesting_api
from src.backtesting.zip_export import ExportSizeLimitExceeded
from src.jobs.service import get_job_service
from src.web_app.server import app
from src.worker.inline import InlineWorker


@pytest.fixture(autouse=True)
def inline_worker():
    with InlineWorker(get_job_service()):
        yield


def _install_fast_rolling_backtest(monkeypatch) -> None:
    def run_rolling(**kwargs):
        return {"aggregate": {"successful": 1}, "config": {}, "results": []}

    monkeypatch.setattr(backtesting_api._bt, "run_screening_backtest_rolling", run_rolling)
    monkeypatch.setattr("src.worker.handlers.backtest._market_db", lambda: "test.db")
    monkeypatch.setattr(backtesting_api, "_resolve_db", lambda: "test.db")
    monkeypatch.setattr(backtesting_api, "_validate_base_currency", lambda currency: currency)
    monkeypatch.setattr(backtesting_api, "_resolve_risk_free_rate", lambda _rate, _currency: 0.02)


def _post_rolling():
    return TestClient(app).post(
        "/api/backtesting/run-rolling",
        json={"criteria": [], "columns": []},
    )


def test_rolling_run_uses_dedicated_artifact_limit(monkeypatch):
    _install_fast_rolling_backtest(monkeypatch)
    captured = {}

    def save_archive(result, base_dir, max_bytes):
        captured.update(result=result, base_dir=base_dir, max_bytes=max_bytes)
        # The real saver creates a directory holding the archive.
        saved = Path(base_dir) / "20260723_120000_1234abcd"
        saved.mkdir()
        (saved / "backtest.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)
        return str(saved)

    monkeypatch.setattr("src.backtesting.zip_export.save_rolling_backtest_zip", save_archive)
    monkeypatch.setattr("src.worker.handlers.backtest._artifact_limit", lambda _context: 4096)

    response = _post_rolling()

    assert response.status_code == 200
    assert '"type": "result"' in response.text
    assert captured["max_bytes"] == 4096


def test_rolling_archive_limit_returns_actionable_error(monkeypatch):
    _install_fast_rolling_backtest(monkeypatch)

    def reject_archive(_result, _base_dir, max_bytes):
        raise ExportSizeLimitExceeded(max_bytes, max_bytes + 1)

    monkeypatch.setattr("src.backtesting.zip_export.save_rolling_backtest_zip", reject_archive)
    monkeypatch.setattr("src.worker.handlers.backtest._artifact_limit", lambda _context: 1024 * 1024)

    response = _post_rolling()

    assert response.status_code == 200
    assert "Backtest completed" in response.text
    assert "1 MiB limit" in response.text
