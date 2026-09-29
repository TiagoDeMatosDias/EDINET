"""Rolling backtest SSE archive integration tests."""

from __future__ import annotations

from dataclasses import replace

from fastapi.testclient import TestClient

import src.backtesting.api as backtesting_api
from src.backtesting.zip_export import ExportSizeLimitExceeded
from src.web_app.server import app


def _install_fast_rolling_backtest(monkeypatch) -> None:
    def run_rolling(**kwargs):
        kwargs["progress_queue"].put(
            {
                "type": "result",
                "aggregate": {"successful": 1},
                "config": {},
                "results": [],
            }
        )

    monkeypatch.setattr(
        backtesting_api._bt,
        "run_screening_backtest_rolling",
        run_rolling,
    )
    monkeypatch.setattr(backtesting_api, "_resolve_db", lambda: "test.db")
    monkeypatch.setattr(
        backtesting_api,
        "_validate_base_currency",
        lambda currency: currency,
    )
    monkeypatch.setattr(
        backtesting_api,
        "_resolve_risk_free_rate",
        lambda _rate, _currency: 0.02,
    )


def _post_rolling():
    return TestClient(app).post(
        "/api/backtesting/run-rolling",
        json={"criteria": [], "columns": []},
    )


def test_rolling_run_uses_dedicated_artifact_limit(monkeypatch, tmp_path):
    _install_fast_rolling_backtest(monkeypatch)
    captured = {}

    def save_archive(result, base_dir, max_bytes):
        captured.update(result=result, base_dir=base_dir, max_bytes=max_bytes)
        # The real saver creates the result directory; the API then adds
        # result.json and owner.json next to the archive.
        saved = tmp_path / "20260723_120000_1234abcd"
        saved.mkdir()
        return str(saved)

    monkeypatch.setattr(backtesting_api, "save_rolling_backtest_zip", save_archive)
    limited = replace(backtesting_api.get_settings(), max_export_bytes=1, max_backtest_artifact_bytes=4096)
    monkeypatch.setattr(backtesting_api, "get_settings", lambda: limited)

    response = _post_rolling()

    assert response.status_code == 200
    assert '"type": "result"' in response.text
    assert captured["max_bytes"] == 4096


def test_rolling_archive_limit_returns_actionable_error(monkeypatch):
    _install_fast_rolling_backtest(monkeypatch)

    def reject_archive(_result, _base_dir, max_bytes):
        raise ExportSizeLimitExceeded(max_bytes, max_bytes + 1)

    monkeypatch.setattr(
        backtesting_api,
        "save_rolling_backtest_zip",
        reject_archive,
    )
    limited = replace(backtesting_api.get_settings(), max_backtest_artifact_bytes=1024 * 1024)
    monkeypatch.setattr(backtesting_api, "get_settings", lambda: limited)

    response = _post_rolling()

    assert response.status_code == 200
    assert "Backtest completed" in response.text
    assert "EDINET_MAX_BACKTEST_ARTIFACT_BYTES" in response.text
