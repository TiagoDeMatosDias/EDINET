"""Filesystem-boundary tests for the backtesting API."""

from __future__ import annotations

import sqlite3

import pytest
from fastapi import HTTPException

import src.backtesting.api as backtesting_api


def _database(path):
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE sample (value INTEGER)")
    connection.close()
    return path


def test_backtests_read_the_configured_database(tmp_path, monkeypatch):
    configured = _database(tmp_path / "Standardized.db")
    monkeypatch.setattr(backtesting_api, "get_db2", lambda: str(configured))
    assert backtesting_api._resolve_db() == str(configured)

    monkeypatch.setattr(backtesting_api, "get_db2", lambda: str(tmp_path / "missing.db"))
    with pytest.raises(HTTPException) as exc_info:
        backtesting_api._resolve_db()
    assert exc_info.value.status_code == 503


@pytest.mark.parametrize(
    "identifier",
    ["../escape", "..\\escape", "20260722", "CON"],
)
def test_backtest_identifier_rejects_traversal_and_invalid_names(identifier):
    with pytest.raises(HTTPException) as exc_info:
        backtesting_api._backtest_directory(identifier, require_existing=False)
    assert exc_info.value.status_code == 404


def test_legacy_timestamp_backtest_identifier_remains_supported():
    directory = backtesting_api._backtest_directory(
        "20260722_120000",
        require_existing=False,
    )
    assert directory.name == "20260722_120000"


def test_generated_backtest_identifiers_are_collision_resistant():
    first = backtesting_api._new_backtest_id()
    second = backtesting_api._new_backtest_id()
    assert first != second
    assert backtesting_api._BACKTEST_ID.fullmatch(first)
    assert backtesting_api._BACKTEST_ID.fullmatch(second)


def test_export_size_limit_is_enforced(monkeypatch):
    from dataclasses import replace

    limited = replace(backtesting_api.get_settings(), max_export_bytes=4)
    monkeypatch.setattr(backtesting_api, "get_settings", lambda: limited)
    with pytest.raises(HTTPException) as exc_info:
        backtesting_api._enforce_export_size(b"12345")
    assert exc_info.value.status_code == 413


def test_backtest_artifact_uses_its_own_size_limit(monkeypatch):
    from dataclasses import replace

    limited = replace(backtesting_api.get_settings(), max_export_bytes=4, max_backtest_artifact_bytes=8)
    monkeypatch.setattr(backtesting_api, "get_settings", lambda: limited)
    assert backtesting_api._enforce_backtest_artifact_size(b"12345") == b"12345"
    with pytest.raises(HTTPException) as exc_info:
        backtesting_api._enforce_backtest_artifact_size(b"123456789")
    assert exc_info.value.status_code == 413


def test_recent_backtest_labels_name_holdings_instead_of_run_ids():
    assert backtesting_api._holdings_label(["7203", "6758"]) == "7203, 6758"
    assert backtesting_api._holdings_label(["7203", "6758", "9984", "8306", " "]) == "7203, 6758, 9984 +1 more"
    assert backtesting_api._backtest_subtitle("2016-01-01 to 2026-01-01", None, "vs ^TPX") == (
        "2016-01-01 to 2026-01-01 · vs ^TPX"
    )
