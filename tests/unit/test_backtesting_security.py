"""Filesystem-boundary tests for the backtesting API."""

from __future__ import annotations

import sqlite3

import pytest
from fastapi import HTTPException

import src.backtesting.api as backtesting_api
from src.web_app.security import PathPolicy


def _database(path):
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE sample (value INTEGER)")
    connection.close()
    return path


def test_database_outside_allowed_root_is_rejected(tmp_path, monkeypatch):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    outside = _database(tmp_path / "outside.db")
    monkeypatch.setattr(
        backtesting_api,
        "_DB_PATH_POLICY",
        PathPolicy(read_roots=(allowed,)),
    )

    with pytest.raises(HTTPException) as exc_info:
        backtesting_api._resolve_db(str(outside))
    assert exc_info.value.status_code == 400


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

    monkeypatch.setattr(
        backtesting_api,
        "_APP_SETTINGS",
        replace(backtesting_api._APP_SETTINGS, max_export_bytes=4),
    )
    with pytest.raises(HTTPException) as exc_info:
        backtesting_api._enforce_export_size(b"12345")
    assert exc_info.value.status_code == 413


def test_backtest_artifact_uses_its_own_size_limit(monkeypatch):
    from dataclasses import replace

    monkeypatch.setattr(
        backtesting_api,
        "_APP_SETTINGS",
        replace(
            backtesting_api._APP_SETTINGS,
            max_export_bytes=4,
            max_backtest_artifact_bytes=8,
        ),
    )
    assert backtesting_api._enforce_backtest_artifact_size(b"12345") == b"12345"
    with pytest.raises(HTTPException) as exc_info:
        backtesting_api._enforce_backtest_artifact_size(b"123456789")
    assert exc_info.value.status_code == 413
