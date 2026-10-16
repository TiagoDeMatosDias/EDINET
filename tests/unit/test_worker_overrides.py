"""Settings and database-path overrides used inside worker processes."""

from __future__ import annotations

import pytest

import src.settings as settings
from src.orchestrator.common import db_config
from src.settings import SettingError


@pytest.fixture
def forbid_app_db(monkeypatch):
    """Any attempt to read or write app.db fails the test."""

    def boom(*_args, **_kwargs):
        raise AssertionError("app.db was opened")

    monkeypatch.setattr(settings, "read_stored_value", boom)
    monkeypatch.setattr(settings, "read_stored_values", boom)
    monkeypatch.setattr(settings, "SettingsStore", boom)
    monkeypatch.setattr(settings, "_app_db", boom)


@pytest.fixture(autouse=True)
def _clean():
    yield
    settings.clear_overrides()
    db_config.clear_worker_paths()


def test_settings_answer_from_overrides_and_defaults(forbid_app_db):
    settings.install_overrides({"edinet.api_key": "secret-key", "limits.max_upload_bytes": 123456})

    assert settings.get_setting("edinet.api_key") == "secret-key"
    assert settings.edinet_api_key() == "secret-key"
    assert settings.get_setting("limits.max_upload_bytes") == 123456
    default = settings.spec_for("limits.max_export_bytes").default
    assert settings.get_setting("limits.max_export_bytes") == default
    loaded = settings.load_settings()
    assert loaded["edinet.api_key"] == "secret-key"
    assert loaded["limits.max_export_bytes"] == default
    described = {item["key"]: item for item in settings.describe_settings()}
    assert described["edinet.api_key"]["value"] is None  # secrets stay hidden
    assert described["edinet.api_key"]["is_set"] is True
    assert described["limits.max_upload_bytes"]["value"] == 123456


def test_settings_writes_are_refused(forbid_app_db):
    settings.install_overrides({})

    with pytest.raises(SettingError):
        settings.set_setting("edinet.api_key", "x")
    with pytest.raises(SettingError):
        settings.unset_setting("edinet.api_key")


def test_clear_overrides_restores_app_db_reads():
    settings.install_overrides({"edinet.api_key": "from-worker"})
    settings.clear_overrides()

    assert settings.get_setting("edinet.api_key") != "from-worker"


def test_worker_paths_replace_database_lookups():
    db_config.install_worker_paths("/x/market.db", "/y/filings.db")

    assert db_config.get_market_db() == "/x/market.db"
    assert db_config.get_filings_db() == "/y/filings.db"
    with pytest.raises(RuntimeError, match="Workers never open app.db"):
        db_config.get_app_db()
    with pytest.raises(RuntimeError, match="Workers never open app.db"):
        db_config.get_chat_db()


def test_worker_paths_skip_the_legacy_layout_check(monkeypatch):
    def boom():
        raise AssertionError("legacy layout was inspected")

    monkeypatch.setattr("src.orchestrator.common.migrate_layout.pending_databases", boom)
    monkeypatch.setattr(db_config, "_layout_checked", False)
    db_config.install_worker_paths("/x/market.db", "/y/filings.db")

    assert db_config.get_market_db() == "/x/market.db"


def test_clear_worker_paths_restores_lookups():
    db_config.install_worker_paths("/x/market.db", "/y/filings.db")
    db_config.clear_worker_paths()

    assert db_config.get_market_db() != "/x/market.db"
    assert db_config.get_app_db().endswith("app.db")
