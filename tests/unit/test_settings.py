"""Operator settings stored in app.db: registry, admin API, and command line."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import main as launcher
from src.settings import (
    SettingError,
    describe_settings,
    edinet_api_key,
    get_setting,
    set_setting,
    unset_setting,
)
from src.settings.api import router as settings_router
from src.web_app.security import AppSettings, install_security


@pytest.fixture
def app_db(tmp_path, monkeypatch):
    monkeypatch.setenv("EDINET_DATA_DIR", str(tmp_path))
    return tmp_path / "app.db"


def test_defaults_apply_until_a_value_is_stored(app_db):
    assert not app_db.exists()
    assert get_setting("auth.mode") == "accounts"
    assert get_setting("jobs.retention_hours") == 24
    assert not app_db.exists(), "reading a setting never creates app.db"

    set_setting("jobs.retention_hours", "48")
    assert get_setting("jobs.retention_hours") == 48
    unset_setting("jobs.retention_hours")
    assert get_setting("jobs.retention_hours") == 24


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("auth.mode", "maybe", "one of"),
        ("jobs.retention_hours", 0, "at least 1"),
        ("limits.max_upload_bytes", "lots", "whole number"),
        ("server.trusted_hosts", 7, "list"),
        ("no.such.setting", "x", "Unknown setting"),
        ("chat.message_keys", {}, "managed by the application"),
    ],
)
def test_invalid_values_are_rejected(app_db, key, value, message):
    with pytest.raises(SettingError, match=message):
        set_setting(key, value)


def test_lists_accept_comma_separated_text(app_db):
    assert set_setting("server.trusted_hosts", "research.example, , shade.example") == [
        "research.example",
        "shade.example",
    ]


def test_secret_values_are_never_described(app_db):
    set_setting("edinet.api_key", "provider-secret")

    described = {item["key"]: item for item in describe_settings()}

    assert edinet_api_key() == "provider-secret"
    assert described["edinet.api_key"]["is_set"] is True
    assert described["edinet.api_key"]["value"] is None
    assert "provider-secret" not in str(described)
    assert "chat.message_keys" not in described


def test_app_settings_come_from_app_db_and_launch_options(app_db):
    set_setting("auth.mode", "accounts")
    set_setting("server.trusted_hosts", ["research.example"])
    set_setting("limits.max_export_bytes", 2048)

    settings = AppSettings.load(host="0.0.0.0", port=8443, allow_remote=True)

    assert settings.trusted_hosts == ("research.example",)
    assert settings.max_export_bytes == 2048
    assert (settings.host, settings.port, settings.allow_remote) == ("0.0.0.0", 8443, True)


def test_admin_api_reads_and_changes_settings(app_db, tmp_path):
    from src.auth.api import router as auth_router

    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(settings_router)
    install_security(app, AppSettings(auth_mode="accounts", auth_db_path=tmp_path / "auth.db"))
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "admin", "password": password})
    client.post("/api/auth/register", json={"username": "member", "password": password})

    def login(name: str) -> dict[str, str]:
        token = client.post("/api/auth/login", json={"login": name, "password": password}).json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    admin, member = login("admin"), login("member")

    assert client.get("/api/admin/settings", headers=member).status_code == 403

    saved = client.put("/api/admin/settings/edinet.api_key", json={"value": "provider-secret"}, headers=admin)
    assert saved.status_code == 200
    assert saved.json()["is_set"] is True
    assert "provider-secret" not in saved.text
    assert edinet_api_key() == "provider-secret"

    listed = client.get("/api/admin/settings", headers=admin).json()["settings"]
    assert {item["key"] for item in listed} >= {"edinet.api_key", "auth.mode", "storage.filings_db_path"}
    assert "provider-secret" not in str(listed)

    invalid = client.put("/api/admin/settings/auth.mode", json={"value": "sometimes"}, headers=admin)
    assert invalid.status_code == 422
    assert client.put("/api/admin/settings/chat.message_keys", json={"value": {}}, headers=admin).status_code == 404

    reset = client.delete("/api/admin/settings/edinet.api_key", headers=admin)
    assert reset.json()["is_set"] is False
    assert edinet_api_key() == ""

    events = app.state.auth_service.store.list_audit_events(100)
    assert {"setting_updated", "setting_reset"} <= {row["event_type"] for row in events}


def test_command_line_sets_and_shows_settings(app_db, capsys):
    assert launcher.main(["config", "set", "server.trusted_hosts", "a.example,b.example"]) == 0
    assert launcher.main(["config", "get", "server.trusted_hosts"]) == 0
    assert capsys.readouterr().out.splitlines()[-1] == "a.example, b.example"

    assert launcher.main(["config", "set", "edinet.api_key", "provider-secret"]) == 0
    assert launcher.main(["config", "list"]) == 0
    listing = capsys.readouterr().out
    assert "edinet.api_key = (set)" in listing
    assert "provider-secret" not in listing

    assert launcher.main(["config", "set", "auth.mode", "sometimes"]) == 2
    assert "must be one of" in capsys.readouterr().err
