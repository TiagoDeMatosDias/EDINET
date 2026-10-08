"""Per-user hotkey settings: storage, isolation between accounts, validation."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.api import router as auth_router
from src.auth.storage import AuthStore
from src.web_app.api.settings import router as settings_router
from src.web_app.security import AppSettings, install_security


def _app(tmp_path, auth_mode: str = "accounts"):
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(settings_router)
    install_security(app, AppSettings(auth_mode=auth_mode, registration_mode="open", auth_db_path=tmp_path / "auth.db"))
    return app


def _signed_in(client: TestClient, username: str) -> dict[str, str]:
    client.post("/api/auth/register", json={"username": username, "password": "correct horse battery staple"})
    login = client.post("/api/auth/login", json={"login": username, "password": "correct horse battery staple"})
    return {"Authorization": f"Bearer {login.json()['access_token']}"}


def test_store_round_trips_and_deletes_opaque_json(tmp_path):
    store = AuthStore(tmp_path / "auth.db")
    assert store.get_user_setting("u1", "hotkeys") is None
    store.set_user_setting("u1", "hotkeys", {"overrides": {"a": [{"key": "x"}]}})
    store.set_user_setting("u1", "hotkeys", {"overrides": {"a": [{"key": "y"}]}})
    assert store.get_user_setting("u1", "hotkeys") == {"overrides": {"a": [{"key": "y"}]}}
    assert store.get_user_setting("u2", "hotkeys") is None
    store.delete_user_setting("u1", "hotkeys")
    assert store.get_user_setting("u1", "hotkeys") is None


def test_hotkey_overrides_are_saved_per_account(tmp_path):
    client = TestClient(_app(tmp_path))
    alice = _signed_in(client, "alice")
    bob = _signed_in(client, "bobby")

    assert client.get("/api/settings/hotkeys").status_code == 401
    assert client.get("/api/settings/hotkeys", headers=alice).json() == {"overrides": {}}

    body = {"overrides": {"screening.run": [{"key": "r", "shift": True}]}}
    saved = client.put("/api/settings/hotkeys", json=body, headers=alice)
    assert saved.status_code == 200
    stored = client.get("/api/settings/hotkeys", headers=alice).json()
    assert stored["overrides"]["screening.run"] == [{"key": "r", "shift": True, "ctrl": False, "alt": False, "meta": False}]
    assert client.get("/api/settings/hotkeys", headers=bob).json() == {"overrides": {}}

    assert client.delete("/api/settings/hotkeys", headers=alice).json() == {"overrides": {}}
    assert client.get("/api/settings/hotkeys", headers=alice).json() == {"overrides": {}}


def test_hotkey_overrides_reject_malformed_bodies(tmp_path):
    client = TestClient(_app(tmp_path))
    alice = _signed_in(client, "alice")
    bad_bodies = [
        {"overrides": {"Bad Id!": [{"key": "x"}]}},
        {"overrides": {"screening.run": []}},
        {"overrides": {"screening.run": [{"key": ""}]}},
        {"overrides": {"screening.run": [{"key": "x", "hyper": True}]}},
        {"overrides": {"screening.run": [{"key": "x"}] * 5}},
        {"extra": True},
    ]
    for body in bad_bodies:
        assert client.put("/api/settings/hotkeys", json=body, headers=alice).status_code == 422, body


def test_local_workspace_stores_hotkeys_under_the_local_principal(tmp_path):
    client = TestClient(_app(tmp_path, auth_mode="disabled"))
    client.put("/api/settings/hotkeys", json={"overrides": {"global.help": [{"key": "h"}]}})
    assert AuthStore(tmp_path / "auth.db").get_user_setting("local", "hotkeys")["overrides"]["global.help"][0]["key"] == "h"
