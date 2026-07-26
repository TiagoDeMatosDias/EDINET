"""Focused account, token, and authentication middleware tests."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.api import admin_router
from src.auth.api import router as auth_router
from src.auth.passwords import hash_password, verify_password
from src.web_app.security import AppSettings, install_security


def _app(tmp_path):
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(admin_router)

    @app.get("/api/private")
    def private():
        return {"ok": True}

    settings = AppSettings(
        auth_mode="accounts",
        registration_mode="open",
        auth_db_path=tmp_path / "auth.db",
    )
    install_security(app, settings)
    return app


def test_argon2_password_hash_round_trip():
    password_hash = hash_password("correct horse battery staple")
    assert password_hash.startswith("$argon2id$")
    assert verify_password(password_hash, "correct horse battery staple")
    assert not verify_password(password_hash, "wrong password")


def test_register_login_refresh_rotation_and_logout(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)

    status = client.get("/api/auth/status")
    assert status.json()["bootstrap_required"] is True

    registered = client.post(
        "/api/auth/register",
        json={
            "username": "alice",
            "password": "correct horse battery staple",
            "email": "alice@example.test",
        },
    )
    assert registered.status_code == 201
    assert registered.json()["bootstrap_admin"] is True
    assert registered.json()["user"]["role"] == "admin"

    login = client.post(
        "/api/auth/login",
        json={"login": "alice", "password": "correct horse battery staple"},
    )
    assert login.status_code == 200
    access = login.json()["access_token"]
    assert access.startswith("ed_at_")
    assert "edinet_refresh" in client.cookies

    private = client.get(
        "/api/private",
        headers={"Authorization": f"Bearer {access}"},
    )
    assert private.status_code == 200

    refreshed = client.post(
        "/api/auth/refresh",
        headers={"Authorization": f"Bearer {access}"},
    )
    assert refreshed.status_code == 200
    new_access = refreshed.json()["access_token"]
    assert new_access != access
    assert client.get(
        "/api/private",
        headers={"Authorization": f"Bearer {new_access}"},
    ).status_code == 200

    assert client.post(
        "/api/auth/logout",
        headers={"Authorization": f"Bearer {new_access}"},
    ).status_code == 204
    assert client.get(
        "/api/private",
        headers={"Authorization": f"Bearer {new_access}"},
    ).status_code == 401


def test_provider_token_is_not_loaded_as_application_auth(monkeypatch, tmp_path):
    monkeypatch.setenv("EDINET_API_TOKEN", "provider-only-secret")
    monkeypatch.delenv("EDINET_APP_TOKEN", raising=False)
    monkeypatch.setenv("EDINET_AUTH_MODE", "accounts")
    monkeypatch.setenv("EDINET_AUTH_DB", str(tmp_path / "auth.db"))

    settings = AppSettings.from_env()
    assert settings.application_token is None
    assert settings.api_token is None


def test_personal_api_token_is_opaque_and_revocable(tmp_path):
    app = _app(tmp_path)
    service = app.state.auth_service
    user = service.register("token-user", "correct horse battery staple")
    token = service.create_api_token(user.user_id, "automation", ["read"])
    assert token.startswith("ed_pat_")
    assert service.authenticate(token).user_id == user.user_id
    token_id = service.store.list_api_tokens(user.user_id)[0]["token_id"]
    assert service.store.revoke_api_token(user.user_id, token_id, datetime.now(timezone.utc))
    assert service.authenticate(token) is None


def test_refresh_reuse_revokes_the_token_family(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "reuse-user", "password": password})
    client.post("/api/auth/login", json={"login": "reuse-user", "password": password})
    old_refresh = client.cookies.get("edinet_refresh")
    assert old_refresh
    rotated = client.post("/api/auth/refresh")
    assert rotated.status_code == 200
    assert client.cookies.get("edinet_refresh") != old_refresh
    client.cookies.set("edinet_refresh", old_refresh, path="/api/auth")
    reused = client.post("/api/auth/refresh")
    assert reused.status_code == 401
    client.cookies.set("edinet_refresh", rotated.cookies.get("edinet_refresh"), path="/api/auth")
    assert client.post("/api/auth/refresh").status_code == 401


def test_logout_revokes_refresh_cookie(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "logout-user", "password": password})
    login = client.post("/api/auth/login", json={"login": "logout-user", "password": password})
    access = login.json()["access_token"]
    refresh = client.cookies.get("edinet_refresh")
    assert refresh
    assert client.post("/api/auth/logout", headers={"Authorization": f"Bearer {access}"}).status_code == 204
    client.cookies.set("edinet_refresh", refresh, path="/api/auth")
    assert client.post("/api/auth/refresh").status_code == 401


def test_change_password_revokes_existing_sessions(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)
    password = "correct horse battery staple"
    new_password = "new password phrase that is long enough"
    client.post("/api/auth/register", json={"username": "pwd-user", "password": password})
    login = client.post("/api/auth/login", json={"login": "pwd-user", "password": password})
    access = login.json()["access_token"]

    response = client.post(
        "/api/auth/change-password",
        headers={"Authorization": f"Bearer {access}"},
        json={"current_password": password, "new_password": new_password},
    )
    assert response.status_code == 204

    assert client.get("/api/auth/me", headers={"Authorization": f"Bearer {access}"}).status_code == 401
    refreshed = client.post("/api/auth/login", json={"login": "pwd-user", "password": new_password})
    assert refreshed.status_code == 200


def test_profile_update_rejects_duplicate_username(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "profile-a", "password": password})
    client.post("/api/auth/register", json={"username": "profile-b", "password": password})
    login = client.post("/api/auth/login", json={"login": "profile-a", "password": password})
    access = login.json()["access_token"]
    response = client.patch(
        "/api/auth/me",
        headers={"Authorization": f"Bearer {access}"},
        json={"username": "profile-b"},
    )
    assert response.status_code == 409


def test_admin_can_list_users_and_disable(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "admin", "password": password})
    client.post("/api/auth/register", json={"username": "member-user", "password": password})
    login = client.post("/api/auth/login", json={"login": "admin", "password": password})
    access = login.json()["access_token"]

    users = client.get("/api/admin/auth/users", headers={"Authorization": f"Bearer {access}"})
    assert users.status_code == 200
    assert len(users.json()) >= 2

    member = next(u for u in users.json() if u["role"] == "member")
    disabled = client.patch(
        f"/api/admin/auth/users/{member['user_id']}/disable",
        headers={"Authorization": f"Bearer {access}"},
    )
    assert disabled.status_code == 200
    assert disabled.json()["status"] == "disabled"


def test_admin_can_configure_password_minimum(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "policy-admin", "password": password})
    access = client.post("/api/auth/login", json={"login": "policy-admin", "password": password}).json()["access_token"]
    headers = {"Authorization": f"Bearer {access}"}

    assert client.get("/api/auth/status").json()["password_min_length"] == 15
    updated = client.patch("/api/admin/auth/settings", headers=headers, json={"password_min_length": 20})
    assert updated.status_code == 200
    assert updated.json()["password_min_length"] == 20
    assert client.get("/api/auth/status").json()["password_min_length"] == 20

    short = client.post("/api/auth/register", json={"username": "short-policy", "password": "a" * 19})
    assert short.status_code == 400
    accepted = client.post("/api/auth/register", json={"username": "long-policy", "password": "a" * 20})
    assert accepted.status_code == 201


def test_member_cannot_access_admin_endpoints(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "boss", "password": password})
    client.post("/api/auth/register", json={"username": "worker", "password": password})
    login = client.post("/api/auth/login", json={"login": "worker", "password": password})
    access = login.json()["access_token"]
    assert client.get("/api/admin/auth/users", headers={"Authorization": f"Bearer {access}"}).status_code == 403


def test_session_list_and_revoke(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "session-user", "password": password})
    login = client.post("/api/auth/login", json={"login": "session-user", "password": password})
    access = login.json()["access_token"]

    sessions = client.get("/api/auth/sessions", headers={"Authorization": f"Bearer {access}"})
    assert sessions.status_code == 200
    assert len(sessions.json()) >= 2

    access_session_id = next(s["session_id"] for s in sessions.json() if s["token_type"] == "access")
    assert client.delete(f"/api/auth/sessions/{access_session_id}", headers={"Authorization": f"Bearer {access}"}).status_code == 204
    assert client.get("/api/auth/me", headers={"Authorization": f"Bearer {access}"}).status_code == 401


def test_last_admin_cannot_be_disabled(tmp_path):
    app = _app(tmp_path)
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "only-admin", "password": password})
    login = client.post("/api/auth/login", json={"login": "only-admin", "password": password})
    access = login.json()["access_token"]

    users = client.get("/api/admin/auth/users", headers={"Authorization": f"Bearer {access}"})
    admin_id = users.json()[0]["user_id"]
    response = client.patch(f"/api/admin/auth/users/{admin_id}/disable", headers={"Authorization": f"Bearer {access}"})
    assert response.status_code == 409
