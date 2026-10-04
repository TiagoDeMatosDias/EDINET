"""Regression tests for account-policy and credential-lifecycle hardening."""

from __future__ import annotations

import threading
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from src.auth import service as auth_service_module
from src.auth.service import AuthError
from src.auth.storage import utc_now
from tests.unit.test_auth import _app

PASSWORD = "correct horse battery staple"
NEW_PASSWORD = "an entirely different passphrase"


def _admin_client(tmp_path, username: str = "root-admin"):
    app = _app(tmp_path)
    client = TestClient(app)
    client.post("/api/auth/register", json={"username": username, "password": PASSWORD})
    access = client.post(
        "/api/auth/login", json={"login": username, "password": PASSWORD}
    ).json()["access_token"]
    return app, client, {"Authorization": f"Bearer {access}"}


# -- saved settings are enforced -------------------------------------------


def test_saved_registration_mode_closes_registration(tmp_path):
    app, client, headers = _admin_client(tmp_path)
    assert client.get("/api/auth/status").json()["registration_open"] is True

    saved = client.patch(
        "/api/admin/auth/settings", headers=headers, json={"registration_mode": "closed"}
    )
    assert saved.status_code == 200
    assert saved.json()["registration_mode"] == "closed"
    assert client.get("/api/auth/status").json()["registration_open"] is False
    refused = client.post("/api/auth/register", json={"username": "late-user", "password": PASSWORD})
    assert refused.status_code == 403


def test_invite_mode_blocks_open_registration_but_accepts_invitations(tmp_path):
    app, client, headers = _admin_client(tmp_path)
    client.patch("/api/admin/auth/settings", headers=headers, json={"registration_mode": "invite"})
    assert client.post(
        "/api/auth/register", json={"username": "walk-in", "password": PASSWORD}
    ).status_code == 403

    service = app.state.auth_service
    admin_id = service.store.list_users()[0]["user_id"]
    token = service.create_invitation(requested_by=admin_id, role="operator")
    user = service.accept_invitation(token, "invited-user", PASSWORD)
    assert user.role == "operator"


def test_saved_default_role_applies_to_new_registrations(tmp_path):
    app, client, headers = _admin_client(tmp_path)
    client.patch("/api/admin/auth/settings", headers=headers, json={"default_role": "operator"})
    registered = client.post("/api/auth/register", json={"username": "new-operator", "password": PASSWORD})
    assert registered.status_code == 201
    assert registered.json()["user"]["role"] == "operator"


def test_first_settings_save_keeps_untouched_deployment_defaults(tmp_path):
    app, client, headers = _admin_client(tmp_path)
    # Saving only the password policy must not flip registration to the
    # database column default ("closed").
    client.patch("/api/admin/auth/settings", headers=headers, json={"password_min_length": 16})
    settings = client.get("/api/admin/auth/settings", headers=headers).json()
    assert settings["registration_mode"] == "open"
    assert settings["password_min_length"] == 16


def test_saved_access_token_lifetime_is_used(tmp_path):
    app, client, headers = _admin_client(tmp_path)
    client.patch("/api/admin/auth/settings", headers=headers, json={"access_token_seconds": 120})
    service = app.state.auth_service
    result = service.login("root-admin", PASSWORD)
    lifetime = result.tokens.access_expires_at - utc_now()
    assert timedelta(seconds=100) < lifetime <= timedelta(seconds=120)


def test_refresh_absolute_lifetime_ends_the_login(tmp_path):
    app, client, headers = _admin_client(tmp_path)
    client.patch(
        "/api/admin/auth/settings", headers=headers, json={"refresh_absolute_seconds": 300}
    )
    service = app.state.auth_service
    login = service.login("root-admin", PASSWORD)
    rotated = service.refresh(login.tokens.refresh_token)
    # Refreshing cannot extend the login past its absolute deadline.
    assert rotated.tokens.refresh_expires_at - utc_now() <= timedelta(seconds=300)

    family = service.store.find_session(
        auth_service_module.token_digest(rotated.tokens.refresh_token), "refresh"
    )["family_id"]
    with service.store.connection() as conn:
        # Backdate the family so the absolute deadline has passed.
        conn.execute(
            "UPDATE sessions SET created_at = ? WHERE family_id = ?",
            ((utc_now() - timedelta(seconds=600)).isoformat(timespec="seconds"), family),
        )
        conn.commit()
    with pytest.raises(AuthError) as exc_info:
        service.refresh(rotated.tokens.refresh_token)
    assert exc_info.value.code == "invalid_refresh"


def test_invalid_settings_are_rejected(tmp_path):
    app, client, headers = _admin_client(tmp_path)
    assert client.patch(
        "/api/admin/auth/settings", headers=headers, json={"access_token_seconds": 5}
    ).status_code == 422
    service = app.state.auth_service
    admin_id = service.store.list_users()[0]["user_id"]
    with pytest.raises(AuthError):
        service.update_auth_settings(requested_by=admin_id, default_role="owner")


# -- invitations, profile, resets -------------------------------------------


def test_invitation_can_only_be_redeemed_once_under_concurrency(tmp_path):
    app, _client, _headers = _admin_client(tmp_path)
    service = app.state.auth_service
    admin_id = service.store.list_users()[0]["user_id"]
    token = service.create_invitation(requested_by=admin_id)

    outcomes: list[str] = []
    barrier = threading.Barrier(4)

    def redeem(index: int) -> None:
        barrier.wait()
        try:
            service.accept_invitation(token, f"racer-{index}", PASSWORD)
            outcomes.append("ok")
        except AuthError as exc:
            outcomes.append(exc.code)

    threads = [threading.Thread(target=redeem, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert outcomes.count("ok") == 1
    assert sorted(set(outcomes) - {"ok"}) == ["invalid_invitation"]
    usernames = {row["username"] for row in service.store.list_users()}
    assert len(usernames & {f"racer-{i}" for i in range(4)}) == 1


def test_profile_update_stores_normalized_username(tmp_path):
    app, client, headers = _admin_client(tmp_path)
    response = client.patch("/api/auth/me", headers=headers, json={"username": "  Renamed.Admin "})
    assert response.status_code == 200
    assert response.json()["username"] == "renamed.admin"
    assert client.post(
        "/api/auth/login", json={"login": "renamed.admin", "password": PASSWORD}
    ).status_code == 200


def test_rejected_reset_password_does_not_consume_the_token(tmp_path):
    app, _client, _headers = _admin_client(tmp_path)
    service = app.state.auth_service
    member = service.register("reset-target", PASSWORD)
    admin_id = next(r["user_id"] for r in service.store.list_users() if r["role"] == "admin")
    reset_token = service.create_credential_reset(member.user_id, requested_by=admin_id)

    with pytest.raises(AuthError) as exc_info:
        service.reset_password(reset_token, "abc")
    assert exc_info.value.code == "invalid_password"

    service.reset_password(reset_token, NEW_PASSWORD)
    assert service.login("reset-target", NEW_PASSWORD).user.user_id == member.user_id
    with pytest.raises(AuthError):
        service.reset_password(reset_token, NEW_PASSWORD)


@pytest.mark.parametrize("via", ["change", "reset"])
def test_password_changes_revoke_api_tokens(tmp_path, via):
    app, _client, _headers = _admin_client(tmp_path)
    service = app.state.auth_service
    member = service.register("token-owner", PASSWORD)
    api_token = service.create_api_token(member.user_id, "automation")
    assert service.authenticate(api_token) is not None

    if via == "change":
        service.change_password(member.user_id, PASSWORD, NEW_PASSWORD)
    else:
        admin_id = next(r["user_id"] for r in service.store.list_users() if r["role"] == "admin")
        service.reset_password(
            service.create_credential_reset(member.user_id, requested_by=admin_id),
            NEW_PASSWORD,
        )
    assert service.authenticate(api_token) is None


# -- login timing and token scopes -------------------------------------------


def test_unknown_login_still_runs_password_verification(tmp_path, monkeypatch):
    app, _client, _headers = _admin_client(tmp_path)
    service = app.state.auth_service
    verified: list[str] = []
    real_verify = auth_service_module.verify_password

    def counting_verify(password_hash, password):
        verified.append(password_hash)
        return real_verify(password_hash, password)

    monkeypatch.setattr(auth_service_module, "verify_password", counting_verify)
    with pytest.raises(AuthError):
        service.login("no-such-user", PASSWORD)
    assert len(verified) == 1
    assert verified[0].startswith("$argon2id$")


def test_read_only_api_token_cannot_mutate(tmp_path):
    app, client, _headers = _admin_client(tmp_path)

    @app.post("/api/private")
    def private_write():
        return {"ok": True}

    service = app.state.auth_service
    admin_id = service.store.list_users()[0]["user_id"]
    read_token = service.create_api_token(admin_id, "reader", ["read"])
    full_token = service.create_api_token(admin_id, "writer", ["*"])

    read_headers = {"Authorization": f"Bearer {read_token}"}
    assert client.get("/api/private", headers=read_headers).status_code == 200
    denied = client.post("/api/private", headers=read_headers)
    assert denied.status_code == 403
    assert denied.json()["code"] == "insufficient_scope"
    assert client.post(
        "/api/private", headers={"Authorization": f"Bearer {full_token}"}
    ).status_code == 200


def test_unknown_token_scopes_are_rejected(tmp_path):
    app, _client, _headers = _admin_client(tmp_path)
    service = app.state.auth_service
    admin_id = service.store.list_users()[0]["user_id"]
    with pytest.raises(AuthError) as exc_info:
        service.create_api_token(admin_id, "bad", ["admin:everything"])
    assert exc_info.value.code == "invalid_scope"
