"""Saved backtests are visible only to the account that produced them."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.backtesting.api as backtesting_api
from src.web_app.security import AppSettings, install_security

PASSWORD = "correct horse battery staple"
ALICE_RUN = "20260101_090000_aaaaaaaa"
BOB_RUN = "20260102_090000_bbbbbbbb"
LEGACY_RUN = "20250101_090000"


def _save(root: Path, backtest_id: str, owner: str | None) -> None:
    directory = root / backtest_id
    directory.mkdir(parents=True)
    (directory / "result.json").write_text(
        json.dumps({"aggregate": {"owner": owner}, "config": {}}), encoding="utf-8"
    )
    (directory / "backtest.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)
    if owner is not None:
        (directory / "owner.json").write_text(
            json.dumps({"owner_user_id": owner}), encoding="utf-8"
        )


@pytest.fixture
def accounts(tmp_path, monkeypatch):
    root = tmp_path / "backtests"
    monkeypatch.setattr(backtesting_api, "_BACKTEST_ROOT", root)
    app = FastAPI()
    app.include_router(backtesting_api.router)
    install_security(
        app,
        AppSettings(
            auth_mode="accounts",
            registration_mode="open",
            auth_db_path=tmp_path / "auth.db",
        ),
    )
    service = app.state.auth_service
    admin = service.register("admin-user", PASSWORD)
    alice = service.register("alice", PASSWORD)
    bob = service.register("bob", PASSWORD)
    _save(root, ALICE_RUN, alice.user_id)
    _save(root, BOB_RUN, bob.user_id)
    _save(root, LEGACY_RUN, None)

    def headers(username: str) -> dict[str, str]:
        token = service.login(username, PASSWORD).tokens.access_token
        return {"Authorization": f"Bearer {token}"}

    assert admin.role == "admin"
    return TestClient(app), headers


def _listed(client, headers) -> set[str]:
    response = client.get("/api/backtesting/list", headers=headers)
    assert response.status_code == 200
    return {item["id"] for item in response.json()["backtests"]}


def test_list_shows_only_own_backtests(accounts):
    client, headers = accounts
    assert _listed(client, headers("alice")) == {ALICE_RUN}
    assert _listed(client, headers("bob")) == {BOB_RUN}


def test_unowned_legacy_backtests_are_admin_only(accounts):
    client, headers = accounts
    assert _listed(client, headers("admin-user")) == {LEGACY_RUN}
    assert client.get(
        f"/api/backtesting/result/{LEGACY_RUN}", headers=headers("alice")
    ).status_code == 404
    assert client.get(
        f"/api/backtesting/result/{LEGACY_RUN}", headers=headers("admin-user")
    ).status_code == 200


@pytest.mark.parametrize("route", ["result", "download"])
def test_other_accounts_backtests_are_not_found(accounts, route):
    client, headers = accounts
    alice = headers("alice")
    assert client.get(f"/api/backtesting/{route}/{ALICE_RUN}", headers=alice).status_code == 200
    # 404, not 403, so ids belonging to other accounts are not confirmed.
    assert client.get(f"/api/backtesting/{route}/{BOB_RUN}", headers=alice).status_code == 404


def test_saved_runs_record_their_owner(tmp_path, monkeypatch):
    from starlette.requests import Request

    from src.auth.models import AuthenticatedUser

    request = Request({"type": "http", "state": {}})
    request.state.user = AuthenticatedUser("owner-1", "owner", None, "member", "active")
    backtesting_api._write_owner(tmp_path, request)
    assert json.loads((tmp_path / "owner.json").read_text()) == {"owner_user_id": "owner-1"}
    assert backtesting_api._can_access(tmp_path, request.state.user)
    other = AuthenticatedUser("owner-2", "other", None, "admin", "active")
    assert not backtesting_api._can_access(tmp_path, other)
