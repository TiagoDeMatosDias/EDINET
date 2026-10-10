"""Shutting the server down from the Admin page."""

from __future__ import annotations

import signal
import threading

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.api import router as auth_router
from src.web_app import shutdown
from src.web_app.api import server_control
from src.web_app.security import AppSettings, install_security


@pytest.fixture
def stops(monkeypatch):
    """Record requests to stop the process instead of stopping the test run."""
    calls: list[bool] = []
    monkeypatch.setattr(server_control, "stop_server", lambda: calls.append(True))
    return calls


def _accounts_app(tmp_path) -> tuple[FastAPI, TestClient]:
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(server_control.router)
    install_security(app, AppSettings(auth_mode="accounts", auth_db_path=tmp_path / "auth.db"))
    return app, TestClient(app)


def test_only_an_administrator_can_shut_the_server_down(tmp_path, stops):
    app, client = _accounts_app(tmp_path)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "admin", "password": password})
    client.post("/api/auth/register", json={"username": "member", "password": password})

    def login(name: str) -> dict[str, str]:
        token = client.post("/api/auth/login", json={"login": name, "password": password}).json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    admin, member = login("admin"), login("member")
    confirm = {"confirm": True}

    assert client.post("/api/admin/server/shutdown", json=confirm).status_code == 401
    assert client.post("/api/admin/server/shutdown", json=confirm, headers=member).status_code == 403
    assert stops == []

    accepted = client.post("/api/admin/server/shutdown", json=confirm, headers=admin)

    assert accepted.status_code == 202
    assert accepted.json() == {"status": "shutting_down"}
    assert stops == [True]
    events = app.state.auth_service.store.list_audit_events(100)
    shutdown = next(row for row in events if row["event_type"] == "server_shutdown")
    assert shutdown["user_id"] == client.get("/api/auth/me", headers=admin).json()["user_id"]


def test_shutdown_needs_a_json_confirmation(tmp_path, stops):
    """A form or body-less POST, which another site could send, is refused."""
    app = FastAPI()
    app.include_router(server_control.router)
    install_security(app, AppSettings(auth_mode="disabled", auth_db_path=tmp_path / "auth.db"))
    client = TestClient(app)

    assert client.post("/api/admin/server/shutdown").status_code == 422
    assert client.post("/api/admin/server/shutdown", json={"confirm": False}).status_code == 422
    for content_type in ("text/plain", "application/x-www-form-urlencoded"):
        cross_site = client.post(
            "/api/admin/server/shutdown",
            content='{"confirm": true}',
            headers={"Content-Type": content_type},
        )
        assert cross_site.status_code == 422
    assert stops == []

    assert client.post("/api/admin/server/shutdown", json={"confirm": True}).status_code == 202
    assert stops == [True]


def test_stopping_interrupts_the_server_once_then_ends_the_process(monkeypatch):
    # The forced exit is the fallback: uvicorn's own shutdown has to fit before it.
    assert shutdown.RESPONSE_SECONDS + shutdown.GRACEFUL_SHUTDOWN_SECONDS < shutdown.FORCED_EXIT_SECONDS
    steps: list[object] = []
    ended = threading.Event()
    monkeypatch.setattr(shutdown, "_stopping", threading.Lock())
    monkeypatch.setattr(shutdown, "RESPONSE_SECONDS", 0)
    monkeypatch.setattr(shutdown, "FORCED_EXIT_SECONDS", 0)
    monkeypatch.setattr(shutdown.multiprocessing, "parent_process", lambda: None)
    monkeypatch.setattr(shutdown.signal, "raise_signal", steps.append)
    monkeypatch.setattr(shutdown.os, "_exit", lambda code: (steps.append(code), ended.set()))

    shutdown.stop_server()
    shutdown.stop_server()

    assert ended.wait(5)
    assert steps == [signal.SIGINT, 0]
