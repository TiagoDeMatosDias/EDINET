"""The Cloudflare tunnel: the settings that open it, its guard rails, and its admin API."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.api import router as auth_router
from src.settings import set_setting
from src.settings.api import router as settings_router
from src.web_app import tunnel
from src.web_app.api import server_control
from src.web_app.security import AppSettings, install_security

ORIGIN = "https://127.0.0.1:8000"

# Stands in for cloudflared: it records how it was started, prints what the
# real program prints (``--output json``), and runs until it is terminated.
_FAKE_CLOUDFLARED = """
import json, os, sys, time

record = os.environ["FAKE_RECORD"]
with open(record, "a") as handle:
    handle.write(json.dumps({"argv": sys.argv[1:], "token": os.environ.get("TUNNEL_TOKEN"), "pid": os.getpid()}) + "\\n")
with open(record) as handle:
    attempt = len(handle.readlines())

def say(**fields):
    print(json.dumps(fields), flush=True)

if os.environ.get("FAKE_FAIL_FIRST") and attempt == 1:
    print("Provided Tunnel token is not valid.", flush=True)
    print("See 'cloudflared tunnel run --help'.", flush=True)
    sys.exit(255)
if "run" in sys.argv:
    say(level="info", message="Registered tunnel connection", connIndex=0)
    rules = [
        {"hostname": "other.example", "service": "http://localhost:3000"},
        {"hostname": "research.example", "service": "https://localhost:8000", "originRequest": {"noTLSVerify": True}},
        {"service": "http_status:404"},
    ]
    say(level="info", message="Updated to new configuration", config=json.dumps({"ingress": rules}), version=1)
else:
    say(level="info", message="|  Your quick Tunnel has been created! Visit it at (it may take some time to be reachable):  |")
    say(level="info", message="|  https://calm-test-words.trycloudflare.com  |")
    say(level="info", message="Registered tunnel connection", connIndex=0)
time.sleep(60)
"""


@pytest.fixture
def cloudflared(tmp_path, monkeypatch):
    """An isolated data folder and a stand-in cloudflared; returns the starts it recorded."""
    monkeypatch.setenv("EDINET_DATA_DIR", str(tmp_path))
    script = tmp_path / "fake_cloudflared.py"
    script.write_text(_FAKE_CLOUDFLARED, encoding="utf-8")
    record = tmp_path / "starts.jsonl"
    monkeypatch.setenv("FAKE_RECORD", str(record))
    monkeypatch.setattr(tunnel, "RETRY_SECONDS", 0.05)
    managers: list[tunnel.TunnelManager] = []

    def manager(**kwargs) -> tunnel.TunnelManager:
        created = tunnel.TunnelManager(ORIGIN, locate=lambda _on_download: [sys.executable, str(script)], **kwargs)
        managers.append(created)
        return created

    def starts() -> list[dict]:
        return [json.loads(line) for line in record.read_text().splitlines()] if record.exists() else []

    yield manager, starts
    for created in managers:
        created.stop()


def _wait(condition, seconds: float = 10.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("condition was not met in time")


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def test_the_setting_opens_a_temporary_address_and_closes_it_again(cloudflared):
    manager, starts = cloudflared
    tunnel_manager = manager()

    tunnel_manager.apply()
    assert tunnel_manager.status()["state"] == "off"
    assert starts() == []

    set_setting("tunnel.enabled", "on")
    tunnel_manager.apply()
    status = _wait(lambda: tunnel_manager.status()["state"] == "running" and tunnel_manager.status())

    assert status["url"] == "https://calm-test-words.trycloudflare.com"
    assert (status["enabled"], status["kind"], status["origin"]) == (True, "quick", ORIGIN)
    (started,) = starts()
    assert started["argv"][-3:] == ["--url", ORIGIN, "--no-tls-verify"]
    assert "--no-autoupdate" in started["argv"]

    set_setting("tunnel.enabled", "off")
    tunnel_manager.apply()

    assert tunnel_manager.status() == {**status, "enabled": False, "state": "off", "url": None}
    _wait(lambda: not _alive(started["pid"]))


def test_a_token_runs_the_accounts_tunnel_without_showing_the_token(cloudflared):
    manager, starts = cloudflared
    # Pasted as the Cloudflare dashboard shows it, at the end of a command.
    set_setting("tunnel.token", "cloudflared.exe service install eyJhIjoidG9rZW4ifQ")
    set_setting("tunnel.enabled", "on")
    tunnel_manager = manager()

    tunnel_manager.apply()
    status = _wait(lambda: tunnel_manager.status()["url"] and tunnel_manager.status())

    # Of the tunnel's public host names, the one that points at this server.
    assert (status["state"], status["kind"], status["url"]) == ("running", "named", "https://research.example")
    (started,) = starts()
    assert started["token"] == "eyJhIjoidG9rZW4ifQ"
    assert started["argv"][-1] == "run"
    assert "eyJhIjoidG9rZW4ifQ" not in " ".join(started["argv"])


def test_the_tunnel_stays_closed_until_nothing_blocks_it(cloudflared):
    manager, starts = cloudflared
    reasons = ["Sign-in is disabled."]
    set_setting("tunnel.enabled", "on")
    tunnel_manager = manager(blocked=lambda: reasons[0] if reasons else None)

    tunnel_manager.apply()
    status = _wait(lambda: tunnel_manager.status()["state"] == "blocked" and tunnel_manager.status())
    time.sleep(0.3)

    assert status["message"] == status["blocked_reason"] == "Sign-in is disabled."
    assert starts() == [], "cloudflared is never started while the tunnel is blocked"

    reasons.clear()
    _wait(lambda: tunnel_manager.status()["state"] == "running")
    assert len(starts()) == 1


def test_a_failed_start_is_reported_and_tried_again(cloudflared, monkeypatch):
    manager, starts = cloudflared
    monkeypatch.setenv("FAKE_FAIL_FIRST", "1")
    monkeypatch.setattr(tunnel, "RETRY_SECONDS", 0.5)
    set_setting("tunnel.enabled", "on")
    tunnel_manager = manager()

    tunnel_manager.apply()
    failed = _wait(lambda: tunnel_manager.status()["state"] == "failed" and tunnel_manager.status())

    assert failed["message"] == "Provided Tunnel token is not valid."
    assert failed["url"] is None
    _wait(lambda: tunnel_manager.status()["state"] == "running")
    assert len(starts()) == 2


@pytest.fixture
def no_cloudflared(tmp_path, monkeypatch):
    """A machine with no cloudflared anywhere; returns the (empty) bundle folder."""
    monkeypatch.setenv("EDINET_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(tunnel, "bundle_dir", lambda: tmp_path / "bundle")
    monkeypatch.setattr(tunnel.shutil, "which", lambda _name: None)
    monkeypatch.setattr(tunnel.sys, "platform", "linux")
    monkeypatch.setattr(tunnel.platform, "machine", lambda: "x86_64")
    return tmp_path / "bundle"


def test_cloudflared_is_downloaded_into_the_data_folder_once(no_cloudflared, tmp_path, monkeypatch):
    downloads: list[str] = []

    def download(url: str, target: Path) -> None:
        downloads.append(url)
        target.parent.mkdir(parents=True)
        target.write_bytes(b"binary")

    monkeypatch.setattr(tunnel, "_download", download)
    announced: list[bool] = []
    program = "cloudflared.exe" if os.name == "nt" else "cloudflared"
    assert tunnel.find_cloudflared() is None

    first = tunnel.ensure_cloudflared(lambda: announced.append(True))
    second = tunnel.ensure_cloudflared(lambda: announced.append(True))

    assert first == second == tmp_path / "data" / "tools" / program
    assert downloads == ["https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64"]
    assert announced == [True]
    assert tunnel.find_cloudflared() == ("downloaded", first)

    monkeypatch.setattr(tunnel.shutil, "which", lambda _name: "/usr/local/bin/cloudflared")
    assert tunnel.find_cloudflared() == ("installed", Path("/usr/local/bin/cloudflared"))


def test_the_copy_a_release_carries_is_used_before_any_other(no_cloudflared, monkeypatch):
    monkeypatch.setattr(tunnel.shutil, "which", lambda _name: "/usr/local/bin/cloudflared")
    monkeypatch.setattr(tunnel, "_download", lambda _url, _target: pytest.fail("nothing is downloaded"))
    bundled = tunnel.bundled_cloudflared()
    assert bundled.parent == no_cloudflared / "tools" / "bin"
    bundled.parent.mkdir(parents=True)
    bundled.write_bytes(b"binary")

    assert tunnel.find_cloudflared() == ("bundled", bundled)
    assert tunnel.ensure_cloudflared() == bundled


@pytest.mark.parametrize(
    ("system", "machine", "asset"),
    [
        ("linux", "aarch64", "cloudflared-linux-arm64"),
        ("win32", "AMD64", "cloudflared-windows-amd64.exe"),
        ("darwin", "arm64", None),
    ],
)
def test_release_file_for_each_supported_machine(monkeypatch, system, machine, asset):
    monkeypatch.setattr(tunnel.sys, "platform", system)
    monkeypatch.setattr(tunnel.platform, "machine", lambda: machine)

    assert tunnel._release_asset() == asset


def test_a_machine_without_a_download_asks_for_an_installed_cloudflared(no_cloudflared, monkeypatch):
    monkeypatch.setattr(tunnel.sys, "platform", "darwin")

    with pytest.raises(tunnel.TunnelError, match="on PATH"):
        tunnel.ensure_cloudflared()


class _RecordingTunnel(tunnel.TunnelManager):
    def __init__(self) -> None:
        super().__init__(ORIGIN)
        self.applied = 0

    def apply(self) -> None:
        self.applied += 1


def test_admin_api_reports_the_tunnel_and_settings_apply_at_once(tmp_path, monkeypatch):
    monkeypatch.setenv("EDINET_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(tunnel, "find_cloudflared", lambda: None)
    app = FastAPI()
    for router in (auth_router, settings_router, server_control.router):
        app.include_router(router)
    install_security(app, AppSettings(auth_mode="accounts", auth_db_path=tmp_path / "auth.db"))
    app.state.tunnel = _RecordingTunnel()
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "admin", "password": password})
    client.post("/api/auth/register", json={"username": "member", "password": password})

    def login(name: str) -> dict[str, str]:
        token = client.post("/api/auth/login", json={"login": name, "password": password}).json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    admin, member = login("admin"), login("member")

    assert client.get("/api/admin/server/tunnel").status_code == 401
    assert client.get("/api/admin/server/tunnel", headers=member).status_code == 403
    assert client.post("/api/admin/server/tunnel/restart", headers=member).status_code == 403
    assert client.get("/api/admin/server/tunnel", headers=admin).json() == {
        "enabled": False,
        "kind": "quick",
        "state": "off",
        "url": None,
        "message": None,
        "origin": ORIGIN,
        "blocked_reason": None,
        "cloudflared": "missing",
    }

    assert client.put("/api/admin/settings/jobs.retention_hours", json={"value": 48}, headers=admin).status_code == 200
    assert app.state.tunnel.applied == 0, "only tunnel settings restart the tunnel"

    saved = client.put("/api/admin/settings/tunnel.token", json={"value": "secret-token"}, headers=admin)
    assert saved.json()["is_set"] is True and "secret-token" not in saved.text
    assert client.put("/api/admin/settings/tunnel.enabled", json={"value": "on"}, headers=admin).json()["value"] == "on"
    assert client.put("/api/admin/settings/tunnel.enabled", json={"value": "maybe"}, headers=admin).status_code == 422
    assert app.state.tunnel.applied == 2

    status = client.get("/api/admin/server/tunnel", headers=admin).json()
    assert (status["enabled"], status["kind"]) == (True, "named")
    assert "secret-token" not in json.dumps(status)

    assert client.delete("/api/admin/settings/tunnel.token", headers=admin).status_code == 200
    assert client.post("/api/admin/server/tunnel/restart", headers=admin).status_code == 200
    assert app.state.tunnel.applied == 4
    events = {row["event_type"] for row in app.state.auth_service.store.list_audit_events(100)}
    assert {"setting_updated", "setting_reset", "tunnel_restarted"} <= events


def test_the_server_keeps_the_tunnel_closed_without_sign_in_or_an_administrator():
    from src.web_app import server

    # The test suite runs with sign-in disabled, which is exactly the blocked case.
    assert "Sign-in is disabled" in (server._tunnel_blocked() or "")
    assert isinstance(server.app.state.tunnel, tunnel.TunnelManager)
    assert server.app.state.tunnel.origin.startswith("https://127.0.0.1:")
