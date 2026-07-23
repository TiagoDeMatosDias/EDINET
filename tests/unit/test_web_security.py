"""Tests for remote-access, error, and filesystem security policy."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import src.web_app.security as security_module
from src.web_app.security import (
    AppSettings,
    PathPolicy,
    PathPolicyError,
    SecurityConfigurationError,
    install_security,
    is_loopback_host,
)


def test_loopback_hosts_do_not_require_remote_opt_in():
    assert is_loopback_host("127.0.0.1")
    assert is_loopback_host("::1")
    assert is_loopback_host("localhost")
    assert not is_loopback_host("0.0.0.0")
    assert not is_loopback_host("192.168.1.10")
    assert AppSettings().validate().authentication_required is False


def test_backtest_artifact_limit_is_independent(monkeypatch):
    monkeypatch.setenv("EDINET_MAX_EXPORT_BYTES", "1024")
    monkeypatch.setenv("EDINET_MAX_BACKTEST_ARTIFACT_BYTES", "4096")

    settings = AppSettings.from_env(host="127.0.0.1", allow_remote=False)

    assert settings.max_export_bytes == 1024
    assert settings.max_backtest_artifact_bytes == 4096


@pytest.mark.parametrize(
    ("allow_remote", "token"),
    [
        (False, None),
        (True, None),
        (True, "too-short"),
    ],
)
def test_remote_settings_fail_closed(allow_remote, token):
    with pytest.raises(SecurityConfigurationError):
        AppSettings(
            host="0.0.0.0",
            allow_remote=allow_remote,
            api_token=token,
        ).validate()


def test_remote_api_requires_valid_bearer_and_hides_500_details():
    settings = AppSettings(
        host="0.0.0.0",
        allow_remote=True,
        api_token="a" * 32,
        trusted_hosts=("testserver",),
    ).validate()
    app = FastAPI()

    @app.get("/health")
    def health():
        return {"status": "healthy"}

    @app.get("/api/private")
    def private():
        return {"allowed": True}

    @app.get("/api/failure")
    def failure():
        raise HTTPException(
            status_code=500,
            detail=r"secret-token C:\private\operator.db",
        )

    install_security(app, settings)
    client = TestClient(app, raise_server_exceptions=False)

    assert client.get("/health").status_code == 200
    assert client.get("/api/private").status_code == 401
    assert client.get(
        "/api/private",
        headers={"Authorization": "Bearer wrong"},
    ).status_code == 401

    headers = {"Authorization": f"Bearer {settings.api_token}"}
    allowed = client.get("/api/private", headers=headers)
    assert allowed.status_code == 200
    assert allowed.headers["X-Correlation-ID"]

    failure_response = client.get("/api/failure", headers=headers)
    assert failure_response.status_code == 500
    payload = failure_response.json()
    assert payload["detail"] == "Internal server error"
    assert payload["correlation_id"]
    assert "secret-token" not in failure_response.text
    assert "operator.db" not in failure_response.text


def test_remote_settings_require_explicit_trusted_hosts():
    with pytest.raises(SecurityConfigurationError, match="TRUSTED_HOSTS"):
        AppSettings(
            host="0.0.0.0",
            allow_remote=True,
            api_token="a" * 32,
        ).validate()


def test_remote_trusted_host_is_enforced():
    settings = AppSettings(
        host="0.0.0.0",
        allow_remote=True,
        api_token="a" * 32,
        trusted_hosts=("allowed.example",),
    )
    app = FastAPI()

    @app.get("/health")
    def health():
        return {"status": "healthy"}

    install_security(app, settings)
    client = TestClient(app)
    assert client.get("/health").status_code == 400
    assert client.get(
        "/health",
        headers={"Host": "allowed.example"},
    ).status_code == 200


def test_path_policy_allows_only_database_files_inside_roots(tmp_path):
    allowed_root = tmp_path / "allowed"
    allowed_root.mkdir()
    database = allowed_root / "Standardized.db"
    database.write_bytes(b"")
    text_file = allowed_root / "not-a-database.txt"
    text_file.write_text("x", encoding="utf-8")
    outside = tmp_path / "outside.db"
    outside.write_bytes(b"")

    policy = PathPolicy(
        read_roots=(allowed_root,),
        write_roots=(allowed_root,),
    )

    assert policy.authorize_database(database) == database.resolve()
    assert policy.authorize_database(database, writable=True) == database.resolve()
    with pytest.raises(PathPolicyError, match="absolute"):
        policy.authorize_database(Path("relative.db"))
    with pytest.raises(PathPolicyError, match="file type"):
        policy.authorize_database(text_file)
    with pytest.raises(PathPolicyError, match="outside"):
        policy.authorize_database(outside)


def test_path_policy_rejects_symlink_escape_when_supported(tmp_path):
    allowed_root = tmp_path / "allowed"
    allowed_root.mkdir()
    outside = tmp_path / "outside.db"
    outside.write_bytes(b"")
    link = allowed_root / "linked.db"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("Creating symlinks is not permitted on this platform")

    policy = PathPolicy(read_roots=(allowed_root,))
    with pytest.raises(PathPolicyError, match="outside"):
        policy.authorize_database(link)


def test_declared_request_body_over_limit_is_rejected(monkeypatch):
    monkeypatch.setattr(
        security_module,
        "_REQUEST_ENVELOPE_OVERHEAD_BYTES",
        0,
    )
    app = FastAPI()

    @app.post("/api/echo")
    async def echo():
        return {"accepted": True}

    install_security(
        app,
        AppSettings(max_upload_bytes=4, max_export_bytes=4),
    )
    response = TestClient(app).post(
        "/api/echo",
        content=b"12345",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert response.status_code == 413
    assert response.json()["code"] == "request_too_large"
    assert response.headers["X-Correlation-ID"]
