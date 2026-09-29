"""Tests for remote-access, error, and filesystem security policy."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

import src.web_app.security as security_module
from src.web_app.security import (
    AppSettings,
    PathPolicy,
    PathPolicyError,
    SecurityConfigurationError,
    configured_database_policy,
    install_security,
    is_loopback_host,
)


def test_loopback_hosts_do_not_require_remote_opt_in():
    assert is_loopback_host("127.0.0.1")
    assert is_loopback_host("::1")
    assert is_loopback_host("localhost")
    assert not is_loopback_host("0.0.0.0")
    assert not is_loopback_host("192.168.1.10")
    assert AppSettings().validate().authentication_required is True


def test_backtest_artifact_limit_is_independent(monkeypatch):
    monkeypatch.setenv("EDINET_MAX_EXPORT_BYTES", "1024")
    monkeypatch.setenv("EDINET_MAX_BACKTEST_ARTIFACT_BYTES", "4096")

    settings = AppSettings.from_env(host="127.0.0.1", allow_remote=False)

    assert settings.max_export_bytes == 1024
    assert settings.max_backtest_artifact_bytes == 4096


@pytest.mark.parametrize(
    ("allow_remote", "auth_mode"),
    [
        (False, "disabled"),
        (True, "disabled"),
    ],
)
def test_remote_settings_fail_closed(allow_remote, auth_mode):
    with pytest.raises(SecurityConfigurationError):
        AppSettings(
            host="0.0.0.0",
            allow_remote=allow_remote,
            auth_mode=auth_mode,
        ).validate()


def test_remote_api_requires_valid_bearer_and_hides_500_details(tmp_path):
    settings = AppSettings(
        host="0.0.0.0",
        allow_remote=True,
        auth_mode="accounts",
        auth_db_path=tmp_path / "auth.db",
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

    user = app.state.auth_service.register(
        "security-test",
        "correct horse battery staple",
    )
    result = app.state.auth_service.login(
        user.username,
        "correct horse battery staple",
    )

    assert client.get("/health").status_code == 200
    assert client.get("/api/private").status_code == 401
    assert client.get(
        "/api/private",
        headers={"Authorization": "Bearer wrong"},
    ).status_code == 401

    headers = {"Authorization": f"Bearer {result.tokens.access_token}"}
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
            auth_mode="accounts",
        ).validate()


def test_remote_trusted_host_is_enforced():
    settings = AppSettings(
        host="0.0.0.0",
        allow_remote=True,
        auth_mode="accounts",
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


def _configure_database_directory(monkeypatch, directory: Path) -> dict[str, Path]:
    """Point db_config at a directory where every store is co-located."""
    from src.orchestrator.common import db_config

    names = {
        "db1": "Base.db",
        "db2": "Standardized.db",
        "db3": "Portfolio.db",
        "auth_db": "auth.db",
        "research_db": "research.db",
        "pipeline_jobs_db": "pipeline_jobs.db",
        "filings_db": "Filings.db",
    }
    paths = {key: directory / name for key, name in names.items()}
    for path in paths.values():
        path.write_bytes(b"")
    monkeypatch.setattr(db_config, "_cache", {key: str(path) for key, path in paths.items()})
    for spec in db_config.DATABASES:
        if spec.env_override:
            monkeypatch.delenv(spec.env_override, raising=False)
    return paths


def test_configured_policy_does_not_authorize_the_shared_database_directory(tmp_path, monkeypatch):
    paths = _configure_database_directory(monkeypatch, tmp_path)

    policy = configured_database_policy()

    assert policy.authorize_database(paths["db1"]) == paths["db1"].resolve()
    assert policy.authorize_database(paths["db2"]) == paths["db2"].resolve()
    for key in ("db3", "auth_db", "research_db", "pipeline_jobs_db", "filings_db"):
        with pytest.raises(PathPolicyError, match="outside"):
            policy.authorize_database(paths[key])


def test_configured_policy_refuses_private_stores_inside_explicit_roots(tmp_path, monkeypatch):
    paths = _configure_database_directory(monkeypatch, tmp_path)
    operator_copy = tmp_path / "Standardized-2025.db"
    operator_copy.write_bytes(b"")

    policy = configured_database_policy((tmp_path,))

    assert policy.authorize_database(operator_copy) == operator_copy.resolve()
    for key in ("db3", "auth_db", "research_db", "pipeline_jobs_db", "filings_db"):
        with pytest.raises(PathPolicyError, match="outside"):
            policy.authorize_database(paths[key])


def test_every_registered_store_is_private_unless_it_opts_in(tmp_path, monkeypatch):
    from src.orchestrator.common import db_config

    paths = _configure_database_directory(monkeypatch, tmp_path)
    new_store = tmp_path / "new_store.db"
    new_store.write_bytes(b"")
    monkeypatch.setattr(db_config, "_cache", {**db_config._cache, "new_store": str(new_store)})
    monkeypatch.setattr(db_config, "DATABASES", (*db_config.DATABASES, db_config.DatabaseSpec("new_store", "new_store.db")))
    monkeypatch.setattr(db_config, "_DATABASES_BY_KEY", {spec.key: spec for spec in db_config.DATABASES})

    policy = configured_database_policy((tmp_path,))

    with pytest.raises(PathPolicyError, match="outside"):
        policy.authorize_database(new_store)
    assert policy.authorize_database(paths["db2"]) == paths["db2"].resolve()


def test_configured_policy_refuses_overridden_auth_and_research_stores(tmp_path, monkeypatch):
    _configure_database_directory(monkeypatch, tmp_path)
    auth_override = tmp_path / "custom-auth.db"
    research_override = tmp_path / "custom-research.db"
    auth_override.write_bytes(b"")
    research_override.write_bytes(b"")
    monkeypatch.setenv("EDINET_AUTH_DB", str(auth_override))
    monkeypatch.setenv("EDINET_RESEARCH_DB", str(research_override))

    policy = configured_database_policy((tmp_path,))

    for path in (auth_override, research_override):
        with pytest.raises(PathPolicyError, match="outside"):
            policy.authorize_database(path)


def test_portfolio_policy_authorizes_only_the_configured_portfolio_addition(tmp_path, monkeypatch):
    paths = _configure_database_directory(monkeypatch, tmp_path)

    policy = configured_database_policy(also_allow=("db3",))

    assert policy.authorize_database(paths["db3"]) == paths["db3"].resolve()
    with pytest.raises(PathPolicyError, match="outside"):
        policy.authorize_database(paths["auth_db"])


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


def test_pipeline_request_uses_large_upload_envelope(monkeypatch):
    monkeypatch.setattr(
        security_module,
        "_REQUEST_ENVELOPE_OVERHEAD_BYTES",
        0,
    )
    app = FastAPI()

    @app.post("/api/pipeline/run")
    async def echo():
        return {"accepted": True}

    install_security(
        app,
        AppSettings(
            auth_mode="disabled",
            max_upload_bytes=4,
            max_export_bytes=4,
        ),
    )
    response = TestClient(app).post(
        "/api/pipeline/run",
        content=b"12345",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert response.status_code == 200
    assert response.json() == {"accepted": True}


def test_disabled_auth_supplies_local_admin_principal():
    app = FastAPI()

    @app.get("/api/whoami")
    async def whoami(request: Request):
        user = request.state.user
        return {
            "user_id": user.user_id,
            "username": user.username,
            "role": user.role,
            "status": user.status,
        }

    install_security(app, AppSettings(auth_mode="disabled"))
    response = TestClient(app).get("/api/whoami")

    assert response.status_code == 200
    assert response.json() == {
        "user_id": "local",
        "username": "local",
        "role": "admin",
        "status": "active",
    }


def _body_app() -> FastAPI:
    app = FastAPI()

    @app.post("/api/echo")
    async def echo(request: Request):
        return {"received": len(await request.body())}

    return app


def test_chunked_body_without_length_is_limited(monkeypatch):
    monkeypatch.setattr(security_module, "_REQUEST_ENVELOPE_OVERHEAD_BYTES", 0)
    app = _body_app()
    install_security(
        app,
        AppSettings(auth_mode="disabled", max_upload_bytes=8, max_export_bytes=8),
    )

    def chunks():
        # A generator body is sent with chunked encoding and no Content-Length.
        for _ in range(4):
            yield b"12345"

    response = TestClient(app).post("/api/echo", content=chunks())
    assert "content-length" not in {key.lower() for key in response.request.headers}
    assert response.status_code == 413
    assert response.json()["code"] == "request_too_large"
    assert response.headers["X-Correlation-ID"]


def test_chunked_body_within_limit_is_accepted(monkeypatch):
    monkeypatch.setattr(security_module, "_REQUEST_ENVELOPE_OVERHEAD_BYTES", 0)
    app = _body_app()
    install_security(
        app,
        AppSettings(auth_mode="disabled", max_upload_bytes=64, max_export_bytes=64),
    )
    response = TestClient(app).post("/api/echo", content=iter([b"abc", b"def"]))
    assert response.status_code == 200
    assert response.json() == {"received": 6}


def test_security_headers_are_sent():
    app = FastAPI()

    @app.get("/page")
    def page():
        return {"ok": True}

    install_security(app, AppSettings(auth_mode="disabled"))
    response = TestClient(app).get("/page")
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["Referrer-Policy"] == "no-referrer"
    assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]
    # HSTS would pin localhost to HTTPS for every local dev server.
    assert "Strict-Transport-Security" not in response.headers


def test_remote_deployments_send_hsts(tmp_path):
    settings = AppSettings(
        host="0.0.0.0",
        allow_remote=True,
        auth_mode="accounts",
        auth_db_path=tmp_path / "auth.db",
        trusted_hosts=("testserver",),
    )
    app = FastAPI()

    @app.get("/health")
    def health():
        return {"status": "healthy"}

    install_security(app, settings)
    response = TestClient(app).get("/health")
    assert response.headers["Strict-Transport-Security"].startswith("max-age=")


def test_operator_guidance_detail_survives_5xx_masking():
    app = FastAPI()

    @app.get("/guidance")
    def guidance():
        raise security_module.OperatorGuidanceError("Run npm run build in frontend-v2.")

    install_security(app, AppSettings(auth_mode="disabled"))
    response = TestClient(app).get("/guidance")
    assert response.status_code == 503
    assert response.json()["detail"] == "Run npm run build in frontend-v2."
    assert response.json()["code"] == "service_unavailable"


def test_unhandled_error_response_carries_correlation_header():
    app = FastAPI()

    @app.get("/boom")
    def boom():
        raise RuntimeError("database password is hunter2")

    install_security(app, AppSettings(auth_mode="disabled"))
    response = TestClient(app, raise_server_exceptions=False).get("/boom")
    assert response.status_code == 500
    assert "hunter2" not in response.text
    assert response.headers["X-Correlation-ID"] == response.json()["correlation_id"]


def test_bare_settings_never_default_to_a_cwd_relative_auth_database():
    from src.orchestrator.common.db_config import get_auth_db

    assert AppSettings().auth_db_path == Path(get_auth_db())


@pytest.mark.parametrize("getter", ["get_auth_db", "get_research_db", "get_pipeline_jobs_db", "get_db3"])
def test_screening_refuses_private_database_paths(getter):
    from src.orchestrator.common import db_config
    from src.web_app.server import app

    private = getattr(db_config, getter)()
    assert Path(private).is_file()
    client = TestClient(app)

    response = client.get("/api/screening/metrics", params={"db_path": private})

    assert response.status_code == 400
    assert "tables" not in response.json()
