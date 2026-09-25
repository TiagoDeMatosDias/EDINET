"""Security boundary tests for the command-line launcher."""

from __future__ import annotations

import pytest
import uvicorn

import main as launcher
from src.web_app.security import SecurityConfigurationError


@pytest.fixture(autouse=True)
def _isolated_launcher(monkeypatch):
    """Keep the launcher's process-wide side effects inside each test.

    ``_run_web`` exports its bind settings through ``os.environ`` and calls
    ``setup_logging``, which writes to (and archives) the project's ``logs/``
    and attaches file handlers to the root logger.
    """
    for name in ("EDINET_HOST", "EDINET_PORT", "EDINET_ALLOW_REMOTE"):
        # delenv records the original value so it is restored afterwards.
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("src.utilities.logger.setup_logging", lambda *args, **kwargs: None)


def test_launcher_rejects_remote_bind_without_opt_in(monkeypatch):
    monkeypatch.delenv("EDINET_AUTH_MODE", raising=False)
    monkeypatch.delenv("EDINET_TRUSTED_HOSTS", raising=False)
    with pytest.raises(SecurityConfigurationError, match="ALLOW_REMOTE"):
        launcher._run_web(host="0.0.0.0", allow_remote=False)


def test_launcher_propagates_validated_remote_settings(monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setenv("EDINET_AUTH_MODE", "accounts")
    monkeypatch.setenv("EDINET_TRUSTED_HOSTS", "research.example")
    monkeypatch.setenv("EDINET_CERT_DIR", str(tmp_path))
    monkeypatch.setattr(
        uvicorn,
        "run",
        lambda target, **kwargs: captured.update(target=target, **kwargs),
    )

    launcher._run_web(
        host="0.0.0.0",
        port=8123,
        reload=False,
        allow_remote=True,
    )

    assert captured == {
        "target": "src.web_app.server:app",
        "host": "0.0.0.0",
        "port": 8123,
        "reload": False,
        "ssl_certfile": tmp_path / "cert.pem",
        "ssl_keyfile": tmp_path / "key.pem",
    }
    assert (tmp_path / "cert.pem").is_file()
    assert (tmp_path / "key.pem").is_file()
