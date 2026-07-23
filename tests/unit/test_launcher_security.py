"""Security boundary tests for the command-line launcher."""

from __future__ import annotations

import pytest
import uvicorn

import main as launcher
from src.web_app.security import SecurityConfigurationError


def test_launcher_rejects_remote_bind_without_opt_in(monkeypatch):
    monkeypatch.delenv("EDINET_API_TOKEN", raising=False)
    monkeypatch.delenv("EDINET_TRUSTED_HOSTS", raising=False)
    with pytest.raises(SecurityConfigurationError, match="ALLOW_REMOTE"):
        launcher._run_web(host="0.0.0.0", allow_remote=False)


def test_launcher_propagates_validated_remote_settings(monkeypatch):
    captured = {}
    monkeypatch.setenv("EDINET_API_TOKEN", "a" * 32)
    monkeypatch.setenv("EDINET_TRUSTED_HOSTS", "research.example")
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
    }
