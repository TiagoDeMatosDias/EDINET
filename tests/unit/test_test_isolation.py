"""Guards that keep the suite away from operator data and live services."""

from __future__ import annotations

import socket
from pathlib import Path

import pytest

from tests.conftest import NetworkAccessBlocked

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_outbound_network_is_blocked():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        with pytest.raises(NetworkAccessBlocked):
            # TEST-NET-1 (RFC 5737): never routable, so no real traffic.
            sock.connect(("192.0.2.1", 443))


def test_runtime_roots_are_outside_the_project():
    import src.backtesting.api as backtesting_api
    import src.reports.runtime as reports_runtime
    from src import paths
    from src.orchestrator.common import db_config
    from src.web_app import security

    roots = [
        paths.data_dir(),
        backtesting_api._BACKTEST_ROOT,
        reports_runtime.REPORT_ROOT,
        security._DEFAULT_JOB_WORKSPACE_ROOT,
        security.AppSettings().auth_db_path,
        db_config.get_app_db(),
        db_config.get_chat_db(),
        db_config.get_market_db(),
        db_config.get_filings_db(),
    ]
    for root in roots:
        resolved = Path(root).resolve()
        assert not resolved.is_relative_to(PROJECT_ROOT / "data"), resolved
        assert not resolved.is_relative_to(PROJECT_ROOT / "config"), resolved
