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
    import src.web_app.api.screening as screening_api
    from src.orchestrator.common import db_config
    from src.utilities import runtime_paths
    from src.web_app import security

    roots = [
        runtime_paths.state_dir(),
        runtime_paths.backtest_root(),
        runtime_paths.report_root(),
        backtesting_api._BACKTEST_ROOT,
        reports_runtime.REPORT_ROOT,
        screening_api._STATE_DIR,
        security._DEFAULT_JOB_WORKSPACE_ROOT,
        security.AppSettings().auth_db_path,
        *(Path(path) for path in db_config._load_config().values()),
    ]
    for root in roots:
        resolved = Path(root).resolve()
        assert not resolved.is_relative_to(PROJECT_ROOT / "data"), resolved
        assert not resolved.is_relative_to(PROJECT_ROOT / "config"), resolved
