"""Where the application reads its bundled files and keeps its data.

``app_dir`` is the folder the operator sees: the one holding
``ShadeResearch.exe`` in a packaged build, the repository root otherwise.

``bundle_dir`` holds the read-only files shipped inside the build (frontend
bundle, brand assets). In a one-file PyInstaller build it is the temporary
folder the executable unpacks into, which is deleted on exit, so nothing may
be written below it. Module ``__file__`` paths point there too, which is why
runtime folders must never be derived from ``__file__``.

``data_dir`` holds everything the application writes: ``data/`` inside the
application folder, or ``EDINET_DATA_DIR`` when set (tests, scratch runs, a
second instance). Its layout::

    app.db       settings, secrets, accounts, research, pipeline jobs, portfolio
    chat.db      chat, kept apart because it grows with use
    market.db    rebuildable market data (default location)
    filings.db   rebuildable filing archive (default location)
    certs/       TLS certificate and key
    logs/        rotating server log
    artifacts/   backtests, reports, exports, job workspaces, downloads
    tools/       cloudflared, downloaded when a tunnel is opened and none is bundled or installed
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_SOURCE_ROOT = Path(__file__).resolve().parents[1]


def app_dir() -> Path:
    """The folder holding the executable, or the repository root from source."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return _SOURCE_ROOT


def bundle_dir() -> Path:
    """The folder holding bundled read-only resources."""
    bundled = getattr(sys, "_MEIPASS", None)
    return Path(bundled) if bundled else _SOURCE_ROOT


def data_dir() -> Path:
    """The folder holding every database and generated file."""
    override = os.getenv("EDINET_DATA_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve(strict=False)
    return app_dir() / "data"


def app_db_path() -> Path:
    return data_dir() / "app.db"


def chat_db_path() -> Path:
    return data_dir() / "chat.db"


def default_market_db_path() -> Path:
    return data_dir() / "market.db"


def default_filings_db_path() -> Path:
    return data_dir() / "filings.db"


def certs_dir() -> Path:
    return data_dir() / "certs"


def logs_dir() -> Path:
    return data_dir() / "logs"


def artifacts_dir() -> Path:
    return data_dir() / "artifacts"


def tools_dir() -> Path:
    """Helper programs the application downloads for itself."""
    return data_dir() / "tools"


def backtests_dir() -> Path:
    """Saved backtest result folders."""
    return artifacts_dir() / "backtests"


def reports_dir() -> Path:
    """Generated report archives."""
    return artifacts_dir() / "reports"


def exports_dir() -> Path:
    """Screening exports."""
    return artifacts_dir() / "exports"


def jobs_dir() -> Path:
    """Per-job workspaces holding pipeline uploads."""
    return artifacts_dir() / "jobs"


def manual_uploads_dir() -> Path:
    """Uploads for pipelines started outside the job manager."""
    return artifacts_dir() / "manual_jobs"


def downloads_dir() -> Path:
    """Raw EDINET documents fetched by the CSV document step."""
    return artifacts_dir() / "raw_documents"
