"""``worker.db``: the connection details of an enrolled worker.

A single table ``worker_config(key, value)`` holds ``server_url``, ``fingerprint``,
``worker_id``, ``token``, ``name`` and ``enrolled_at``. It is the worker's only local
file; everything else comes from the server at registration.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.paths import app_dir

_SCHEMA = "CREATE TABLE IF NOT EXISTS worker_config (key TEXT PRIMARY KEY, value TEXT)"
_REQUIRED = ("server_url", "fingerprint", "worker_id", "token")


@dataclass(frozen=True)
class WorkerCredentials:
    server_url: str
    fingerprint: str
    worker_id: str
    token: str
    name: str = ""
    enrolled_at: str = ""


def default_path() -> Path:
    return app_dir() / "worker.db"


def load(path: str | Path | None = None) -> WorkerCredentials | None:
    """The saved credentials, or ``None`` when the worker is not enrolled."""
    target = Path(path) if path is not None else default_path()
    if not target.is_file():
        return None
    try:
        with closing(sqlite3.connect(f"{target.resolve().as_uri()}?mode=ro", uri=True)) as conn:
            values = dict(conn.execute("SELECT key, value FROM worker_config").fetchall())
    except sqlite3.DatabaseError:
        return None
    if any(not values.get(key) for key in _REQUIRED):
        return None
    return WorkerCredentials(
        server_url=values["server_url"],
        fingerprint=values["fingerprint"],
        worker_id=values["worker_id"],
        token=values["token"],
        name=values.get("name") or "",
        enrolled_at=values.get("enrolled_at") or "",
    )


def save(path: str | Path | None, credentials: WorkerCredentials) -> Path:
    target = Path(path) if path is not None else default_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    enrolled_at = credentials.enrolled_at or datetime.now(timezone.utc).isoformat()
    rows = {
        "server_url": credentials.server_url,
        "fingerprint": credentials.fingerprint,
        "worker_id": credentials.worker_id,
        "token": credentials.token,
        "name": credentials.name,
        "enrolled_at": enrolled_at,
    }
    with closing(sqlite3.connect(target)) as conn:
        conn.execute(_SCHEMA)
        conn.executemany("INSERT OR REPLACE INTO worker_config (key, value) VALUES (?, ?)", rows.items())
        conn.commit()
    try:
        target.chmod(0o600)  # holds the worker token
    except OSError:  # Windows permissions are advisory here
        pass
    return target
