"""Owner-scoped persistence for the most recent screening result.

Screening results are deliberately kept in the portfolio database because
they are user workspace state and should survive a trip away from the Screen
page without rerunning the expensive query.  One row per user means a new run
atomically replaces that user's previous result.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.orchestrator.common.db_config import get_db3
from src.orchestrator.common.sqlite import connect_read, transaction
from src.portfolio.schema import create_tables


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_default(value: Any) -> Any:
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:
            pass
    return str(value)


def _path(db_path: str | None = None) -> str:
    resolved = str(db_path or get_db3()).strip()
    if not resolved:
        raise ValueError("Portfolio database is not configured")
    return resolved


def replace_last_screening_result(
    user_id: str,
    result: dict[str, Any],
    definition: dict[str, Any],
    *,
    db_path: str | None = None,
) -> dict[str, Any]:
    """Replace the latest result for one user and return its metadata."""
    if not user_id:
        raise ValueError("user_id is required")
    path = _path(db_path)
    create_tables(path)
    now = _timestamp()
    result_json = json.dumps(result, ensure_ascii=False, default=_json_default, separators=(",", ":"))
    definition_json = json.dumps(definition, ensure_ascii=False, default=_json_default, separators=(",", ":"))
    row_count = int(result.get("row_count") or 0)
    with transaction(path) as conn:
        conn.execute(
            """INSERT INTO Screening_Results
               (user_id, result_json, definition_json, row_count, requested_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
                 result_json = excluded.result_json,
                 definition_json = excluded.definition_json,
                 row_count = excluded.row_count,
                 requested_at = excluded.requested_at,
                 updated_at = excluded.updated_at""",
            (user_id, result_json, definition_json, row_count, now, now),
        )
    return {
        "user_id": user_id,
        "row_count": row_count,
        "requested_at": now,
        "updated_at": now,
    }


def get_last_screening_result(
    user_id: str,
    *,
    db_path: str | None = None,
) -> dict[str, Any] | None:
    """Load the latest screening result for one user, if one exists."""
    if not user_id:
        raise ValueError("user_id is required")
    path = _path(db_path)
    try:
        conn = connect_read(path)
    except (FileNotFoundError, OSError):
        return None
    try:
        row = conn.execute(
            "SELECT user_id, result_json, definition_json, row_count, requested_at, updated_at "
            "FROM Screening_Results WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    except Exception:
        return None
    finally:
        conn.close()
    if row is None:
        return None
    try:
        result = json.loads(row["result_json"])
        definition = json.loads(row["definition_json"])
    except (TypeError, json.JSONDecodeError):
        return None
    return {
        "user_id": row["user_id"],
        "result": result,
        "definition": definition,
        "row_count": int(row["row_count"] or 0),
        "requested_at": row["requested_at"],
        "updated_at": row["updated_at"],
    }

