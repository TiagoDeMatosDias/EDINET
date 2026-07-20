"""Company tags backed by the Company_Tags database table.

Tags are simple key-value pairs (edinetCode → tag) stored in the screening
database.  The screening engine auto-discovers the table so users can add
criteria like ``Company_Tags.tag = 'Watchlist'`` through the normal rules
builder — no special UI is needed.

Tag management (add / remove) lives on the company analysis page.
"""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.orchestrator.common.db_config import get_db2

router = APIRouter(prefix="/api/tags", tags=["tags"])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS Company_Tags ("
    "  edinetCode TEXT NOT NULL,"
    "  tag        TEXT NOT NULL,"
    "  PRIMARY KEY (edinetCode, tag)"
    ")"
)


def _get_db() -> sqlite3.Connection:
    """Return a connection to the default screening database."""
    path = get_db2()
    if not path:
        raise HTTPException(status_code=503, detail="Database not available")
    conn = sqlite3.connect(path)
    conn.execute(_CREATE_TABLE_SQL)
    conn.commit()
    return conn


def _clean_tag(tag: str) -> str:
    cleaned = tag.strip()
    if not cleaned or len(cleaned) > 80:
        raise HTTPException(status_code=400, detail="Tag must be 1–80 characters.")
    return cleaned


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class TagSummary(BaseModel):
    name: str
    member_count: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("")
def list_all_tags() -> dict:
    """Return every distinct tag with its member count."""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT tag, COUNT(*) AS cnt"
            " FROM Company_Tags"
            " GROUP BY tag"
            " ORDER BY tag"
        ).fetchall()
    finally:
        conn.close()

    return {"tags": [{"name": r[0], "member_count": r[1]} for r in rows]}


@router.get("/{company_code}")
def get_company_tags(company_code: str) -> dict:
    """Return the tags assigned to a single company."""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT tag FROM Company_Tags WHERE edinetCode = ? ORDER BY tag",
            [company_code.strip()],
        ).fetchall()
    finally:
        conn.close()

    return {"tags": [r[0] for r in rows]}


@router.post("/{company_code}/{tag}")
def add_tag(company_code: str, tag: str) -> dict:
    """Assign a tag to a company (idempotent)."""
    code = company_code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="company_code is required.")

    cleaned = _clean_tag(tag)
    conn = _get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO Company_Tags (edinetCode, tag) VALUES (?, ?)",
            [code, cleaned],
        )
        conn.commit()
    finally:
        conn.close()

    return {"ok": True, "company_code": code, "tag": cleaned}


@router.delete("/{company_code}/{tag}")
def remove_tag(company_code: str, tag: str) -> dict:
    """Remove a tag from a company."""
    code = company_code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="company_code is required.")

    cleaned = _clean_tag(tag)
    conn = _get_db()
    try:
        conn.execute(
            "DELETE FROM Company_Tags WHERE edinetCode = ? AND tag = ?",
            [code, cleaned],
        )
        conn.commit()
    finally:
        conn.close()

    return {"ok": True, "company_code": code, "tag": cleaned}
