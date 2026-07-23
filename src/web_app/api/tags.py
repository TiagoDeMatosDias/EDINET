"""Company tags backed by the Company_Tags database table.

Tags are simple key-value pairs (edinetCode → tag) stored in the screening
database.  The screening engine auto-discovers the table so users can add
criteria like ``Company_Tags.tag = 'Watchlist'`` through the normal rules
builder — no special UI is needed.

Tag management (add / remove) lives on the company analysis page.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.orchestrator.common.db_config import get_db2
from src.orchestrator.common.sqlite import connect_read, transaction

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


def _database_path() -> Path:
    """Return the configured screening database path."""
    path = get_db2()
    if not path:
        raise HTTPException(status_code=503, detail="Database not available")
    return Path(path)


def _ensure_table(path: Path) -> None:
    with transaction(path) as connection:
        connection.execute(_CREATE_TABLE_SQL)


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


class TagListResponse(BaseModel):
    tags: list[TagSummary]


class CompanyTagsResponse(BaseModel):
    tags: list[str]


class TagMutationResponse(BaseModel):
    ok: bool
    company_code: str
    tag: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=TagListResponse)
def list_all_tags() -> TagListResponse:
    """Return every distinct tag with its member count."""
    path = _database_path()
    _ensure_table(path)
    conn = connect_read(path)
    try:
        rows = conn.execute(
            "SELECT tag, COUNT(*) AS cnt"
            " FROM Company_Tags"
            " GROUP BY tag"
            " ORDER BY tag"
        ).fetchall()
    finally:
        conn.close()

    return TagListResponse(
        tags=[TagSummary(name=row[0], member_count=row[1]) for row in rows]
    )


@router.get("/{company_code}", response_model=CompanyTagsResponse)
def get_company_tags(company_code: str) -> CompanyTagsResponse:
    """Return the tags assigned to a single company."""
    path = _database_path()
    _ensure_table(path)
    conn = connect_read(path)
    try:
        rows = conn.execute(
            "SELECT tag FROM Company_Tags WHERE edinetCode = ? ORDER BY tag",
            [company_code.strip()],
        ).fetchall()
    finally:
        conn.close()

    return CompanyTagsResponse(tags=[row[0] for row in rows])


@router.post("/{company_code}/{tag}", response_model=TagMutationResponse)
def add_tag(company_code: str, tag: str) -> TagMutationResponse:
    """Assign a tag to a company (idempotent)."""
    code = company_code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="company_code is required.")

    cleaned = _clean_tag(tag)
    with transaction(_database_path()) as connection:
        connection.execute(_CREATE_TABLE_SQL)
        connection.execute(
            "INSERT OR IGNORE INTO Company_Tags (edinetCode, tag) VALUES (?, ?)",
            [code, cleaned],
        )

    return TagMutationResponse(ok=True, company_code=code, tag=cleaned)


@router.delete("/{company_code}/{tag}", response_model=TagMutationResponse)
def remove_tag(company_code: str, tag: str) -> TagMutationResponse:
    """Remove a tag from a company."""
    code = company_code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="company_code is required.")

    cleaned = _clean_tag(tag)
    with transaction(_database_path()) as connection:
        connection.execute(_CREATE_TABLE_SQL)
        connection.execute(
            "DELETE FROM Company_Tags WHERE edinetCode = ? AND tag = ?",
            [code, cleaned],
        )

    return TagMutationResponse(ok=True, company_code=code, tag=cleaned)
