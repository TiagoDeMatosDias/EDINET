"""Company tags backed by the per-user research database.

Tags are stored in ``research.db.company_tags``, scoped to each user.
The screening engine treats ``Company_Tags`` as a synthetic owner-scoped
source built from the current user's tags at query time.

Legacy tags from ``Standardized.db.Company_Tags`` are available for admin
claim via a migration endpoint.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from src.auth.models import AuthenticatedUser
from src.research.runtime import store as _research_store

router = APIRouter(prefix="/api/tags", tags=["tags"])


def _require_user(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    return user


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
def list_all_tags(request: Request) -> TagListResponse:
    """Return every distinct tag for the authenticated user with member counts."""
    user = _require_user(request)
    all_tags = _research_store.list_all_tags(user.user_id)
    # Aggregate by tag name
    counts: dict[str, int] = {}
    for t in all_tags:
        tag_name = t["tag"]
        counts[tag_name] = counts.get(tag_name, 0) + 1
    return TagListResponse(
        tags=[TagSummary(name=k, member_count=v) for k, v in sorted(counts.items())]
    )


@router.get("/{company_code}", response_model=CompanyTagsResponse)
def get_company_tags(request: Request, company_code: str) -> CompanyTagsResponse:
    """Return the tags assigned to a single company for the authenticated user."""
    user = _require_user(request)
    rows = _research_store.list_company_tags(user.user_id, company_code.strip())
    return CompanyTagsResponse(tags=[r["tag"] for r in rows])


@router.post("/{company_code}/{tag}", response_model=TagMutationResponse)
def add_tag(request: Request, company_code: str, tag: str) -> TagMutationResponse:
    """Assign a tag to a company for the authenticated user (idempotent)."""
    user = _require_user(request)
    code = company_code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="company_code is required.")
    cleaned = tag.strip()
    if not cleaned or len(cleaned) > 80:
        raise HTTPException(status_code=400, detail="Tag must be 1–80 characters.")

    existing = _research_store.list_company_tags(user.user_id, code)
    if not any(t["tag"] == cleaned for t in existing):
        _research_store.set_company_tags(user.user_id, code, [t["tag"] for t in existing] + [cleaned])

    return TagMutationResponse(ok=True, company_code=code, tag=cleaned)


@router.delete("/{company_code}/{tag}", response_model=TagMutationResponse)
def remove_tag(request: Request, company_code: str, tag: str) -> TagMutationResponse:
    """Remove a tag from a company for the authenticated user."""
    user = _require_user(request)
    code = company_code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="company_code is required.")
    cleaned = tag.strip()
    if not cleaned or len(cleaned) > 80:
        raise HTTPException(status_code=400, detail="Tag must be 1–80 characters.")

    existing = _research_store.list_company_tags(user.user_id, code)
    remaining = [t["tag"] for t in existing if t["tag"] != cleaned]
    _research_store.set_company_tags(user.user_id, code, remaining)

    return TagMutationResponse(ok=True, company_code=code, tag=cleaned)


# ---------------------------------------------------------------------------
# Legacy migration (admin only)
# ---------------------------------------------------------------------------


@router.post("/migrate-legacy")
def migrate_legacy_tags(request: Request) -> dict:
    """Copy tags from Standardized.db.Company_Tags into research.db for the
    authenticated user.  Only administrators may call this endpoint."""
    user = _require_user(request)
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Administrator permission required")

    from src.orchestrator.common.db_config import get_db2
    from src.orchestrator.common.sqlite import connect_read

    try:
        conn = connect_read(get_db2())
        rows = conn.execute(
            "SELECT edinetCode, tag FROM Company_Tags ORDER BY edinetCode, tag"
        ).fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read legacy tags: {e}") from e

    if not rows:
        return {"migrated": 0, "message": "No legacy tags found in Standardized.db"}

    imported = 0
    for row in rows:
        code = row["edinetCode"] or row[0]
        tag = row["tag"] or row[1]
        existing = _research_store.list_company_tags(user.user_id, code)
        existing_tags = [t["tag"] for t in existing]
        if tag not in existing_tags:
            _research_store.set_company_tags(user.user_id, code, existing_tags + [tag])
            imported += 1

    return {"migrated": imported, "message": f"Migrated {imported} legacy tags to your account"}
