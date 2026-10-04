"""Public profiles: what signed-in users show each other, and a directory to find people."""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator

from . import service
from .api import _user
from .channels import resolve_companies
from .runtime import cipher, store

router = APIRouter(prefix="/api/profiles", tags=["profiles"])

_WEBSITE = re.compile(r"^https?://[^\s/$.?#].[^\s]*$", re.IGNORECASE)
_EDINET = re.compile(r"^E\d{5}$")


class ProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    display_name: str | None = Field(default=None, max_length=60)
    bio: str | None = Field(default=None, max_length=1000)
    location: str | None = Field(default=None, max_length=80)
    website: str | None = Field(default=None, max_length=200)
    interests: list[str] | None = Field(default=None, max_length=12)
    companies: list[str] | None = Field(default=None, max_length=12)
    allow_dms: str | None = Field(default=None, pattern=r"^(everyone|nobody)$")
    discoverable: bool | None = None

    @field_validator("website")
    @classmethod
    def _website(cls, value: str | None) -> str | None:
        if value and value.strip() and not _WEBSITE.match(value.strip()):
            raise ValueError("Use a full http:// or https:// address")
        return value

    @field_validator("interests")
    @classmethod
    def _interests(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        cleaned = [item.strip()[:30] for item in value if item.strip()]
        return list(dict.fromkeys(cleaned))

    @field_validator("companies")
    @classmethod
    def _companies(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        codes = [item.strip().upper() for item in value if item.strip()]
        if any(not _EDINET.match(code) for code in codes):
            raise ValueError("Companies are EDINET codes such as E02144")
        return list(dict.fromkeys(codes))


def _accounts(request: Request) -> list[dict[str, Any]]:
    auth = getattr(request.app.state, "auth_service", None)
    if auth is None:
        return []
    return [dict(row) for row in auth.store.list_users() if row["status"] == "active"]


def _public(profile: dict[str, Any], viewer_id: str, *, account: dict[str, Any] | None = None) -> dict[str, Any]:
    companies = profile.get("companies") or []
    names = resolve_companies(companies) if companies else {}
    key = store.public_keys(user_ids=[profile["user_id"]])
    return {
        "user_id": profile["user_id"],
        "username": profile["username"],
        "display_name": profile.get("display_name"),
        "bio": profile.get("bio"),
        "location": profile.get("location"),
        "website": profile.get("website"),
        "interests": profile.get("interests") or [],
        "companies": [{"code": code, "name": names[code].company_name if code in names else code} for code in companies],
        "accepts_messages": profile.get("allow_dms") != "nobody",
        "member_since": (account or {}).get("created_at") or profile.get("created_at"),
        "role": (account or {}).get("role"),
        "key_fingerprint": key[0]["fingerprint"] if key else None,
        "is_me": profile["user_id"] == viewer_id,
    }


@router.get("/me")
def my_profile(request: Request) -> dict[str, Any]:
    user = _user(request)
    profile = store.profile(user.user_id) or {}
    return {**profile, "public": _public(profile, user.user_id)}


@router.patch("/me")
def update_my_profile(request: Request, payload: ProfileUpdate) -> dict[str, Any]:
    user = _user(request)
    fields = payload.model_dump(exclude_unset=True)
    for key in ("display_name", "bio", "location", "website"):
        if key in fields and fields[key] is not None:
            fields[key] = fields[key].strip() or None
    profile = store.update_profile(user.user_id, fields)
    return {**profile, "public": _public(profile, user.user_id)}


@router.get("")
def directory(request: Request, q: str = Query(default="", max_length=60), limit: int = Query(default=20, ge=1, le=100)) -> dict[str, Any]:
    """People to message or invite: discoverable accounts matching ``q`` by username or display name."""
    user = _user(request)
    query = q.strip().casefold()
    accounts = {row["user_id"]: row for row in _accounts(request)}
    accounts.setdefault(user.user_id, {"user_id": user.user_id, "username": user.username})
    profiles = store.profiles(accounts)
    blocked = store.blocked(user.user_id, "user")
    people = []
    for user_id, account in accounts.items():
        profile = profiles.get(user_id) or {"username": account["username"], "display_name": None, "discoverable": True, "allow_dms": "everyone"}
        username = str(account["username"])
        haystack = f"{username} {profile.get('display_name') or ''}".casefold()
        exact = query and username.casefold() == query
        if query and query not in haystack:
            continue
        if not profile.get("discoverable", True) and not exact and user_id != user.user_id:
            continue
        people.append({
            "user_id": user_id,
            "username": username,
            "display_name": profile.get("display_name"),
            "accepts_messages": profile.get("allow_dms") != "nobody",
            "blocked": user_id in blocked,
            "is_me": user_id == user.user_id,
        })
    people.sort(key=lambda item: (not item["username"].casefold().startswith(query), item["username"]))
    return {"people": people[:limit]}


@router.get("/{username}")
def profile(request: Request, username: str) -> dict[str, Any]:
    """Someone's public profile, their recent public posts, and whether you can message them."""
    viewer = _user(request)
    name = username.strip().casefold()
    account = next((row for row in _accounts(request) if str(row["username"]).casefold() == name), None)
    if account is None and name == viewer.username.casefold():
        account = {"user_id": viewer.user_id, "username": viewer.username}
    if account is None:
        raise HTTPException(status_code=404, detail="No active account has this username")
    store.ensure_profile(account["user_id"], account["username"])
    found = store.profile(account["user_id"]) or {}
    blocked_channels = store.blocked(viewer.user_id, "channel")
    posts = store.messages(public_only=True, sender_id=account["user_id"], limit=15)
    posts = [row for row in posts if row["kind"] == "message" and not row["deleted_at"] and row["channel_id"] not in blocked_channels]
    channel_names = {row["channel_id"]: row["name"] for row in store.channels({row["channel_id"] for row in posts})}
    stats = store.message_stats(account["user_id"])
    stats["top_channels"] = [{**item, "name": channel_names.get(item["channel_id"]) or item["channel_id"]} for item in stats["top_channels"]]
    return {
        **_public(found, viewer.user_id, account=account),
        "blocked_by_me": store.blocks_user(viewer.user_id, account["user_id"]),
        "can_message": (
            account["user_id"] != viewer.user_id
            and found.get("allow_dms") != "nobody"
            and not store.blocks_user(account["user_id"], viewer.user_id)
        ),
        "stats": stats,
        "recent_posts": [
            {**item, "channel_name": channel_names.get(item["channel_id"]) or item["channel_id"]}
            for item in service.serialize_messages(store, cipher, posts)
        ],
    }
