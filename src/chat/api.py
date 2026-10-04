"""Chat endpoints: channels, end-to-end encrypted conversations, keys, blocks, and live updates."""

from __future__ import annotations

import json
import re
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from src.auth.models import AuthenticatedUser

from . import service
from .channels import TOPICS, company_channel_id, parse_channel_id, resolve_companies, topic_channel_id
from .crypto import E2E_SCHEME, SERVER_SCHEME, channel_associated_data
from .notifier import notifier
from .runtime import cipher, store
from .storage import ChatError

router = APIRouter(prefix="/api/chat", tags=["chat"])

_B64 = r"^[A-Za-z0-9+/]+={0,2}$"
_UUID = r"^[0-9a-fA-F-]{36}$"
_send_limit = service.RateLimiter(limit=20, window=10.0)
_synced_profiles: set[tuple[str, str]] = set()


def _user(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    if (user.user_id, user.username) not in _synced_profiles:
        store.ensure_profile(user.user_id, user.username)
        _synced_profiles.add((user.user_id, user.username))
    return user


def _fail(exc: ChatError) -> HTTPException:
    return HTTPException(status_code=exc.status, detail=str(exc))


def _rate_limit(user: AuthenticatedUser) -> None:
    if not _send_limit.allow(user.user_id):
        raise HTTPException(status_code=429, detail="You are sending messages too quickly; wait a few seconds")


def _active_user(request: Request, user_id: str) -> dict[str, Any] | None:
    """An active account by id, from the account database (or the local user when accounts are off)."""
    current = _user(request)
    if user_id == current.user_id:
        return {"user_id": current.user_id, "username": current.username}
    auth = getattr(request.app.state, "auth_service", None)
    row = auth.store.get_user(user_id) if auth is not None else None
    if row is None or row["status"] != "active":
        return None
    store.ensure_profile(row["user_id"], row["username"])
    return {"user_id": row["user_id"], "username": row["username"]}


# -- request models ------------------------------------------------------------


class ChannelMessageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message_id: str | None = Field(default=None, pattern=_UUID)
    text: str = Field(min_length=1, max_length=service.MAX_TEXT)
    refs: dict[str, dict[str, str]] | None = None
    reply_to: str | None = Field(default=None, max_length=64)


class ReadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seq: int = Field(ge=0)


class KeyWrap(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str = Field(min_length=1, max_length=64)
    recipient_key_id: str = Field(pattern=_UUID)
    key_version: int = Field(ge=1)
    wrapped_key: str = Field(min_length=16, max_length=200, pattern=_B64)
    iv: str = Field(min_length=12, max_length=32, pattern=_B64)


class IdentityKeyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    public_key: str = Field(min_length=40, max_length=400, pattern=_B64)
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    wrapped_private_key: str = Field(min_length=40, max_length=1000, pattern=_B64)
    wrap_salt: str = Field(min_length=16, max_length=64, pattern=_B64)
    wrap_iv: str = Field(min_length=12, max_length=32, pattern=_B64)
    wrap_iterations: int = Field(ge=100_000, le=10_000_000)


class RewrapRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    wrapped_private_key: str = Field(min_length=40, max_length=1000, pattern=_B64)
    wrap_salt: str = Field(min_length=16, max_length=64, pattern=_B64)
    wrap_iv: str = Field(min_length=12, max_length=32, pattern=_B64)
    wrap_iterations: int = Field(ge=100_000, le=10_000_000)


class ConversationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversation_id: str | None = Field(default=None, pattern=_UUID)
    kind: str = Field(pattern=r"^(dm|group)$")
    member_ids: list[str] = Field(min_length=1, max_length=49)
    title: str | None = Field(default=None, max_length=80)
    wrapper_key_id: str = Field(pattern=_UUID)
    wraps: list[KeyWrap] = Field(min_length=1, max_length=50)


class ConversationUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = Field(default=None, min_length=1, max_length=80)
    hidden: bool | None = None


class WrapsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    wrapper_key_id: str = Field(pattern=_UUID)
    wraps: list[KeyWrap] = Field(min_length=1, max_length=500)


class RotateRequest(WrapsRequest):
    key_version: int = Field(ge=2)


class InviteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_ids: list[str] = Field(min_length=1, max_length=49)
    wrapper_key_id: str | None = Field(default=None, pattern=_UUID)
    wraps: list[KeyWrap] = Field(default_factory=list, max_length=500)


class InvitationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    accept: bool


class ConversationMessageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message_id: str = Field(pattern=_UUID)
    ciphertext: str = Field(min_length=16, max_length=24_000, pattern=_B64)
    nonce: str = Field(min_length=12, max_length=32, pattern=_B64)
    key_version: int = Field(ge=1)
    reply_to: str | None = Field(default=None, max_length=64)


# -- summary and live updates --------------------------------------------------


def _conversation_view(user_id: str, conversation: dict[str, Any], unread: int, my_wraps: int = 0) -> dict[str, Any]:
    members = store.members(conversation["conversation_id"])
    profiles = store.profiles(member["user_id"] for member in members)
    keys = {key["user_id"]: key for key in store.public_keys(user_ids=[member["user_id"] for member in members])}
    return {
        "conversation_id": conversation["conversation_id"],
        "kind": conversation["kind"],
        "title": conversation["title"],
        "key_version": conversation["key_version"],
        "rekey_needed": bool(conversation["rekey_needed"]),
        "my_status": conversation["my_status"],
        "my_role": conversation["my_role"],
        "hidden": bool(conversation["hidden"]),
        "last_seq": conversation["last_seq"],
        "last_read_seq": conversation["last_read_seq"],
        "last_message_at": conversation["last_message_at"],
        "unread": unread,
        "my_wraps": my_wraps,
        "members": [
            {
                "user_id": member["user_id"],
                "username": (profiles.get(member["user_id"]) or {}).get("username", "unknown"),
                "display_name": (profiles.get(member["user_id"]) or {}).get("display_name"),
                "role": member["role"],
                "status": member["status"],
                "key_id": (keys.get(member["user_id"]) or {}).get("key_id"),
                "fingerprint": (keys.get(member["user_id"]) or {}).get("fingerprint"),
            }
            for member in members
        ],
    }


def _conversations(user_id: str) -> list[dict[str, Any]]:
    blocked = store.blocked(user_id, "user")
    visible = set(service.visible_conversation_ids(store, user_id, blocked))
    rows = [row for row in store.conversations_for(user_id) if row["conversation_id"] in visible]
    unread = store.conversation_unread(user_id, rows, blocked)
    wraps = store.wrap_counts(user_id)
    return [_conversation_view(user_id, row, unread.get(row["conversation_id"], 0), wraps.get(row["conversation_id"], 0)) for row in rows]


def _channel_list(user_id: str) -> list[dict[str, Any]]:
    subscriptions = store.subscriptions(user_id)
    blocked_channels = store.blocked(user_id, "channel")
    unread = store.channel_unread(user_id, subscriptions, store.blocked(user_id, "user"))
    topics = store.channels(kind="topic")
    subscribed_companies = store.channels([cid for cid in subscriptions if cid.startswith("company:")])
    active = store.active_company_channels(12)
    rows = {row["channel_id"]: row for row in [*topics, *subscribed_companies, *active]}
    result = []
    for channel_id, row in rows.items():
        item = service.describe_channel(channel_id, row)
        item["subscribed"] = channel_id in subscriptions
        item["blocked"] = channel_id in blocked_channels
        item["unread"] = unread.get(channel_id, 0)
        item["last_read_seq"] = subscriptions.get(channel_id)
        result.append(item)
    return result


@router.get("/summary")
def summary(request: Request) -> dict[str, Any]:
    """Everything the chat sidebar needs in one request."""
    user = _user(request)
    key = store.own_key(user.user_id)
    return {
        "me": {"user_id": user.user_id, "username": user.username, "role": user.role, "profile": store.profile(user.user_id)},
        "key": None if key is None else {"key_id": key["key_id"], "fingerprint": key["fingerprint"], "created_at": key["created_at"]},
        "channels": _channel_list(user.user_id),
        "conversations": _conversations(user.user_id),
        "blocks": _blocks_view(user.user_id),
        "cursor": store.latest_seq(),
    }


@router.get("/unread")
def unread(request: Request) -> dict[str, int]:
    """Unread totals for the navigation badge."""
    user = _user(request)
    blocked = store.blocked(user.user_id, "user")
    channel_counts = store.channel_unread(user.user_id, store.subscriptions(user.user_id), blocked)
    visible = set(service.visible_conversation_ids(store, user.user_id, blocked))
    rows = [row for row in store.conversations_for(user.user_id) if row["conversation_id"] in visible]
    conversation_counts = store.conversation_unread(user.user_id, rows, blocked)
    invitations = sum(1 for row in rows if row["my_status"] == "invited")
    direct = sum(conversation_counts.values())
    return {"channels": sum(channel_counts.values()), "conversations": direct, "invitations": invitations, "total": sum(channel_counts.values()) + direct + invitations}


_WATCH = re.compile(r"^(topic|company):[A-Za-z0-9_-]{1,32}$")


@router.get("/updates")
async def updates(
    request: Request,
    since: int = Query(default=0, ge=0),
    wait: float = Query(default=25.0, ge=0, le=55),
    channels: str = Query(default="", max_length=800),
) -> dict[str, Any]:
    """Long poll: new messages after ``since`` in followed and watched channels and in conversations.

    Answers as soon as there is something, or with an empty list after ``wait`` seconds.
    """
    user = await run_in_threadpool(_user, request)
    watched = [item for item in channels.split(",") if _WATCH.match(item)][:20]
    while True:
        event = notifier.waiter()
        result = await run_in_threadpool(service.collect_updates, store, cipher, user.user_id, since, watched)
        if result["messages"] or wait <= 0:
            return result
        if not await notifier.wait(event, wait) or await request.is_disconnected():
            return result
        wait = 0  # one more look, then answer


# -- channels --------------------------------------------------------------------


@router.get("/channels")
def search_channels(request: Request, q: str = Query(default="", max_length=100), limit: int = Query(default=20, ge=1, le=50)) -> dict[str, Any]:
    """Topic channels and companies matching ``q``; a company's channel need not exist yet."""
    user = _user(request)
    query = q.strip().casefold()
    subscriptions = store.subscriptions(user.user_id)
    blocked = store.blocked(user.user_id, "channel")
    results = [
        {**service.describe_channel(topic_channel_id(item.slug)), "subscribed": topic_channel_id(item.slug) in subscriptions}
        for item in TOPICS
        if not query or query in item.name.casefold() or query in item.slug or query in item.description.casefold()
    ]
    if len(query) >= 2:
        from src import security_analysis as security
        from src.web_app.api.security_analysis import _resolve_db

        try:
            matches = security.search_securities(_resolve_db(), q.strip(), limit=limit)
        except HTTPException:
            matches = []
        for match in matches:
            code = match.get("company_code")
            if not code or not parse_channel_id(company_channel_id(code)):
                continue
            channel_id = company_channel_id(code)
            results.append({
                "channel_id": channel_id, "kind": "company", "name": match.get("company_name") or code, "company_code": code,
                "ticker": match.get("ticker"), "industry": match.get("industry"), "subscribed": channel_id in subscriptions,
            })
    for item in results:
        item["blocked"] = item["channel_id"] in blocked
    return {"channels": results[:limit + len(TOPICS)]}


def _channel_detail(user_id: str, channel_id: str) -> dict[str, Any]:
    try:
        info = service.ensure_channel(store, channel_id)
    except ChatError as exc:
        raise _fail(exc) from exc
    subscriptions = store.subscriptions(user_id)
    info["subscribed"] = channel_id in subscriptions
    info["blocked"] = channel_id in store.blocked(user_id, "channel")
    info["last_read_seq"] = subscriptions.get(channel_id)
    return info


@router.get("/channels/{channel_id}")
def channel_detail(request: Request, channel_id: str) -> dict[str, Any]:
    return _channel_detail(_user(request).user_id, channel_id)


@router.get("/channels/{channel_id}/messages")
def channel_messages(
    request: Request,
    channel_id: str,
    before: int | None = Query(default=None, ge=1),
    limit: int = Query(default=60, ge=1, le=200),
) -> dict[str, Any]:
    user = _user(request)
    info = _channel_detail(user.user_id, channel_id)
    rows = store.messages(channel_ids=[channel_id], before=before, limit=limit, exclude_senders=store.blocked(user.user_id, "user"))
    return {"channel": info, "messages": service.serialize_messages(store, cipher, rows), "has_more": len(rows) == limit}


@router.post("/channels/{channel_id}/messages", status_code=status.HTTP_201_CREATED)
def post_channel_message(request: Request, channel_id: str, payload: ChannelMessageRequest) -> dict[str, Any]:
    user = _user(request)
    info = _channel_detail(user.user_id, channel_id)
    if info["blocked"]:
        raise HTTPException(status_code=403, detail="You blocked this channel; unblock it to post here")
    text = payload.text.strip()
    if not text:
        raise HTTPException(status_code=422, detail="A message needs some text")
    _rate_limit(user)
    message_id = payload.message_id or str(uuid.uuid4())
    body = service.channel_body(text, payload.refs)
    sealed = cipher.seal(json.dumps(body, ensure_ascii=False), channel_associated_data(channel_id, user.user_id, message_id))
    try:
        row = store.add_message(
            message_id=message_id, sender_id=user.user_id, channel_id=channel_id, ciphertext=sealed.ciphertext,
            nonce=sealed.nonce, key_version=sealed.key_version, encryption=SERVER_SCHEME, reply_to=payload.reply_to,
            company_codes=[ref["code"] for ref in body["refs"].values()],
        )
    except ChatError as exc:
        raise _fail(exc) from exc
    notifier.notify()
    return service.serialize_messages(store, cipher, [row])[0]


@router.put("/channels/{channel_id}/subscription", status_code=status.HTTP_204_NO_CONTENT)
def subscribe(request: Request, channel_id: str) -> None:
    user = _user(request)
    info = _channel_detail(user.user_id, channel_id)
    if info["blocked"]:
        raise HTTPException(status_code=409, detail="Unblock this channel before subscribing")
    store.subscribe(user.user_id, channel_id)


@router.delete("/channels/{channel_id}/subscription", status_code=status.HTTP_204_NO_CONTENT)
def unsubscribe(request: Request, channel_id: str) -> None:
    store.unsubscribe(_user(request).user_id, channel_id)


@router.post("/channels/{channel_id}/read", status_code=status.HTTP_204_NO_CONTENT)
def read_channel(request: Request, channel_id: str, payload: ReadRequest) -> None:
    store.mark_channel_read(_user(request).user_id, channel_id, payload.seq)


@router.get("/feed")
def feed(
    request: Request,
    before: int | None = Query(default=None, ge=1),
    limit: int = Query(default=80, ge=1, le=200),
    scope: str = Query(default="all", pattern=r"^(all|channels|conversations)$"),
) -> dict[str, Any]:
    """Every followed channel and every conversation, newest last."""
    user = _user(request)
    blocked = store.blocked(user.user_id, "user")
    channel_ids = service.visible_channel_ids(store, user.user_id) if scope != "conversations" else []
    conversation_ids = service.visible_conversation_ids(store, user.user_id, blocked) if scope != "channels" else []
    rows = store.messages(channel_ids=channel_ids, conversation_ids=conversation_ids, before=before, limit=limit, exclude_senders=blocked)
    return {"messages": service.serialize_messages(store, cipher, rows), "has_more": len(rows) == limit}


@router.get("/companies/{edinet_code}/mentions")
def company_mentions(request: Request, edinet_code: str, limit: int = Query(default=30, ge=1, le=100)) -> dict[str, Any]:
    """Public messages in any channel that reference the company."""
    user = _user(request)
    blocked_channels = store.blocked(user.user_id, "channel")
    rows = store.messages(company_code=edinet_code.strip().upper(), limit=limit, exclude_senders=store.blocked(user.user_id, "user"))
    rows = [row for row in rows if row["channel_id"] and row["channel_id"] not in blocked_channels]
    return {"messages": service.serialize_messages(store, cipher, rows)}


@router.get("/resolve")
def resolve(request: Request, tokens: str = Query(default="", max_length=600)) -> dict[str, Any]:
    """Companies for ``$`` reference tokens typed into messages."""
    _user(request)
    found = resolve_companies([token for token in tokens.split(",") if token][:50])
    return {"refs": {token: {"code": item.company_code, "name": item.company_name, "ticker": item.ticker} for token, item in found.items()}}


@router.delete("/messages/{message_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_message(request: Request, message_id: str) -> None:
    """Delete your own message; administrators may also remove public ones."""
    user = _user(request)
    row = store.message(message_id)
    if row is None or row["kind"] != "message" or row["deleted_at"]:
        raise HTTPException(status_code=404, detail="Message not found")
    moderator = user.role == "admin" and row["channel_id"] is not None
    if row["sender_id"] != user.user_id and not moderator:
        raise HTTPException(status_code=403, detail="You can only delete your own messages")
    store.delete_message(message_id, user.user_id)
    notifier.notify()


# -- identity keys -----------------------------------------------------------------


@router.get("/keys/me")
def own_key(request: Request) -> dict[str, Any]:
    """Your active key pair, private half still wrapped under your passphrase."""
    key = store.own_key(_user(request).user_id)
    return {"key": key}


@router.post("/keys", status_code=status.HTTP_201_CREATED)
def publish_key(request: Request, payload: IdentityKeyRequest) -> dict[str, Any]:
    """Publish a new key pair; it replaces any earlier one for new conversations."""
    user = _user(request)
    key = store.add_identity_key(user.user_id, payload.model_dump())
    notifier.notify()
    return {"key": key}


@router.put("/keys/{key_id}/wrap", status_code=status.HTTP_204_NO_CONTENT)
def rewrap_key(request: Request, key_id: str, payload: RewrapRequest) -> None:
    """Store the private key wrapped under a new passphrase."""
    if not store.rewrap_identity_key(_user(request).user_id, key_id, payload.model_dump()):
        raise HTTPException(status_code=404, detail="Key not found")


@router.get("/keys")
def public_keys(request: Request, user_ids: str = Query(default="", max_length=2000), key_ids: str = Query(default="", max_length=4000)) -> dict[str, Any]:
    _user(request)
    users = [item for item in user_ids.split(",") if item][:100]
    keys = [item for item in key_ids.split(",") if item][:100]
    return {"keys": store.public_keys(user_ids=users, key_ids=keys)}


# -- conversations -------------------------------------------------------------------


def _require_member(user_id: str, conversation_id: str, *, active: bool = False) -> dict[str, Any]:
    membership = store.membership(conversation_id, user_id)
    allowed = ("active",) if active else ("active", "invited")
    if membership is None or membership["status"] not in allowed:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return membership


def _check_can_message(request: Request, sender: AuthenticatedUser, target_id: str) -> dict[str, Any]:
    target = _active_user(request, target_id)
    if target is None:
        raise HTTPException(status_code=404, detail="No active account has this id")
    if target_id == sender.user_id:
        raise HTTPException(status_code=400, detail="You cannot start a conversation with yourself")
    if store.blocks_user(sender.user_id, target_id):
        raise HTTPException(status_code=409, detail=f"Unblock {target['username']} first")
    profile = store.profile(target_id) or {}
    if store.blocks_user(target_id, sender.user_id) or profile.get("allow_dms") == "nobody":
        raise HTTPException(status_code=403, detail=f"{target['username']} is not accepting messages")
    return target


@router.get("/conversations")
def list_conversations(request: Request) -> dict[str, Any]:
    return {"conversations": _conversations(_user(request).user_id)}


@router.post("/conversations", status_code=status.HTTP_201_CREATED)
def create_conversation(request: Request, payload: ConversationRequest) -> dict[str, Any]:
    user = _user(request)
    members = [member for member in dict.fromkeys(payload.member_ids) if member != user.user_id]
    if payload.kind == "dm" and len(members) != 1:
        raise HTTPException(status_code=422, detail="A direct conversation has exactly one other person")
    if payload.kind == "group" and not (payload.title or "").strip():
        raise HTTPException(status_code=422, detail="Name the group")
    for member in members:
        _check_can_message(request, user, member)
    if payload.kind == "dm":
        existing = store.find_dm(user.user_id, members[0])
        if existing:
            if store.membership(existing, user.user_id):
                store.set_hidden(user.user_id, existing, False)
            raise HTTPException(status_code=409, detail={"message": "You already have a conversation with this person", "conversation_id": existing})
    try:
        conversation_id = store.create_conversation(
            kind=payload.kind, created_by=user.user_id, member_ids=members, title=(payload.title or "").strip() or None,
            wrapper_key_id=payload.wrapper_key_id, wraps=[wrap.model_dump() for wrap in payload.wraps],
            conversation_id=payload.conversation_id,
        )
    except ChatError as exc:
        raise _fail(exc) from exc
    notifier.notify()
    return _conversation_detail(user.user_id, conversation_id)


def _conversation_detail(user_id: str, conversation_id: str) -> dict[str, Any]:
    for item in _conversations(user_id):
        if item["conversation_id"] == conversation_id:
            return item
    raise HTTPException(status_code=404, detail="Conversation not found")


@router.get("/conversations/{conversation_id}")
def conversation_detail(request: Request, conversation_id: str) -> dict[str, Any]:
    user = _user(request)
    _require_member(user.user_id, conversation_id)
    return _conversation_detail(user.user_id, conversation_id)


@router.patch("/conversations/{conversation_id}")
def update_conversation(request: Request, conversation_id: str, payload: ConversationUpdateRequest) -> dict[str, Any]:
    user = _user(request)
    membership = _require_member(user.user_id, conversation_id)
    if payload.title is not None:
        conversation = store.conversation(conversation_id) or {}
        if conversation.get("kind") != "group" or membership["status"] != "active":
            raise HTTPException(status_code=403, detail="Only members of a group can rename it")
        store.rename(conversation_id, user.user_id, payload.title.strip())
        notifier.notify()
    if payload.hidden is not None:
        store.set_hidden(user.user_id, conversation_id, payload.hidden)
    return _conversation_detail(user.user_id, conversation_id)


@router.get("/conversations/{conversation_id}/keys")
def conversation_keys(request: Request, conversation_id: str) -> dict[str, Any]:
    """Your wrapped copies of each key version, the wrappers' public keys, and who still lacks a copy."""
    user = _user(request)
    _require_member(user.user_id, conversation_id)
    wraps = store.wraps_for(conversation_id, user.user_id)
    wrapper_keys = store.public_keys(key_ids=[wrap["wrapper_key_id"] for wrap in wraps])
    conversation = store.conversation(conversation_id) or {}
    return {
        "key_version": conversation.get("key_version"),
        "rekey_needed": bool(conversation.get("rekey_needed")),
        "wraps": wraps,
        "wrapper_keys": wrapper_keys,
        "coverage": store.wrap_coverage(conversation_id),
    }


@router.post("/conversations/{conversation_id}/keys", status_code=status.HTTP_204_NO_CONTENT)
def share_keys(request: Request, conversation_id: str, payload: WrapsRequest) -> None:
    """Wrap conversation keys you hold for members who lack them (new members or new devices)."""
    user = _user(request)
    _require_member(user.user_id, conversation_id, active=True)
    try:
        store.add_wraps(conversation_id, user.user_id, payload.wrapper_key_id, [wrap.model_dump() for wrap in payload.wraps])
    except ChatError as exc:
        raise _fail(exc) from exc
    notifier.notify()


@router.post("/conversations/{conversation_id}/rotate", status_code=status.HTTP_204_NO_CONTENT)
def rotate_key(request: Request, conversation_id: str, payload: RotateRequest) -> None:
    """Replace the conversation key, for example after someone leaves."""
    user = _user(request)
    _require_member(user.user_id, conversation_id, active=True)
    try:
        store.rotate(conversation_id, user.user_id, payload.key_version, payload.wrapper_key_id, [wrap.model_dump() for wrap in payload.wraps])
    except ChatError as exc:
        raise _fail(exc) from exc
    notifier.notify()


@router.get("/conversations/{conversation_id}/messages")
def conversation_messages(
    request: Request,
    conversation_id: str,
    before: int | None = Query(default=None, ge=1),
    limit: int = Query(default=60, ge=1, le=200),
) -> dict[str, Any]:
    user = _user(request)
    _require_member(user.user_id, conversation_id)
    rows = store.messages(conversation_ids=[conversation_id], before=before, limit=limit, exclude_senders=store.blocked(user.user_id, "user"))
    return {"messages": service.serialize_messages(store, cipher, rows), "has_more": len(rows) == limit}


@router.post("/conversations/{conversation_id}/messages", status_code=status.HTTP_201_CREATED)
def post_conversation_message(request: Request, conversation_id: str, payload: ConversationMessageRequest) -> dict[str, Any]:
    user = _user(request)
    _require_member(user.user_id, conversation_id, active=True)
    conversation = store.conversation(conversation_id) or {}
    if conversation.get("rekey_needed"):
        raise HTTPException(status_code=409, detail="Someone left this conversation; its key must be replaced before sending")
    if payload.key_version != conversation.get("key_version"):
        raise HTTPException(status_code=409, detail="The conversation key changed; reload the conversation")
    if conversation.get("kind") == "dm":
        others = [m["user_id"] for m in store.members(conversation_id) if m["user_id"] != user.user_id]
        if any(store.blocks_user(other, user.user_id) for other in others):
            raise HTTPException(status_code=403, detail="This person is not accepting messages from you")
        if any(store.blocks_user(user.user_id, other) for other in others):
            raise HTTPException(status_code=409, detail="Unblock this person to message them")
    _rate_limit(user)
    try:
        row = store.add_message(
            message_id=payload.message_id, sender_id=user.user_id, conversation_id=conversation_id, ciphertext=payload.ciphertext,
            nonce=payload.nonce, key_version=payload.key_version, encryption=E2E_SCHEME, reply_to=payload.reply_to,
        )
    except ChatError as exc:
        raise _fail(exc) from exc
    notifier.notify()
    return service.serialize_messages(store, cipher, [row])[0]


@router.post("/conversations/{conversation_id}/members", status_code=status.HTTP_201_CREATED)
def invite_members(request: Request, conversation_id: str, payload: InviteRequest) -> dict[str, Any]:
    user = _user(request)
    _require_member(user.user_id, conversation_id, active=True)
    if (store.conversation(conversation_id) or {}).get("kind") != "group":
        raise HTTPException(status_code=400, detail="People can only be added to group conversations")
    for member in payload.user_ids:
        _check_can_message(request, user, member)
    if len(store.members(conversation_id)) + len(payload.user_ids) > 50:
        raise HTTPException(status_code=400, detail="A group can have at most 50 members")
    try:
        invited = store.invite(conversation_id, user.user_id, payload.user_ids, [wrap.model_dump() for wrap in payload.wraps], payload.wrapper_key_id)
    except ChatError as exc:
        raise _fail(exc) from exc
    notifier.notify()
    return {"invited": invited, "conversation": _conversation_detail(user.user_id, conversation_id)}


@router.delete("/conversations/{conversation_id}/members/{member_id}", status_code=status.HTTP_204_NO_CONTENT)
def remove_member(request: Request, conversation_id: str, member_id: str) -> None:
    """Leave (your own id) or, as the group's owner, remove someone."""
    user = _user(request)
    membership = _require_member(user.user_id, conversation_id)
    conversation = store.conversation(conversation_id) or {}
    if conversation.get("kind") != "group":
        raise HTTPException(status_code=400, detail="Direct conversations cannot be left; hide or block instead")
    if member_id != user.user_id and membership["role"] != "owner":
        raise HTTPException(status_code=403, detail="Only the group's owner can remove people")
    target = store.membership(conversation_id, member_id)
    if target is None or target["status"] not in ("active", "invited"):
        raise HTTPException(status_code=404, detail="Not a member of this conversation")
    store.remove_member(conversation_id, user.user_id, member_id)
    notifier.notify()


@router.post("/conversations/{conversation_id}/invitation", status_code=status.HTTP_204_NO_CONTENT)
def respond_to_invitation(request: Request, conversation_id: str, payload: InvitationResponse) -> None:
    user = _user(request)
    try:
        store.respond_to_invitation(conversation_id, user.user_id, payload.accept)
    except ChatError as exc:
        raise _fail(exc) from exc
    notifier.notify()


@router.post("/conversations/{conversation_id}/read", status_code=status.HTTP_204_NO_CONTENT)
def read_conversation(request: Request, conversation_id: str, payload: ReadRequest) -> None:
    user = _user(request)
    _require_member(user.user_id, conversation_id)
    store.mark_conversation_read(user.user_id, conversation_id, payload.seq)


# -- blocks ----------------------------------------------------------------------------


def _blocks_view(user_id: str) -> dict[str, list[dict[str, Any]]]:
    rows = store.blocks(user_id)
    users = [row for row in rows if row["target_kind"] == "user"]
    profiles = store.profiles(row["target_id"] for row in users)
    channel_rows = {row["channel_id"]: row for row in store.channels([row["target_id"] for row in rows if row["target_kind"] == "channel"])}
    return {
        "users": [
            {"user_id": row["target_id"], "username": (profiles.get(row["target_id"]) or {}).get("username", "unknown"),
             "display_name": (profiles.get(row["target_id"]) or {}).get("display_name"), "created_at": row["created_at"]}
            for row in users
        ],
        "channels": [
            {**service.describe_channel(row["target_id"], channel_rows.get(row["target_id"])), "created_at": row["created_at"]}
            for row in rows if row["target_kind"] == "channel"
        ],
    }


@router.get("/blocks")
def list_blocks(request: Request) -> dict[str, Any]:
    return _blocks_view(_user(request).user_id)


@router.put("/blocks/{kind}/{target_id}", status_code=status.HTTP_204_NO_CONTENT)
def block(request: Request, kind: str, target_id: str) -> None:
    """Hide a person's messages everywhere and stop them messaging you, or hide a channel."""
    user = _user(request)
    if kind == "user":
        if target_id == user.user_id:
            raise HTTPException(status_code=400, detail="You cannot block yourself")
        if _active_user(request, target_id) is None and store.profile(target_id) is None:
            raise HTTPException(status_code=404, detail="No account has this id")
    elif kind == "channel":
        if parse_channel_id(target_id) is None:
            raise HTTPException(status_code=404, detail="Channel not found")
        try:
            service.ensure_channel(store, target_id)
        except ChatError as exc:
            raise _fail(exc) from exc
    else:
        raise HTTPException(status_code=404, detail="Unknown block kind")
    store.block(user.user_id, kind, target_id)


@router.delete("/blocks/{kind}/{target_id}", status_code=status.HTTP_204_NO_CONTENT)
def unblock(request: Request, kind: str, target_id: str) -> None:
    if kind not in {"user", "channel"} or not store.unblock(_user(request).user_id, kind, target_id):
        raise HTTPException(status_code=404, detail="Block not found")
