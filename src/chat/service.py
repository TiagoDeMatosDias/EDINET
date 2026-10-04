"""Chat operations shared by the routes: message bodies, serialization, visibility."""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import deque
from typing import Any, Iterable

from .channels import Company, company_channel_id, parse_channel_id, resolve_companies, topic
from .crypto import E2E_SCHEME, SERVER_SCHEME, ChannelCipher, Sealed, channel_associated_data
from .storage import ChatError, ChatStore

logger = logging.getLogger(__name__)

MAX_TEXT = 4000
# "$7203", "$130A", "$E02144": a TSE code or an EDINET code after a dollar sign.
COMPANY_TOKEN = re.compile(r"(?<![\w$])\$(E\d{5}|\d{3}[0-9A-Z])(?![\w])")
_EDINET = re.compile(r"^E\d{5}$")


def company_tokens(text: str) -> list[str]:
    return list(dict.fromkeys(match.group(1) for match in COMPANY_TOKEN.finditer(text)))


def clean_refs(refs: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    """Keep well-formed ``token → {code, name}`` references the client resolved."""
    cleaned: dict[str, dict[str, str]] = {}
    for token, value in list((refs or {}).items())[:20]:
        if not isinstance(value, dict):
            continue
        code, name = str(value.get("code") or ""), str(value.get("name") or "")[:200]
        if len(token) <= 12 and _EDINET.match(code):
            cleaned[token] = {"code": code, "name": name or code}
    return cleaned


def channel_body(text: str, refs: dict[str, Any] | None) -> dict[str, Any]:
    """The stored body of a public message, with every ``$`` reference it can resolve."""
    body_refs = clean_refs(refs)
    missing = [token for token in company_tokens(text) if token not in body_refs]
    for token, found in resolve_companies(missing).items():
        body_refs[token] = {"code": found.company_code, "name": found.company_name}
    return {"text": text, "refs": body_refs}


def describe_channel(channel_id: str, row: dict[str, Any] | None = None, company: Company | None = None) -> dict[str, Any]:
    parsed = parse_channel_id(channel_id)
    kind, value = parsed if parsed else ("topic", channel_id)
    info: dict[str, Any] = {
        "channel_id": channel_id,
        "kind": kind,
        "name": (row or {}).get("name") or (company.company_name if company else value),
        "description": (row or {}).get("description"),
        "subscribers": (row or {}).get("subscribers", 0),
        "message_count": (row or {}).get("message_count", 0),
        "last_message_at": (row or {}).get("last_message_at"),
    }
    if kind == "topic":
        found = topic(value)
        info["slug"] = value
        info["name"] = found.name if found else value
        info["description"] = found.description if found else None
    else:
        info["company_code"] = value
        if company:
            info["ticker"] = company.ticker
    return info


def ensure_channel(store: ChatStore, channel_id: str) -> dict[str, Any]:
    """The channel's row, creating a company's channel the first time it is opened."""
    parsed = parse_channel_id(channel_id)
    if parsed is None:
        raise ChatError("Channel not found", 404)
    rows = store.channels([channel_id])
    if rows:
        return describe_channel(channel_id, rows[0])
    kind, value = parsed
    if kind != "company":
        raise ChatError("Channel not found", 404)
    found = resolve_companies([value]).get(value)
    if found is None:
        raise ChatError("No company has this EDINET code", 404)
    store.ensure_company_channel(channel_id, found.company_name, f"Discussion of {found.company_name}")
    return describe_channel(channel_id, store.channels([channel_id])[0], found)


def company_channel(store: ChatStore, edinet_code: str) -> dict[str, Any]:
    return ensure_channel(store, company_channel_id(edinet_code))


def _sender(profiles: dict[str, dict[str, Any]], user_id: str) -> dict[str, Any]:
    profile = profiles.get(user_id) or {}
    return {"user_id": user_id, "username": profile.get("username") or "unknown", "display_name": profile.get("display_name")}


def serialize_messages(store: ChatStore, cipher: ChannelCipher, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    profiles = store.profiles(row["sender_id"] for row in rows)
    result = []
    for row in rows:
        item: dict[str, Any] = {
            "seq": row["seq"],
            "message_id": row["message_id"],
            "kind": row["kind"],
            "channel_id": row["channel_id"],
            "conversation_id": row["conversation_id"],
            "sender": _sender(profiles, row["sender_id"]),
            "created_at": row["created_at"],
            "reply_to": row["reply_to"],
            "deleted": row["deleted_at"] is not None,
        }
        if row["kind"] == "system":
            item["system"] = json.loads(row["system_json"] or "{}")
        elif item["deleted"]:
            pass
        elif row["encryption"] == E2E_SCHEME:
            item["encrypted"] = {"ciphertext": row["ciphertext"], "nonce": row["nonce"], "key_version": row["key_version"]}
        elif row["encryption"] == SERVER_SCHEME:
            try:
                plaintext = cipher.open(
                    Sealed(row["ciphertext"], row["nonce"], row["key_version"]),
                    channel_associated_data(row["channel_id"], row["sender_id"], row["message_id"]),
                )
                item["body"] = json.loads(plaintext)
            except Exception:  # noqa: BLE001 - one unreadable row must not hide the others
                logger.exception("Chat message %s could not be decrypted", row["message_id"])
                item["body"] = {"text": "", "refs": {}, "unreadable": True}
        result.append(item)
    return result


class RateLimiter:
    """At most ``limit`` events per ``window`` seconds for each key."""

    def __init__(self, limit: int, window: float) -> None:
        self.limit, self.window = limit, window
        self._events: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            events = self._events.setdefault(key, deque())
            while events and now - events[0] > self.window:
                events.popleft()
            if len(events) >= self.limit:
                return False
            events.append(now)
            return True


def visible_channel_ids(store: ChatStore, user_id: str, watched: Iterable[str] = ()) -> list[str]:
    """Subscribed channels plus any being watched, minus blocked ones."""
    blocked = store.blocked(user_id, "channel")
    ids = [*store.subscriptions(user_id), *watched]
    return [channel_id for channel_id in dict.fromkeys(ids) if channel_id not in blocked and parse_channel_id(channel_id)]


def visible_conversation_ids(store: ChatStore, user_id: str, blocked_users: set[str]) -> list[str]:
    """Current conversations, leaving out direct ones with someone the user blocked."""
    ids = []
    for conversation in store.conversations_for(user_id):
        if conversation["kind"] == "dm":
            others = [m["user_id"] for m in store.members(conversation["conversation_id"]) if m["user_id"] != user_id]
            if any(other in blocked_users for other in others):
                continue
        ids.append(conversation["conversation_id"])
    return ids


def collect_updates(store: ChatStore, cipher: ChannelCipher, user_id: str, since: int, watched: list[str]) -> dict[str, Any]:
    # Read the high-water mark first: anything stored after it is left for the next poll, never skipped.
    latest = store.latest_seq()
    blocked_users = store.blocked(user_id, "user")
    rows = store.messages(
        channel_ids=visible_channel_ids(store, user_id, watched),
        conversation_ids=visible_conversation_ids(store, user_id, blocked_users),
        after=since,
        limit=200,
        exclude_senders=blocked_users,
    )
    rows = [row for row in rows if row["seq"] <= latest]
    cursor = rows[-1]["seq"] if len(rows) == 200 else max(latest, since)
    return {"cursor": cursor, "messages": serialize_messages(store, cipher, rows)}
