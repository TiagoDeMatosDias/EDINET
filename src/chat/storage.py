"""SQLite storage for chat: profiles, encryption keys, channels, conversations, messages.

Two kinds of message share one table and one ordering (``seq``):

* Channel messages are public to signed-in users. The API encrypts them at
  rest with the server key ring (``crypto.ChannelCipher``) before they reach
  this store.
* Conversation messages (direct and group) are end-to-end encrypted. Browsers
  encrypt them with a per-conversation AES key; the store only ever holds
  ciphertext, plus that key wrapped separately for each member under an
  ECDH-derived key (``conversation_keys``). The server cannot unwrap them.

Identity keys hold each user's ECDH public key and their private key wrapped
under a passphrase-derived key, so a user can unlock it on any device. The
passphrase never reaches the server.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.orchestrator.common.sqlite import connect_write, initialize_managed_database, transaction

from .channels import TOPICS, topic_channel_id

ACTIVE_MEMBER_STATUSES = ("active", "invited")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _placeholders(values: Iterable[Any]) -> str:
    return ",".join("?" for _ in values)


class ChatError(ValueError):
    """A request the chat store refuses; ``status`` is the HTTP status to answer with."""

    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


class ChatStore:
    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 30_000) -> None:
        self.path = Path(path).expanduser()
        self.busy_timeout_ms = busy_timeout_ms
        self.initialize()

    # -- schema --------------------------------------------------------------

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            initialize_managed_database(conn)
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS profiles (
                    user_id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    display_name TEXT,
                    bio TEXT,
                    location TEXT,
                    website TEXT,
                    interests_json TEXT NOT NULL DEFAULT '[]',
                    companies_json TEXT NOT NULL DEFAULT '[]',
                    allow_dms TEXT NOT NULL DEFAULT 'everyone' CHECK (allow_dms IN ('everyone', 'nobody')),
                    discoverable INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_profiles_username ON profiles(username);
                CREATE TABLE IF NOT EXISTS identity_keys (
                    key_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    public_key TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    wrapped_private_key TEXT NOT NULL,
                    wrap_salt TEXT NOT NULL,
                    wrap_iv TEXT NOT NULL,
                    wrap_iterations INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    revoked_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_identity_keys_user ON identity_keys(user_id, created_at DESC);
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL CHECK (kind IN ('topic', 'company')),
                    name TEXT NOT NULL,
                    description TEXT,
                    position INTEGER NOT NULL DEFAULT 1000,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS channel_subscriptions (
                    user_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL REFERENCES channels(channel_id) ON DELETE CASCADE,
                    last_read_seq INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (user_id, channel_id)
                );
                CREATE INDEX IF NOT EXISTS idx_subscriptions_channel ON channel_subscriptions(channel_id);
                CREATE TABLE IF NOT EXISTS blocks (
                    user_id TEXT NOT NULL,
                    target_kind TEXT NOT NULL CHECK (target_kind IN ('user', 'channel')),
                    target_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (user_id, target_kind, target_id)
                );
                CREATE INDEX IF NOT EXISTS idx_blocks_target ON blocks(target_kind, target_id);
                CREATE TABLE IF NOT EXISTS conversations (
                    conversation_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL CHECK (kind IN ('dm', 'group')),
                    title TEXT,
                    dm_key TEXT UNIQUE,
                    created_by TEXT NOT NULL,
                    key_version INTEGER NOT NULL DEFAULT 1,
                    rekey_needed INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conversation_members (
                    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner', 'member')),
                    status TEXT NOT NULL CHECK (status IN ('active', 'invited', 'left', 'removed', 'declined')),
                    invited_by TEXT,
                    hidden INTEGER NOT NULL DEFAULT 0,
                    last_read_seq INTEGER NOT NULL DEFAULT 0,
                    joined_at TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (conversation_id, user_id)
                );
                CREATE INDEX IF NOT EXISTS idx_members_user ON conversation_members(user_id, status);
                CREATE TABLE IF NOT EXISTS conversation_keys (
                    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
                    key_version INTEGER NOT NULL,
                    user_id TEXT NOT NULL,
                    recipient_key_id TEXT NOT NULL,
                    wrapper_user_id TEXT NOT NULL,
                    wrapper_key_id TEXT NOT NULL,
                    wrapped_key TEXT NOT NULL,
                    iv TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (conversation_id, key_version, user_id, recipient_key_id)
                );
                CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL DEFAULT 'message' CHECK (kind IN ('message', 'system')),
                    channel_id TEXT,
                    conversation_id TEXT,
                    sender_id TEXT NOT NULL,
                    ciphertext TEXT,
                    nonce TEXT,
                    key_version INTEGER,
                    encryption TEXT,
                    system_json TEXT,
                    reply_to TEXT,
                    company_codes TEXT,
                    created_at TEXT NOT NULL,
                    deleted_at TEXT,
                    CHECK ((channel_id IS NULL) <> (conversation_id IS NULL))
                );
                CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id, seq);
                CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id, seq);
                CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id, seq);
                CREATE TABLE IF NOT EXISTS message_companies (
                    message_id TEXT NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
                    company_code TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    PRIMARY KEY (message_id, company_code)
                );
                CREATE INDEX IF NOT EXISTS idx_message_companies ON message_companies(company_code, seq);
                """
            )
            now = _now()
            conn.executemany(
                "INSERT INTO channels(channel_id, kind, name, description, position, created_at) "
                "VALUES (?, 'topic', ?, ?, ?, ?) "
                "ON CONFLICT(channel_id) DO UPDATE SET name = excluded.name, "
                "description = excluded.description, position = excluded.position",
                [(topic_channel_id(item.slug), item.name, item.description, index, now) for index, item in enumerate(TOPICS)],
            )
            conn.commit()
        finally:
            conn.close()

    def _read(self) -> sqlite3.Connection:
        return connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)

    def _tx(self):
        return transaction(self.path, busy_timeout_ms=self.busy_timeout_ms)

    # -- profiles ------------------------------------------------------------

    def ensure_profile(self, user_id: str, username: str) -> None:
        """Create the user's profile on first use and keep its username in step with the account."""
        now = _now()
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO profiles(user_id, username, created_at, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET username = excluded.username "
                "WHERE profiles.username <> excluded.username",
                (user_id, username, now, now),
            )

    def profiles(self, user_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        ids = list(dict.fromkeys(user_ids))
        if not ids:
            return {}
        conn = self._read()
        try:
            rows = conn.execute(f"SELECT * FROM profiles WHERE user_id IN ({_placeholders(ids)})", ids).fetchall()
            return {row["user_id"]: _profile(row) for row in rows}
        finally:
            conn.close()

    def profile(self, user_id: str) -> dict[str, Any] | None:
        return self.profiles([user_id]).get(user_id)

    def profile_by_username(self, username: str) -> dict[str, Any] | None:
        conn = self._read()
        try:
            row = conn.execute("SELECT * FROM profiles WHERE username = ?", (username.strip().casefold(),)).fetchone()
            return _profile(row) if row else None
        finally:
            conn.close()

    def update_profile(self, user_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        columns = {
            "display_name": "display_name", "bio": "bio", "location": "location", "website": "website",
            "interests": "interests_json", "companies": "companies_json", "allow_dms": "allow_dms",
            "discoverable": "discoverable",
        }
        assignments, values = [], []
        for key, value in fields.items():
            if key not in columns:
                continue
            if key in {"interests", "companies"}:
                value = json.dumps(value, ensure_ascii=False)
            if key == "discoverable":
                value = 1 if value else 0
            assignments.append(f"{columns[key]} = ?")
            values.append(value)
        if assignments:
            with self._tx() as conn:
                conn.execute(
                    f"UPDATE profiles SET {', '.join(assignments)}, updated_at = ? WHERE user_id = ?",
                    (*values, _now(), user_id),
                )
        result = self.profile(user_id)
        if result is None:
            raise ChatError("Profile not found", 404)
        return result

    def message_stats(self, user_id: str) -> dict[str, Any]:
        conn = self._read()
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS posts, MIN(created_at) AS first_post, MAX(created_at) AS last_post "
                "FROM messages WHERE sender_id = ? AND channel_id IS NOT NULL AND kind = 'message' AND deleted_at IS NULL",
                (user_id,),
            ).fetchone()
            channels = conn.execute(
                "SELECT channel_id, COUNT(*) AS posts FROM messages WHERE sender_id = ? AND channel_id IS NOT NULL "
                "AND kind = 'message' AND deleted_at IS NULL GROUP BY channel_id ORDER BY posts DESC LIMIT 5",
                (user_id,),
            ).fetchall()
            return {"posts": row["posts"], "first_post": row["first_post"], "last_post": row["last_post"], "top_channels": [dict(item) for item in channels]}
        finally:
            conn.close()

    # -- identity keys -------------------------------------------------------

    def add_identity_key(self, user_id: str, values: dict[str, Any]) -> dict[str, Any]:
        """Publish a new key pair for the user; earlier keys stay for reading old wraps."""
        key_id = str(uuid.uuid4())
        now = _now()
        with self._tx() as conn:
            replaced = conn.execute(
                "UPDATE identity_keys SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL", (now, user_id)
            ).rowcount
            conn.execute(
                "INSERT INTO identity_keys(key_id, user_id, public_key, fingerprint, wrapped_private_key, wrap_salt, "
                "wrap_iv, wrap_iterations, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (key_id, user_id, values["public_key"], values["fingerprint"], values["wrapped_private_key"],
                 values["wrap_salt"], values["wrap_iv"], int(values["wrap_iterations"]), now),
            )
            # Tell every conversation: members' browsers then share its keys with the new one, and
            # a replaced key is shown, as it would be if someone were impersonating the user.
            for row in conn.execute(
                f"SELECT conversation_id FROM conversation_members WHERE user_id = ? AND status IN ({_placeholders(ACTIVE_MEMBER_STATUSES)})",
                (user_id, *ACTIVE_MEMBER_STATUSES),
            ).fetchall():
                self._system(conn, row["conversation_id"], user_id, {"event": "key_changed" if replaced else "key_added"})
        return self.own_key(user_id) or {}

    def own_key(self, user_id: str) -> dict[str, Any] | None:
        """The user's active key, including the passphrase-wrapped private key."""
        conn = self._read()
        try:
            row = conn.execute(
                "SELECT * FROM identity_keys WHERE user_id = ? AND revoked_at IS NULL ORDER BY created_at DESC LIMIT 1",
                (user_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def rewrap_identity_key(self, user_id: str, key_id: str, values: dict[str, Any]) -> bool:
        with self._tx() as conn:
            result = conn.execute(
                "UPDATE identity_keys SET wrapped_private_key = ?, wrap_salt = ?, wrap_iv = ?, wrap_iterations = ? "
                "WHERE key_id = ? AND user_id = ? AND revoked_at IS NULL",
                (values["wrapped_private_key"], values["wrap_salt"], values["wrap_iv"], int(values["wrap_iterations"]), key_id, user_id),
            )
            return result.rowcount == 1

    def public_keys(self, *, user_ids: Iterable[str] = (), key_ids: Iterable[str] = ()) -> list[dict[str, Any]]:
        """Public halves only: the active key of each user, plus any keys asked for by id."""
        users, keys = list(dict.fromkeys(user_ids)), list(dict.fromkeys(key_ids))
        conn = self._read()
        try:
            rows: list[sqlite3.Row] = []
            if users:
                rows += conn.execute(
                    "SELECT key_id, user_id, public_key, fingerprint, created_at, revoked_at FROM identity_keys "
                    f"WHERE revoked_at IS NULL AND user_id IN ({_placeholders(users)})",
                    users,
                ).fetchall()
            if keys:
                rows += conn.execute(
                    "SELECT key_id, user_id, public_key, fingerprint, created_at, revoked_at FROM identity_keys "
                    f"WHERE key_id IN ({_placeholders(keys)})",
                    keys,
                ).fetchall()
            unique = {row["key_id"]: {**dict(row), "active": row["revoked_at"] is None} for row in rows}
            return list(unique.values())
        finally:
            conn.close()

    def _key_owner(self, conn: sqlite3.Connection, key_id: str) -> str | None:
        row = conn.execute("SELECT user_id FROM identity_keys WHERE key_id = ?", (key_id,)).fetchone()
        return row["user_id"] if row else None

    # -- channels ------------------------------------------------------------

    def ensure_company_channel(self, channel_id: str, name: str, description: str | None) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO channels(channel_id, kind, name, description, created_at) VALUES (?, 'company', ?, ?, ?) "
                "ON CONFLICT(channel_id) DO UPDATE SET name = excluded.name, description = excluded.description",
                (channel_id, name, description, _now()),
            )

    def channels(self, channel_ids: Iterable[str] | None = None, *, kind: str | None = None) -> list[dict[str, Any]]:
        conn = self._read()
        try:
            clauses, values = [], []
            if channel_ids is not None:
                ids = list(dict.fromkeys(channel_ids))
                if not ids:
                    return []
                clauses.append(f"c.channel_id IN ({_placeholders(ids)})")
                values += ids
            if kind:
                clauses.append("c.kind = ?")
                values.append(kind)
            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            rows = conn.execute(
                "SELECT c.*, "
                "(SELECT COUNT(*) FROM channel_subscriptions s WHERE s.channel_id = c.channel_id) AS subscribers, "
                "(SELECT COUNT(*) FROM messages m WHERE m.channel_id = c.channel_id AND m.deleted_at IS NULL AND m.kind = 'message') AS message_count, "
                "(SELECT MAX(m.created_at) FROM messages m WHERE m.channel_id = c.channel_id AND m.deleted_at IS NULL) AS last_message_at "
                f"FROM channels c {where} ORDER BY c.position, c.name",
                values,
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def active_company_channels(self, limit: int = 20) -> list[dict[str, Any]]:
        """Company channels with the most recent activity."""
        conn = self._read()
        try:
            rows = conn.execute(
                "SELECT c.channel_id FROM channels c JOIN messages m ON m.channel_id = c.channel_id "
                "WHERE c.kind = 'company' AND m.deleted_at IS NULL GROUP BY c.channel_id "
                "ORDER BY MAX(m.seq) DESC LIMIT ?",
                (limit,),
            ).fetchall()
        finally:
            conn.close()
        order = [row["channel_id"] for row in rows]
        found = {item["channel_id"]: item for item in self.channels(order)}
        return [found[channel_id] for channel_id in order if channel_id in found]

    def subscriptions(self, user_id: str) -> dict[str, int]:
        """Subscribed channel id → last read ``seq``."""
        conn = self._read()
        try:
            rows = conn.execute("SELECT channel_id, last_read_seq FROM channel_subscriptions WHERE user_id = ?", (user_id,)).fetchall()
            return {row["channel_id"]: row["last_read_seq"] for row in rows}
        finally:
            conn.close()

    def subscribe(self, user_id: str, channel_id: str) -> None:
        with self._tx() as conn:
            latest = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM messages WHERE channel_id = ?", (channel_id,)).fetchone()[0]
            conn.execute(
                "INSERT INTO channel_subscriptions(user_id, channel_id, last_read_seq, created_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(user_id, channel_id) DO NOTHING",
                (user_id, channel_id, latest, _now()),
            )

    def unsubscribe(self, user_id: str, channel_id: str) -> bool:
        with self._tx() as conn:
            return conn.execute("DELETE FROM channel_subscriptions WHERE user_id = ? AND channel_id = ?", (user_id, channel_id)).rowcount == 1

    def mark_channel_read(self, user_id: str, channel_id: str, seq: int) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE channel_subscriptions SET last_read_seq = MAX(last_read_seq, ?) WHERE user_id = ? AND channel_id = ?",
                (seq, user_id, channel_id),
            )

    def channel_unread(self, user_id: str, last_read: dict[str, int], blocked_users: set[str]) -> dict[str, int]:
        if not last_read:
            return {}
        conn = self._read()
        try:
            counts: dict[str, int] = {}
            blocked = list(blocked_users)
            exclude = f" AND sender_id NOT IN ({_placeholders(blocked)})" if blocked else ""
            for channel_id, seq in last_read.items():
                counts[channel_id] = conn.execute(
                    "SELECT COUNT(*) FROM messages WHERE channel_id = ? AND seq > ? AND sender_id <> ? "
                    f"AND kind = 'message' AND deleted_at IS NULL{exclude}",
                    (channel_id, seq, user_id, *blocked),
                ).fetchone()[0]
            return counts
        finally:
            conn.close()

    # -- blocks --------------------------------------------------------------

    def block(self, user_id: str, kind: str, target_id: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO blocks(user_id, target_kind, target_id, created_at) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (user_id, kind, target_id, _now()),
            )
            if kind == "channel":
                conn.execute("DELETE FROM channel_subscriptions WHERE user_id = ? AND channel_id = ?", (user_id, target_id))

    def unblock(self, user_id: str, kind: str, target_id: str) -> bool:
        with self._tx() as conn:
            return conn.execute(
                "DELETE FROM blocks WHERE user_id = ? AND target_kind = ? AND target_id = ?", (user_id, kind, target_id)
            ).rowcount == 1

    def blocks(self, user_id: str) -> list[dict[str, Any]]:
        conn = self._read()
        try:
            rows = conn.execute("SELECT target_kind, target_id, created_at FROM blocks WHERE user_id = ? ORDER BY created_at DESC", (user_id,)).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def blocked(self, user_id: str, kind: str) -> set[str]:
        return {item["target_id"] for item in self.blocks(user_id) if item["target_kind"] == kind}

    def blocks_user(self, user_id: str, target_user_id: str) -> bool:
        conn = self._read()
        try:
            return conn.execute(
                "SELECT 1 FROM blocks WHERE user_id = ? AND target_kind = 'user' AND target_id = ?", (user_id, target_user_id)
            ).fetchone() is not None
        finally:
            conn.close()

    # -- conversations -------------------------------------------------------

    def find_dm(self, user_a: str, user_b: str) -> str | None:
        conn = self._read()
        try:
            row = conn.execute("SELECT conversation_id FROM conversations WHERE dm_key = ?", (_dm_key(user_a, user_b),)).fetchone()
            return row["conversation_id"] if row else None
        finally:
            conn.close()

    def create_conversation(
        self,
        *,
        kind: str,
        created_by: str,
        member_ids: list[str],
        wraps: list[dict[str, Any]],
        wrapper_key_id: str,
        title: str | None = None,
        conversation_id: str | None = None,
    ) -> str:
        """Create a direct or group conversation with its first key wrapped for its members.

        Direct conversations start with both people active; group members
        other than the creator are invited and must accept.
        """
        # Browsers bind each key wrap to the conversation id, so they choose it before creating.
        conversation_id = conversation_id or str(uuid.uuid4())
        now = _now()
        others = [member for member in dict.fromkeys(member_ids) if member != created_by]
        dm_key = _dm_key(created_by, others[0]) if kind == "dm" else None
        with self._tx() as conn:
            try:
                conn.execute(
                    "INSERT INTO conversations(conversation_id, kind, title, dm_key, created_by, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (conversation_id, kind, title, dm_key, created_by, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise ChatError("This conversation already exists", 409) from exc
            conn.execute(
                "INSERT INTO conversation_members(conversation_id, user_id, role, status, joined_at, updated_at) "
                "VALUES (?, ?, 'owner', 'active', ?, ?)",
                (conversation_id, created_by, now, now),
            )
            other_status = "active" if kind == "dm" else "invited"
            for member in others:
                conn.execute(
                    "INSERT INTO conversation_members(conversation_id, user_id, role, status, invited_by, joined_at, updated_at) "
                    "VALUES (?, ?, 'member', ?, ?, ?, ?)",
                    (conversation_id, member, other_status, created_by, now if kind == "dm" else None, now),
                )
            self._system(conn, conversation_id, created_by, {"event": "created", "kind": kind, "title": title, "members": others})
            if not any(wrap["user_id"] == created_by for wrap in wraps):
                raise ChatError("The conversation key must be wrapped for its creator")
            self._insert_wraps(conn, conversation_id, created_by, wrapper_key_id, wraps, current_version=1)
        return conversation_id

    def conversation(self, conversation_id: str) -> dict[str, Any] | None:
        conn = self._read()
        try:
            row = conn.execute("SELECT * FROM conversations WHERE conversation_id = ?", (conversation_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def members(self, conversation_id: str, *, current_only: bool = True) -> list[dict[str, Any]]:
        conn = self._read()
        try:
            where = f" AND status IN ({_placeholders(ACTIVE_MEMBER_STATUSES)})" if current_only else ""
            rows = conn.execute(
                f"SELECT * FROM conversation_members WHERE conversation_id = ?{where} ORDER BY role DESC, joined_at",
                (conversation_id, *(ACTIVE_MEMBER_STATUSES if current_only else ())),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def membership(self, conversation_id: str, user_id: str) -> dict[str, Any] | None:
        conn = self._read()
        try:
            row = conn.execute(
                "SELECT * FROM conversation_members WHERE conversation_id = ? AND user_id = ?", (conversation_id, user_id)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def conversations_for(self, user_id: str) -> list[dict[str, Any]]:
        """The user's current conversations (active or invited), newest activity first."""
        conn = self._read()
        try:
            rows = conn.execute(
                "SELECT c.*, m.status AS my_status, m.role AS my_role, m.hidden, m.last_read_seq, "
                "(SELECT MAX(seq) FROM messages x WHERE x.conversation_id = c.conversation_id) AS last_seq, "
                "(SELECT MAX(created_at) FROM messages x WHERE x.conversation_id = c.conversation_id AND x.kind = 'message') AS last_message_at "
                "FROM conversations c JOIN conversation_members m ON m.conversation_id = c.conversation_id "
                f"WHERE m.user_id = ? AND m.status IN ({_placeholders(ACTIVE_MEMBER_STATUSES)}) "
                "ORDER BY COALESCE(last_seq, 0) DESC",
                (user_id, *ACTIVE_MEMBER_STATUSES),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def conversation_ids_for(self, user_id: str) -> list[str]:
        return [row["conversation_id"] for row in self.conversations_for(user_id)]

    def conversation_unread(self, user_id: str, conversations: list[dict[str, Any]], blocked_users: set[str]) -> dict[str, int]:
        conn = self._read()
        try:
            blocked = list(blocked_users)
            exclude = f" AND sender_id NOT IN ({_placeholders(blocked)})" if blocked else ""
            return {
                item["conversation_id"]: conn.execute(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id = ? AND seq > ? AND sender_id <> ? "
                    f"AND kind = 'message' AND deleted_at IS NULL{exclude}",
                    (item["conversation_id"], item["last_read_seq"], user_id, *blocked),
                ).fetchone()[0]
                for item in conversations
            }
        finally:
            conn.close()

    def mark_conversation_read(self, user_id: str, conversation_id: str, seq: int) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE conversation_members SET last_read_seq = MAX(last_read_seq, ?) WHERE conversation_id = ? AND user_id = ?",
                (seq, conversation_id, user_id),
            )

    def set_hidden(self, user_id: str, conversation_id: str, hidden: bool) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE conversation_members SET hidden = ? WHERE conversation_id = ? AND user_id = ?",
                (1 if hidden else 0, conversation_id, user_id),
            )

    def rename(self, conversation_id: str, actor: str, title: str) -> None:
        with self._tx() as conn:
            conn.execute("UPDATE conversations SET title = ?, updated_at = ? WHERE conversation_id = ?", (title, _now(), conversation_id))
            self._system(conn, conversation_id, actor, {"event": "renamed", "title": title})

    def invite(self, conversation_id: str, inviter: str, user_ids: list[str], wraps: list[dict[str, Any]], wrapper_key_id: str | None) -> list[str]:
        now = _now()
        invited: list[str] = []
        with self._tx() as conn:
            for user_id in dict.fromkeys(user_ids):
                current = conn.execute(
                    "SELECT status FROM conversation_members WHERE conversation_id = ? AND user_id = ?", (conversation_id, user_id)
                ).fetchone()
                if current is not None and current["status"] in ACTIVE_MEMBER_STATUSES:
                    continue
                conn.execute(
                    "INSERT INTO conversation_members(conversation_id, user_id, role, status, invited_by, updated_at) "
                    "VALUES (?, ?, 'member', 'invited', ?, ?) ON CONFLICT(conversation_id, user_id) DO UPDATE SET "
                    "status = 'invited', role = 'member', invited_by = excluded.invited_by, hidden = 0, updated_at = excluded.updated_at",
                    (conversation_id, user_id, inviter, now),
                )
                invited.append(user_id)
            if wraps and wrapper_key_id:
                version = conn.execute("SELECT key_version FROM conversations WHERE conversation_id = ?", (conversation_id,)).fetchone()[0]
                self._insert_wraps(conn, conversation_id, inviter, wrapper_key_id, wraps, current_version=version)
            if invited:
                self._system(conn, conversation_id, inviter, {"event": "invited", "members": invited})
        return invited

    def respond_to_invitation(self, conversation_id: str, user_id: str, accept: bool) -> None:
        with self._tx() as conn:
            row = conn.execute(
                "SELECT status FROM conversation_members WHERE conversation_id = ? AND user_id = ?", (conversation_id, user_id)
            ).fetchone()
            if row is None or row["status"] != "invited":
                raise ChatError("There is no pending invitation to this conversation", 404)
            now = _now()
            conn.execute(
                "UPDATE conversation_members SET status = ?, joined_at = ?, updated_at = ? WHERE conversation_id = ? AND user_id = ?",
                ("active" if accept else "declined", now if accept else None, now, conversation_id, user_id),
            )
            if not accept:
                conn.execute("DELETE FROM conversation_keys WHERE conversation_id = ? AND user_id = ?", (conversation_id, user_id))
            self._system(conn, conversation_id, user_id, {"event": "joined" if accept else "declined"})

    def remove_member(self, conversation_id: str, actor: str, user_id: str) -> None:
        """Remove someone (or leave). Their key copies go, and the next sender must rotate the key."""
        leaving = actor == user_id
        now = _now()
        with self._tx() as conn:
            conn.execute(
                "UPDATE conversation_members SET status = ?, role = 'member', updated_at = ? WHERE conversation_id = ? AND user_id = ?",
                ("left" if leaving else "removed", now, conversation_id, user_id),
            )
            conn.execute("DELETE FROM conversation_keys WHERE conversation_id = ? AND user_id = ?", (conversation_id, user_id))
            conn.execute("UPDATE conversations SET rekey_needed = 1, updated_at = ? WHERE conversation_id = ?", (now, conversation_id))
            owner = conn.execute(
                "SELECT 1 FROM conversation_members WHERE conversation_id = ? AND role = 'owner' AND status = 'active'", (conversation_id,)
            ).fetchone()
            if owner is None:
                successor = conn.execute(
                    "SELECT user_id FROM conversation_members WHERE conversation_id = ? AND status = 'active' ORDER BY joined_at LIMIT 1",
                    (conversation_id,),
                ).fetchone()
                if successor is not None:
                    conn.execute(
                        "UPDATE conversation_members SET role = 'owner' WHERE conversation_id = ? AND user_id = ?",
                        (conversation_id, successor["user_id"]),
                    )
            self._system(conn, conversation_id, actor, {"event": "left" if leaving else "removed", "members": [user_id]})

    # -- conversation keys ---------------------------------------------------

    def _insert_wraps(
        self,
        conn: sqlite3.Connection,
        conversation_id: str,
        wrapper_user_id: str,
        wrapper_key_id: str,
        wraps: list[dict[str, Any]],
        *,
        current_version: int,
    ) -> None:
        if self._key_owner(conn, wrapper_key_id) != wrapper_user_id:
            raise ChatError("The wrapping key does not belong to you", 403)
        current = {
            row["user_id"]
            for row in conn.execute(
                f"SELECT user_id FROM conversation_members WHERE conversation_id = ? AND status IN ({_placeholders(ACTIVE_MEMBER_STATUSES)})",
                (conversation_id, *ACTIVE_MEMBER_STATUSES),
            ).fetchall()
        }
        now = _now()
        for wrap in wraps:
            version = int(wrap["key_version"])
            if not 1 <= version <= current_version:
                raise ChatError("Unknown conversation key version")
            if wrap["user_id"] not in current:
                raise ChatError("Keys can only be shared with members of the conversation", 403)
            if self._key_owner(conn, wrap["recipient_key_id"]) != wrap["user_id"]:
                raise ChatError("A recipient key does not belong to its member", 400)
            conn.execute(
                "INSERT INTO conversation_keys(conversation_id, key_version, user_id, recipient_key_id, wrapper_user_id, "
                "wrapper_key_id, wrapped_key, iv, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(conversation_id, key_version, user_id, recipient_key_id) DO NOTHING",
                (conversation_id, version, wrap["user_id"], wrap["recipient_key_id"], wrapper_user_id, wrapper_key_id,
                 wrap["wrapped_key"], wrap["iv"], now),
            )
        recipients = sorted({wrap["user_id"] for wrap in wraps} - {wrapper_user_id})
        if recipients:
            # Hidden from the conversation; it prompts the recipients' browsers to unwrap their new copies.
            self._system(conn, conversation_id, wrapper_user_id, {"event": "keys_shared", "members": recipients})

    def add_wraps(self, conversation_id: str, wrapper_user_id: str, wrapper_key_id: str, wraps: list[dict[str, Any]]) -> None:
        with self._tx() as conn:
            version = conn.execute("SELECT key_version FROM conversations WHERE conversation_id = ?", (conversation_id,)).fetchone()[0]
            self._insert_wraps(conn, conversation_id, wrapper_user_id, wrapper_key_id, wraps, current_version=version)

    def wraps_for(self, conversation_id: str, user_id: str) -> list[dict[str, Any]]:
        conn = self._read()
        try:
            rows = conn.execute(
                "SELECT key_version, recipient_key_id, wrapper_user_id, wrapper_key_id, wrapped_key, iv FROM conversation_keys "
                "WHERE conversation_id = ? AND user_id = ? ORDER BY key_version",
                (conversation_id, user_id),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def wrap_counts(self, user_id: str) -> dict[str, int]:
        """Conversation id → how many key copies the user holds there; a change means new keys to unwrap."""
        conn = self._read()
        try:
            rows = conn.execute(
                "SELECT conversation_id, COUNT(*) AS wraps FROM conversation_keys WHERE user_id = ? GROUP BY conversation_id",
                (user_id,),
            ).fetchall()
            return {row["conversation_id"]: row["wraps"] for row in rows}
        finally:
            conn.close()

    def wrap_coverage(self, conversation_id: str) -> dict[str, dict[str, list[int]]]:
        """Member → recipient key id → key versions wrapped for it."""
        conn = self._read()
        try:
            rows = conn.execute(
                "SELECT user_id, recipient_key_id, key_version FROM conversation_keys WHERE conversation_id = ? ORDER BY key_version",
                (conversation_id,),
            ).fetchall()
        finally:
            conn.close()
        coverage: dict[str, dict[str, list[int]]] = {}
        for row in rows:
            coverage.setdefault(row["user_id"], {}).setdefault(row["recipient_key_id"], []).append(row["key_version"])
        return coverage

    def rotate(self, conversation_id: str, user_id: str, key_version: int, wrapper_key_id: str, wraps: list[dict[str, Any]]) -> None:
        with self._tx() as conn:
            current = conn.execute("SELECT key_version FROM conversations WHERE conversation_id = ?", (conversation_id,)).fetchone()[0]
            if key_version != current + 1:
                raise ChatError("The conversation key changed meanwhile; reload and try again", 409)
            if not any(wrap["user_id"] == user_id and int(wrap["key_version"]) == key_version for wrap in wraps):
                raise ChatError("The new key must be wrapped for you")
            if any(int(wrap["key_version"]) != key_version for wrap in wraps):
                raise ChatError("A rotation carries wraps of the new key only")
            conn.execute(
                "UPDATE conversations SET key_version = ?, rekey_needed = 0, updated_at = ? WHERE conversation_id = ?",
                (key_version, _now(), conversation_id),
            )
            self._insert_wraps(conn, conversation_id, user_id, wrapper_key_id, wraps, current_version=key_version)
            self._system(conn, conversation_id, user_id, {"event": "rotated", "key_version": key_version})

    # -- messages ------------------------------------------------------------

    def _system(self, conn: sqlite3.Connection, conversation_id: str, actor: str, event: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO messages(message_id, kind, conversation_id, sender_id, system_json, created_at) VALUES (?, 'system', ?, ?, ?, ?)",
            (str(uuid.uuid4()), conversation_id, actor, json.dumps(event, ensure_ascii=False), _now()),
        )

    def add_message(
        self,
        *,
        message_id: str,
        sender_id: str,
        ciphertext: str,
        nonce: str,
        key_version: int,
        encryption: str,
        channel_id: str | None = None,
        conversation_id: str | None = None,
        reply_to: str | None = None,
        company_codes: list[str] | None = None,
    ) -> dict[str, Any]:
        now = _now()
        codes = list(dict.fromkeys(company_codes or []))[:20]
        with self._tx() as conn:
            try:
                cursor = conn.execute(
                    "INSERT INTO messages(message_id, kind, channel_id, conversation_id, sender_id, ciphertext, nonce, key_version, "
                    "encryption, reply_to, company_codes, created_at) VALUES (?, 'message', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (message_id, channel_id, conversation_id, sender_id, ciphertext, nonce, key_version, encryption, reply_to,
                     json.dumps(codes) if codes else None, now),
                )
            except sqlite3.IntegrityError as exc:
                raise ChatError("A message with this id already exists", 409) from exc
            seq = cursor.lastrowid
            conn.executemany(
                "INSERT INTO message_companies(message_id, company_code, seq) VALUES (?, ?, ?)",
                [(message_id, code, seq) for code in codes],
            )
            if conversation_id:
                # A new message brings a hidden conversation back for everyone, and counts as read by its sender.
                conn.execute("UPDATE conversation_members SET hidden = 0 WHERE conversation_id = ?", (conversation_id,))
                conn.execute(
                    "UPDATE conversation_members SET last_read_seq = ? WHERE conversation_id = ? AND user_id = ?",
                    (seq, conversation_id, sender_id),
                )
            elif channel_id:
                conn.execute(
                    "UPDATE channel_subscriptions SET last_read_seq = ? WHERE channel_id = ? AND user_id = ?",
                    (seq, channel_id, sender_id),
                )
            row = conn.execute("SELECT * FROM messages WHERE seq = ?", (seq,)).fetchone()
        return dict(row)

    def message(self, message_id: str) -> dict[str, Any] | None:
        conn = self._read()
        try:
            row = conn.execute("SELECT * FROM messages WHERE message_id = ?", (message_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def delete_message(self, message_id: str, actor: str) -> None:
        """Erase a message's content, and record the deletion so live clients drop it too."""
        with self._tx() as conn:
            row = conn.execute("SELECT channel_id, conversation_id FROM messages WHERE message_id = ?", (message_id,)).fetchone()
            if row is None:
                return
            conn.execute(
                "UPDATE messages SET ciphertext = NULL, nonce = NULL, company_codes = NULL, deleted_at = ? WHERE message_id = ?",
                (_now(), message_id),
            )
            conn.execute("DELETE FROM message_companies WHERE message_id = ?", (message_id,))
            conn.execute(
                "INSERT INTO messages(message_id, kind, channel_id, conversation_id, sender_id, system_json, created_at) "
                "VALUES (?, 'system', ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), row["channel_id"], row["conversation_id"], actor,
                 json.dumps({"event": "deleted", "message_id": message_id}), _now()),
            )

    def messages(
        self,
        *,
        channel_ids: Iterable[str] = (),
        conversation_ids: Iterable[str] = (),
        before: int | None = None,
        after: int | None = None,
        limit: int = 50,
        exclude_senders: Iterable[str] = (),
        sender_id: str | None = None,
        company_code: str | None = None,
        public_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Messages in the given channels and conversations, oldest first.

        With ``before`` (or neither bound) the newest ``limit`` messages
        older than it; with ``after`` the oldest ``limit`` newer than it.
        """
        channels, conversations = list(dict.fromkeys(channel_ids)), list(dict.fromkeys(conversation_ids))
        scopes, values = [], []
        if channels:
            scopes.append(f"m.channel_id IN ({_placeholders(channels)})")
            values += channels
        if conversations:
            scopes.append(f"m.conversation_id IN ({_placeholders(conversations)})")
            values += conversations
        if public_only:
            scopes.append("m.channel_id IS NOT NULL")
        if not scopes and not company_code:
            return []
        clauses = [f"({' OR '.join(scopes)})"] if scopes else []
        if company_code:
            clauses.append("m.message_id IN (SELECT message_id FROM message_companies WHERE company_code = ?)")
            values.append(company_code)
        excluded = list(dict.fromkeys(exclude_senders))
        if excluded:
            clauses.append(f"m.sender_id NOT IN ({_placeholders(excluded)})")
            values += excluded
        if sender_id:
            clauses.append("m.sender_id = ?")
            values.append(sender_id)
        if before is not None:
            clauses.append("m.seq < ?")
            values.append(before)
        if after is not None:
            clauses.append("m.seq > ?")
            values.append(after)
        order = "ASC" if after is not None else "DESC"
        conn = self._read()
        try:
            rows = conn.execute(
                f"SELECT m.* FROM messages m WHERE {' AND '.join(clauses)} ORDER BY m.seq {order} LIMIT ?",
                (*values, max(1, min(int(limit), 500))),
            ).fetchall()
        finally:
            conn.close()
        result = [dict(row) for row in rows]
        return result if order == "ASC" else result[::-1]

    def latest_seq(self) -> int:
        conn = self._read()
        try:
            return int(conn.execute("SELECT COALESCE(MAX(seq), 0) FROM messages").fetchone()[0])
        finally:
            conn.close()


def _dm_key(user_a: str, user_b: str) -> str:
    return ":".join(sorted((user_a, user_b)))


def _profile(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    result["interests"] = json.loads(result.pop("interests_json") or "[]")
    result["companies"] = json.loads(result.pop("companies_json") or "[]")
    result["discoverable"] = bool(result["discoverable"])
    return result
