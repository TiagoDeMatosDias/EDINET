"""Chat: public channels encrypted at rest, end-to-end conversations, blocks, profiles."""

from __future__ import annotations

import base64
import hashlib
import secrets
import sqlite3
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.chat.api as chat_api
import src.chat.channels as chat_channels
import src.chat.profiles_api as profiles_api
from src.chat.crypto import ChannelCipher, Sealed, channel_associated_data
from src.chat.storage import ChatStore
from src.web_app.security import AppSettings, install_security

PASSWORD = "correct horse battery staple"


def _b64(size: int) -> str:
    return base64.b64encode(secrets.token_bytes(size)).decode()


def _key_payload() -> dict:
    public = _b64(91)
    return {
        "public_key": public,
        "fingerprint": hashlib.sha256(public.encode()).hexdigest(),
        "wrapped_private_key": _b64(150),
        "wrap_salt": _b64(16),
        "wrap_iv": _b64(12),
        "wrap_iterations": 600_000,
    }


def _wrap(user_id: str, key_id: str, version: int = 1) -> dict:
    return {"user_id": user_id, "recipient_key_id": key_id, "key_version": version, "wrapped_key": _b64(48), "iv": _b64(12)}


@pytest.fixture
def market(tmp_path):
    path = tmp_path / "market.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE CompanyInfo (Company_Code TEXT, Company_Name TEXT, Company_Ticker TEXT)")
    conn.executemany(
        "INSERT INTO CompanyInfo VALUES (?, ?, ?)",
        [("E02144", "TOYOTA MOTOR CORPORATION", "72030"), ("E01777", "SONY GROUP CORPORATION", "67580")],
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def chat(tmp_path, monkeypatch, market):
    store = ChatStore(tmp_path / "chat.db")
    cipher = ChannelCipher(tmp_path / "secrets" / "keys.json")
    for module in (chat_api, profiles_api):
        monkeypatch.setattr(module, "store", store)
        monkeypatch.setattr(module, "cipher", cipher)
    monkeypatch.setattr(chat_api, "_synced_profiles", set())
    monkeypatch.setattr(chat_api, "_send_limit", chat_api.service.RateLimiter(limit=20, window=10.0))
    monkeypatch.setattr(chat_channels, "_market_db", lambda: market)
    app = FastAPI()
    app.include_router(chat_api.router)
    app.include_router(profiles_api.router)
    install_security(app, AppSettings(auth_mode="accounts", registration_mode="open", auth_db_path=tmp_path / "auth.db"))
    service = app.state.auth_service
    users = {name: service.register(name, PASSWORD) for name in ("admin-user", "alice", "bob", "carol")}
    tokens = {name: service.login(name, PASSWORD).tokens.access_token for name in users}
    client = TestClient(app)

    class Chat:
        def __init__(self) -> None:
            self.store, self.cipher, self.client, self.users = store, cipher, client, users

        def headers(self, name: str) -> dict[str, str]:
            return {"Authorization": f"Bearer {tokens[name]}"}

        def get(self, name: str, path: str, **kwargs):
            return client.get(path, headers=self.headers(name), **kwargs)

        def post(self, name: str, path: str, json=None):
            return client.post(path, headers=self.headers(name), json=json)

        def put(self, name: str, path: str, json=None):
            return client.put(path, headers=self.headers(name), json=json)

        def patch(self, name: str, path: str, json=None):
            return client.patch(path, headers=self.headers(name), json=json)

        def delete(self, name: str, path: str):
            return client.delete(path, headers=self.headers(name))

        def id(self, name: str) -> str:
            return users[name].user_id

        def publish_key(self, name: str) -> str:
            response = self.post(name, "/api/chat/keys", _key_payload())
            assert response.status_code == 201
            return response.json()["key"]["key_id"]

    return Chat()


# -- public channels ------------------------------------------------------------


def test_channel_messages_are_encrypted_at_rest_and_resolve_company_references(chat):
    response = chat.post("alice", "/api/chat/channels/topic:stocks/messages", {"text": "Watching $7203 and $E01777 this week"})
    assert response.status_code == 201
    message = response.json()
    assert message["body"]["text"] == "Watching $7203 and $E01777 this week"
    assert message["body"]["refs"] == {
        "7203": {"code": "E02144", "name": "TOYOTA MOTOR CORPORATION"},
        "E01777": {"code": "E01777", "name": "SONY GROUP CORPORATION"},
    }

    conn = sqlite3.connect(chat.store.path)
    raw = conn.execute("SELECT ciphertext, encryption FROM messages WHERE message_id = ?", (message["message_id"],)).fetchone()
    conn.close()
    assert raw[1] == "server-aes-gcm-v1"
    assert b"Watching" not in base64.b64decode(raw[0])

    listed = chat.get("bob", "/api/chat/channels/topic:stocks/messages").json()
    assert [item["body"]["text"] for item in listed["messages"]] == ["Watching $7203 and $E01777 this week"]
    assert listed["messages"][0]["sender"]["username"] == "alice"
    mentions = chat.get("bob", "/api/chat/companies/E02144/mentions").json()["messages"]
    assert [item["message_id"] for item in mentions] == [message["message_id"]]


def test_a_ciphertext_cannot_be_moved_to_another_channel_or_sender(tmp_path):
    cipher = ChannelCipher(tmp_path / "keys.json")
    sealed = cipher.seal("hello", channel_associated_data("topic:stocks", "u1", "m1"))
    assert cipher.open(sealed, channel_associated_data("topic:stocks", "u1", "m1")) == "hello"
    with pytest.raises(Exception):
        cipher.open(sealed, channel_associated_data("topic:bonds", "u1", "m1"))
    with pytest.raises(Exception):
        cipher.open(Sealed(sealed.ciphertext, sealed.nonce, sealed.key_version), channel_associated_data("topic:stocks", "u2", "m1"))
    # The key ring survives a restart.
    assert ChannelCipher(tmp_path / "keys.json").open(sealed, channel_associated_data("topic:stocks", "u1", "m1")) == "hello"


def test_company_channels_open_for_known_companies_only(chat):
    channel = chat.get("alice", "/api/chat/channels/company:E02144").json()
    assert (channel["kind"], channel["name"], channel["subscribed"]) == ("company", "TOYOTA MOTOR CORPORATION", False)
    assert chat.get("alice", "/api/chat/channels/company:E99999").status_code == 404
    assert chat.get("alice", "/api/chat/channels/topic:nope").status_code == 404


def test_subscriptions_unread_counts_and_the_feed(chat):
    assert chat.put("bob", "/api/chat/channels/topic:bonds/subscription").status_code == 204
    chat.post("alice", "/api/chat/channels/topic:bonds/messages", {"text": "JGB 10y at 1.1%"})
    chat.post("alice", "/api/chat/channels/topic:fx/messages", {"text": "Yen weaker"})

    summary = chat.get("bob", "/api/chat/summary").json()
    bonds = next(item for item in summary["channels"] if item["channel_id"] == "topic:bonds")
    assert (bonds["subscribed"], bonds["unread"]) == (True, 1)
    assert chat.get("bob", "/api/chat/unread").json()["total"] == 1

    feed = chat.get("bob", "/api/chat/feed").json()["messages"]
    assert [item["body"]["text"] for item in feed] == ["JGB 10y at 1.1%"]
    chat.post("bob", "/api/chat/channels/topic:bonds/read", {"seq": feed[-1]["seq"]})
    assert chat.get("bob", "/api/chat/unread").json()["total"] == 0


def test_blocking_a_person_hides_their_messages_and_blocking_a_channel_hides_it(chat):
    chat.put("bob", "/api/chat/channels/topic:general/subscription")
    chat.post("alice", "/api/chat/channels/topic:general/messages", {"text": "from alice"})
    chat.post("carol", "/api/chat/channels/topic:general/messages", {"text": "from carol"})

    assert chat.put("bob", f"/api/chat/blocks/user/{chat.id('alice')}").status_code == 204
    texts = [item["body"]["text"] for item in chat.get("bob", "/api/chat/channels/topic:general/messages").json()["messages"]]
    assert texts == ["from carol"]
    assert [item["username"] for item in chat.get("bob", "/api/chat/blocks").json()["users"]] == ["alice"]

    assert chat.put("bob", "/api/chat/blocks/channel/topic:general").status_code == 204
    assert chat.get("bob", "/api/chat/feed").json()["messages"] == []
    assert chat.post("bob", "/api/chat/channels/topic:general/messages", {"text": "hi"}).status_code == 403
    assert chat.put("bob", "/api/chat/channels/topic:general/subscription").status_code == 409

    assert chat.delete("bob", "/api/chat/blocks/channel/topic:general").status_code == 204
    assert chat.delete("bob", f"/api/chat/blocks/user/{chat.id('alice')}").status_code == 204
    texts = [item["body"]["text"] for item in chat.get("bob", "/api/chat/channels/topic:general/messages").json()["messages"]]
    assert texts == ["from alice", "from carol"]
    assert chat.put("bob", f"/api/chat/blocks/user/{chat.id('bob')}").status_code == 400


def test_updates_return_new_messages_in_followed_and_watched_channels(chat):
    cursor = chat.get("bob", "/api/chat/summary").json()["cursor"]
    chat.put("bob", "/api/chat/channels/topic:macro/subscription")
    chat.post("alice", "/api/chat/channels/topic:macro/messages", {"text": "CPI tomorrow"})
    chat.post("alice", "/api/chat/channels/company:E02144/messages", {"text": "Results out"})

    followed = chat.get("bob", "/api/chat/updates", params={"since": cursor, "wait": 0}).json()
    assert [item["body"]["text"] for item in followed["messages"]] == ["CPI tomorrow"]
    watched = chat.get("bob", "/api/chat/updates", params={"since": cursor, "wait": 0, "channels": "company:E02144"}).json()
    assert [item["body"]["text"] for item in watched["messages"]] == ["CPI tomorrow", "Results out"]
    assert chat.get("bob", "/api/chat/updates", params={"since": watched["cursor"], "wait": 0}).json()["messages"] == []


def test_a_waiting_update_request_wakes_when_a_message_arrives(chat):
    import threading
    import time

    chat.put("bob", "/api/chat/channels/topic:etfs/subscription")
    cursor = chat.get("bob", "/api/chat/summary").json()["cursor"]
    timer = threading.Timer(0.4, lambda: chat.post("alice", "/api/chat/channels/topic:etfs/messages", {"text": "new listing"}))
    started = time.monotonic()
    timer.start()
    try:
        result = chat.get("bob", "/api/chat/updates", params={"since": cursor, "wait": 20}).json()
    finally:
        timer.join()
    assert [item["body"]["text"] for item in result["messages"]] == ["new listing"]
    assert time.monotonic() - started < 10


def test_people_can_delete_their_own_messages_and_administrators_any_public_one(chat):
    mine = chat.post("alice", "/api/chat/channels/topic:ideas/messages", {"text": "idea"}).json()
    assert chat.delete("bob", f"/api/chat/messages/{mine['message_id']}").status_code == 403
    assert chat.delete("alice", f"/api/chat/messages/{mine['message_id']}").status_code == 204
    shown, event = chat.get("bob", "/api/chat/channels/topic:ideas/messages").json()["messages"]
    assert shown["deleted"] and "body" not in shown
    assert event["system"] == {"event": "deleted", "message_id": mine["message_id"]}

    other = chat.post("bob", "/api/chat/channels/topic:ideas/messages", {"text": "spam"}).json()
    assert chat.delete("admin-user", f"/api/chat/messages/{other['message_id']}").status_code == 204


def test_sending_is_rate_limited(chat):
    for index in range(20):
        assert chat.post("alice", "/api/chat/channels/topic:general/messages", {"text": f"m{index}"}).status_code == 201
    assert chat.post("alice", "/api/chat/channels/topic:general/messages", {"text": "too many"}).status_code == 429


# -- end-to-end conversations ---------------------------------------------------


def _dm(chat, sender: str, recipient: str, sender_key: str, recipient_key: str | None):
    wraps = [_wrap(chat.id(sender), sender_key)] + ([_wrap(chat.id(recipient), recipient_key)] if recipient_key else [])
    return chat.post(sender, "/api/chat/conversations", {"kind": "dm", "member_ids": [chat.id(recipient)], "wrapper_key_id": sender_key, "wraps": wraps})


def test_direct_messages_store_only_ciphertext_and_only_members_read_them(chat):
    alice_key, bob_key = chat.publish_key("alice"), chat.publish_key("bob")
    created = _dm(chat, "alice", "bob", alice_key, bob_key)
    assert created.status_code == 201
    conversation = created.json()
    assert {member["username"] for member in conversation["members"]} == {"alice", "bob"}
    assert all(member["status"] == "active" for member in conversation["members"])
    conversation_id = conversation["conversation_id"]

    message = {"message_id": str(uuid.uuid4()), "ciphertext": _b64(64), "nonce": _b64(12), "key_version": 1}
    sent = chat.post("alice", f"/api/chat/conversations/{conversation_id}/messages", message)
    assert sent.status_code == 201
    assert sent.json()["encrypted"] == {"ciphertext": message["ciphertext"], "nonce": message["nonce"], "key_version": 1}

    listed = chat.get("bob", f"/api/chat/conversations/{conversation_id}/messages").json()["messages"]
    assert [item["encrypted"]["ciphertext"] for item in listed if item["kind"] == "message"] == [message["ciphertext"]]
    keys = chat.get("bob", f"/api/chat/conversations/{conversation_id}/keys").json()
    assert [(wrap["key_version"], wrap["recipient_key_id"], wrap["wrapper_key_id"]) for wrap in keys["wraps"]] == [(1, bob_key, alice_key)]
    assert [key["key_id"] for key in keys["wrapper_keys"]] == [alice_key]

    assert chat.get("carol", f"/api/chat/conversations/{conversation_id}/messages").status_code == 404
    assert chat.get("carol", f"/api/chat/conversations/{conversation_id}/keys").status_code == 404
    stale = {**message, "message_id": str(uuid.uuid4()), "key_version": 2}
    assert chat.post("alice", f"/api/chat/conversations/{conversation_id}/messages", stale).status_code == 409

    again = _dm(chat, "bob", "alice", bob_key, alice_key)
    assert again.status_code == 409
    assert again.json()["detail"]["conversation_id"] == conversation_id


def test_key_wraps_must_name_the_members_own_keys(chat):
    alice_key, bob_key, carol_key = chat.publish_key("alice"), chat.publish_key("bob"), chat.publish_key("carol")
    wrong_recipient = chat.post("alice", "/api/chat/conversations", {
        "kind": "dm", "member_ids": [chat.id("bob")], "wrapper_key_id": alice_key,
        "wraps": [_wrap(chat.id("alice"), alice_key), _wrap(chat.id("bob"), carol_key)],
    })
    assert wrong_recipient.status_code == 400
    not_mine = chat.post("alice", "/api/chat/conversations", {
        "kind": "dm", "member_ids": [chat.id("bob")], "wrapper_key_id": bob_key, "wraps": [_wrap(chat.id("alice"), alice_key)],
    })
    assert not_mine.status_code == 403
    assert chat.store.find_dm(chat.id("alice"), chat.id("bob")) is None


def test_a_person_without_a_key_yet_can_be_messaged_and_receive_the_key_later(chat):
    alice_key = chat.publish_key("alice")
    conversation = _dm(chat, "alice", "bob", alice_key, None).json()
    conversation_id = conversation["conversation_id"]
    bob_key = chat.publish_key("bob")
    # Bob's new key is announced in the conversation, which prompts Alice's browser to share.
    events = [item["system"] for item in chat.get("alice", f"/api/chat/conversations/{conversation_id}/messages").json()["messages"] if item["kind"] == "system"]
    assert events[-1] == {"event": "key_added"}
    coverage = chat.get("alice", f"/api/chat/conversations/{conversation_id}/keys").json()["coverage"]
    assert chat.id("bob") not in coverage
    assert chat.get("bob", "/api/chat/conversations").json()["conversations"][0]["my_wraps"] == 0
    assert chat.post("alice", f"/api/chat/conversations/{conversation_id}/keys", {"wrapper_key_id": alice_key, "wraps": [_wrap(chat.id("bob"), bob_key)]}).status_code == 204
    assert chat.get("bob", f"/api/chat/conversations/{conversation_id}/keys").json()["wraps"][0]["recipient_key_id"] == bob_key
    assert chat.get("bob", "/api/chat/conversations").json()["conversations"][0]["my_wraps"] == 1
    chat.publish_key("bob")
    events = [item["system"] for item in chat.get("alice", f"/api/chat/conversations/{conversation_id}/messages").json()["messages"] if item["kind"] == "system"]
    assert events[-2:] == [{"event": "keys_shared", "members": [chat.id("bob")]}, {"event": "key_changed"}]


def test_blocks_and_privacy_settings_stop_direct_messages(chat):
    alice_key, bob_key = chat.publish_key("alice"), chat.publish_key("bob")
    chat.patch("bob", "/api/profiles/me", {"allow_dms": "nobody"})
    assert _dm(chat, "alice", "bob", alice_key, bob_key).status_code == 403
    chat.patch("bob", "/api/profiles/me", {"allow_dms": "everyone"})
    conversation_id = _dm(chat, "alice", "bob", alice_key, bob_key).json()["conversation_id"]

    chat.put("bob", f"/api/chat/blocks/user/{chat.id('alice')}")
    message = {"message_id": str(uuid.uuid4()), "ciphertext": _b64(64), "nonce": _b64(12), "key_version": 1}
    assert chat.post("alice", f"/api/chat/conversations/{conversation_id}/messages", message).status_code == 403
    assert chat.get("bob", "/api/chat/conversations").json()["conversations"] == []


def test_groups_invite_accept_remove_and_rotate_the_key(chat):
    keys = {name: chat.publish_key(name) for name in ("alice", "bob", "carol")}
    created = chat.post("alice", "/api/chat/conversations", {
        "kind": "group", "title": "Value", "member_ids": [chat.id("bob")], "wrapper_key_id": keys["alice"],
        "wraps": [_wrap(chat.id("alice"), keys["alice"]), _wrap(chat.id("bob"), keys["bob"])],
    }).json()
    conversation_id = created["conversation_id"]
    statuses = {member["username"]: member["status"] for member in created["members"]}
    assert statuses == {"alice": "active", "bob": "invited"}
    assert chat.get("bob", "/api/chat/unread").json()["invitations"] == 1

    message = {"message_id": str(uuid.uuid4()), "ciphertext": _b64(64), "nonce": _b64(12), "key_version": 1}
    assert chat.post("bob", f"/api/chat/conversations/{conversation_id}/messages", message).status_code == 404
    assert chat.post("bob", f"/api/chat/conversations/{conversation_id}/invitation", {"accept": True}).status_code == 204

    invited = chat.post("bob", f"/api/chat/conversations/{conversation_id}/members", {
        "user_ids": [chat.id("carol")], "wrapper_key_id": keys["bob"], "wraps": [_wrap(chat.id("carol"), keys["carol"])],
    })
    assert invited.status_code == 201 and invited.json()["invited"] == [chat.id("carol")]
    assert chat.delete("bob", f"/api/chat/conversations/{conversation_id}/members/{chat.id('carol')}").status_code == 403
    assert chat.delete("alice", f"/api/chat/conversations/{conversation_id}/members/{chat.id('carol')}").status_code == 204
    assert chat.get("carol", f"/api/chat/conversations/{conversation_id}/messages").status_code == 404

    # Removing someone requires a new key before anyone sends again.
    assert chat.post("alice", f"/api/chat/conversations/{conversation_id}/messages", message).status_code == 409
    rotation = {"key_version": 2, "wrapper_key_id": keys["alice"], "wraps": [_wrap(chat.id("alice"), keys["alice"], 2), _wrap(chat.id("bob"), keys["bob"], 2)]}
    assert chat.post("alice", f"/api/chat/conversations/{conversation_id}/rotate", {**rotation, "key_version": 3}).status_code == 409
    assert chat.post("alice", f"/api/chat/conversations/{conversation_id}/rotate", rotation).status_code == 204
    assert chat.post("alice", f"/api/chat/conversations/{conversation_id}/messages", {**message, "key_version": 2}).status_code == 201

    events = [item["system"]["event"] for item in chat.get("bob", f"/api/chat/conversations/{conversation_id}/messages").json()["messages"] if item["kind"] == "system"]
    assert events == ["created", "keys_shared", "joined", "keys_shared", "invited", "removed", "keys_shared", "rotated"]
    assert chat.delete("alice", f"/api/chat/conversations/{conversation_id}/members/{chat.id('alice')}").status_code == 204
    owner = next(member for member in chat.get("bob", f"/api/chat/conversations/{conversation_id}").json()["members"] if member["role"] == "owner")
    assert owner["username"] == "bob"


def test_identity_keys_can_be_rewrapped_and_replaced(chat):
    first = chat.publish_key("alice")
    rewrap = {key: value for key, value in _key_payload().items() if key.startswith("wrap")}
    assert chat.put("alice", f"/api/chat/keys/{first}/wrap", rewrap).status_code == 204
    assert chat.get("alice", "/api/chat/keys/me").json()["key"]["wrap_salt"] == rewrap["wrap_salt"]
    second = chat.publish_key("alice")
    keys = chat.get("bob", "/api/chat/keys", params={"user_ids": chat.id("alice"), "key_ids": first}).json()["keys"]
    assert {(key["key_id"], key["active"]) for key in keys} == {(first, False), (second, True)}
    assert all("wrapped_private_key" not in key for key in keys)
    assert chat.put("bob", f"/api/chat/keys/{second}/wrap", rewrap).status_code == 404


# -- profiles --------------------------------------------------------------------


def test_profiles_are_editable_viewable_and_searchable(chat):
    updated = chat.patch("alice", "/api/profiles/me", {
        "display_name": "Alice A.", "bio": "Value investor", "website": "https://example.com",
        "interests": ["Banks", "Banks", "Autos"], "companies": ["e02144"],
    })
    assert updated.status_code == 200
    assert updated.json()["interests"] == ["Banks", "Autos"]
    assert chat.patch("alice", "/api/profiles/me", {"website": "javascript:alert(1)"}).status_code == 422
    chat.post("alice", "/api/chat/channels/topic:stocks/messages", {"text": "Banks look cheap"})

    profile = chat.get("bob", "/api/profiles/alice").json()
    assert (profile["display_name"], profile["can_message"], profile["is_me"]) == ("Alice A.", True, False)
    assert profile["companies"] == [{"code": "E02144", "name": "TOYOTA MOTOR CORPORATION"}]
    assert [post["body"]["text"] for post in profile["recent_posts"]] == ["Banks look cheap"]
    assert profile["stats"]["posts"] == 1

    found = chat.get("bob", "/api/profiles", params={"q": "ali"}).json()["people"]
    assert [person["username"] for person in found] == ["alice"]
    chat.patch("alice", "/api/profiles/me", {"discoverable": False})
    assert chat.get("bob", "/api/profiles", params={"q": "ali"}).json()["people"] == []
    assert [person["username"] for person in chat.get("bob", "/api/profiles", params={"q": "alice"}).json()["people"]] == ["alice"]
    assert chat.get("bob", "/api/profiles/nobody-here").status_code == 404
