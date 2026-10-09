"""At-rest encryption for public chat messages.

Channel messages are readable by anyone who can open the channel, so the
server must be able to decrypt them to serve them. They are still stored
encrypted (AES-256-GCM) so a copy of ``chat.db`` alone reveals nothing: the
key ring is a secret row in ``app.db``, a separate file.

Direct and group messages never pass through here. They are encrypted in the
browser with keys the server never sees (see ``storage.ChatStore``).
"""

from __future__ import annotations

import base64
import secrets
import threading
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from src.orchestrator.common.db_config import get_app_db
from src.settings.store import SettingsStore

SERVER_SCHEME = "server-aes-gcm-v1"
E2E_SCHEME = "e2e-aes-gcm-v1"
KEY_RING_SETTING = "chat.message_keys"
_NONCE_BYTES = 12


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _unb64(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"), validate=True)


@dataclass(frozen=True)
class Sealed:
    ciphertext: str
    nonce: str
    key_version: int


class ChannelCipher:
    """A versioned AES-GCM key ring; new messages use the newest key.

    The ring is created on first use and stored in the ``settings`` table of
    the given application database (``app.db`` by default).
    """

    def __init__(self, app_db: str | Path | None = None) -> None:
        self.app_db = Path(app_db) if app_db is not None else Path(get_app_db())
        self._lock = threading.Lock()
        self._keys: dict[int, AESGCM] | None = None
        self._active = 0

    def _load(self) -> dict[int, AESGCM]:
        with self._lock:
            if self._keys is not None:
                return self._keys
            fresh = {"active": 1, "keys": {"1": _b64(secrets.token_bytes(32))}}
            data = SettingsStore(self.app_db).set_if_missing(KEY_RING_SETTING, fresh)
            keys = {int(version): AESGCM(_unb64(value)) for version, value in data["keys"].items()}
            active = int(data["active"])
            if active not in keys:
                raise ValueError("The chat key ring names a missing active key")
            self._keys, self._active = keys, active
            return keys

    def seal(self, plaintext: str, associated: str) -> Sealed:
        keys = self._load()
        nonce = secrets.token_bytes(_NONCE_BYTES)
        ciphertext = keys[self._active].encrypt(nonce, plaintext.encode("utf-8"), associated.encode("utf-8"))
        return Sealed(_b64(ciphertext), _b64(nonce), self._active)

    def open(self, sealed: Sealed, associated: str) -> str:
        keys = self._load()
        key = keys.get(sealed.key_version)
        if key is None:
            raise KeyError(f"Chat key version {sealed.key_version} is not in the key ring")
        return key.decrypt(_unb64(sealed.nonce), _unb64(sealed.ciphertext), associated.encode("utf-8")).decode("utf-8")


def channel_associated_data(channel_id: str, sender_id: str, message_id: str) -> str:
    """Binds a ciphertext to its channel, sender, and id, so rows cannot be swapped."""
    return f"shade-chat-v1|channel:{channel_id}|sender:{sender_id}|message:{message_id}"
