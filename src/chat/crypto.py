"""At-rest encryption for public chat messages.

Channel messages are readable by anyone who can open the channel, so the
server must be able to decrypt them to serve them. They are still stored
encrypted (AES-256-GCM) so a copy of ``chat.db`` alone reveals nothing: the
key ring lives in a separate file in the state directory (or the
``EDINET_CHAT_KEYS`` file), created with owner-only permissions.

Direct and group messages never pass through here. They are encrypted in the
browser with keys the server never sees (see ``storage.ChatStore``).
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import threading
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from src.utilities.runtime_paths import state_dir

SERVER_SCHEME = "server-aes-gcm-v1"
E2E_SCHEME = "e2e-aes-gcm-v1"
_NONCE_BYTES = 12


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _unb64(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"), validate=True)


def default_keyring_path() -> Path:
    configured = os.getenv("EDINET_CHAT_KEYS", "").strip()
    if configured:
        return Path(configured).expanduser()
    return state_dir() / "secrets" / "chat_message_keys.json"


@dataclass(frozen=True)
class Sealed:
    ciphertext: str
    nonce: str
    key_version: int


class ChannelCipher:
    """A versioned AES-GCM key ring; new messages use the newest key."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else default_keyring_path()
        self._lock = threading.Lock()
        self._keys: dict[int, AESGCM] | None = None
        self._active = 0

    def _load(self) -> dict[int, AESGCM]:
        with self._lock:
            if self._keys is not None:
                return self._keys
            if self.path.exists():
                data = json.loads(self.path.read_text(encoding="utf-8"))
            else:
                data = {"active": 1, "keys": {"1": _b64(secrets.token_bytes(32))}}
                self._write_new(data)
            keys = {int(version): AESGCM(_unb64(value)) for version, value in data["keys"].items()}
            active = int(data["active"])
            if active not in keys:
                raise ValueError(f"Chat key ring {self.path} names a missing active key")
            self._keys, self._active = keys, active
            return keys

    def _write_new(self, data: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # O_EXCL: two processes starting together must not each write a key.
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            descriptor = os.open(self.path, flags, 0o600)
        except FileExistsError:
            data.clear()
            data.update(json.loads(self.path.read_text(encoding="utf-8")))
            return
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(data, handle)

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
