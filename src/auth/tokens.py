"""Opaque application token generation and storage-safe digests."""

from __future__ import annotations

import hashlib
import hmac
import secrets


def new_token(prefix: str = "") -> str:
    """Generate an opaque token; the optional prefix is non-secret metadata."""
    return f"{prefix}{secrets.token_urlsafe(48)}"


def token_digest(token: str) -> str:
    """Return a deterministic digest suitable for indexed database lookup."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def token_matches(token: str, expected_digest: str) -> bool:
    """Compare a presented token digest in constant time."""
    return hmac.compare_digest(token_digest(token), expected_digest)
