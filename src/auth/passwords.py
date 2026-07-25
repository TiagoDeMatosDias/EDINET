"""Password hashing policy for application accounts."""

from __future__ import annotations

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

_HASHER = PasswordHasher(
    time_cost=3,
    memory_cost=64 * 1024,
    parallelism=2,
    hash_len=32,
    salt_len=16,
)


def hash_password(password: str) -> str:
    """Hash a password with the configured Argon2id parameters."""
    if not isinstance(password, str) or not password:
        raise ValueError("Password is required")
    return _HASHER.hash(password)


def verify_password(password_hash: str, password: str) -> bool:
    """Verify a password without exposing whether the account exists."""
    try:
        return bool(_HASHER.verify(password_hash, password))
    except (InvalidHashError, VerificationError, VerifyMismatchError, TypeError):
        return False


def password_needs_rehash(password_hash: str) -> bool:
    """Return whether a successful login should replace an old hash."""
    try:
        return _HASHER.check_needs_rehash(password_hash)
    except (InvalidHashError, TypeError):
        return True
