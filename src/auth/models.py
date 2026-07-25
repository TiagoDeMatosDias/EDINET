"""Small immutable auth-domain values shared by API and middleware."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class AuthenticatedUser:
    user_id: str
    username: str
    email: str | None
    role: str
    status: str


@dataclass(frozen=True)
class IssuedTokens:
    access_token: str
    refresh_token: str
    access_expires_at: datetime
    refresh_expires_at: datetime
