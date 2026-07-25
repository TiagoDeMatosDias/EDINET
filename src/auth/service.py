"""Application account lifecycle and opaque token service."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .models import AuthenticatedUser, IssuedTokens
from .passwords import hash_password, password_needs_rehash, verify_password
from .storage import AuthStore, timestamp, utc_now
from .tokens import new_token, token_digest

_USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{2,63}$")
_ACCESS_TTL = timedelta(minutes=15)
_REFRESH_TTL = timedelta(days=30)
_API_TOKEN_PREFIX = "ed_pat_"


class AuthError(ValueError):
    """Expected authentication or account-policy failure."""

    def __init__(self, message: str, *, code: str = "auth_error", status_code: int = 400):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True)
class LoginResult:
    user: AuthenticatedUser
    tokens: IssuedTokens


def _normalize_username(value: str) -> str:
    username = value.strip().casefold()
    if not _USERNAME_RE.fullmatch(username):
        raise AuthError(
            "Username must be 3-64 characters using letters, numbers, '.', '_' or '-'.",
            code="invalid_username",
        )
    return username


def _normalize_email(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    email = value.strip().casefold()
    if len(email) > 254 or "@" not in email or email.startswith("@"):
        raise AuthError("Email address is invalid", code="invalid_email")
    return email


def _validate_password(value: str) -> None:
    if not isinstance(value, str) or not 15 <= len(value) <= 128:
        raise AuthError(
            "Password must be between 15 and 128 characters.",
            code="invalid_password",
        )


def _user_from_row(row: Any) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=row["user_id"],
        username=row["username"],
        email=row["email"],
        role=row["role"],
        status=row["status"],
    )


def _login_key(login: str, remote_addr: str | None) -> str:
    value = f"{login.strip().casefold()}|{remote_addr or ''}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()


class AuthService:
    """Own account creation, password verification, sessions, and API tokens."""

    def __init__(
        self,
        store: AuthStore,
        *,
        registration_mode: str = "open",
        access_ttl: timedelta = _ACCESS_TTL,
        refresh_ttl: timedelta = _REFRESH_TTL,
    ) -> None:
        if registration_mode not in {"open", "closed"}:
            raise ValueError("registration_mode must be open or closed")
        self.store = store
        self.registration_mode = registration_mode
        self.access_ttl = access_ttl
        self.refresh_ttl = refresh_ttl

    @property
    def bootstrap_required(self) -> bool:
        return self.store.count_users() == 0

    def register(
        self,
        username: str,
        password: str,
        email: str | None = None,
        *,
        remote_addr: str | None = None,
    ) -> AuthenticatedUser:
        normalized_username = _normalize_username(username)
        normalized_email = _normalize_email(email)
        _validate_password(password)
        first_user = self.bootstrap_required
        if not first_user and self.registration_mode == "closed":
            raise AuthError("Registration is closed", code="registration_closed", status_code=403)
        now = utc_now()
        user = AuthenticatedUser(
            user_id=str(uuid.uuid4()),
            username=normalized_username,
            email=normalized_email,
            role="admin" if first_user else "member",
            status="active",
        )
        try:
            stored_as_first = self.store.create_user(
                {
                    "user_id": user.user_id,
                    "username": user.username,
                    "email": user.email,
                    "password_hash": hash_password(password),
                    "role": user.role,
                    "status": user.status,
                    "created_at": timestamp(now),
                    "updated_at": timestamp(now),
                }
            )
        except Exception as exc:
            if "UNIQUE" in str(exc).upper():
                raise AuthError("Username or email is already registered", code="account_exists", status_code=409) from exc
            raise
        if stored_as_first != first_user:
            user = AuthenticatedUser(
                user_id=user.user_id,
                username=user.username,
                email=user.email,
                role="admin" if stored_as_first else "member",
                status=user.status,
            )
        self.store.audit("account_created", user.user_id, remote_addr=remote_addr)
        return user

    def login(
        self,
        login: str,
        password: str,
        *,
        user_agent: str | None = None,
        remote_addr: str | None = None,
    ) -> LoginResult:
        login_key = _login_key(login, remote_addr)
        throttle = self.store.throttle_row(login_key)
        if throttle is not None and throttle["locked_until"]:
            if throttle["locked_until"] > timestamp(utc_now()):
                raise AuthError("Invalid credentials", code="invalid_credentials", status_code=401)
        row = self.store.get_user_by_login(login)
        if row is None or row["status"] != "active" or not verify_password(row["password_hash"], password):
            self.store.record_login_failure(login_key, utc_now())
            self.store.audit("login_failed", None, remote_addr=remote_addr)
            raise AuthError("Invalid credentials", code="invalid_credentials", status_code=401)
        self.store.clear_login_throttle(login_key)
        now = utc_now()
        replacement = hash_password(password) if password_needs_rehash(row["password_hash"]) else None
        self.store.update_last_login(row["user_id"], now, replacement)
        tokens = self._issue_sessions(row["user_id"], now, user_agent, remote_addr)
        user = _user_from_row(row)
        self.store.audit("login_succeeded", user.user_id, remote_addr=remote_addr)
        return LoginResult(user=user, tokens=tokens)

    def _issue_sessions(
        self,
        user_id: str,
        now: datetime,
        user_agent: str | None,
        remote_addr: str | None,
    ) -> IssuedTokens:
        access = new_token("ed_at_")
        refresh = new_token("ed_rt_")
        access_expires = now + self.access_ttl
        refresh_expires = now + self.refresh_ttl
        family_id = str(uuid.uuid4())
        self.store.create_session(self._session_values(user_id, access, "access", now, access_expires, user_agent, remote_addr, family_id))
        self.store.create_session(self._session_values(user_id, refresh, "refresh", now, refresh_expires, user_agent, remote_addr, family_id))
        return IssuedTokens(access, refresh, access_expires, refresh_expires)

    @staticmethod
    def _session_values(
        user_id: str,
        token: str,
        token_type: str,
        created: datetime,
        expires: datetime,
        user_agent: str | None,
        remote_addr: str | None,
        family_id: str | None = None,
    ) -> dict[str, Any]:
        return {
            "session_id": str(uuid.uuid4()),
            "user_id": user_id,
            "family_id": family_id or str(uuid.uuid4()),
            "token_hash": token_digest(token),
            "token_type": token_type,
            "created_at": timestamp(created),
            "expires_at": timestamp(expires),
            "user_agent": (user_agent or "")[:500],
            "remote_addr": (remote_addr or "")[:100],
        }

    def refresh(self, refresh_token: str, *, user_agent: str | None = None, remote_addr: str | None = None) -> LoginResult:
        now = utc_now()
        refresh_hash = token_digest(refresh_token)
        row = self.store.find_active_session(refresh_hash, "refresh", now)
        if row is None:
            previous = self.store.find_session(refresh_hash, "refresh")
            if previous is not None and previous["revoked_at"] is not None:
                self.store.revoke_family(previous["family_id"], now)
                self.store.audit("refresh_reuse_detected", previous["user_id"], remote_addr=remote_addr)
            raise AuthError("Refresh token is invalid or expired", code="invalid_refresh", status_code=401)
        access = new_token("ed_at_")
        replacement = new_token("ed_rt_")
        access_expires = now + self.access_ttl
        refresh_expires = now + self.refresh_ttl
        family_id = row["family_id"] or str(uuid.uuid4())
        access_values = self._session_values(row["user_id"], access, "access", now, access_expires, user_agent, remote_addr, family_id)
        refresh_values = self._session_values(row["user_id"], replacement, "refresh", now, refresh_expires, user_agent, remote_addr, family_id)
        try:
            self.store.rotate_refresh(row["session_id"], access_values, refresh_values, now)
        except ValueError as exc:
            self.store.revoke_family(family_id, now)
            self.store.audit("refresh_reuse_detected", row["user_id"], remote_addr=remote_addr)
            raise AuthError("Refresh token is invalid or expired", code="invalid_refresh", status_code=401) from exc
        user = _user_from_row(row)
        self.store.audit("refresh_succeeded", user.user_id, remote_addr=remote_addr)
        return LoginResult(user, IssuedTokens(access, replacement, access_expires, refresh_expires))

    def authenticate(self, token: str) -> AuthenticatedUser | None:
        if not token or token.startswith("ed_pat_"):
            row = self.store.find_active_api_token(token_digest(token), utc_now()) if token else None
            if row is None:
                return None
            self.store.touch_api_token(row["token_id"], utc_now())
            return _user_from_row(row)
        row = self.store.find_active_session(token_digest(token), "access", utc_now())
        return _user_from_row(row) if row is not None else None

    def logout(self, token: str, *, remote_addr: str | None = None) -> bool:
        now = utc_now()
        row = self.store.find_session(token_digest(token), "refresh" if token.startswith("ed_rt_") else "access")
        if row is not None and token.startswith("ed_rt_"):
            revoked = self.store.revoke_family(row["family_id"], now) > 0
        else:
            revoked = self.store.revoke_session(token_digest(token), now)
        if revoked:
            self.store.audit("logout", row["user_id"] if row is not None else None, remote_addr=remote_addr)
        return revoked

    def create_api_token(
        self,
        user_id: str,
        name: str,
        scopes: list[str] | None = None,
        expires_at: datetime | None = None,
    ) -> str:
        if not name.strip() or len(name.strip()) > 100:
            raise AuthError("Token name is required", code="invalid_token_name")
        raw = new_token(_API_TOKEN_PREFIX)
        now = utc_now()
        self.store.add_api_token(
            {
                "token_id": str(uuid.uuid4()),
                "user_id": user_id,
                "name": name.strip(),
                "token_hash": token_digest(raw),
                "token_prefix": raw[:12],
                "scopes_json": json.dumps(sorted(set(scopes or ["*"])), separators=(",", ":")),
                "created_at": timestamp(now),
                "expires_at": timestamp(expires_at) if expires_at else None,
            }
        )
        self.store.audit("api_token_created", user_id, detail=name.strip())
        return raw

    def change_password(self, user_id: str, current_password: str, new_password: str) -> None:
        row = self.store.get_user(user_id)
        if row is None or row["status"] != "active":
            raise AuthError("Account not found", code="account_not_found", status_code=401)
        if not verify_password(row["password_hash"], current_password):
            self.store.audit("password_change_failed", user_id)
            raise AuthError("Current password is incorrect", code="invalid_password", status_code=401)
        _validate_password(new_password)
        now = utc_now()
        self.store.change_user_password(user_id, hash_password(new_password), now)
        self.store.audit("password_changed", user_id)

    def update_profile(self, user_id: str, *, username: str | None = None, email: str | None = None) -> AuthenticatedUser:
        if username is not None:
            _normalize_username(username)
        if email is not None:
            _normalize_email(email)
        now = utc_now()
        try:
            self.store.update_user_profile(user_id, username=username, email=email, when=now)
        except Exception as exc:
            if "UNIQUE" in str(exc).upper():
                raise AuthError("Username or email is already registered", code="account_exists", status_code=409) from exc
            raise
        row = self.store.get_user(user_id)
        if row is None:
            raise AuthError("Account not found", code="account_not_found", status_code=401)
        self.store.audit("profile_updated", user_id)
        return _user_from_row(row)

    def list_sessions(self, user_id: str) -> list[dict[str, object]]:
        rows = self.store.list_user_sessions(user_id)
        return [dict(row) for row in rows]

    def revoke_session(self, user_id: str, session_id: str) -> bool:
        revoke = self.store.revoke_user_session(user_id, session_id, utc_now())
        if revoke:
            self.store.audit("session_revoked", user_id, detail=f"session={session_id}")
        return revoke

    def revoke_all_sessions(self, user_id: str) -> int:
        count = self.store.revoke_all_user_sessions(user_id, utc_now())
        self.store.audit("all_sessions_revoked", user_id, detail=f"count={count}")
        return count

    # -- admin operations --

    def _require_admin(self, user_id: str) -> AuthenticatedUser:
        row = self.store.get_user(user_id)
        if row is None or row["status"] != "active" or row["role"] != "admin":
            raise AuthError("Administrator permission required", code="forbidden", status_code=403)
        return _user_from_row(row)

    def list_users(self, *, requested_by: str) -> list[dict[str, object]]:
        self._require_admin(requested_by)
        rows = self.store.list_users()
        return [dict(row) for row in rows]

    def disable_user(self, user_id: str, *, requested_by: str) -> AuthenticatedUser:
        self._require_admin(requested_by)
        now = utc_now()
        try:
            self.store.disable_user(user_id, now)
        except ValueError as exc:
            raise AuthError(str(exc), code="last_admin_protection", status_code=409) from exc
        row = self.store.get_user(user_id)
        if row is None:
            raise AuthError("User not found", code="not_found", status_code=404)
        self.store.audit("user_disabled", user_id, detail=f"by={requested_by}")
        return _user_from_row(row)

    def update_user_role(self, user_id: str, role: str, *, requested_by: str) -> AuthenticatedUser:
        self._require_admin(requested_by)
        now = utc_now()
        try:
            self.store.update_user_role(user_id, role, now)
        except ValueError as exc:
            raise AuthError(str(exc), code="last_admin_protection", status_code=409) from exc
        row = self.store.get_user(user_id)
        if row is None:
            raise AuthError("User not found", code="not_found", status_code=404)
        self.store.audit("role_changed", user_id, detail=f"role={role} by={requested_by}")
        return _user_from_row(row)

    def list_audit(self, *, requested_by: str, limit: int = 100) -> list[dict[str, object]]:
        self._require_admin(requested_by)
        rows = self.store.list_audit_events(limit)
        return [dict(row) for row in rows]

    # -- invitations (admin only) --

    def create_invitation(self, *, requested_by: str, role: str = "member", email: str | None = None) -> str:
        self._require_admin(requested_by)
        if role not in {"admin", "operator", "member"}:
            raise AuthError("Invalid role", code="invalid_role")
        token = new_token("ed_inv_")
        token_hash_val = token_digest(token)
        expires = utc_now() + timedelta(days=7)
        self.store.create_invitation(
            token_hash_val,
            role=role,
            email_normalized=_normalize_email(email),
            created_by=requested_by,
            expires_at=expires,
        )
        self.store.audit("invitation_created", requested_by, detail=f"role={role}")
        return token

    def accept_invitation(self, invitation_token: str, username: str, password: str) -> AuthenticatedUser:
        now = utc_now()
        token_hash_val = token_digest(invitation_token)
        invitation = self.store.find_valid_invitation(token_hash_val, now)
        if invitation is None:
            raise AuthError("Invitation is invalid or expired", code="invalid_invitation", status_code=410)
        normalized_username = _normalize_username(username)
        _validate_password(password)
        user = AuthenticatedUser(
            user_id=str(uuid.uuid4()),
            username=normalized_username,
            email=invitation["email_normalized"],
            role=invitation["role"],
            status="active",
        )
        try:
            self.store.create_user({
                "user_id": user.user_id,
                "username": user.username,
                "email": user.email,
                "password_hash": hash_password(password),
                "role": user.role,
                "status": user.status,
                "created_at": timestamp(now),
                "updated_at": timestamp(now),
            })
            self.store.accept_invitation(token_hash_val, user.user_id, now)
            self.store.audit("invitation_accepted", user.user_id)
        except Exception as exc:
            if "UNIQUE" in str(exc).upper():
                raise AuthError("Username or email is already registered", code="account_exists", status_code=409) from exc
            raise
        return user

    def revoke_invitation(self, invitation_id: str, *, requested_by: str) -> bool:
        self._require_admin(requested_by)
        revoked = self.store.revoke_invitation(invitation_id, utc_now())
        if revoked:
            self.store.audit("invitation_revoked", requested_by, detail=f"invitation={invitation_id}")
        return revoked

    # -- credential reset (admin only) --

    def create_credential_reset(self, user_id: str, *, requested_by: str) -> str:
        self._require_admin(requested_by)
        token = new_token("ed_reset_")
        expires = utc_now() + timedelta(hours=1)
        self.store.create_credential_reset(user_id, token_digest(token), created_by=requested_by, expires_at=expires)
        self.store.audit("credential_reset_created", user_id, detail=f"by={requested_by}")
        return token

    def reset_password(self, reset_token: str, new_password: str) -> None:
        now = utc_now()
        row = self.store.consume_credential_reset(token_digest(reset_token), now)
        if row is None:
            raise AuthError("Reset token is invalid or expired", code="invalid_reset", status_code=410)
        _validate_password(new_password)
        self.store.change_user_password(row["user_id"], hash_password(new_password), now)
        self.store.audit("password_reset", row["user_id"], detail="via credential reset")

    # -- auth settings (admin only) --

    def get_auth_settings(self, *, requested_by: str) -> dict[str, object]:
        self._require_admin(requested_by)
        return self.store.get_auth_settings()

    def update_auth_settings(self, *, requested_by: str, **kwargs: Any) -> dict[str, object]:
        self._require_admin(requested_by)
        allowed = {"registration_mode", "default_role", "access_token_seconds", "refresh_idle_seconds", "refresh_absolute_seconds"}
        filtered = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        if "registration_mode" in filtered and filtered["registration_mode"] not in {"open", "closed", "invite"}:
            raise AuthError("registration_mode must be open, closed, or invite", code="invalid_setting")
        self.store.update_auth_settings(**filtered, updated_by=requested_by)
        self.store.audit("auth_settings_updated", requested_by)
        return self.store.get_auth_settings()
