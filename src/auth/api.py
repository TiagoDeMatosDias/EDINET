"""FastAPI endpoints for account creation, login, refresh, and identity."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field

from .models import AuthenticatedUser
from .service import AuthError, AuthService

router = APIRouter(prefix="/api/auth", tags=["auth"])
REFRESH_COOKIE = "edinet_refresh"


class RegisterRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=1, max_length=128)
    email: str | None = Field(default=None, max_length=254)


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    login: str = Field(min_length=1, max_length=254)
    password: str = Field(min_length=1, max_length=128)


class UserResponse(BaseModel):
    user_id: str
    username: str
    email: str | None
    role: str
    status: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_at: datetime
    user: UserResponse


class RegisterResponse(BaseModel):
    user: UserResponse
    bootstrap_admin: bool


class AuthStatusResponse(BaseModel):
    mode: str
    registration_open: bool
    bootstrap_required: bool
    password_min_length: int


class ApiTokenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    scopes: list[str] = Field(default_factory=lambda: ["*"])
    expires_at: datetime | None = None


class ApiTokenResponse(BaseModel):
    token_id: str
    name: str
    token_prefix: str
    scopes_json: str
    created_at: str
    expires_at: str | None
    last_used_at: str | None
    revoked_at: str | None


def _service(request: Request) -> AuthService:
    service = getattr(request.app.state, "auth_service", None)
    if not isinstance(service, AuthService):
        raise HTTPException(status_code=503, detail="Authentication is unavailable")
    return service


def _user_response(user: AuthenticatedUser) -> UserResponse:
    return UserResponse(
        user_id=user.user_id,
        username=user.username,
        email=user.email,
        role=user.role,
        status=user.status,
    )


def _error(exc: AuthError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.args[0])


def _set_refresh_cookie(response: Response, request: Request, token: str, expires_at: datetime) -> None:
    response.set_cookie(
        REFRESH_COOKIE,
        token,
        max_age=max(0, int((expires_at - datetime.now(expires_at.tzinfo)).total_seconds())),
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="strict",
        path="/api/auth",
    )


@router.get("/status", response_model=AuthStatusResponse)
def auth_status(request: Request) -> AuthStatusResponse:
    service = _service(request)
    settings = request.app.state.settings
    return AuthStatusResponse(
        mode=settings.auth_mode,
        registration_open=service.registration_mode == "open",
        bootstrap_required=service.bootstrap_required,
        password_min_length=service.password_min_length,
    )


@router.post("/register", response_model=RegisterResponse, status_code=status.HTTP_201_CREATED)
def register(request: Request, payload: RegisterRequest) -> RegisterResponse:
    service = _service(request)
    try:
        user = service.register(
            payload.username,
            payload.password,
            payload.email,
            remote_addr=request.client.host if request.client else None,
        )
    except AuthError as exc:
        raise _error(exc) from exc
    return RegisterResponse(user=_user_response(user), bootstrap_admin=user.role == "admin")


@router.post("/login", response_model=TokenResponse)
def login(request: Request, response: Response, payload: LoginRequest) -> TokenResponse:
    try:
        result = _service(request).login(
            payload.login,
            payload.password,
            user_agent=request.headers.get("User-Agent"),
            remote_addr=request.client.host if request.client else None,
        )
    except AuthError as exc:
        raise _error(exc) from exc
    _set_refresh_cookie(response, request, result.tokens.refresh_token, result.tokens.refresh_expires_at)
    return TokenResponse(
        access_token=result.tokens.access_token,
        expires_at=result.tokens.access_expires_at,
        user=_user_response(result.user),
    )


@router.post("/refresh", response_model=TokenResponse)
def refresh(request: Request, response: Response) -> TokenResponse:
    refresh_token = request.cookies.get(REFRESH_COOKIE)
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Refresh token is required")
    try:
        result = _service(request).refresh(
            refresh_token,
            user_agent=request.headers.get("User-Agent"),
            remote_addr=request.client.host if request.client else None,
        )
    except AuthError as exc:
        raise _error(exc) from exc
    _set_refresh_cookie(response, request, result.tokens.refresh_token, result.tokens.refresh_expires_at)
    return TokenResponse(
        access_token=result.tokens.access_token,
        expires_at=result.tokens.access_expires_at,
        user=_user_response(result.user),
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(request: Request, response: Response) -> None:
    authorization = request.headers.get("Authorization", "")
    _, _, token = authorization.partition(" ")
    if token:
        _service(request).logout(token, remote_addr=request.client.host if request.client else None)
    refresh_token = request.cookies.get(REFRESH_COOKIE)
    if refresh_token:
        _service(request).logout(refresh_token, remote_addr=request.client.host if request.client else None)
    response.delete_cookie(REFRESH_COOKIE, path="/api/auth")


@router.get("/me", response_model=UserResponse)
def me(request: Request) -> UserResponse:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Authentication required")
    return _user_response(user)


@router.get("/tokens", response_model=list[ApiTokenResponse])
def list_tokens(request: Request) -> list[ApiTokenResponse]:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Authentication required")
    return [ApiTokenResponse(**dict(row)) for row in _service(request).store.list_api_tokens(user.user_id)]


@router.post("/tokens")
def create_token(request: Request, payload: ApiTokenRequest) -> dict[str, object]:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Authentication required")
    try:
        token = _service(request).create_api_token(
            user.user_id,
            payload.name,
            payload.scopes,
            payload.expires_at,
        )
    except AuthError as exc:
        raise _error(exc) from exc
    return {"token": token, "warning": "Copy this token now; it is not shown again."}


@router.delete("/tokens/{token_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_token(request: Request, token_id: str) -> None:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Authentication required")
    if not _service(request).store.revoke_api_token(user.user_id, token_id, datetime.now(timezone.utc)):
        raise HTTPException(status_code=404, detail="API token not found")


class ProfileUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: str | None = Field(default=None, min_length=3, max_length=64)
    email: str | None = Field(default=None, max_length=254)


class ChangePasswordRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    current_password: str = Field(min_length=1, max_length=128)
    new_password: str = Field(min_length=1, max_length=128)


class SessionResponse(BaseModel):
    session_id: str
    token_type: str
    created_at: str
    expires_at: str | None
    revoked_at: str | None
    user_agent: str | None


def _require_user(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


@router.patch("/me", response_model=UserResponse)
def update_profile(request: Request, payload: ProfileUpdateRequest) -> UserResponse:
    user = _require_user(request)
    try:
        updated = _service(request).update_profile(
            user.user_id,
            username=payload.username,
            email=payload.email,
        )
    except AuthError as exc:
        raise _error(exc) from exc
    return _user_response(updated)


@router.post("/change-password", status_code=status.HTTP_204_NO_CONTENT)
def change_password(request: Request, payload: ChangePasswordRequest) -> None:
    user = _require_user(request)
    try:
        _service(request).change_password(user.user_id, payload.current_password, payload.new_password)
    except AuthError as exc:
        raise _error(exc) from exc


@router.get("/sessions", response_model=list[SessionResponse])
def list_sessions(request: Request) -> list[SessionResponse]:
    user = _require_user(request)
    return [
        SessionResponse(
            session_id=row["session_id"],
            token_type=row["token_type"],
            created_at=str(row["created_at"]),
            expires_at=str(row["expires_at"]) if row.get("expires_at") else None,
            revoked_at=str(row["revoked_at"]) if row.get("revoked_at") else None,
            user_agent=row.get("user_agent"),
        )
        for row in _service(request).list_sessions(user.user_id)
    ]


@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_session(request: Request, session_id: str) -> None:
    user = _require_user(request)
    if not _service(request).revoke_session(user.user_id, session_id):
        raise HTTPException(status_code=404, detail="Session not found")


# -- administrator endpoints --

admin_router = APIRouter(prefix="/api/admin/auth", tags=["admin-auth"])


class AdminUserResponse(BaseModel):
    user_id: str
    username: str
    email: str | None
    role: str
    status: str
    token_version: int
    created_at: str
    updated_at: str
    last_login_at: str | None


class UpdateRoleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: str = Field(pattern=r"^(admin|operator|member)$")


class AuditEventResponse(BaseModel):
    event_id: str
    user_id: str | None
    event_type: str
    occurred_at: str
    remote_addr: str | None
    detail: str | None


@admin_router.get("/users", response_model=list[AdminUserResponse])
def admin_list_users(request: Request) -> list[AdminUserResponse]:
    user = _require_user(request)
    try:
        return [
            AdminUserResponse(
                user_id=row["user_id"],
                username=row["username"],
                email=row.get("email"),
                role=row["role"],
                status=row["status"],
                token_version=row.get("token_version", 1),
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
                last_login_at=str(row["last_login_at"]) if row.get("last_login_at") else None,
            )
            for row in _service(request).list_users(requested_by=user.user_id)
        ]
    except AuthError as exc:
        raise _error(exc) from exc


@admin_router.patch("/users/{target_user_id}/role", response_model=AdminUserResponse)
def admin_update_role(request: Request, target_user_id: str, payload: UpdateRoleRequest) -> AdminUserResponse:
    user = _require_user(request)
    try:
        updated = _service(request).update_user_role(target_user_id, payload.role, requested_by=user.user_id)
    except AuthError as exc:
        raise _error(exc) from exc
    return AdminUserResponse(
        user_id=updated.user_id,
        username=updated.username,
        email=updated.email,
        role=updated.role,
        status=updated.status,
        token_version=1,
        created_at="",
        updated_at="",
        last_login_at=None,
    )


@admin_router.patch("/users/{target_user_id}/disable", response_model=AdminUserResponse)
def admin_disable_user(request: Request, target_user_id: str) -> AdminUserResponse:
    user = _require_user(request)
    try:
        updated = _service(request).disable_user(target_user_id, requested_by=user.user_id)
    except AuthError as exc:
        raise _error(exc) from exc
    return AdminUserResponse(
        user_id=updated.user_id,
        username=updated.username,
        email=updated.email,
        role=updated.role,
        status=updated.status,
        token_version=1,
        created_at="",
        updated_at="",
        last_login_at=None,
    )


@admin_router.get("/audit", response_model=list[AuditEventResponse])
def admin_list_audit(request: Request, limit: int = 100) -> list[AuditEventResponse]:
    user = _require_user(request)
    try:
        return [
            AuditEventResponse(
                event_id=row["event_id"],
                user_id=row.get("user_id"),
                event_type=row["event_type"],
                occurred_at=str(row["occurred_at"]),
                remote_addr=row.get("remote_addr"),
                detail=row.get("detail"),
            )
            for row in _service(request).list_audit(requested_by=user.user_id, limit=limit)
        ]
    except AuthError as exc:
        raise _error(exc) from exc


class CreateInvitationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: str = Field(default="member", pattern=r"^(admin|operator|member)$")
    email: str | None = Field(default=None, max_length=254)


class AcceptInvitationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    invitation_token: str = Field(min_length=1, max_length=256)
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=1, max_length=128)


class ResetPasswordRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reset_token: str = Field(min_length=1, max_length=256)
    new_password: str = Field(min_length=1, max_length=128)


class AuthSettingsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    registration_mode: str | None = Field(default=None, pattern=r"^(open|closed|invite)$")
    default_role: str | None = Field(default=None, pattern=r"^(admin|operator|member)$")
    password_min_length: int | None = Field(default=None, ge=15, le=128)
    access_token_seconds: int | None = None


@admin_router.post("/invitations")
def admin_create_invitation(request: Request, payload: CreateInvitationRequest) -> dict[str, str]:
    user = _require_user(request)
    try:
        token = _service(request).create_invitation(
            requested_by=user.user_id,
            role=payload.role,
            email=payload.email,
        )
    except AuthError as exc:
        raise _error(exc) from exc
    return {"invitation_token": token, "warning": "Share this token securely; it is not shown again."}


@admin_router.delete("/invitations/{invitation_id}", status_code=status.HTTP_204_NO_CONTENT)
def admin_revoke_invitation(request: Request, invitation_id: str) -> None:
    user = _require_user(request)
    if not _service(request).revoke_invitation(invitation_id, requested_by=user.user_id):
        raise HTTPException(status_code=404, detail="Invitation not found")


@admin_router.post("/credential-resets")
def admin_create_reset(request: Request, target_user_id: str) -> dict[str, str]:
    user = _require_user(request)
    try:
        token = _service(request).create_credential_reset(target_user_id, requested_by=user.user_id)
    except AuthError as exc:
        raise _error(exc) from exc
    return {"reset_token": token, "warning": "Share this token securely; it is not shown again."}


@router.post("/accept-invitation", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def accept_invitation(request: Request, payload: AcceptInvitationRequest) -> UserResponse:
    """Accept an invitation token to create an account."""
    try:
        user = _service(request).accept_invitation(
            payload.invitation_token,
            payload.username,
            payload.password,
        )
    except AuthError as exc:
        raise _error(exc) from exc
    return _user_response(user)


@router.post("/reset-password", status_code=status.HTTP_204_NO_CONTENT)
def reset_password(request: Request, payload: ResetPasswordRequest) -> None:
    """Reset a password using a credential-reset token."""
    try:
        _service(request).reset_password(payload.reset_token, payload.new_password)
    except AuthError as exc:
        raise _error(exc) from exc


@admin_router.get("/settings")
def admin_get_settings(request: Request) -> dict[str, object]:
    user = _require_user(request)
    try:
        return _service(request).get_auth_settings(requested_by=user.user_id)
    except AuthError as exc:
        raise _error(exc) from exc


@admin_router.patch("/settings")
def admin_update_settings(request: Request, payload: AuthSettingsRequest) -> dict[str, object]:
    user = _require_user(request)
    try:
        return _service(request).update_auth_settings(
            requested_by=user.user_id,
            registration_mode=payload.registration_mode,
            default_role=payload.default_role,
            password_min_length=payload.password_min_length,
            access_token_seconds=payload.access_token_seconds,
        )
    except AuthError as exc:
        raise _error(exc) from exc
