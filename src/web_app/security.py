"""Central security, error, and filesystem policy for the web application."""

from __future__ import annotations

import ipaddress
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware

from src.utilities.runtime_paths import state_dir

logger = logging.getLogger(__name__)

_DATABASE_SUFFIXES = frozenset({".db", ".sqlite", ".sqlite3"})
_DEFAULT_MAX_UPLOAD_BYTES = 10 * 1024 * 1024
_DEFAULT_MAX_PIPELINE_UPLOAD_BYTES = 500 * 1024 * 1024
_DEFAULT_MAX_EXPORT_BYTES = 25 * 1024 * 1024
_DEFAULT_MAX_BACKTEST_ARTIFACT_BYTES = 256 * 1024 * 1024
_DEFAULT_MAX_REPORT_ARTIFACT_BYTES = 128 * 1024 * 1024
_REQUEST_ENVELOPE_OVERHEAD_BYTES = 1024 * 1024
_DEFAULT_JOB_WORKSPACE_ROOT = state_dir() / "jobs"
_DEFAULT_AUTH_DB_PATH: Path | None = None


def _default_auth_db_path() -> Path:
    global _DEFAULT_AUTH_DB_PATH
    if _DEFAULT_AUTH_DB_PATH is None:
        from src.orchestrator.common.db_config import get_auth_db

        _DEFAULT_AUTH_DB_PATH = Path(get_auth_db())
    return _DEFAULT_AUTH_DB_PATH
_PUBLIC_AUTH_PATHS = frozenset(
    {
        "/api/auth/status",
        "/api/auth/register",
        "/api/auth/login",
        "/api/auth/refresh",
        "/api/auth/accept-invitation",
        "/api/auth/reset-password",
    }
)


_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
# FastAPI's interactive docs load their UI from a CDN, so the SPA policy below
# would blank them; they keep FastAPI's defaults.
_CSP_EXEMPT_PATHS = frozenset({"/docs", "/docs/oauth2-redirect", "/redoc"})
_CONTENT_SECURITY_POLICY = "; ".join(
    (
        "default-src 'self'",
        "script-src 'self'",
        # React style props, chart.js, and filing HTML rely on inline styles.
        "style-src 'self' 'unsafe-inline'",
        "img-src 'self' data: blob:",
        "font-src 'self' data:",
        "connect-src 'self'",
        "object-src 'none'",
        "base-uri 'self'",
        "form-action 'self'",
        "frame-ancestors 'none'",
    )
)


def _max_request_bytes(settings: "AppSettings", path: str) -> int:
    limit = (
        max(settings.max_upload_bytes, settings.max_export_bytes)
        + _REQUEST_ENVELOPE_OVERHEAD_BYTES
    )
    if path == "/api/pipeline/run":
        limit = max(
            limit,
            _DEFAULT_MAX_PIPELINE_UPLOAD_BYTES + _REQUEST_ENVELOPE_OVERHEAD_BYTES,
        )
    return limit


def _too_large_response(correlation_id: str) -> JSONResponse:
    return JSONResponse(
        status_code=413,
        content={
            "code": "request_too_large",
            "detail": "Request body exceeds the configured size limit",
            "correlation_id": correlation_id,
        },
        headers={"X-Correlation-ID": correlation_id},
    )


class _BodyTooLarge(Exception):
    """Raised from the wrapped ``receive`` once a body passes its limit."""


class BodySizeLimitMiddleware:
    """Enforce request size limits on the bytes actually received.

    The ``Content-Length`` check in ``request_security`` rejects honest
    oversized requests early; this catches chunked or mislabelled bodies,
    which carry no trustworthy length header.
    """

    def __init__(self, app, settings: "AppSettings") -> None:
        self.app = app
        self.settings = settings

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        limit = _max_request_bytes(self.settings, scope.get("path", ""))
        received = 0
        response_started = False

        async def limited_receive():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    raise _BodyTooLarge
            return message

        async def tracking_send(message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracking_send)
        except _BodyTooLarge:
            if response_started:
                raise
            state = scope.get("state") or {}
            correlation_id = state.get("correlation_id") or str(uuid4())
            await _too_large_response(correlation_id)(scope, receive, send)


def _apply_security_headers(response, path: str, settings: "AppSettings") -> None:
    headers = response.headers
    headers.setdefault("X-Content-Type-Options", "nosniff")
    headers.setdefault("Referrer-Policy", "no-referrer")
    headers.setdefault("X-Frame-Options", "DENY")
    if path not in _CSP_EXEMPT_PATHS:
        headers.setdefault("Content-Security-Policy", _CONTENT_SECURITY_POLICY)
    # HSTS pins a host to HTTPS in the browser for a long time. Only send it
    # for remote deployments: pinning "localhost" would affect every other
    # local development server on this machine.
    if settings.remote:
        headers.setdefault("Strict-Transport-Security", "max-age=31536000")


class OperatorGuidanceError(StarletteHTTPException):
    """A 5xx whose detail is a fixed, hand-written instruction for the operator.

    Ordinary 5xx details are replaced with a generic message because they may
    carry exception text; this subclass marks a detail as safe to show.
    """

    def __init__(self, detail: str, status_code: int = 503) -> None:
        super().__init__(status_code=status_code, detail=detail)


class SecurityConfigurationError(ValueError):
    """Raised when remote access is requested without safe configuration."""


class PathPolicyError(ValueError):
    """Raised when an untrusted filesystem path is outside the allowed scope."""


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise SecurityConfigurationError(
        f"{name} must be one of true/false, yes/no, on/off, or 1/0"
    )


def is_loopback_host(host: str) -> bool:
    """Return whether a bind host is limited to the local machine."""
    normalized = host.strip().strip("[]").casefold()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True)
class AppSettings:
    """Validated settings loaded once while the application is assembled."""

    host: str = "127.0.0.1"
    port: int = 8000
    allow_remote: bool = False
    auth_mode: str = "accounts"
    registration_mode: str = "open"
    # Resolved through db_config so a bare AppSettings() never falls back to
    # a cwd-relative path that could be an operator database.
    auth_db_path: Path = field(default_factory=lambda: _default_auth_db_path())
    application_token: str | None = None
    # Deprecated compatibility field; never populated from a provider token.
    api_token: str | None = None
    allowed_data_roots: tuple[Path, ...] = ()
    max_upload_bytes: int = _DEFAULT_MAX_UPLOAD_BYTES
    max_export_bytes: int = _DEFAULT_MAX_EXPORT_BYTES
    max_backtest_artifact_bytes: int = _DEFAULT_MAX_BACKTEST_ARTIFACT_BYTES
    max_report_artifact_bytes: int = _DEFAULT_MAX_REPORT_ARTIFACT_BYTES
    sqlite_busy_timeout_ms: int = 30_000
    job_retention_hours: int = 24
    job_workspace_root: Path = _DEFAULT_JOB_WORKSPACE_ROOT
    trusted_hosts: tuple[str, ...] = ()

    @property
    def remote(self) -> bool:
        return not is_loopback_host(self.host)

    @property
    def authentication_required(self) -> bool:
        return self.auth_mode == "accounts"

    def validate(self) -> "AppSettings":
        if not 1 <= self.port <= 65_535:
            raise SecurityConfigurationError("EDINET_PORT must be between 1 and 65535")
        if (
            self.max_upload_bytes < 1
            or self.max_export_bytes < 1
            or self.max_backtest_artifact_bytes < 1
            or self.max_report_artifact_bytes < 1
        ):
            raise SecurityConfigurationError(
                "Upload, export, backtest, and report artifact limits must be positive"
            )
        if self.sqlite_busy_timeout_ms < 1:
            raise SecurityConfigurationError(
                "EDINET_SQLITE_BUSY_TIMEOUT_MS must be positive"
            )
        if self.job_retention_hours < 1:
            raise SecurityConfigurationError(
                "EDINET_JOB_RETENTION_HOURS must be positive"
            )
        if self.auth_mode not in {"disabled", "accounts"}:
            raise SecurityConfigurationError(
                "EDINET_AUTH_MODE must be disabled or accounts"
            )
        if self.registration_mode not in {"open", "closed"}:
            raise SecurityConfigurationError(
                "EDINET_REGISTRATION_MODE must be open or closed"
            )
        if self.remote and not self.allow_remote:
            raise SecurityConfigurationError(
                "Non-loopback binding requires EDINET_ALLOW_REMOTE=true"
            )
        if self.remote and self.auth_mode != "accounts":
            raise SecurityConfigurationError(
                "Non-loopback binding requires EDINET_AUTH_MODE=accounts"
            )
        if self.remote and not self.trusted_hosts:
            raise SecurityConfigurationError(
                "Non-loopback binding requires EDINET_TRUSTED_HOSTS"
            )
        return self

    @classmethod
    def from_env(
        cls,
        *,
        host: str | None = None,
        port: int | None = None,
        allow_remote: bool | None = None,
    ) -> "AppSettings":
        roots_value = os.getenv("EDINET_ALLOWED_DATA_ROOTS", "")
        roots = tuple(
            Path(item.strip()).expanduser()
            for item in roots_value.split(os.pathsep)
            if item.strip()
        )
        trusted_hosts = tuple(
            item.strip()
            for item in os.getenv("EDINET_TRUSTED_HOSTS", "").split(",")
            if item.strip()
        )
        configured_host = (
            host
            if host is not None
            else (os.getenv("EDINET_HOST") or "127.0.0.1")
        )
        settings = cls(
            host=configured_host.strip(),
            port=port if port is not None else int(os.getenv("EDINET_PORT", "8000")),
            allow_remote=(
                allow_remote
                if allow_remote is not None
                else _env_flag("EDINET_ALLOW_REMOTE")
            ),
            auth_mode=(os.getenv("EDINET_AUTH_MODE") or "accounts").strip().casefold(),
            registration_mode=(
                os.getenv("EDINET_REGISTRATION_MODE") or "open"
            ).strip().casefold(),
            auth_db_path=Path(
                os.getenv("EDINET_AUTH_DB") or str(_default_auth_db_path())
            ).expanduser(),
            application_token=os.getenv("EDINET_APP_TOKEN") or None,
            allowed_data_roots=roots,
            max_upload_bytes=int(
                os.getenv(
                    "EDINET_MAX_UPLOAD_BYTES",
                    str(_DEFAULT_MAX_UPLOAD_BYTES),
                )
            ),
            max_export_bytes=int(
                os.getenv(
                    "EDINET_MAX_EXPORT_BYTES",
                    str(_DEFAULT_MAX_EXPORT_BYTES),
                )
            ),
            max_backtest_artifact_bytes=int(
                os.getenv(
                    "EDINET_MAX_BACKTEST_ARTIFACT_BYTES",
                    str(_DEFAULT_MAX_BACKTEST_ARTIFACT_BYTES),
                )
            ),
            max_report_artifact_bytes=int(
                os.getenv(
                    "EDINET_MAX_REPORT_ARTIFACT_BYTES",
                    str(_DEFAULT_MAX_REPORT_ARTIFACT_BYTES),
                )
            ),
            sqlite_busy_timeout_ms=int(
                os.getenv("EDINET_SQLITE_BUSY_TIMEOUT_MS", "30000")
            ),
            job_retention_hours=int(
                os.getenv("EDINET_JOB_RETENTION_HOURS", "24")
            ),
            job_workspace_root=Path(
                os.getenv(
                    "EDINET_JOB_WORKSPACE_ROOT",
                    str(_DEFAULT_JOB_WORKSPACE_ROOT),
                )
            ).expanduser(),
            trusted_hosts=trusted_hosts,
        )
        return settings.validate()


class PathPolicy:
    """Authorize resolved files against explicit roots and exact files."""

    def __init__(
        self,
        *,
        read_roots: Iterable[str | Path] = (),
        write_roots: Iterable[str | Path] = (),
        allowed_files: Iterable[str | Path] = (),
    ) -> None:
        self.read_roots = self._normalize_roots(read_roots)
        self.write_roots = self._normalize_roots(write_roots)
        self.allowed_files = frozenset(
            Path(path).expanduser().resolve(strict=False)
            for path in allowed_files
        )

    @staticmethod
    def _normalize_roots(
        roots: Iterable[str | Path],
    ) -> tuple[Path, ...]:
        return tuple(
            Path(root).expanduser().resolve(strict=False)
            for root in roots
        )

    @staticmethod
    def _is_within(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False

    def authorize_database(
        self,
        value: str | Path,
        *,
        writable: bool = False,
    ) -> Path:
        """Resolve and authorize an existing SQLite database file."""
        raw = str(value).strip()
        if not raw:
            raise PathPolicyError("A database path is required")
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            raise PathPolicyError("Database paths must be absolute")
        if ":" in candidate.name:
            raise PathPolicyError("Alternate data stream paths are not allowed")
        if candidate.suffix.casefold() not in _DATABASE_SUFFIXES:
            raise PathPolicyError("Unsupported database file type")

        try:
            resolved = candidate.resolve(strict=True)
        except (FileNotFoundError, OSError) as exc:
            raise PathPolicyError("Database file was not found") from exc
        if not resolved.is_file():
            raise PathPolicyError("Database path is not a normal file")

        roots = self.write_roots if writable else self.read_roots
        if resolved in self.allowed_files:
            return resolved
        if any(self._is_within(resolved, root) for root in roots):
            return resolved
        raise PathPolicyError("Database path is outside the configured data roots")


def configured_database_policy(
    extra_roots: Iterable[str | Path] = (),
) -> PathPolicy:
    """Build a policy around configured database files and their directories."""
    from src.orchestrator.common.db_config import get_db1, get_db2, get_db3

    configured: list[Path] = []
    for getter in (get_db1, get_db2, get_db3):
        try:
            configured.append(Path(getter()).expanduser().resolve(strict=False))
        except (OSError, ValueError):
            logger.warning("Could not resolve a configured database path", exc_info=True)

    roots = {
        path.parent
        for path in configured
    }
    roots.update(
        Path(root).expanduser().resolve(strict=False)
        for root in extra_roots
    )
    return PathPolicy(
        read_roots=roots,
        write_roots=roots,
        allowed_files=configured,
    )


def install_security(app: FastAPI, settings: AppSettings) -> None:
    """Install authentication, request IDs, and safe exception responses once."""
    settings.validate()
    if getattr(app.state, "security_installed", False):
        return
    app.state.security_installed = True
    app.state.settings = settings
    from src.auth.models import AuthenticatedUser
    from src.auth.service import AuthService
    from src.auth.storage import AuthStore

    app.state.auth_service = AuthService(
        AuthStore(
            settings.auth_db_path,
            busy_timeout_ms=settings.sqlite_busy_timeout_ms,
        ),
        registration_mode=settings.registration_mode,
    )
    local_user = AuthenticatedUser(
        user_id="local",
        username="local",
        email=None,
        role="admin",
        status="active",
    )
    # Added before ``request_security`` so it runs inside it: the 413 it
    # produces still receives the correlation id and security headers.
    app.add_middleware(BodySizeLimitMiddleware, settings=settings)
    if settings.remote:
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=list(settings.trusted_hosts),
        )

    @app.middleware("http")
    async def request_security(request: Request, call_next):
        correlation_id = str(uuid4())
        request.state.correlation_id = correlation_id
        if not settings.authentication_required:
            request.state.user = local_user
        max_request_bytes = _max_request_bytes(settings, request.url.path)
        content_length = request.headers.get("Content-Length")
        if content_length:
            try:
                too_large = int(content_length) > max_request_bytes
            except ValueError:
                too_large = True
            if too_large:
                response = _too_large_response(correlation_id)
                _apply_security_headers(response, request.url.path, settings)
                return response

        if (
            settings.authentication_required
            and request.url.path.startswith("/api/")
            and request.url.path not in _PUBLIC_AUTH_PATHS
        ):
            authorization = request.headers.get("Authorization", "")
            scheme, _, supplied = authorization.partition(" ")
            user = None
            if scheme.casefold() == "bearer" and supplied:
                user = request.app.state.auth_service.authenticate(supplied)
            if user is None:
                response = JSONResponse(
                    status_code=401,
                    content={
                        "code": "unauthorized",
                        "detail": "Authentication required",
                        "correlation_id": correlation_id,
                    },
                    headers={
                        "WWW-Authenticate": "Bearer",
                        "X-Correlation-ID": correlation_id,
                    },
                )
                _apply_security_headers(response, request.url.path, settings)
                return response
            if (
                user.scopes is not None
                and "*" not in user.scopes
                and request.method.upper() not in _SAFE_METHODS
            ):
                response = JSONResponse(
                    status_code=403,
                    content={
                        "code": "insufficient_scope",
                        "detail": "This API token is read-only",
                        "correlation_id": correlation_id,
                    },
                    headers={
                        "WWW-Authenticate": 'Bearer error="insufficient_scope"',
                        "X-Correlation-ID": correlation_id,
                    },
                )
                _apply_security_headers(response, request.url.path, settings)
                return response
            request.state.user = user

        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        _apply_security_headers(response, request.url.path, settings)
        return response

    @app.exception_handler(StarletteHTTPException)
    async def safe_http_exception(
        request: Request,
        exc: StarletteHTTPException,
    ):
        correlation_id = getattr(
            request.state,
            "correlation_id",
            str(uuid4()),
        )
        guidance = isinstance(exc, OperatorGuidanceError)
        masked = exc.status_code >= 500 and not guidance
        if exc.status_code >= 500:
            logger.error(
                "HTTP %d response [%s]: %s",
                exc.status_code,
                correlation_id,
                exc.detail,
            )
        if guidance:
            code = "service_unavailable"
        elif masked:
            code = "internal_error"
        else:
            code = "request_error"
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": code,
                "detail": "Internal server error" if masked else exc.detail,
                "correlation_id": correlation_id,
            },
            headers={**(exc.headers or {}), "X-Correlation-ID": correlation_id},
        )

    @app.exception_handler(Exception)
    async def safe_unhandled_exception(request: Request, exc: Exception):
        correlation_id = getattr(
            request.state,
            "correlation_id",
            str(uuid4()),
        )
        logger.exception("Unhandled API error [%s]", correlation_id)
        # Unhandled exceptions are answered by Starlette's outermost error
        # middleware, which bypasses ``request_security``, so set headers here.
        response = JSONResponse(
            status_code=500,
            content={
                "code": "internal_error",
                "detail": "Internal server error",
                "correlation_id": correlation_id,
            },
            headers={"X-Correlation-ID": correlation_id},
        )
        _apply_security_headers(response, request.url.path, settings)
        return response
