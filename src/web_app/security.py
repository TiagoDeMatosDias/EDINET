"""Central security, error, and filesystem policy for the web application."""

from __future__ import annotations

import ipaddress
import logging
import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware

logger = logging.getLogger(__name__)

_DATABASE_SUFFIXES = frozenset({".db", ".sqlite", ".sqlite3"})
_DEFAULT_MAX_UPLOAD_BYTES = 10 * 1024 * 1024
_DEFAULT_MAX_EXPORT_BYTES = 25 * 1024 * 1024
_DEFAULT_MAX_BACKTEST_ARTIFACT_BYTES = 256 * 1024 * 1024
_REQUEST_ENVELOPE_OVERHEAD_BYTES = 1024 * 1024
_DEFAULT_JOB_WORKSPACE_ROOT = (
    Path(__file__).resolve().parents[2] / "config" / "state" / "jobs"
)


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
    api_token: str | None = None
    allowed_data_roots: tuple[Path, ...] = ()
    max_upload_bytes: int = _DEFAULT_MAX_UPLOAD_BYTES
    max_export_bytes: int = _DEFAULT_MAX_EXPORT_BYTES
    max_backtest_artifact_bytes: int = _DEFAULT_MAX_BACKTEST_ARTIFACT_BYTES
    sqlite_busy_timeout_ms: int = 30_000
    job_retention_hours: int = 24
    job_workspace_root: Path = _DEFAULT_JOB_WORKSPACE_ROOT
    trusted_hosts: tuple[str, ...] = ()

    @property
    def remote(self) -> bool:
        return not is_loopback_host(self.host)

    @property
    def authentication_required(self) -> bool:
        return self.remote

    def validate(self) -> "AppSettings":
        if not 1 <= self.port <= 65_535:
            raise SecurityConfigurationError("EDINET_PORT must be between 1 and 65535")
        if (
            self.max_upload_bytes < 1
            or self.max_export_bytes < 1
            or self.max_backtest_artifact_bytes < 1
        ):
            raise SecurityConfigurationError(
                "Upload, export, and backtest artifact limits must be positive"
            )
        if self.sqlite_busy_timeout_ms < 1:
            raise SecurityConfigurationError(
                "EDINET_SQLITE_BUSY_TIMEOUT_MS must be positive"
            )
        if self.job_retention_hours < 1:
            raise SecurityConfigurationError(
                "EDINET_JOB_RETENTION_HOURS must be positive"
            )
        if self.remote and not self.allow_remote:
            raise SecurityConfigurationError(
                "Non-loopback binding requires EDINET_ALLOW_REMOTE=true"
            )
        if self.remote and (self.api_token is None or len(self.api_token) < 32):
            raise SecurityConfigurationError(
                "Non-loopback binding requires EDINET_API_TOKEN with at least 32 characters"
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
            api_token=os.getenv("EDINET_API_TOKEN") or None,
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
    if settings.remote:
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=list(settings.trusted_hosts),
        )

    @app.middleware("http")
    async def request_security(request: Request, call_next):
        correlation_id = str(uuid4())
        request.state.correlation_id = correlation_id
        max_request_bytes = (
            max(settings.max_upload_bytes, settings.max_export_bytes)
            + _REQUEST_ENVELOPE_OVERHEAD_BYTES
        )
        content_length = request.headers.get("Content-Length")
        if content_length:
            try:
                too_large = int(content_length) > max_request_bytes
            except ValueError:
                too_large = True
            if too_large:
                return JSONResponse(
                    status_code=413,
                    content={
                        "code": "request_too_large",
                        "detail": "Request body exceeds the configured size limit",
                        "correlation_id": correlation_id,
                    },
                    headers={"X-Correlation-ID": correlation_id},
                )

        if (
            settings.authentication_required
            and request.url.path.startswith("/api/")
        ):
            authorization = request.headers.get("Authorization", "")
            scheme, _, supplied = authorization.partition(" ")
            expected = settings.api_token or ""
            authenticated = (
                scheme.casefold() == "bearer"
                and bool(supplied)
                and secrets.compare_digest(supplied, expected)
            )
            if not authenticated:
                return JSONResponse(
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

        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
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
        detail = (
            "Internal server error"
            if exc.status_code >= 500
            else exc.detail
        )
        if exc.status_code >= 500:
            logger.error(
                "HTTP %d response [%s]",
                exc.status_code,
                correlation_id,
            )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": "internal_error" if exc.status_code >= 500 else "request_error",
                "detail": detail,
                "correlation_id": correlation_id,
            },
            headers=exc.headers,
        )

    @app.exception_handler(Exception)
    async def safe_unhandled_exception(request: Request, exc: Exception):
        correlation_id = getattr(
            request.state,
            "correlation_id",
            str(uuid4()),
        )
        logger.exception("Unhandled API error [%s]", correlation_id)
        return JSONResponse(
            status_code=500,
            content={
                "code": "internal_error",
                "detail": "Internal server error",
                "correlation_id": correlation_id,
            },
        )
