"""The role each HTTP route needs: enforced on every request and stated in the API docs.

``src.web_app.security`` refuses a signed-in caller whose role is below the
route's level before the route runs, so a route that forgets its own check is
still protected. The routes keep their own checks too (``require_admin``,
``require_operator``, in-handler checks, ``AuthService``). The API docs and the
OpenAPI schema show each caller only the routes these rules let them call.
"""

from __future__ import annotations

from typing import Any, Literal

from starlette.routing import Match

Access = Literal["public", "member", "operator", "admin"]

#: Shown next to each endpoint.
ACCESS_LABELS: dict[str, str] = {
    "public": "Public (no token)",
    "member": "Any signed-in user",
    "operator": "Operators and administrators",
    "admin": "Administrators",
}

#: The answer a caller gets when their role is below the route's.
REFUSALS: dict[str, str] = {
    "operator": "Operator permission required",
    "admin": "Administrator permission required",
}

#: The only ``/api`` paths answered without a token: signing in and creating an account.
PUBLIC_AUTH_PATHS = frozenset(
    {
        "/api/auth/status",
        "/api/auth/register",
        "/api/auth/login",
        "/api/auth/refresh",
        "/api/auth/accept-invitation",
        "/api/auth/reset-password",
    }
)

#: The worker protocol authenticates workers with their own enrollment tokens.
WORKER_PREFIX = "/api/worker/"

# Endpoints that change shared market data or run the data pipeline.
_OPERATOR = frozenset(
    {
        ("GET", "/api/config"),
        ("GET", "/api/steps"),
        ("GET", "/api/steps/{step_name}"),
        ("GET", "/api/system/status"),
        ("POST", "/api/pipeline/run"),
        ("GET", "/api/jobs"),
        ("GET", "/api/jobs/{job_id}"),
        ("POST", "/api/jobs/{job_id}/cancel"),
        ("GET", "/api/jobs/{job_id}/output"),
        ("POST", "/api/filings/acquire"),
        ("POST", "/api/filings/xbrl-backfill"),
        ("PUT", "/api/splits/{split_id}"),
        ("POST", "/api/splits/detect"),
        ("POST", "/api/security/update-price"),
        ("POST", "/api/screening/update-prices"),
        ("POST", "/api/portfolio/refresh-market-data"),
    }
)
_ADMIN = frozenset({("POST", "/api/tags/migrate-legacy")})
_ADMIN_PREFIX = "/api/admin/"

_LEVEL_RANK = {"public": 0, "member": 1, "operator": 2, "admin": 3}
_ROLE_RANK = {"member": 1, "operator": 2, "admin": 3}


def access_level(method: str, path: str) -> Access:
    """The role a route needs; ``member`` is any signed-in account.

    ``path`` is the route's template (``/api/jobs/{job_id}``). Anything under
    ``/api/admin/`` needs an administrator, whether or not such a route exists.
    """
    method = "GET" if method.upper() == "HEAD" else method.upper()
    key = (method, path)
    if path == "/health" or path in PUBLIC_AUTH_PATHS:
        return "public"
    if key in _ADMIN or path.startswith(_ADMIN_PREFIX):
        return "admin"
    if key in _OPERATOR:
        return "operator"
    return "member"


def read_only_token_allowed(method: str, path: str) -> bool:
    """Whether a read-only personal token can call the route (reads only)."""
    return access_level(method, path) == "public" or method.upper() in {"GET", "HEAD", "OPTIONS"}


def role_allows(role: str | None, level: str) -> bool:
    """Whether an account with ``role`` may call a route needing ``level``; unknown roles get nothing."""
    if level == "public":
        return True
    return _ROLE_RANK.get(role or "", 0) >= _LEVEL_RANK[level]


def route_path(app: Any, scope: dict[str, Any]) -> str | None:
    """The template of the route that will answer the request, matched as the router matches it."""
    for route in app.router.routes:
        match, _child = route.matches(scope)
        if match is Match.FULL:
            return getattr(route, "path_format", None)
    return None
