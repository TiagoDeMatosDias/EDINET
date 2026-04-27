"""Web application API routes.

Exposes the backend orchestrator routes so the web server can mount them
without importing directly from the unrelated ``src.api`` package.
Screens that need new API endpoints should add route modules to this package.
"""

from src.api.router import app as _api_app, cleanup_completed_jobs  # noqa: F401

# The FastAPI sub-application that carries all /api/* and /health routes.
router_app = _api_app

__all__ = ["router_app", "cleanup_completed_jobs"]
