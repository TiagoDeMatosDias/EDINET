"""FastAPI server for the Shade Research workstation.

The frontend is the React SPA at ``frontend-v2``.  API routes are built by
``src.web_app.api`` and mounted directly here.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.orchestrator.common.database_bootstrap import ensure_application_databases
from src.version import __version__
from src.web_app.api import router_app
from src.web_app.security import OperatorGuidanceError, get_settings, install_security

BASE_DIR = Path(__file__).resolve().parent
BRAND_ASSETS_DIR = BASE_DIR.parent.parent / "assets" / "brand"
FRONTEND_V2_DIST = Path(
    os.getenv("EDINET_FRONTEND_DIST", BASE_DIR.parent.parent / "frontend-v2" / "dist")
).expanduser().resolve(strict=False)

# The API router_app from src.web_app.api already includes all API routes
# (orchestrator, screening, security_analysis, portfolio, and auto-discovered
# view routers).
app = router_app
app.title = "Shade Research"
app.description = "Value in context: source-linked company research and analysis."
app.version = __version__
SETTINGS = get_settings()
install_security(app, SETTINGS)

_api_lifespan = app.router.lifespan_context


@asynccontextmanager
async def _lifespan(lifespan_app: FastAPI):
    """Create missing databases at startup rather than at import time.

    Importing this module (tests, tools, route inspection) therefore never
    touches the configured databases.
    """
    ensure_application_databases(settings=SETTINGS)
    async with _api_lifespan(lifespan_app):
        yield


app.router.lifespan_context = _lifespan


if FRONTEND_V2_DIST.exists():
    app.mount(
        "/app-assets",
        StaticFiles(directory=FRONTEND_V2_DIST / "app-assets"),
        name="app-assets",
    )

if BRAND_ASSETS_DIR.exists():
    app.mount("/brand-assets", StaticFiles(directory=BRAND_ASSETS_DIR), name="brand-assets")


def _frontend_v2() -> FileResponse:
    index = FRONTEND_V2_DIST / "index.html"
    if not index.exists():
        raise OperatorGuidanceError(
            "Frontend build missing. Run npm run build in frontend-v2.",
        )
    # The shell names content-hashed assets, so a cached copy goes stale the
    # moment the bundle is rebuilt and can hide newer pages in a browser that
    # never revalidates. Always revalidate it (cheap 304 while unchanged).
    return FileResponse(index, headers={"Cache-Control": "no-cache"})


# ── Static / fallback ──


@app.get("/favicon.ico")
def page_favicon() -> FileResponse:
    return FileResponse(BRAND_ASSETS_DIR / "shade-icon.ico")


@app.get("/{path:path}")
def spa_fallback(path: str) -> FileResponse:
    """Serve the SPA for every page route; React Router resolves (and 404s) it."""
    if path.startswith("api/") or path == "health":
        raise HTTPException(status_code=404, detail="Not found")
    # Treat unknown paths as SPA routes (React Router handles 404s client-side)
    return _frontend_v2()


def _assert_unique_method_paths() -> None:
    """Fail at import time when two handlers own the same method and path."""
    seen: set[tuple[str, str]] = set()
    duplicates: set[tuple[str, str]] = set()
    for route in app.router.routes:
        path = getattr(route, "path", None)
        if not isinstance(path, str):
            continue
        for method in getattr(route, "methods", None) or ():
            key = (method, path)
            if key in seen:
                duplicates.add(key)
            seen.add(key)
    if duplicates:
        formatted = ", ".join(
            f"{method} {path}" for method, path in sorted(duplicates)
        )
        raise RuntimeError(f"Duplicate FastAPI routes registered: {formatted}")


_assert_unique_method_paths()


def main() -> None:
    import uvicorn

    from src.web_app.tls import provision_tls

    cert_path, key_path = provision_tls(host=SETTINGS.host)
    # Pass the app object: this module may already be running as ``__main__``,
    # and importing it again by string would re-register every route.
    uvicorn.run(
        app,
        host=SETTINGS.host,
        port=SETTINGS.port,
        reload=False,
        ssl_certfile=cert_path,
        ssl_keyfile=key_path,
    )


if __name__ == "__main__":
    main()
