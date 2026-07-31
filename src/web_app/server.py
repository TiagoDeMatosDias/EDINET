"""FastAPI server for the Shade Research workstation.

The frontend is the React SPA at ``frontend-v2``.  API routes are built by
``src.web_app.api`` and mounted directly here.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.orchestrator.common.database_bootstrap import ensure_application_databases
from src.version import __version__
from src.web_app.api import router_app
from src.web_app.security import AppSettings, install_security

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
SETTINGS = AppSettings.from_env()
install_security(app, SETTINGS)
ensure_application_databases(settings=SETTINGS)


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
        raise HTTPException(
            status_code=503,
            detail="Frontend build missing. Run npm run build in frontend-v2.",
        )
    return FileResponse(index)


# ── React SPA routes ──


@app.get("/")
def page_main() -> FileResponse:
    return _frontend_v2()


@app.get("/overview")
def page_overview() -> FileResponse:
    return _frontend_v2()


@app.get("/pricing")
def page_pricing() -> FileResponse:
    return _frontend_v2()


@app.get("/screen")
def page_screen() -> FileResponse:
    return _frontend_v2()


@app.get("/analyze")
@app.get("/analyze/{subpath:path}")
@app.get("/security")
def page_analyze(subpath: str = "") -> FileResponse:
    return _frontend_v2()


@app.get("/backtest")
@app.get("/backtesting")
def page_backtest() -> FileResponse:
    return _frontend_v2()


@app.get("/pipeline")
def page_pipeline() -> FileResponse:
    return _frontend_v2()


@app.get("/portfolio")
def page_portfolio() -> FileResponse:
    return _frontend_v2()


@app.get("/filings")
@app.get("/filings/{subpath:path}")
def page_filings(subpath: str = "") -> FileResponse:
    return _frontend_v2()


@app.get("/compare")
def page_compare() -> FileResponse:
    return _frontend_v2()


@app.get("/research")
def page_research() -> FileResponse:
    return _frontend_v2()


@app.get("/account")
def page_account() -> FileResponse:
    return _frontend_v2()


@app.get("/admin")
def page_admin() -> FileResponse:
    return _frontend_v2()


@app.get("/login")
def page_login() -> FileResponse:
    return _frontend_v2()


@app.get("/register")
def page_register() -> FileResponse:
    return _frontend_v2()


# ── Static / fallback ──


@app.get("/favicon.ico")
def page_favicon() -> FileResponse:
    return FileResponse(BRAND_ASSETS_DIR / "shade-icon.ico")


@app.get("/{path:path}")
def spa_fallback(path: str) -> FileResponse:
    """Serve the SPA for unknown paths so client-side routing works on reload."""
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

    uvicorn.run(
        "src.web_app.server:app",
        host=SETTINGS.host,
        port=SETTINGS.port,
        reload=False,
    )


if __name__ == "__main__":
    main()
