"""FastAPI server for the web-based EDINET workstation.

The frontend is the React SPA at ``frontend-v2``.  API routes are built by
``src.web_app.api`` and mounted directly here.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.web_app.api import router_app
from src.web_app.security import AppSettings, install_security
from src.version import __version__

BASE_DIR = Path(__file__).resolve().parent
BRAND_ASSETS_DIR = BASE_DIR.parent.parent / "assets"
FRONTEND_V2_DIST = BASE_DIR.parent.parent / "frontend-v2" / "dist"

# The API router_app from src.web_app.api already includes all API routes
# (orchestrator, screening, security_analysis, portfolio, and auto-discovered
# view routers).
app = router_app
app.title = "EDINET Web Workstation"
app.description = "Bloomberg-style web frontend for EDINET research workflows."
app.version = __version__
SETTINGS = AppSettings.from_env()
install_security(app, SETTINGS)


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


# ── Static / fallback ──


@app.get("/favicon.ico")
def page_favicon() -> FileResponse:
    return FileResponse(BRAND_ASSETS_DIR / "icon.ico")


@app.get("/{path:path}")
def not_found(path: str) -> FileResponse:
    if path.startswith("api/") or path == "health":
        raise HTTPException(status_code=404, detail="Not found")
    raise HTTPException(status_code=404, detail="Page not found")


def _assert_unique_method_paths() -> None:
    """Fail at import time when two handlers own the same method and path."""
    seen: set[tuple[str, str]] = set()
    duplicates: set[tuple[str, str]] = set()
    for route in app.router.routes:
        path = getattr(route, "path", None)
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
