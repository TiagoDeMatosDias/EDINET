"""FastAPI server for the web-based EDINET workstation.

The frontend is a multi-page application. Each top-level route serves its own
HTML file from ``frontend/pages/``. Static assets are served from
``/assets`` (frontend directory) and ``/brand-assets`` (root assets/).

API routes are built by ``src.web_app.api`` and mounted directly here —
no intermediate ``extend()`` hack.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.web_app.api import cleanup_completed_jobs, router_app

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "frontend"
PAGES_DIR = FRONTEND_DIR / "pages"
BRAND_ASSETS_DIR = BASE_DIR.parent.parent / "assets"

# The API router_app from src.web_app.api already includes all API routes
# (orchestrator, screening, security_analysis, portfolio, and auto-discovered
# view routers). We extend it with page routes and static mounts to create
# the complete application.
app = router_app
app.title = "EDINET Web Workstation"
app.description = "Bloomberg-style web frontend for EDINET research workflows."
app.version = "1.0.0"

# Serve frontend static assets.
app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")

if BRAND_ASSETS_DIR.exists():
    app.mount("/brand-assets", StaticFiles(directory=BRAND_ASSETS_DIR), name="brand-assets")


@app.on_event("startup")
def _startup() -> None:
    cleanup_completed_jobs()
    # Log registered API routes for diagnostics
    api_routes = [r.path for r in app.router.routes if hasattr(r, 'path')]
    portfolio_count = sum(1 for r in api_routes if 'portfolio' in r)
    if portfolio_count > 0:
        from src.web_app.api import logger as _api_logger
        _api_logger.info(f"Portfolio module loaded: {portfolio_count} routes registered")


@app.get("/")
def page_main() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "main" / "main.html")


@app.get("/orchestrator")
def page_orchestrator() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "orchestrator" / "orchestrator.html")


@app.get("/screening")
def page_screening() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "screening" / "screening.html")


@app.get("/backtesting")
def page_backtesting() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "backtesting" / "backtesting.html")


@app.get("/security")
def page_security() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "security_analysis" / "security.html")


@app.get("/portfolio")
def page_portfolio() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "portfolio" / "portfolio.html")


@app.get("/favicon.ico")
def page_favicon() -> FileResponse:
    return FileResponse(BRAND_ASSETS_DIR / "icon.ico")


@app.get("/{path:path}")
def not_found(path: str) -> FileResponse:
    if path.startswith("api/") or path == "health":
        raise HTTPException(status_code=404, detail="Not found")
    raise HTTPException(status_code=404, detail="Page not found")


def main() -> None:
    import uvicorn

    uvicorn.run("src.web_app.server:app", host="127.0.0.1", port=8000, reload=True)


if __name__ == "__main__":
    main()
