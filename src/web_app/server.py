"""FastAPI server for the web-based EDINET workstation.

The frontend is a multi-page application. Each top-level route serves its own
HTML file from ``frontend/pages/``. Static assets are served from
``/assets`` (frontend directory) and ``/brand-assets`` (root assets/).
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.web_app.api import cleanup_completed_jobs, router_app

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "frontend"
PAGES_DIR = FRONTEND_DIR / "pages"
BRAND_ASSETS_DIR = BASE_DIR.parent.parent / "assets"

app = FastAPI(
    title="EDINET Web Workstation",
    description="Bloomberg-style web frontend for EDINET research workflows.",
    version="1.0.0",
)

# Serve frontend static assets.
app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")

if BRAND_ASSETS_DIR.exists():
    app.mount("/brand-assets", StaticFiles(directory=BRAND_ASSETS_DIR), name="brand-assets")

# Register the API routes from the dedicated api package.
app.router.routes.extend(router_app.router.routes)


@app.on_event("startup")
def _startup() -> None:
    cleanup_completed_jobs()


@app.get("/")
def page_main() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "main" / "main.html")


@app.get("/orchestrator")
def page_orchestrator() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "orchestrator" / "orchestrator.html")


@app.get("/screening")
def page_screening() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "screening" / "screening.html")


@app.get("/security")
def page_security() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "security_analysis" / "security.html")


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
