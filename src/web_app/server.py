"""FastAPI server for the Shade Research workstation.

The frontend is the React SPA at ``frontend-v2``.  API routes are built by
``src.web_app.api`` and mounted directly here.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from src.jobs.launcher import LocalWorkerLauncher, default_server_url
from src.jobs.protocol import LOCAL_WORKER_ID
from src.jobs.service import get_job_service, start_job_service
from src.orchestrator.common.database_bootstrap import (
    ensure_data_databases,
    ensure_server_databases,
)
from src.orchestrator.common.db_config import get_filings_db, get_market_db
from src.orchestrator.common.sqlite import clear_protected_database_paths, protect_database_paths
from src.paths import bundle_dir
from src.version import __version__
from src.web_app.api import router_app
from src.web_app.security import OperatorGuidanceError, get_settings, install_security
from src.web_app.tunnel import TunnelManager

logger = logging.getLogger(__name__)

BRAND_ASSETS_DIR = bundle_dir() / "assets" / "brand"
FRONTEND_V2_DIST = (
    Path(os.getenv("EDINET_FRONTEND_DIST", bundle_dir() / "frontend-v2" / "dist"))
    .expanduser()
    .resolve(strict=False)
)

# The API router_app from src.web_app.api already includes all API routes
# (orchestrator, screening, security_analysis, portfolio, and auto-discovered
# view routers).
app = router_app
app.title = "Shade Research"
app.description = "Value in context: source-linked company research and analysis."
app.version = __version__
SETTINGS = get_settings()
install_security(app, SETTINGS)



def _tunnel_blocked() -> str | None:
    """Why a Cloudflare tunnel must stay closed, if it must."""
    if not SETTINGS.authentication_required:
        return "Sign-in is disabled (the auth.mode setting), so the tunnel stays closed."
    if app.state.auth_service.bootstrap_required:
        return "No account exists yet. Create the administrator account on this machine first."
    return None


def _local_origin() -> str:
    """The address cloudflared reaches this server at."""
    host = "127.0.0.1" if SETTINGS.host in ("0.0.0.0", "::") else SETTINGS.host
    return f"https://{f'[{host}]' if ':' in host else host}:{SETTINGS.port}"


app.state.tunnel = TunnelManager(_local_origin(), blocked=_tunnel_blocked)

_api_lifespan = app.router.lifespan_context


def _on_stop_signal(callback: Callable[[], None]) -> None:
    """Run ``callback`` as soon as a stop signal arrives, then let uvicorn handle it.

    uvicorn closes the listener and waits for open requests before the lifespan
    shutdown runs, so workers' long-poll claims would hold the shutdown open until
    they are cancelled. ``callback`` runs inside the signal handler and must only
    set flags. Signals can be handled only on the main thread (not under tests).
    """
    if threading.current_thread() is not threading.main_thread():
        return
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        previous = signal.getsignal(sig) if sig is not None else None
        if sig is None or not callable(previous):
            continue

        def handler(signum, frame, previous=previous):
            callback()
            previous(signum, frame)

        signal.signal(sig, handler)


def _finish_filing_storage_split() -> None:
    from src.filings.split import switch_over

    try:
        outcome = switch_over(get_filings_db())
    except Exception:  # noqa: BLE001 - never block startup; Admin keeps offering "restart to finish"
        logger.exception("The filing storage switch-over failed")
        return
    if outcome["status"] == "failed":
        logger.warning("Filing storage split not finished, restart to try again: %s", outcome.get("error"))
    elif outcome["status"] == "switched":
        logger.info("Filing storage now uses the split layout; the old file is %s", outcome["retired"])


def _worker_written_paths() -> tuple[list[str], list[str]]:
    """Files and folders only workers write: market.db, the filing index and every yearly filing file."""
    from src.filings.storage import FilingStorage, next_index_path, retired_index_path

    filings_db = get_filings_db()
    storage = FilingStorage(filings_db)
    files = [get_market_db(), filings_db, str(next_index_path(filings_db)), str(retired_index_path(filings_db))]
    # Years moved to another disk are recorded with absolute paths.
    files.extend(str(row["file"]) for row in storage.shards())
    return files, [str(storage.shards_dir)]


@asynccontextmanager
async def _lifespan(lifespan_app: FastAPI):
    """Create missing databases at startup rather than at import time.

    Importing this module (tests, tools, route inspection) therefore never
    touches the configured databases.
    """
    # Set up logging in the *serving* process.  With ``reload=True`` (the
    # default launcher mode) uvicorn spawns a child process that imports this
    # module and serves requests; that child never runs ``main.py``'s
    # ``setup_logging()``, so without this call the file handler would be
    # missing and ``logs/server.log`` would stay silent during normal traffic.
    # ``setup_logging`` is idempotent and also re-points uvicorn's loggers at
    # the root logger so the per-request access log reaches the file handler.
    from src.utilities.logger import setup_logging

    setup_logging()
    ensure_server_databases(settings=SETTINGS)
    # A finished filing storage split is put in place now, before anything
    # (this server, a worker) opens filings.db: Windows cannot rename an open file.
    await run_in_threadpool(_finish_filing_storage_split)
    # Workers write market.db and filings.db. Their fixed schema is still created
    # (and migrated) here once, before any worker starts and before request
    # handling, so a clean install or an upgrade never serves pages against a
    # missing file or an older schema.
    await run_in_threadpool(ensure_data_databases)
    # From here on only workers write market.db, filings.db and the yearly
    # filing files: any write the server attempts fails loudly instead of
    # contending with a worker.
    protect_database_paths(*_worker_written_paths())
    maintenance = start_job_service()
    service = get_job_service()
    service.stopping = False

    def _stop_signal() -> None:
        service.begin_shutdown()
        launcher = getattr(lifespan_app.state, "launcher", None)
        if launcher is not None:
            launcher.stopping_soon()

    _on_stop_signal(_stop_signal)
    try:
        async with _api_lifespan(lifespan_app):
            launcher = LocalWorkerLauncher(
                service.registry,
                server_url=default_server_url(SETTINGS),
            )
            lifespan_app.state.launcher = launcher
            await run_in_threadpool(launcher.start)
            lifespan_app.state.tunnel.apply()
            try:
                yield
            finally:
                service.begin_shutdown()
                lifespan_app.state.tunnel.stop()
                await run_in_threadpool(launcher.stop)
                # The listener is already closed, so the local worker could not
                # release its jobs itself; settle them now that its process is gone.
                await run_in_threadpool(service.release_worker_jobs, LOCAL_WORKER_ID)
    finally:
        maintenance.stop()
        clear_protected_database_paths()


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
        formatted = ", ".join(f"{method} {path}" for method, path in sorted(duplicates))
        raise RuntimeError(f"Duplicate FastAPI routes registered: {formatted}")


_assert_unique_method_paths()


def main() -> None:
    import uvicorn

    from src.utilities.logger import setup_logging
    from src.web_app.shutdown import GRACEFUL_SHUTDOWN_SECONDS
    from src.web_app.tls import provision_tls

    # Ensure the server logs to <project_root>/logs/ even when launched directly
    # via ``python -m src.web_app.server`` (the primary launcher, main.py, also
    # calls this; setup_logging is idempotent).
    setup_logging()
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
        timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_SECONDS,
    )


if __name__ == "__main__":
    main()
