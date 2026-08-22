"""Explicit composition root for the orchestrator HTTP API."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.version import __version__

from . import runtime
from .job_routes import router as job_router
from .pipeline_routes import router as pipeline_router
from .schedule_routes import router as schedule_router
from .system_routes import router as system_router


def cleanup_completed_jobs(max_age_hours: int | None = None) -> None:
    """Compatibility facade for application startup cleanup."""
    runtime.cleanup_completed_jobs(max_age_hours)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Apply cleanup and run automatic pipeline schedules while the API lives."""
    cleanup_completed_jobs()
    runtime.scheduler.start()
    try:
        yield
    finally:
        runtime.scheduler.stop()


app = FastAPI(
    title="Shade Research API",
    description="HTTP API for Shade Research data and research pipelines",
    version=__version__,
    lifespan=lifespan,
)

for api_router in (system_router, pipeline_router, schedule_router, job_router):
    app.include_router(api_router)


def main() -> None:
    """Run the standalone orchestrator API with validated bind settings."""
    import uvicorn

    uvicorn.run(
        "src.api.router:app",
        host=runtime.SETTINGS.host,
        port=runtime.SETTINGS.port,
        reload=False,
    )


if __name__ == "__main__":
    main()
