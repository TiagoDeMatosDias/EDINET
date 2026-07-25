"""Explicit composition root for all HTTP API routers."""

from __future__ import annotations

import logging

from src.api.router import app as _api_app  # noqa: F401
from src.api.router import cleanup_completed_jobs
from src.auth.api import admin_router as _admin_router
from src.auth.api import router as _auth_router
from src.backtesting.api import router as _backtesting_router
from src.comparison.api import router as _comparison_router
from src.filings.api import router as _filings_router
from src.portfolio.api import router as _portfolio_router
from src.research.api import router as _research_router
from src.reports.api import router as _reports_router
from src.web_app.api.screening import router as _screening_router
from src.web_app.api.security_analysis import router as _security_router
from src.web_app.api.tags import router as _tags_router

logger = logging.getLogger(__name__)

router_app = _api_app

_ROUTERS = (
    _auth_router,
    _admin_router,
    _filings_router,
    _research_router,
    _reports_router,
    _screening_router,
    _security_router,
    _tags_router,
    _backtesting_router,
    _comparison_router,
    _portfolio_router,
)

for _router in _ROUTERS:
    router_app.include_router(_router)
    logger.info("Registered API router: prefix=%s", _router.prefix)

__all__ = ["router_app", "cleanup_completed_jobs"]
