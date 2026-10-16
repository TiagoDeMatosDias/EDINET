"""Explicit composition root for all HTTP API routers."""

from __future__ import annotations

import logging

from src.api.router import app as _api_app  # noqa: F401
from src.api_docs.api import install_openapi_security
from src.api_docs.api import router as _api_docs_router
from src.auth.api import admin_router as _admin_router
from src.auth.api import router as _auth_router
from src.backtesting.api import router as _backtesting_router
from src.bonds.api import router as _bonds_router
from src.chat.api import router as _chat_router
from src.chat.profiles_api import router as _profiles_router
from src.comparison.api import router as _comparison_router
from src.filings.api import router as _filings_router
from src.filings.storage_api import install_filing_storage_error_handlers
from src.filings.storage_api import router as _filing_storage_router
from src.jobs.admin_api import router as _workers_admin_router
from src.jobs.user_api import install_job_error_handlers
from src.jobs.user_api import router as _my_jobs_router
from src.jobs.worker_api import router as _worker_router
from src.ownership.api import router as _ownership_router
from src.portfolio.api import router as _portfolio_router
from src.reports.api import router as _reports_router
from src.research.api import router as _research_router
from src.settings.api import router as _admin_settings_router
from src.web_app.api.overview import router as _overview_router
from src.web_app.api.screening import router as _screening_router
from src.web_app.api.security_analysis import router as _security_router
from src.web_app.api.server_control import router as _server_control_router
from src.web_app.api.settings import router as _settings_router
from src.web_app.api.splits import router as _splits_router
from src.web_app.api.tags import router as _tags_router

logger = logging.getLogger(__name__)

router_app = _api_app
install_job_error_handlers(router_app)
install_filing_storage_error_handlers(router_app)

_ROUTERS = (
    _auth_router,
    _admin_router,
    _admin_settings_router,
    _server_control_router,
    _filings_router,
    _research_router,
    _reports_router,
    _screening_router,
    _security_router,
    _splits_router,
    _tags_router,
    _backtesting_router,
    _bonds_router,
    _ownership_router,
    _comparison_router,
    _portfolio_router,
    _chat_router,
    _profiles_router,
    _overview_router,
    _settings_router,
    _worker_router,
    _my_jobs_router,
    _workers_admin_router,
    _filing_storage_router,
    _api_docs_router,
)

for _router in _ROUTERS:
    router_app.include_router(_router)
    logger.info("Registered API router: prefix=%s", _router.prefix)
install_openapi_security(router_app)

__all__ = ["router_app"]
