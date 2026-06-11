"""Web application API routes.

Builds a single FastAPI app that includes all API routers:
- Orchestrator endpoints (from ``src.api.router``)
- Screening, security_analysis, portfolio (manual registration)
- Auto-discovered view-package routers (pkgutil)
"""

import importlib
import logging
import pkgutil

from fastapi import APIRouter
from fastapi import FastAPI

from src.api.router import app as _api_app, cleanup_completed_jobs  # noqa: F401

logger = logging.getLogger(__name__)

router_app = _api_app

# ---------------------------------------------------------------------------
# Manually-registered routers — use include_router() instead of extend()
# ---------------------------------------------------------------------------
from src.web_app.api.screening import router as _screening_router
from src.web_app.api.security_analysis import router as _security_router

router_app.include_router(_screening_router)
router_app.include_router(_security_router)

try:
    from src.portfolio.api import router as _portfolio_router
    router_app.include_router(_portfolio_router)
    logger.info("Portfolio API router registered: prefix=%s", _portfolio_router.prefix)
except Exception as e:
    logger.error("Failed to register portfolio router: %s", e)

# ---------------------------------------------------------------------------
# Dynamic discovery of view-package API routers
# ---------------------------------------------------------------------------
_DISCOVERY_EXCLUDED = frozenset({
    "api", "orchestrator", "screening", "security_analysis",
    "utilities", "web_app", "__pycache__",
})


def _discover_view_routers(package_name: str = "src") -> list[APIRouter]:
    """Scan src/ subpackages for api.py modules exporting a 'router'."""
    package = importlib.import_module(package_name)
    routers = []
    for module_info in sorted(
        pkgutil.iter_modules(package.__path__), key=lambda i: i.name
    ):
        name = module_info.name
        if name.startswith("_") or name in _DISCOVERY_EXCLUDED:
            continue
        api_module_name = f"{package_name}.{name}.api"
        try:
            api_module = importlib.import_module(api_module_name)
            if hasattr(api_module, "router"):
                routers.append(api_module.router)
                logger.info("Discovered view API router: %s", api_module_name)
        except ModuleNotFoundError:
            pass
        except Exception as exc:
            logger.warning("Failed to load %s: %s", api_module_name, exc)
    return routers


for _r in _discover_view_routers():
    router_app.include_router(_r)

__all__ = ["router_app", "cleanup_completed_jobs"]
