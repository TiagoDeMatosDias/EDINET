"""Where saved backtests live and who may see them.

Shared by the HTTP routes (``api``) and the job finalizers (``job_finalize``),
which both run in the server process. Workers never import this module: it
opens ``app.db`` when imported.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from src.auth.models import AuthenticatedUser
from src.backtesting.catalog import BacktestCatalog
from src.orchestrator.common.db_config import get_app_db
from src.paths import backtests_dir

logger = logging.getLogger(__name__)

BACKTEST_ROOT = backtests_dir().resolve(strict=False)
catalog = BacktestCatalog(get_app_db())
BACKTEST_ID = re.compile(r"^\d{8}_\d{6}(?:_[0-9a-f]{8})?$")


def new_backtest_id() -> str:
    return f"{datetime.now():%Y%m%d_%H%M%S}_{uuid4().hex[:8]}"


def backtest_directory(backtest_id: str, *, require_existing: bool) -> Path:
    if not BACKTEST_ID.fullmatch(backtest_id):
        raise HTTPException(status_code=404, detail="Backtest not found.")
    raw_candidate = BACKTEST_ROOT / backtest_id
    if raw_candidate.is_symlink():
        raise HTTPException(status_code=404, detail="Backtest not found.")
    candidate = raw_candidate.resolve(strict=False)
    if candidate.parent != BACKTEST_ROOT:
        raise HTTPException(status_code=404, detail="Backtest not found.")
    if require_existing and not candidate.is_dir():
        raise HTTPException(status_code=404, detail="Backtest not found.")
    return candidate


def write_owner(out_dir: Path, owner_user_id: str | None) -> None:
    """Record which account produced a saved backtest directory."""
    catalog.record_owner(out_dir.name, owner_user_id)


def write_meta(out_dir: Path, *, kind: str, title: str, subtitle: str = "", headline: dict[str, Any] | None = None) -> None:
    """A small description of a saved result, for the saved-results list."""
    catalog.describe(out_dir.name, kind=kind, title=title, subtitle=subtitle, headline=headline)


def read_meta(directory: Path) -> dict[str, Any]:
    return catalog.get(directory.name) or {}


def owner_may_see(owner: str | None, user: AuthenticatedUser | None) -> bool:
    """Owners see their own results; unowned (legacy) results are admin-only."""
    if user is None:
        return False
    if not owner:
        return user.role == "admin"
    return owner == user.user_id


def holdings_label(tickers: list[str], shown: int = 3) -> str:
    """Name a portfolio by its first few holdings, e.g. ``7203, 6758 +2 more``."""
    names = [ticker.strip() for ticker in tickers if ticker.strip()]
    label = ", ".join(names[:shown])
    return f"{label} +{len(names) - shown} more" if len(names) > shown else label


def backtest_subtitle(*parts: str | None) -> str:
    return " · ".join(part for part in parts if part)


def record_recent_backtest(
    owner_user_id: str | None,
    backtest_id: str,
    *,
    title: str = "Backtest",
    subtitle: str | None = None,
) -> None:
    if not owner_user_id:
        return
    try:
        from src.research.runtime import store as research_store

        research_store.record_recent_work(
            owner_user_id,
            "backtest",
            f"backtest:{backtest_id}",
            title,
            subtitle,
            f"/backtest?result={backtest_id}",
            {"backtest_id": backtest_id},
        )
    except Exception as exc:  # noqa: BLE001 - activity history is best effort
        logger.info("Could not record recent backtest %s: %s", backtest_id, exc)
