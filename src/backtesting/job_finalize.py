"""Server-side finalizers: store a finished backtest job where the result routes find it.

A worker leaves ``result.json`` and ``backtest.zip`` (and for single runs
``per_company_per_day.csv``) as job artifacts. When the job completes, the
finalizer for its kind moves them into a new backtest directory, records the
owner and the saved-results description in ``app.db``, and returns the public
result: ``{"result_id", "title", "subtitle", "link"}``.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from src.backtesting import store
from src.jobs.errors import FinalizeError
from src.jobs.service import JobRecord

logger = logging.getLogger(__name__)

_REQUIRED = ("result.json", "backtest.zip")
_OPTIONAL = ("per_company_per_day.csv",)


def _benchmark_label(request: dict[str, Any]) -> str | None:
    if request.get("benchmark_mode") == "portfolio":
        return "vs own portfolio"
    ticker = request.get("benchmark_ticker")
    return f"vs {ticker}" if ticker else None


def describe_single(request: dict[str, Any]) -> tuple[str, str]:
    """Title and subtitle of a single backtest from its request."""
    holdings = store.holdings_label(list(request.get("portfolio") or {}))
    title = f"Backtest · {holdings}" if holdings else "Backtest"
    subtitle = store.backtest_subtitle(f"{request.get('start_date')} to {request.get('end_date')}", _benchmark_label(request))
    return title, subtitle


def describe_csv(request: dict[str, Any]) -> tuple[str, str]:
    durations = request.get("durations")
    subtitle = store.backtest_subtitle(", ".join(durations) if durations else "Configured periods", _benchmark_label(request))
    return "CSV portfolio backtest", subtitle


def describe_rolling(request: dict[str, Any], cadence: str | None = None) -> tuple[str, str]:
    title = f"Rolling screen · {cadence or request.get('cadence')}"
    subtitle = store.backtest_subtitle(
        ", ".join(request.get("durations") or []),
        f"top {request.get('max_companies')}",
        _benchmark_label(request),
    )
    return title, subtitle


def _place(source: Path, target: Path) -> None:
    """Hard-link when the artifact is on the same volume (large archives), else copy."""
    try:
        os.link(source, target)
    except OSError:
        shutil.copyfile(source, target)


def _store_files(job: JobRecord, artifacts: dict[str, Path]) -> Path:
    missing = [name for name in _REQUIRED if name not in artifacts or not artifacts[name].is_file()]
    if missing:
        raise FinalizeError("The backtest finished but its result files were not delivered.")
    backtest_id = str(job.payload.get("backtest_id") or "") or store.new_backtest_id()
    try:
        out_dir = store.backtest_directory(backtest_id, require_existing=False)
    except HTTPException:
        out_dir = store.backtest_directory(store.new_backtest_id(), require_existing=False)
    if out_dir.exists():  # a retried finalize
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    try:
        for name in (*_REQUIRED, *_OPTIONAL):
            if name in artifacts and artifacts[name].is_file():
                _place(artifacts[name], out_dir / name)
    except OSError as exc:
        shutil.rmtree(out_dir, ignore_errors=True)
        logger.warning("Could not store backtest files for job %s: %s", job.job_id, exc)
        raise FinalizeError("The backtest finished but its files could not be saved.") from exc
    return out_dir


def _publish(job: JobRecord, out_dir: Path, *, kind: str, title: str, subtitle: str, headline: dict[str, Any]) -> dict[str, Any]:
    try:
        store.write_owner(out_dir, job.owner_user_id)
        store.write_meta(out_dir, kind=kind, title=title, subtitle=subtitle, headline=headline)
    except Exception as exc:
        shutil.rmtree(out_dir, ignore_errors=True)
        logger.warning("Could not register backtest %s: %s", out_dir.name, exc)
        raise FinalizeError("The backtest finished but could not be registered.") from exc
    store.record_recent_backtest(job.owner_user_id, out_dir.name, title=title, subtitle=subtitle)
    return {"result_id": out_dir.name, "title": title, "subtitle": subtitle, "link": f"/backtest?result={out_dir.name}"}


def finalize_single(job: JobRecord, result: dict, artifacts: dict[str, Path]) -> dict:
    out_dir = _store_files(job, artifacts)
    title, subtitle = describe_single(job.payload.get("request") or {})
    metrics = result.get("metrics") or {}
    headline = {
        "total_return": metrics.get("total_return"),
        "annualized_return": metrics.get("annualized_return"),
        "excess_return": metrics.get("excess_return"),
    }
    return _publish(job, out_dir, kind="single", title=title, subtitle=subtitle, headline=headline)


def finalize_csv(job: JobRecord, result: dict, artifacts: dict[str, Path]) -> dict:
    out_dir = _store_files(job, artifacts)
    title, subtitle = describe_csv(job.payload.get("request") or {})
    aggregate = result.get("aggregate") or {}
    stats = aggregate.get("stats") or {}
    headline = {
        "mean_annualized_return": (stats.get("annualized_return") or {}).get("mean"),
        "runs": aggregate.get("total_runs"),
    }
    return _publish(job, out_dir, kind="csv", title=title, subtitle=subtitle, headline=headline)


def finalize_rolling(job: JobRecord, result: dict, artifacts: dict[str, Path]) -> dict:
    out_dir = _store_files(job, artifacts)
    title, subtitle = describe_rolling(job.payload.get("request") or {}, (result.get("config") or {}).get("cadence"))
    aggregate = result.get("aggregate") or {}
    stats = aggregate.get("stats") or {}
    comparison = aggregate.get("benchmark_comparison") or {}
    headline = {
        "mean_annualized_return": (stats.get("total_return") or {}).get("mean"),
        "win_rate": comparison.get("win_rate"),
        "runs": aggregate.get("complete_backtests"),
    }
    return _publish(job, out_dir, kind="rolling", title=title, subtitle=subtitle, headline=headline)
