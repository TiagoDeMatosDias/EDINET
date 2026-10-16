"""Backtest handlers: single portfolio, CSV set, and rolling screening runs.

Each runs the same computation the HTTP route ran before backtests moved to
workers and leaves ``result.json`` and ``backtest.zip`` (single runs also
``per_company_per_day.csv``) in ``context.output_dir``. The server's finalizer
(``src.backtesting.job_finalize``) stores them as a saved backtest.

Everything a run needs from ``app.db`` arrives in the payload: the account's
portfolio daily values (written here to a throwaway SQLite file for the
portfolio benchmark) and the account's company tags (installed as a temporary
table for the screens of a rolling run).
"""

from __future__ import annotations

import csv
import io
import json
import shutil
import sqlite3
import tempfile
import threading
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from src.orchestrator.common.db_config import get_market_db
from src.worker.context import JobCancelledError, JobContext, JobError

_DEFAULT_ARTIFACT_LIMIT = 256 * 1024 * 1024
_SIZE_MESSAGE = "Generated backtest files exceed the configured size limit"


def _artifact_limit(context: JobContext) -> int:
    value = context.settings.get("limits.max_backtest_artifact_bytes")
    return int(value) if isinstance(value, int) and value > 0 else _DEFAULT_ARTIFACT_LIMIT


def _market_db() -> str:
    path = get_market_db()
    if not path or not Path(path).is_file():
        raise JobError("The backtesting database is not available.")
    return str(path)


@contextmanager
def _portfolio_db(context: JobContext) -> Iterator[str]:
    """Path of a temporary ``Portfolio_Daily`` database, or ``""`` when the run needs none."""
    rows = context.payload.get("portfolio_daily")
    owner = str(context.payload.get("portfolio_owner") or "")
    if rows is None:
        yield ""
        return
    directory = Path(tempfile.mkdtemp(prefix="portfolio-"))
    try:
        path = directory / "portfolio.db"
        conn = sqlite3.connect(path)
        try:
            conn.execute(
                "CREATE TABLE Portfolio_Daily (owner_user_id TEXT, date TEXT, total_value REAL, net_inflow REAL)"
            )
            conn.executemany(
                "INSERT INTO Portfolio_Daily(owner_user_id, date, total_value, net_inflow) VALUES (?, ?, ?, ?)",
                [(owner, row["date"], row["total_value"], row.get("net_inflow") or 0.0) for row in rows],
            )
            conn.commit()
        finally:
            conn.close()
        yield str(path)
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def _tags(payload: dict[str, Any]) -> list[tuple[str, str]] | None:
    raw = payload.get("company_tags")
    if raw is None:
        return None
    return [(str(code), str(tag)) for code, tag in raw]


def _check_size(*sizes: int, limit: int) -> None:
    if any(size > limit for size in sizes) or sum(sizes) > limit:
        raise JobError(_SIZE_MESSAGE)


def _write(context: JobContext, name: str, content: bytes) -> None:
    (context.output_dir / name).write_bytes(content)


def run_single(context: JobContext) -> dict:
    from src import backtesting as bt
    from src.backtesting.zip_export import build_single_backtest_zip

    payload = context.payload
    request = payload["request"]
    portfolio = {ticker: {"mode": spec["mode"], "value": spec["value"]} for ticker, spec in request["portfolio"].items()}
    limit = _artifact_limit(context)
    with _portfolio_db(context) as db3:
        try:
            result = bt.run_backtest_web(
                db_path=_market_db(),
                portfolio=portfolio,
                start_date=request["start_date"],
                end_date=request["end_date"],
                benchmark_ticker=request.get("benchmark_ticker", ""),
                benchmark_mode=request.get("benchmark_mode", "ticker"),
                base_currency=payload.get("base_currency", ""),
                db3_path=db3,
                initial_capital=request.get("initial_capital", 0.0),
                risk_free_rate=payload.get("risk_free_rate", 0.0),
                commission_bps=request.get("commission_bps", 0.0),
                slippage_bps=request.get("slippage_bps", 0.0),
                spread_bps=request.get("spread_bps", 0.0),
                portfolio_owner=str(payload.get("portfolio_owner") or ""),
            )
        except ValueError as exc:
            raise JobError(str(exc)) from exc
    context.checkpoint()

    result_bytes = json.dumps(result, default=str).encode("utf-8")
    _check_size(len(result_bytes), limit=limit)
    zip_bytes = build_single_backtest_zip(result)
    _check_size(len(zip_bytes), limit=limit)
    daily = result.get("daily")
    daily_bytes = b""
    if daily:
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=list(daily[0].keys()), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(daily)
        daily_bytes = stream.getvalue().encode("utf-8")
    _check_size(len(result_bytes), len(zip_bytes), len(daily_bytes), limit=limit)

    _write(context, "result.json", result_bytes)
    _write(context, "backtest.zip", zip_bytes)
    if daily_bytes:
        _write(context, "per_company_per_day.csv", daily_bytes)
    metrics = result.get("metrics") or {}
    keys = ("total_return", "annualized_return", "excess_return")
    return {"metrics": {key: metrics.get(key) for key in keys}}


def run_csv(context: JobContext) -> dict:
    from src import backtesting as bt
    from src.backtesting.html_report import render_report

    payload = context.payload
    request = payload["request"]
    limit = _artifact_limit(context)
    with _portfolio_db(context) as db3:
        try:
            result = bt.run_backtest_set_web(
                db_path=_market_db(),
                csv_content=request["csv_content"],
                durations=request.get("durations"),
                benchmark_ticker=request.get("benchmark_ticker", ""),
                benchmark_mode=request.get("benchmark_mode", "ticker"),
                base_currency=payload.get("base_currency", ""),
                db3_path=db3,
                initial_capital=request.get("initial_capital", 0.0),
                risk_free_rate=payload.get("risk_free_rate", 0.0),
                portfolio_owner=str(payload.get("portfolio_owner") or ""),
            )
        except ValueError as exc:
            raise JobError(str(exc)) from exc
    context.checkpoint()

    result_bytes = json.dumps(result, default=str).encode("utf-8")
    _check_size(len(result_bytes), limit=limit)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("result.json", json.dumps(result, default=str))
        aggregate = result.get("aggregate", {})
        archive.writestr("summary.json", json.dumps({
            "total_runs": aggregate.get("total_runs", 0),
            "successful": aggregate.get("successful", 0),
            "failed": aggregate.get("failed", 0),
        }, indent=2))
        archive.writestr("report.html", render_report(
            result,
            meta={"kind": "csv", "title": "CSV portfolio backtest"},
            backtest_id=str(payload.get("backtest_id") or ""),
        ))
    zip_bytes = buffer.getvalue()
    _check_size(len(zip_bytes), limit=limit)
    _check_size(len(result_bytes), len(zip_bytes), limit=limit)

    _write(context, "result.json", result_bytes)
    _write(context, "backtest.zip", zip_bytes)
    aggregate = result.get("aggregate") or {}
    stats = aggregate.get("stats") or {}
    return {"aggregate": {
        "total_runs": aggregate.get("total_runs"),
        "stats": {"annualized_return": {"mean": (stats.get("annualized_return") or {}).get("mean")}},
    }}


class _Progress:
    """The runner's ``progress_queue``: forwards each message as job progress."""

    def __init__(self, context: JobContext) -> None:
        self.context = context

    def put(self, message: dict[str, Any]) -> None:
        if message.get("type") != "progress":
            return
        progress = {key: value for key, value in message.items() if key != "type"}
        total = progress.get("total_backtests") or 0
        percent = 100.0 * min(1.0, (progress.get("completed_backtests") or 0) / total) if total else None
        self.context.report_progress(percent, str(progress.get("phase") or "")[:500] or None, progress)


def run_rolling(context: JobContext) -> dict:
    from src import backtesting as bt
    from src.backtesting.zip_export import ExportSizeLimitExceeded, save_rolling_backtest_zip

    payload = context.payload
    request = payload["request"]
    limit = _artifact_limit(context)
    cancel_event: threading.Event = context.cancel_event
    with _portfolio_db(context) as db3:
        try:
            result = bt.run_screening_backtest_rolling(
                db_path=_market_db(),
                criteria=request["criteria"],
                criteria_match=request.get("criteria_match", "all"),
                columns=request["columns"],
                cadence=request.get("cadence", "monthly"),
                durations=request.get("durations"),
                weighting_modes=request.get("weighting_modes"),
                max_companies=request.get("max_companies", 25),
                ranking_algorithm=request.get("ranking_algorithm", "none"),
                ranking_rules=request.get("ranking_rules"),
                computed_columns=request.get("computed_columns"),
                benchmark_ticker=request.get("benchmark_ticker", ""),
                benchmark_mode=request.get("benchmark_mode", "ticker"),
                base_currency=payload.get("base_currency", ""),
                db3_path=db3,
                initial_capital=request.get("initial_capital", 0.0),
                risk_free_rate=payload.get("risk_free_rate", 0.0),
                start_period=request.get("start_period"),
                end_period=request.get("end_period"),
                progress_queue=_Progress(context),
                cancel_event=cancel_event,
                portfolio_owner=str(payload.get("portfolio_owner") or ""),
                company_tags=_tags(payload),
            )
        except RuntimeError as exc:
            if "cancelled" in str(exc).lower():
                raise JobCancelledError() from exc
            raise
        except ValueError as exc:
            raise JobError(str(exc)) from exc
    context.checkpoint()
    context.report_progress(None, "Saving results", {"phase": "saving"})

    scratch = Path(tempfile.mkdtemp(prefix="archive-"))
    try:
        try:
            saved = Path(save_rolling_backtest_zip(result, str(scratch), limit))
        except ExportSizeLimitExceeded as exc:
            limit_mib = exc.limit_bytes // (1024 * 1024)
            raise JobError(
                "Backtest completed, but its download archive exceeded "
                f"the {limit_mib} MiB limit. Reduce the period range or "
                "raise the limits.max_backtest_artifact_bytes setting on the Admin page and restart."
            ) from exc
        shutil.move(str(saved / "backtest.zip"), str(context.output_dir / "backtest.zip"))
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    aggregate = result.get("aggregate", {})
    config = result.get("config", {})
    # The archive is the full artifact; result.json holds what the workspace
    # draws: the aggregate, one row per run, and monthly equity paths.
    (context.output_dir / "result.json").write_text(
        json.dumps({
            "aggregate": aggregate, "config": config,
            "runs": result.get("runs", []),
            "paths": result.get("paths", {}),
            "period_holdings": result.get("period_holdings", {}),
            "run_holdings": result.get("run_holdings", {}),
            "names": result.get("names", {}),
        }, default=str),
        encoding="utf-8",
    )
    stats = aggregate.get("stats") or {}
    comparison = aggregate.get("benchmark_comparison") or {}
    return {
        "aggregate": {
            "complete_backtests": aggregate.get("complete_backtests"),
            "stats": {"total_return": {"mean": (stats.get("total_return") or {}).get("mean")}},
            "benchmark_comparison": {"win_rate": comparison.get("win_rate")},
        },
        "config": {"cadence": config.get("cadence")},
    }
