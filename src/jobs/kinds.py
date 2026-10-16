"""Every kind of job a worker can run: its lane, exclusivity, and retry policy.

Handlers (worker side) and finalizers (server side) are named by dotted
``"module:function"`` paths and imported only when used, so neither side
imports the other's modules, and ``src.jobs`` imports no feature module.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any, Callable

from .protocol import MAX_ATTEMPTS, Lane


@dataclass(frozen=True)
class KindSpec:
    name: str
    label: str
    lane: Lane
    handler: str
    finalizer: str | None = None
    # Resources no other running job may hold at the same time (long rebuilds).
    exclusive: frozenset[str] = frozenset()
    # Databases the job writes; triggers maintenance after the job.
    writes: frozenset[str] = frozenset()
    # Whether a job whose worker vanished may run again from the start.
    retry_on_lost: bool = True
    # Show a toast when it finishes even if the request that queued it waited for it.
    notify: bool = False
    admin_only: bool = False

    @property
    def max_attempts(self) -> int:
        return MAX_ATTEMPTS if self.retry_on_lost else 1


_MARKET = frozenset({"market"})
_FILINGS = frozenset({"filings"})
_BOTH = frozenset({"market", "filings"})

KINDS: dict[str, KindSpec] = {
    spec.name: spec
    for spec in (
        KindSpec(
            "pipeline", "Pipeline", "batch", "src.worker.handlers.pipeline:run",
            exclusive=_BOTH, writes=_BOTH, retry_on_lost=False, notify=True,
        ),
        KindSpec(
            "backtest_single", "Backtest", "interactive", "src.worker.handlers.backtest:run_single",
            finalizer="src.backtesting.job_finalize:finalize_single", notify=True,
        ),
        KindSpec(
            "backtest_csv", "CSV backtest", "interactive", "src.worker.handlers.backtest:run_csv",
            finalizer="src.backtesting.job_finalize:finalize_csv", notify=True,
        ),
        KindSpec(
            "backtest_rolling", "Rolling backtest", "batch", "src.worker.handlers.backtest:run_rolling",
            finalizer="src.backtesting.job_finalize:finalize_rolling", notify=True,
        ),
        KindSpec(
            "screening", "Screening", "interactive", "src.worker.handlers.screening:run",
            finalizer="src.web_app.api.screening_jobs:finalize",
        ),
        KindSpec("translation", "Translation", "interactive", "src.worker.handlers.translation:run", writes=_FILINGS),
        KindSpec(
            "price_refresh", "Price refresh", "interactive", "src.worker.handlers.market:price_refresh",
            writes=_MARKET, retry_on_lost=False,
        ),
        KindSpec(
            "portfolio_prices", "Portfolio prices", "interactive", "src.worker.handlers.market:portfolio_prices",
            writes=_MARKET, retry_on_lost=False,
        ),
        KindSpec(
            "portfolio_market_refresh", "Portfolio market refresh", "interactive",
            "src.worker.handlers.market:portfolio_market_refresh", writes=_MARKET, retry_on_lost=False,
        ),
        KindSpec(
            "split_review", "Split review", "interactive", "src.worker.handlers.market:split_review",
            writes=_MARKET, retry_on_lost=False,
        ),
        KindSpec(
            "split_detection", "Split detection", "batch", "src.worker.handlers.market:split_detection",
            exclusive=_MARKET, writes=_MARKET, retry_on_lost=False, notify=True,
        ),
        KindSpec(
            "description_fetch", "Company description", "interactive",
            "src.worker.handlers.market:description_fetch", writes=_MARKET,
        ),
        KindSpec(
            "filing_acquire", "Filing download", "batch", "src.worker.handlers.filings:acquire",
            exclusive=_FILINGS, writes=_FILINGS, retry_on_lost=False, notify=True,
        ),
        KindSpec(
            "xbrl_backfill", "XBRL backfill", "batch", "src.worker.handlers.filings:xbrl_backfill",
            exclusive=_FILINGS, writes=_FILINGS, retry_on_lost=False, notify=True,
        ),
        KindSpec("echo", "Diagnostic", "interactive", "src.worker.handlers.diagnostic:echo", admin_only=True),
    )
}


class UnknownJobKind(KeyError):
    """Raised for a kind that is not in ``KINDS``."""


def kind_spec(name: str) -> KindSpec:
    try:
        return KINDS[name]
    except KeyError:
        raise UnknownJobKind(name) from None


def resolve(path: str) -> Callable[..., Any]:
    """Import ``"package.module:function"`` and return the function."""
    module_name, _, attribute = path.partition(":")
    if not module_name or not attribute:
        raise ValueError(f"Invalid callable path {path!r}")
    return getattr(import_module(module_name), attribute)


def handler_for(name: str) -> Callable[..., Any]:
    return resolve(kind_spec(name).handler)


def finalizer_for(name: str) -> Callable[..., Any] | None:
    spec = kind_spec(name)
    return resolve(spec.finalizer) if spec.finalizer else None


__all__ = ["KINDS", "KindSpec", "UnknownJobKind", "finalizer_for", "handler_for", "kind_spec", "resolve"]
