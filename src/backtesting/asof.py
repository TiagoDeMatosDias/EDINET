"""Point-in-time observation and execution primitives for backtests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable


@dataclass(frozen=True)
class HistoricalObservation:
    company_code: str
    metric: str
    period_end: date
    value: float | None
    submitted_at: datetime
    available_at: datetime
    withdrawn_at: datetime | None = None
    source_id: str = ""


def select_as_of(
    observations: Iterable[HistoricalObservation],
    decision_time: datetime,
    *,
    execution_lag: timedelta = timedelta(0),
) -> dict[tuple[str, str, date], HistoricalObservation]:
    """Select the latest non-withdrawn observation known at a decision time.

    Execution lag affects when a selected signal may trade; it must never
    expand the information set and therefore cannot be added to the cutoff.
    """
    if execution_lag < timedelta(0):
        raise ValueError("execution_lag must not be negative")
    available_time = decision_time
    selected: dict[tuple[str, str, date], HistoricalObservation] = {}
    for observation in observations:
        if observation.available_at > available_time:
            continue
        if observation.withdrawn_at is not None and observation.withdrawn_at <= available_time:
            continue
        key = (observation.company_code, observation.metric, observation.period_end)
        current = selected.get(key)
        if current is None or observation.submitted_at > current.submitted_at:
            selected[key] = observation
    return selected


@dataclass(frozen=True)
class ExecutionCostModel:
    commission_bps: float = 0.0
    slippage_bps: float = 0.0
    spread_bps: float = 0.0

    def fill_price(self, market_price: float, side: str) -> float:
        """Apply deterministic adverse execution costs to a market price."""
        if market_price <= 0:
            raise ValueError("market_price must be positive")
        if side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        bps = (self.slippage_bps + self.spread_bps / 2) / 10_000
        return market_price * (1 + bps if side == "buy" else 1 - bps)

    def commission(self, notional: float) -> float:
        if notional < 0:
            raise ValueError("notional must not be negative")
        return notional * self.commission_bps / 10_000


def coverage_report(
    requested_companies: Iterable[str],
    observations: Iterable[HistoricalObservation],
) -> dict[str, Any]:
    """Describe missing point-in-time observations instead of silently filling them."""
    companies = sorted(set(requested_companies))
    observed = {item.company_code for item in observations}
    missing = [company for company in companies if company not in observed]
    return {
        "requested_companies": companies,
        "observed_companies": sorted(observed),
        "missing_companies": missing,
        "complete": not missing,
    }
