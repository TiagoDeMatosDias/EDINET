"""Point-in-time signal generation from screening results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .asof import HistoricalObservation, select_as_of


@dataclass(frozen=True)
class TradingSignal:
    """A decision to buy or sell a security at a given weight."""

    company_code: str
    action: str  # "buy" | "sell" | "hold"
    target_weight: float
    decision_time: datetime
    signal_policy_version: str = "1"
    data_watermark: str = ""


def signals_from_observations(
    observations: list[HistoricalObservation],
    decision_time: datetime,
    *,
    min_weight: float = 0.0,
    policy_version: str = "1",
    watermark: str = "",
) -> list[TradingSignal]:
    """Convert as-of observations into actionable trading signals.

    A positive signal (weight > min_weight) becomes a buy; zero triggers a sell.
    """
    selected = select_as_of(observations, decision_time)
    signals: list[TradingSignal] = []
    for (company, metric, _), obs in selected.items():
        if obs.value is None:
            continue
        if obs.value > min_weight:
            signals.append(TradingSignal(
                company_code=company,
                action="buy",
                target_weight=obs.value,
                decision_time=decision_time,
                signal_policy_version=policy_version,
                data_watermark=watermark,
            ))
        else:
            signals.append(TradingSignal(
                company_code=company,
                action="sell",
                target_weight=0.0,
                decision_time=decision_time,
                signal_policy_version=policy_version,
                data_watermark=watermark,
            ))
    return signals
