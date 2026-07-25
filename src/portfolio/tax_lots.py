"""Deterministic tax-lot ledger primitives for portfolio analysis."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class TaxLot:
    lot_id: str
    ticker: str
    acquired_on: date
    shares: float
    cost_per_share: float


@dataclass(frozen=True)
class RealizedLot:
    lot_id: str
    ticker: str
    shares: float
    cost_basis: float
    proceeds: float
    gain: float


class LotLedger:
    """Track open lots and realize gains using a declared matching method."""

    def __init__(self, method: str = "fifo") -> None:
        if method not in {"fifo", "average", "specific"}:
            raise ValueError("method must be fifo, average, or specific")
        self.method = method
        self._lots: dict[str, list[TaxLot]] = {}

    def buy(self, lot: TaxLot) -> None:
        if lot.shares <= 0 or lot.cost_per_share < 0:
            raise ValueError("Lot shares must be positive and cost must not be negative")
        self._lots.setdefault(lot.ticker, []).append(lot)

    def open_lots(self, ticker: str) -> list[TaxLot]:
        return [lot for lot in self._lots.get(ticker, []) if lot.shares > 1e-12]

    def sell(self, ticker: str, shares: float, proceeds_per_share: float, *, lot_ids: list[str] | None = None) -> list[RealizedLot]:
        if shares <= 0 or proceeds_per_share < 0:
            raise ValueError("Sale shares must be positive and proceeds must not be negative")
        lots = self.open_lots(ticker)
        if sum(lot.shares for lot in lots) + 1e-12 < shares:
            raise ValueError("Sale exceeds open shares")
        if self.method == "average":
            return self._sell_average(ticker, shares, proceeds_per_share, lots)
        ordered = self._ordered_lots(lots, lot_ids)
        remaining = shares
        realized: list[RealizedLot] = []
        for lot in ordered:
            if remaining <= 1e-12:
                break
            matched = min(remaining, lot.shares)
            cost = matched * lot.cost_per_share
            proceeds = matched * proceeds_per_share
            lot.shares -= matched
            remaining -= matched
            realized.append(RealizedLot(lot.lot_id, ticker, matched, cost, proceeds, proceeds - cost))
        return realized

    def _ordered_lots(self, lots: list[TaxLot], lot_ids: list[str] | None) -> list[TaxLot]:
        if self.method == "specific":
            if not lot_ids:
                raise ValueError("specific matching requires lot_ids")
            by_id = {lot.lot_id: lot for lot in lots}
            if any(lot_id not in by_id for lot_id in lot_ids):
                raise ValueError("specific lot was not found")
            return [by_id[lot_id] for lot_id in lot_ids]
        return sorted(lots, key=lambda lot: (lot.acquired_on, lot.lot_id))

    def _sell_average(self, ticker: str, shares: float, proceeds_per_share: float, lots: list[TaxLot]) -> list[RealizedLot]:
        total_shares = sum(lot.shares for lot in lots)
        average_cost = sum(lot.shares * lot.cost_per_share for lot in lots) / total_shares
        remaining = shares
        for lot in lots:
            matched = min(remaining, lot.shares)
            lot.shares -= matched
            remaining -= matched
            if remaining <= 1e-12:
                break
        proceeds = shares * proceeds_per_share
        return [RealizedLot("average", ticker, shares, shares * average_cost, proceeds, proceeds - shares * average_cost)]
