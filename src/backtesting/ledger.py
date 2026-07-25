"""Cash, position, and lot-aware general ledger for backtest simulations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .execution import Fill


@dataclass
class PortfolioLedger:
    """Track cash, positions, and transaction history over a simulation run."""

    initial_cash: float
    cash: float = field(init=False)
    positions: dict[str, float] = field(default_factory=dict)  # ticker -> shares
    transactions: list[dict[str, Any]] = field(default_factory=list)
    dividends: list[dict[str, Any]] = field(default_factory=list)
    corporate_actions: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.cash = self.initial_cash

    def apply_fill(self, fill: Fill) -> None:
        """Apply a filled order to cash and positions."""
        ticker = fill.order.ticker
        shares = fill.order.shares
        if shares == float("inf"):
            shares = self.positions.get(ticker, 0.0)
        if fill.order.side == "buy":
            cost = shares * fill.fill_price + fill.commission
            if cost > self.cash:
                shares = self.cash / (fill.fill_price + fill.commission / max(shares, 1e-12))
                cost = shares * fill.fill_price + fill.commission
            self.cash -= cost
            self.positions[ticker] = self.positions.get(ticker, 0.0) + shares
        else:
            available = min(shares, self.positions.get(ticker, 0.0))
            proceeds = available * fill.fill_price - fill.commission
            self.cash += proceeds
            self.positions[ticker] = self.positions.get(ticker, 0.0) - available
            if self.positions[ticker] <= 1e-12:
                del self.positions[ticker]
        self.transactions.append({
            "ticker": ticker,
            "side": fill.order.side,
            "shares": shares,
            "price": fill.fill_price,
            "commission": fill.commission,
            "date": fill.fill_date.isoformat() if hasattr(fill.fill_date, "isoformat") else str(fill.fill_date),
        })

    def apply_dividend(self, ticker: str, amount_per_share: float, ex_date: str) -> None:
        """Receive a cash dividend for current holdings."""
        holding = self.positions.get(ticker, 0.0)
        if holding <= 0:
            return
        received = holding * amount_per_share
        self.cash += received
        self.dividends.append({"ticker": ticker, "per_share": amount_per_share, "ex_date": ex_date, "received": received})

    def apply_split(self, ticker: str, ratio: float) -> None:
        """Adjust shares for a stock split (e.g., ratio=2 for a 2:1 split)."""
        old = self.positions.get(ticker, 0.0)
        if old <= 0 or ratio <= 0:
            return
        self.positions[ticker] = old * ratio
        self.corporate_actions.append({"ticker": ticker, "action": "split", "ratio": ratio})

    def mark_to_market(self, prices: dict[str, float]) -> float:
        """Return total portfolio value (cash + marked positions)."""
        positions_value = sum(
            shares * prices.get(ticker, 0.0)
            for ticker, shares in self.positions.items()
            if shares > 1e-12
        )
        return self.cash + positions_value

    def nav(self, prices: dict[str, float]) -> float:
        """Alias for mark_to_market."""
        return self.mark_to_market(prices)

    def performance(self, prices: dict[str, float]) -> dict[str, Any]:
        """Return summary performance metrics."""
        current_nav = self.mark_to_market(prices)
        gross_return = (current_nav - self.initial_cash) / self.initial_cash if self.initial_cash else 0.0
        total_cost = sum(t["commission"] for t in self.transactions)
        net_return = ((current_nav - self.initial_cash) - total_cost) / self.initial_cash if self.initial_cash else 0.0
        return {
            "initial_cash": self.initial_cash,
            "current_cash": self.cash,
            "current_nav": current_nav,
            "gross_return": gross_return,
            "total_costs": total_cost,
            "net_return": net_return,
            "positions": len(self.positions),
            "transactions": len(self.transactions),
        }
