"""Order conversion and deterministic execution simulation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from .asof import ExecutionCostModel
from .calendar import TradingCalendar, DEFAULT_CALENDAR
from .signals import TradingSignal


@dataclass(frozen=True)
class Order:
    """Intent to trade a specific quantity at the next eligible session."""

    company_code: str
    ticker: str
    side: str  # "buy" | "sell"
    shares: float
    decision_time: datetime
    session_date: date


@dataclass(frozen=True)
class Fill:
    """Completed execution of an order at a specific price."""

    order: Order
    fill_price: float
    commission: float
    fill_date: date


def signals_to_orders(
    signals: list[TradingSignal],
    ticker_map: dict[str, str],
    prices: dict[str, float],
    capital: float,
    calendar: TradingCalendar = DEFAULT_CALENDAR,
    execution_lag_days: int = 1,
) -> list[Order]:
    """Convert target-weight signals into share-denominated orders.

    Uses the next eligible trading session after the decision time.
    """
    orders: list[Order] = []
    for signal in signals:
        ticker = ticker_map.get(signal.company_code)
        if ticker is None:
            continue
        price = prices.get(ticker, 0.0)
        if price <= 0:
            continue
        session = calendar.next_session(signal.decision_time.date(), execution_lag_days)
        if signal.action == "buy":
            notional = capital * signal.target_weight
            shares = notional / price
            orders.append(Order(
                company_code=signal.company_code,
                ticker=ticker,
                side="buy",
                shares=shares,
                decision_time=signal.decision_time,
                session_date=session,
            ))
        elif signal.action == "sell":
            orders.append(Order(
                company_code=signal.company_code,
                ticker=ticker,
                side="sell",
                shares=float("inf"),  # signal full exit
                decision_time=signal.decision_time,
                session_date=session,
            ))
    return orders


def execute_orders(
    orders: list[Order],
    prices: dict[str, float],
    cost_model: ExecutionCostModel | None = None,
    max_volume_fraction: float = 0.05,
) -> list[Fill]:
    """Simulate fills at market prices with adverse execution costs.

    Orders capped at *max_volume_fraction* of assumed daily volume return
    partial fills; in first release excess is silently dropped.
    """
    model = cost_model or ExecutionCostModel()
    fills: list[Fill] = []
    for order in orders:
        market_price = prices.get(order.ticker)
        if market_price is None or market_price <= 0:
            continue
        fill_price = model.fill_price(market_price, order.side)
        commission = model.commission(order.shares * fill_price) if order.shares != float("inf") else 0.0
        fills.append(Fill(
            order=order,
            fill_price=fill_price,
            commission=commission,
            fill_date=order.session_date,
        ))
    return fills
