"""Typed API contracts for the Portfolio module."""

from __future__ import annotations

from pydantic import BaseModel, Field


class TransactionEntry(BaseModel):
    """A normalized IBKR transaction stored in the Portfolio database."""

    id: int | None = None
    transaction_id: str
    trade_id: str | None = None
    account_id: str | None = None
    activity_type: str
    asset_category: str | None = None
    symbol: str | None = None
    description: str | None = None
    isin: str | None = None
    conid: str | None = None
    currency: str
    trade_date: str
    settle_date: str | None = None
    quantity: float = 0
    trade_price: float | None = None
    trade_money: float | None = None
    amount: float = 0
    proceeds: float | None = None
    commission: float = 0
    taxes: float = 0
    net_cash: float | None = None
    buy_sell: str | None = None
    fx_rate_to_base: float | None = None
    strike: float | None = None
    expiry: str | None = None
    put_call: str | None = None
    underlying_symbol: str | None = None
    underlying_conid: str | None = None
    multiplier: float = 1
    action_description: str | None = None
    action_id: str | None = None
    source_file: str | None = None
    imported_at: str | None = None
    notes: str | None = None


class UploadResponse(BaseModel):
    source_file: str
    total_entries: int
    inserted: int
    skipped: int
    by_activity: dict[str, int] = Field(default_factory=dict)
    new_tickers_fetched: list[str] = Field(default_factory=list)
    ticker_fetch_failures: list[str] = Field(default_factory=list)


class HoldingItem(BaseModel):
    symbol: str
    asset_category: str
    quantity: float
    avg_cost: float | None = None
    market_price: float | None = None
    market_value: float | None = None
    market_value_native: float | None = None
    currency: str
    fx_rate: float | None = None
    weight: float | None = None
    is_option: bool = False
    strike: float | None = None
    expiry: str | None = None
    put_call: str | None = None
    underlying: str | None = None


class BenchmarkInfo(BaseModel):
    ticker: str | None = None
    total_return: float | None = None
    excess_return: float | None = None
    alpha: float | None = None
    beta: float | None = None
    information_ratio: float | None = None
    tracking_error: float | None = None
    series: list[dict] = Field(default_factory=list)


class DividendBreakdown(BaseModel):
    total_gross: float = 0
    total_tax: float = 0
    total_net: float = 0


class ReturnDistribution(BaseModel):
    min: float = 0
    p25: float = 0
    median: float = 0
    p75: float = 0
    max: float = 0
    skewness: float = 0
    kurtosis: float = 0
    positive_days: int = 0
    negative_days: int = 0
    zero_days: int = 0


class ReturnAttribution(BaseModel):
    total_return: float = 0
    dividend_yield: float = 0
    capital_appreciation: float = 0
    real_return: float = 0
    inflation_total: float = 0


class PerformanceResponse(BaseModel):
    start_date: str
    end_date: str
    base_currency: str
    total_return: float | None = None
    annualized_return: float | None = None
    volatility: float | None = None
    sharpe_ratio: float | None = None
    sortino_ratio: float | None = None
    max_drawdown: float | None = None
    max_dd_peak_date: str | None = None
    max_dd_trough_date: str | None = None
    calmar_ratio: float | None = None
    win_rate: float | None = None
    avg_win: float | None = None
    avg_loss: float | None = None
    profit_factor: float | None = None
    var_95: float | None = None
    cvar_95: float | None = None
    total_dividend_income: float = 0
    risk_free_rate: float | None = None
    benchmark: BenchmarkInfo | None = None
    dividend_breakdown: DividendBreakdown | None = None
    return_distribution: ReturnDistribution | None = None
    return_attribution: ReturnAttribution | None = None
    inflation_series: list[dict] = Field(default_factory=list)


class DateRangeResponse(BaseModel):
    min_date: str | None = None
    max_date: str | None = None


class ActivitySummaryResponse(BaseModel):
    by_activity: dict[str, int] = Field(default_factory=dict)


class RebuildResponse(BaseModel):
    message: str
    daily_rows: int
    holdings_count: int
