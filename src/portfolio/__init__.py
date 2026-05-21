"""Portfolio Management Module.

Import IBKR FlexQuery XML transaction files, store all activity (trades,
dividends, corporate actions, cash flows) in a dedicated database, generate
portfolio performance analytics, and backtest against model portfolios.
"""

from src.portfolio.schema import create_tables
from src.portfolio.ibkr_parser import parse_ibkr_xml, parse_ibkr_xml_file, normalize_entries
from src.portfolio.price_fetcher import ensure_prices_for_tickers, _build_currency_map
from src.portfolio.transactions import insert_entries
from src.portfolio.option_pricing import (
    black_scholes, binomial_tree, option_greeks, get_option_price
)
from src.portfolio.portfolio_state import (
    build_portfolio_state, get_daily_values, get_current_holdings
)
from src.portfolio.performance import calculate_metrics, get_risk_free_rate

__all__ = [
    "create_tables",
    "parse_ibkr_xml",
    "parse_ibkr_xml_file",
    "normalize_entries",
    "ensure_prices_for_tickers",
    "_build_currency_map",
    "insert_entries",
    "black_scholes",
    "binomial_tree",
    "option_greeks",
    "get_option_price",
    "build_portfolio_state",
    "get_daily_values",
    "get_current_holdings",
    "calculate_metrics",
    "get_risk_free_rate",
]
