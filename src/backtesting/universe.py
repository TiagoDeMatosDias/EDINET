"""Listing-aware security universe for point-in-time backtests."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from .market_data import MarketDataStore


@dataclass
class PointInTimeUniverse:
    """Track which securities are listed and have a known ticker at each decision point."""

    store: MarketDataStore

    def is_eligible(self, company_code: str, as_of: str) -> bool:
        """A security must be listed and have a known ticker alias at the decision time."""
        if not self.store.is_listed(company_code, as_of):
            return False
        ticker = self.store.get_ticker_for(company_code, as_of)
        return ticker is not None

    def eligible_companies(self, company_codes: list[str], as_of: str) -> list[str]:
        """Filter a candidate list to those eligible at a decision timestamp."""
        return [c for c in company_codes if self.is_eligible(c, as_of)]

    def delisted_companies(self, company_codes: list[str], as_of: str) -> list[str]:
        """Return companies that were previously listed but are no longer active."""
        return [c for c in company_codes if not self.is_eligible(c, as_of)]
