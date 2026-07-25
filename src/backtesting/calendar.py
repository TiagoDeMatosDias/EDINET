"""Eligible trading session calendar for point-in-time backtests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class TradingCalendar:
    """Determine the next eligible session from a signal timestamp.

    Japanese market: Monday–Friday, excluding exchange holidays.
    """

    holidays: frozenset[date] = frozenset()

    def is_trading_day(self, day: date) -> bool:
        """Return whether the Tokyo Stock Exchange is scheduled to trade."""
        if day.weekday() >= 5:  # Saturday–Sunday
            return False
        if day in self.holidays:
            return False
        return True

    def next_session(self, from_date: date, lag_days: int = 1) -> date:
        """Return the first eligible session at least *lag_days* after *from_date*."""
        candidate = from_date + timedelta(days=lag_days)
        while not self.is_trading_day(candidate):
            candidate += timedelta(days=1)
        return candidate

    def sessions_between(self, start: date, end: date) -> list[date]:
        """Return all trading days between two dates inclusive."""
        sessions: list[date] = []
        current = start
        while current <= end:
            if self.is_trading_day(current):
                sessions.append(current)
            current += timedelta(days=1)
        return sessions


# Default: no holidays in testing; real use requires exchange calendar data.
DEFAULT_CALENDAR = TradingCalendar()
