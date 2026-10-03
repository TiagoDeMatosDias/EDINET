"""Market prices and reference FX rates for valuing a portfolio day by day.

One rebuild values every holding on every calendar day, so each ticker's
history and each currency's ECB reference series is read once and looked up
with a binary search instead of a query per holding per day.

Prices are returned in the holding's own (broker) currency.  A ticker can be
stored under another form (``5984.T`` is stored as ``59840``) and quoted in
another currency (``CSPX`` priced from the London USD line while the broker
books it in EUR); both are resolved here so the ledger never mixes units.
"""

from __future__ import annotations

import logging
import sqlite3
from bisect import bisect_right
from dataclasses import dataclass
from fractions import Fraction

from src.utilities.price_provenance import table_columns
from src.utilities.stock_prices import tse_code

logger = logging.getLogger(__name__)

# A quote this many days older than the valuation date is reported as stale.
STALE_PRICE_DAYS = 7


def price_ticker_candidates(symbol: str) -> list[str]:
    """Stored forms of a broker symbol, most specific first (``5984.T`` → ``59840``)."""
    clean = str(symbol or "").strip()
    code = tse_code(clean)
    candidates = [clean, f"{code}0", f"{code}.T", code] if code else [clean]
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def is_share_split(ratio_from: float, ratio_to: float) -> bool:
    """Whether a provider "split" changes the share count.

    Yahoo records spinoff price adjustments as fractional splits (3M's
    Solventum spinoff is 1196:1000).  Those restate old prices but issue no
    new shares of the parent, so only ratios of small whole numbers (2:1,
    3:2, 1:25, 21:20) are applied to ledger quantities.
    """
    try:
        ratio = Fraction(str(ratio_to)) / Fraction(str(ratio_from))
    except (ValueError, ZeroDivisionError, TypeError):
        return False
    if ratio <= 0 or ratio == 1:
        return False
    return ratio.denominator <= 100 and ratio.numerator <= 100


@dataclass
class PriceSeries:
    """A ticker's closes in the holding currency, sorted by date."""

    ticker: str
    currency: str
    source_currency: str
    dates: list[str]
    prices: list[float]

    def on(self, date: str) -> tuple[float, str] | None:
        """The latest close on or before *date* with its date."""
        index = bisect_right(self.dates, date) - 1
        if index < 0:
            return None
        return self.prices[index], self.dates[index]

    @property
    def first_date(self) -> str | None:
        return self.dates[0] if self.dates else None

    @property
    def last_date(self) -> str | None:
        return self.dates[-1] if self.dates else None


class MarketData:
    """Cached price and ECB FX lookups over one read connection to the market database."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._columns = table_columns(conn, "Stock_Prices")
        self._fx: dict[str, tuple[list[str], list[float]]] = {}
        self._prices: dict[tuple[str, str], PriceSeries | None] = {}
        self._split_factors: dict[str, list[tuple[str, float]]] = {}

    # -- FX -----------------------------------------------------------------

    def _fx_series(self, currency: str) -> tuple[list[str], list[float]]:
        currency = currency.upper()
        if currency not in self._fx:
            try:
                rows = self.conn.execute(
                    "SELECT Date, Price FROM Stock_Prices WHERE Ticker = 'EUR' AND Currency = ? "
                    "AND Price > 0 ORDER BY Date",
                    (currency,),
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
            self._fx[currency] = ([str(row[0])[:10] for row in rows], [float(row[1]) for row in rows])
        return self._fx[currency]

    def per_eur(self, currency: str, date: str) -> float | None:
        """Units of *currency* per euro on *date* (latest ECB reference rate on or before it)."""
        currency = (currency or "EUR").upper()
        if currency == "EUR":
            return 1.0
        dates, rates = self._fx_series(currency)
        index = bisect_right(dates, date) - 1
        return rates[index] if index >= 0 else None

    def fx_last_date(self, currency: str) -> str | None:
        if (currency or "EUR").upper() == "EUR":
            return None
        dates, _rates = self._fx_series(currency)
        return dates[-1] if dates else None

    def rate(self, from_currency: str, to_currency: str, date: str) -> float | None:
        """How many *to_currency* one unit of *from_currency* buys on *date*."""
        source = (from_currency or "EUR").upper()
        target = (to_currency or "EUR").upper()
        if source == target:
            return 1.0
        source_per_eur = self.per_eur(source, date)
        target_per_eur = self.per_eur(target, date)
        if not source_per_eur or not target_per_eur:
            return None
        return target_per_eur / source_per_eur

    # -- Prices -------------------------------------------------------------

    def split_factors(self, ticker: str) -> list[tuple[str, float]]:
        if ticker not in self._split_factors:
            from src.portfolio.portfolio_state import _load_split_factors

            self._split_factors[ticker] = _load_split_factors(self.conn, ticker)
        return self._split_factors[ticker]

    def _rows(self, ticker: str) -> list[tuple[str, float, str, str]]:
        basis = "Price_Basis" if "Price_Basis" in self._columns else "'raw'"
        currency = "Currency" if "Currency" in self._columns else "''"
        try:
            rows = self.conn.execute(
                f"SELECT Date, Price, {currency}, {basis} FROM Stock_Prices "
                "WHERE Ticker = ? AND Price IS NOT NULL ORDER BY Date",
                (ticker,),
            ).fetchall()
        except sqlite3.OperationalError:
            return []
        return [(str(row[0])[:10], float(row[1]), str(row[2] or "").upper(), str(row[3] or "raw").lower()) for row in rows]

    def price_series(self, symbol: str, currency: str) -> PriceSeries | None:
        """The as-traded close history for *symbol*, expressed in *currency*."""
        key = (symbol, (currency or "").upper())
        if key in self._prices:
            return self._prices[key]
        series = None
        for ticker in price_ticker_candidates(symbol):
            rows = self._rows(ticker)
            if rows:
                series = self._build_series(ticker, rows, key[1])
                break
        self._prices[key] = series
        return series

    def _build_series(self, ticker: str, rows: list[tuple[str, float, str, str]], currency: str) -> PriceSeries:
        by_currency: dict[str, int] = {}
        for _date, _price, row_currency, _basis in rows:
            by_currency[row_currency] = by_currency.get(row_currency, 0) + 1
        # One label per series: the holding's own currency when it is quoted
        # in it, otherwise the most common label (converted below).
        labelled = [code for code in by_currency if code]
        source_currency = currency if currency in by_currency else (max(labelled, key=by_currency.get) if labelled else currency)
        if len(by_currency) > 1:
            logger.info("%s has prices under %s; using %s rows", ticker, sorted(by_currency), source_currency or "unlabelled")
        factors = self.split_factors(ticker)
        dates: list[str] = []
        prices: list[float] = []
        for date, price, row_currency, basis in rows:
            if row_currency and row_currency != source_currency:
                continue
            if basis == "adjusted":
                # Provider closes are restated for later splits; the ledger
                # applies splits to quantities on their date, so it needs the
                # quote as traded on the day.
                for split_date, cumulative in factors:
                    if split_date > date and cumulative:
                        price = price / cumulative
                        break
            if currency and source_currency and source_currency != currency:
                rate = self.rate(source_currency, currency, date)
                if rate is None:
                    continue
                price *= rate
            if dates and dates[-1] == date:
                prices[-1] = price
                continue
            dates.append(date)
            prices.append(price)
        return PriceSeries(ticker=ticker, currency=currency or source_currency, source_currency=source_currency or currency, dates=dates, prices=prices)

    def price(self, symbol: str, currency: str, date: str) -> tuple[float, str, PriceSeries] | None:
        """The close for *symbol* in *currency* on or before *date*, its date, and the series."""
        series = self.price_series(symbol, currency)
        if series is None:
            return None
        found = series.on(date)
        if found is None:
            return None
        return found[0], found[1], series
