"""Walk-forward portfolio state reconstruction.

Rebuilds holdings, daily values, and dividend income from Transactions
(db3) + market prices (db2) + option pricing models.

The algorithm walks day-by-day from the earliest transaction to today,
applying trades, corporate actions, and pricing current holdings at each
step.  Results are stored in ``Portfolio_Daily``, ``Portfolio_Holdings``,
and ``Holdings_History`` tables for fast retrieval by the API.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import date as Date
from datetime import timedelta

from src.orchestrator.common.db_config import get_app_db, get_market_db
from src.orchestrator.common.sqlite import connect_read, connect_write, is_protected_database
from src.portfolio import option_pricing as _op
from src.portfolio.market_data import is_share_split
from src.portfolio.schema import create_tables
from src.utilities.price_provenance import distinct_split_records, is_squeeze_out, table_columns

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Split-adjusted price helpers
# ---------------------------------------------------------------------------

_split_table_checked: set[str] = set()
# Cache keyed by (connection_id, ticker) to avoid cross-database contamination
# (e.g. when tests switch between different database files).
_split_factor_cache: dict[tuple[int, str], list[tuple[str, float]]] = {}


def _split_table_ready(db2_path: str) -> bool:
    """Whether Stock_Splits (current schema) and price provenance columns exist (read-only)."""
    from src.portfolio.split_schema import _column_names
    from src.utilities.price_provenance import PRICE_PROVENANCE_COLUMNS

    conn = connect_read(db2_path)
    try:
        if "superseded_by" not in _column_names(conn, "Stock_Splits"):
            return False
        price_columns = _column_names(conn, "Stock_Prices")
        return not price_columns or set(PRICE_PROVENANCE_COLUMNS) <= price_columns
    finally:
        conn.close()


def _ensure_split_table_exists(db2_path: str) -> None:
    """Ensure the Stock_Splits table exists in db2 (idempotent).

    A read-only check first; the migration only runs when something is missing
    and never in the web server (workers create the schema at startup), where
    a ledger rebuild simply treats a missing table as "no splits".
    """
    global _split_table_checked
    db_key = str(db2_path)
    if db_key in _split_table_checked:
        return
    try:
        if _split_table_ready(db2_path):
            _split_table_checked.add(db_key)
            return
        if is_protected_database(db2_path):
            logger.warning("Stock_Splits is missing or outdated in %s; a worker creates it", db2_path)
            return
        from src.portfolio.split_schema import ensure_split_tables
        from src.utilities.price_provenance import ensure_price_provenance_columns

        migration_conn = connect_write(db2_path)
        try:
            ensure_split_tables(conn=migration_conn)
            ensure_price_provenance_columns(migration_conn, "Stock_Prices")
            migration_conn.commit()
        finally:
            migration_conn.close()
        _split_table_checked.add(db_key)
    except Exception:
        logger.warning("Could not ensure Stock_Splits table", exc_info=True)


def _load_split_factors(
    conn2: sqlite3.Connection,
    ticker: str,
) -> list[tuple[str, float]]:
    """Return [(split_date, cumulative_factor), ...] for confirmed splits.

    cumulative_factor is the multiplier to apply to a price from BEFORE that
    split date to bring it to a post-split basis.  For example, after a
    2:1 split on 2024-03-15, prices on or before 2024-03-14 need to be
    divided by 2.  The factor for those dates is 0.5.

    Returns an empty list if the ticker has no confirmed split events.  The
    caller decides whether a row needs this factor from its own
    ``price_basis``; provider-adjusted rows are never transformed a second
    time, but the events are still needed to reconstruct an as-traded ledger
    quote.
    """
    try:
        split_columns = table_columns(conn2, "Stock_Splits")
        basis_clause = "AND COALESCE(price_basis, 'raw') = 'raw'" \
            if "price_basis" in split_columns else ""
        superseded_clause = "AND COALESCE(superseded_by, 0) = 0" \
            if "superseded_by" in split_columns else ""
        method_select = "detection_method" if "detection_method" in split_columns else "NULL"
        id_select = "id" if "id" in split_columns else "NULL"
        method_order = (
            "CASE WHEN detection_method = 'provider' THEN 0 "
            "WHEN detection_method = 'manual' THEN 1 ELSE 2 END"
            if "detection_method" in split_columns else "0"
        )
        id_order = "id DESC" if "id" in split_columns else "rowid DESC"
        rows = conn2.execute(
            "SELECT split_date, ratio_from, ratio_to, "
            f"{method_select}, {id_select} FROM Stock_Splits "
            "WHERE ticker = ? AND confirmation = 'confirmed' "
            f"{basis_clause} {superseded_clause} "
            f"ORDER BY split_date ASC, {method_order}, {id_order}",
            (ticker,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    if not rows:
        return []

    seen_dates: set[str] = set()
    records: list[tuple[str, float, object]] = []
    for split_date, ratio_from, ratio_to, method, _event_id in rows:
        date_text = str(split_date)[:10]
        if date_text in seen_dates:
            continue
        seen_dates.add(date_text)
        try:
            ratio = float(ratio_to) / float(ratio_from)
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        if ratio > 0 and not is_squeeze_out(ratio_from, ratio_to):
            records.append((date_text, ratio, method))
    splits = [(date_text, ratio) for date_text, ratio, _method in distinct_split_records(records)]
    factors: list[tuple[str, float]] = []
    cumulative: float = 1.0
    for split_date, ratio in reversed(splits):
        cumulative /= ratio
        factors.append((split_date, cumulative))
    factors.reverse()
    return factors


def _get_adjusted_price(
    conn2: sqlite3.Connection,
    ticker: str,
    date_str: str,
) -> float | None:
    """Look up a ticker's **split-adjusted** price for a given date.

    Uses the same forward-fill logic as :func:`_get_price`, then applies
    cumulative split adjustments so all prices are on a current-share basis.

    The comparison uses the **actual price source date** (not the query date)
    to determine whether a split adjustment is needed.  This correctly handles
    forward-fill where the most recent price may predate a split even though
    the query date is after it.
    """
    # Do the lookup ourselves so we know the source date of the price
    columns = table_columns(conn2, "Stock_Prices")
    basis_sql = ", Price_Basis" if "Price_Basis" in columns else ""
    currency_order = ", Currency" if "Currency" in columns else ""
    row = conn2.execute(
        f"SELECT Date, Price{basis_sql} FROM Stock_Prices "
        f"WHERE Ticker = ? AND Date = ? ORDER BY Date{currency_order} LIMIT 1",
        (ticker, date_str),
    ).fetchone()
    if row:
        price_date = row[0]
        raw = row[1]
    else:
        # Forward-fill: most recent price on or before target date
        row = conn2.execute(
            f"SELECT Date, Price{basis_sql} FROM Stock_Prices "
            f"WHERE Ticker = ? AND Date <= ? ORDER BY Date DESC{currency_order} LIMIT 1",
            (ticker, date_str),
        ).fetchone()
        if row is None:
            return None
        price_date = row[0]
        raw = row[1]
    basis = (str(row[2]).strip().lower() if "Price_Basis" in columns and row[2] else "raw")
    if basis != "raw":
        # ``unknown`` is deliberately not adjusted.  Applying a split to a
        # legacy/provider-adjusted row without evidence is worse than leaving
        # the row unadjusted; callers can surface the provenance for review.
        return raw

    cache_key = (id(conn2), ticker)
    if cache_key not in _split_factor_cache:
        _split_factor_cache[cache_key] = _load_split_factors(conn2, ticker)

    factors = _split_factor_cache[cache_key]
    if not factors:
        return raw

    # Apply the cumulative factor for the most recent split whose date
    # is AFTER the actual price source date (i.e., the price is pre-split
    # and needs to be adjusted down to a post-split basis).
    for split_date, cum_factor in factors:
        if split_date > price_date:
            return raw * cum_factor

    return raw


def _invalidate_split_cache(ticker: str | None = None) -> None:
    """Clear the split-factor cache so the next lookup reloads from DB."""
    global _split_factor_cache
    if ticker:
        _split_factor_cache = {
            key: value for key, value in _split_factor_cache.items()
            if key[1] != ticker
        }
    else:
        _split_factor_cache.clear()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _today_str() -> str:
    return Date.today().isoformat()


def _parse_date(val: str | None) -> Date | None:
    """Parse YYYY-MM-DD to date, or None."""
    if not val:
        return None
    try:
        return Date.fromisoformat(val)
    except (ValueError, TypeError):
        return None


def _get_price(
    conn2: sqlite3.Connection,
    ticker: str,
    date_str: str,
) -> float | None:
    """Look up a ticker's price in Stock_Prices for a given date.

    If no exact match, falls back to the most recent available price
    on or before the target date (forward-fill / last-observation-carried-forward).
    This handles weekly data, weekends, and holidays transparently.
    """
    row = conn2.execute(
        "SELECT Price FROM Stock_Prices WHERE Ticker = ? AND Date = ?",
        (ticker, date_str),
    ).fetchone()
    if row:
        return row[0]

    # Forward-fill: most recent price on or before target date
    row = conn2.execute(
        "SELECT Price FROM Stock_Prices WHERE Ticker = ? AND Date <= ? ORDER BY Date DESC LIMIT 1",
        (ticker, date_str),
    ).fetchone()
    if row:
        return row[0]
    return None


def _get_as_traded_price(
    conn2: sqlite3.Connection,
    ticker: str,
    date_str: str,
) -> float | None:
    """Return a quote on the ledger's as-traded share basis.

    Yahoo's daily historical close is already split-adjusted.  Portfolio
    quantities are changed on the effective split date, so pre-split holdings
    need the inverse factor when an adjusted provider row is used.  Raw rows
    and rows with unknown basis are returned unchanged; unknown data is never
    silently transformed.
    """
    columns = table_columns(conn2, "Stock_Prices")
    basis_sql = ", Price_Basis" if "Price_Basis" in columns else ""
    currency_order = ", Currency" if "Currency" in columns else ""
    row = conn2.execute(
        f"SELECT Date, Price{basis_sql} FROM Stock_Prices "
        f"WHERE Ticker = ? AND Date = ? ORDER BY Date{currency_order} LIMIT 1",
        (ticker, date_str),
    ).fetchone()
    if row is None:
        row = conn2.execute(
            f"SELECT Date, Price{basis_sql} FROM Stock_Prices "
            f"WHERE Ticker = ? AND Date <= ? ORDER BY Date DESC{currency_order} LIMIT 1",
            (ticker, date_str),
        ).fetchone()
    if row is None:
        return None

    price_date = str(row[0])[:10]
    price = row[1]
    if price is None:
        return None
    basis = (
        str(row[2] or "raw").strip().lower()
        if "Price_Basis" in columns else "raw"
    )
    if basis != "adjusted":
        return price

    cache_key = (id(conn2), ticker)
    if cache_key not in _split_factor_cache:
        _split_factor_cache[cache_key] = _load_split_factors(conn2, ticker)
    for split_date, cumulative_factor in _split_factor_cache[cache_key]:
        if split_date > price_date and cumulative_factor:
            return price / cumulative_factor
    return price


def _load_confirmed_split_events(
    conn2: sqlite3.Connection,
) -> dict[str, list[tuple[str, float]]]:
    """Load confirmed split multipliers grouped by ticker.

    The portfolio ledger applies these actions to quantities/cost bases.  This
    is separate from price adjustment: a split must not be applied to both the
    number of shares and an already-adjusted quote.
    """
    columns = table_columns(conn2, "Stock_Splits")
    if not columns:
        return {}
    basis_clause = "AND COALESCE(price_basis, 'raw') = 'raw'" \
        if "price_basis" in columns else ""
    superseded_clause = "AND COALESCE(superseded_by, 0) = 0" \
        if "superseded_by" in columns else ""
    method_select = "detection_method" if "detection_method" in columns else "NULL"
    id_select = "id" if "id" in columns else "NULL"
    method_order = (
        "CASE WHEN detection_method = 'provider' THEN 0 "
        "WHEN detection_method = 'manual' THEN 1 ELSE 2 END"
        if "detection_method" in columns else "0"
    )
    id_order = "id DESC" if "id" in columns else "rowid DESC"
    try:
        rows = conn2.execute(
            "SELECT ticker, split_date, ratio_from, ratio_to, "
            f"{method_select}, {id_select} FROM Stock_Splits "
            "WHERE confirmation = 'confirmed' "
            f"{basis_clause} {superseded_clause} "
            f"ORDER BY ticker, split_date, {method_order}, {id_order}",
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    events: dict[str, list[tuple[str, float]]] = defaultdict(list)
    seen_events: set[tuple[str, str]] = set()
    for ticker, split_date, ratio_from, ratio_to, _method, _event_id in rows:
        event_key = (str(ticker), str(split_date)[:10])
        if event_key in seen_events:
            continue
        seen_events.add(event_key)
        try:
            multiplier = float(ratio_to) / float(ratio_from)
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        if not is_share_split(ratio_from, ratio_to):
            # A spinoff price adjustment, not new shares: the provider's old
            # closes are restated by it, but the ledger quantity is not.
            logger.debug("Ignoring %s:%s on %s for ledger quantities", ratio_to, ratio_from, ticker)
            continue
        if multiplier > 0:
            events[str(ticker)].append((str(split_date), multiplier))
    return events


def _apply_split_actions(
    holdings: dict[tuple[str, str], dict],
    events: dict[str, list[tuple[str, float]]],
    date_str: str,
) -> None:
    """Apply split actions effective on *date_str* to open holdings."""
    for (symbol, asset_category), holding in holdings.items():
        if holding.get("quantity", 0) == 0:
            continue
        event_rows = events.get(symbol, [])
        # Options are keyed by their contract symbol, but their split action is
        # declared against the underlying ticker.
        if not event_rows:
            event_rows = events.get(holding.get("underlying") or "", [])
        for split_date, multiplier in event_rows:
            if split_date != date_str:
                continue
            if asset_category == "OPT" or holding.get("is_option"):
                if holding.get("strike") is not None:
                    holding["strike"] = holding["strike"] / multiplier
                holding["multiplier"] = (holding.get("multiplier") or 1) * multiplier
            else:
                holding["quantity"] *= multiplier
                total_cost = holding.get("total_cost")
                if holding["quantity"] and total_cost is not None:
                    holding["avg_cost"] = total_cost / holding["quantity"]
            # A holding is priced after actions below; stale values from the
            # prior day must not leak into the split day.
            holding["market_price"] = None
            holding["market_value"] = None
            logger.info(
                "Applied %s:%s split to %s on %s",
                multiplier, 1, symbol, date_str,
            )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _alias_split_events(
    events: dict[str, list[tuple[str, float]]],
    symbols: set[str],
) -> dict[str, list[tuple[str, float]]]:
    """Key split events by the broker symbol too (``59840`` events also apply to ``5984.T``)."""
    from src.portfolio.market_data import price_ticker_candidates

    aliased = dict(events)
    for symbol in symbols:
        if not symbol or symbol in aliased:
            continue
        for candidate in price_ticker_candidates(symbol)[1:]:
            if candidate in events:
                aliased[symbol] = events[candidate]
                break
    return aliased


# A split entered by hand within this many days of one the market data has
# (with the same ratio) is the same event, and is applied once.
MANUAL_SPLIT_MATCH_DAYS = 7


def _with_manual_splits(
    events: dict[str, list[tuple[str, float]]],
    records: list[dict],
) -> tuple[dict[str, list[tuple[str, float]]], list[dict]]:
    """Add the owner's ``SPLIT`` records (factor in ``multiplier``) to the market's split events.

    Returns the merged events and the records left out because the market
    data already has that split.  Neither is ever written to market.db: a
    split entered by hand only changes its owner's ledger.
    """
    merged = {symbol: list(rows) for symbol, rows in events.items()}
    ignored: list[dict] = []
    for record in records:
        if record.get("activity_type") != "SPLIT":
            continue
        symbol = (record.get("symbol") or "").strip()
        day = _parse_date(str(record.get("trade_date") or "")[:10])
        try:
            factor = float(record.get("multiplier") or 0)
        except (TypeError, ValueError):
            continue
        if not symbol or day is None or factor <= 0 or factor == 1:
            continue
        known = any(
            abs(float(market_factor) - factor) <= 1e-6 * factor
            and (event_day := _parse_date(str(market_day)[:10])) is not None
            and abs((event_day - day).days) <= MANUAL_SPLIT_MATCH_DAYS
            for market_day, market_factor in events.get(symbol, [])
        )
        if known:
            ignored.append(record)
            continue
        merged[symbol] = [*merged.get(symbol, []), (day.isoformat(), factor)]
    return merged, ignored


def _position_split_events(
    conn3: sqlite3.Connection,
    conn2: sqlite3.Connection,
    owner_user_id: str,
    symbols: set[str],
) -> dict[str, list[tuple[str, float]]]:
    """The split events the ledger applies to *symbols*: the market's and the owner's own."""
    try:
        market_events = _alias_split_events(_load_confirmed_split_events(conn2), symbols)
    except sqlite3.Error:
        market_events = {}
    records = [
        dict(row) for row in conn3.execute(
            "SELECT activity_type, symbol, trade_date, multiplier FROM Transactions "
            "WHERE owner_user_id = ? AND activity_type = 'SPLIT'",
            (owner_user_id,),
        ).fetchall()
    ]
    return _with_manual_splits(market_events, records)[0]


def build_portfolio_state(
    db3_path: str | None = None,
    db2_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    base_currency: str = "EUR",
    owner_user_id: str = "",
) -> dict:
    """Rebuild portfolio state from scratch.

    Every calendar day from the first transaction is valued in EUR: holdings
    at their latest close (in the listing's currency, converted to the
    holding's), and holdings and per-currency cash at that day's ECB
    reference rate.  The broker's ``fxRateToBase`` is only a fallback for a
    currency without ECB data, so currency moves show up on the days they
    happen rather than when the next transaction arrives.

    Args:
        db3_path: Path to Portfolio.db (default ``get_app_db()``).
        db2_path: Path to Standardized.db (default ``get_market_db()``).
        start_date: Override earliest date (YYYY-MM-DD). None = auto.
        end_date: Override latest date (YYYY-MM-DD). None = today.
        base_currency: Kept for callers; stored values are always EUR and
            converted to a display currency when read.

    Returns:
        ``{'daily_rows': N, 'holdings_count': N}``
    """
    from src.portfolio.market_data import MarketData

    db3_path = db3_path or get_app_db()
    db2_path = db2_path or get_market_db()

    create_tables(db3_path)

    conn3 = connect_write(db3_path)
    # Ensure Stock_Splits table exists for split-adjusted price lookups
    _ensure_split_table_exists(db2_path)
    conn2 = connect_read(db2_path)

    try:
        conn3.execute("BEGIN IMMEDIATE")
        # 0. Clear previous rebuild state for this owner only
        conn3.execute("DELETE FROM Holdings_History WHERE owner_user_id = ?", (owner_user_id,))
        conn3.execute("DELETE FROM Portfolio_Daily WHERE owner_user_id = ?", (owner_user_id,))
        conn3.execute("DELETE FROM Portfolio_Holdings WHERE owner_user_id = ?", (owner_user_id,))

        # 1. Load transactions sorted by date, scoped to owner
        rows = conn3.execute(
            "SELECT * FROM Transactions WHERE owner_user_id = ? ORDER BY trade_date, id",
            (owner_user_id,),
        ).fetchall()
        transactions = [dict(r) for r in rows]
        account_currency = _account_currency(transactions)

        if not transactions:
            logger.info("No transactions found — nothing to build")
            conn3.commit()
            return {"daily_rows": 0, "holdings_count": 0}

        # Determine date range
        all_dates = sorted(
            _parse_date(t["trade_date"])
            for t in transactions
            if _parse_date(t["trade_date"])
        )
        if not all_dates:
            conn3.commit()
            return {"daily_rows": 0, "holdings_count": 0}

        first_date = all_dates[0]
        last_date = _parse_date(end_date) or Date.today()

        # 2. Walk forward.  Prices entered by hand belong to this owner only:
        # they value this ledger and never reach the shared market data.
        from src.portfolio.assets import manual_only_symbols, manual_prices

        split_events, _known_splits = _with_manual_splits(
            _alias_split_events(
                _load_confirmed_split_events(conn2),
                {(t.get("symbol") or "").strip() for t in transactions},
            ),
            transactions,
        )
        market = MarketData(
            conn2,
            manual_prices=manual_prices(conn3, owner_user_id),
            manual_only=manual_only_symbols(conn3, owner_user_id),
            splits=split_events,
        )
        holdings: dict[tuple[str, str], dict] = {}  # (symbol, asset_category) → holding dict
        cash_by_currency: dict[str, float] = {"EUR": 0.0}
        broker_fx: dict[str, float] = {}  # currency → latest fxRateToBase (fallback only)
        cumulative_return = 1.0
        prev_total_value = 0.0
        txn_index = 0
        daily_rows = 0
        hh_rows = 0

        def eur_per_unit(currency: str, day: str) -> float:
            per_eur = market.per_eur(currency or "EUR", day)
            if per_eur:
                return 1.0 / per_eur
            return broker_fx.get(currency, 1.0)

        import json as _json

        current_date = first_date
        while current_date <= last_date:
            date_str = current_date.isoformat()
            flows = {"inflow": 0.0, "income": 0.0}

            # Apply corporate actions before trades and valuation for the
            # effective date. Quantities/cost bases then match as-traded
            # prices without double-adjusting either side.
            _apply_split_actions(holdings, split_events, date_str)

            # --- Apply all transactions for this day ---
            while txn_index < len(transactions):
                txn = transactions[txn_index]
                txn_date = _parse_date(txn["trade_date"])
                if txn_date is None or txn_date > current_date:
                    break
                if txn_date < current_date:
                    txn_index += 1
                    continue
                _apply_transaction(
                    txn, holdings, cash_by_currency, flows, broker_fx,
                    lambda amount, currency, day=date_str: amount * eur_per_unit(currency, day),
                    account_currency,
                )
                txn_index += 1

            # --- Price current holdings and value them in EUR ---
            stock_value_base = 0.0
            option_value_base = 0.0
            for h in holdings.values():
                if h["quantity"] == 0:
                    continue
                priced = _price_holding(h, date_str, market)
                if priced is not None:
                    price, price_date, source = priced
                    h["market_price"] = price
                    h["price_date"] = price_date
                    h["price_source"] = source
                    multiplier = h.get("multiplier", 1) or 1
                    # Negative for a short position: what buying it back would cost.
                    h["market_value"] = price * h["quantity"] * multiplier
                h["fx_rate"] = eur_per_unit(h["currency"], date_str)
                if h.get("market_value"):
                    value_base = h["market_value"] * h["fx_rate"]
                    if h["is_option"]:
                        option_value_base += value_base
                    else:
                        stock_value_base += value_base

            cash_value = sum(
                amount * eur_per_unit(currency, date_str)
                for currency, amount in cash_by_currency.items()
            )
            total_value = cash_value + stock_value_base + option_value_base
            daily_inflow = flows["inflow"]

            # Daily return with external flows at the start of the day
            # (Modified Dietz for one day).
            if prev_total_value > 0:
                denom = prev_total_value + daily_inflow
                if abs(denom) > 0.01:  # avoid division by near-zero
                    dr_raw = (total_value - prev_total_value - daily_inflow) / denom
                    # Cap at ±100% to prevent single bad-data day from
                    # permanently destroying cumulative return
                    daily_return = max(min(dr_raw, 1.0), -1.0)
                else:
                    daily_return = 0.0
                cumulative_return *= (1 + daily_return)
            else:
                daily_return = 0.0
                # cumulative_return stays at 1.0 until first real data point

            conn3.execute(
                """INSERT OR REPLACE INTO Portfolio_Daily
                   (date, owner_user_id, total_value, cash_balance, stock_value, option_value,
                    daily_return, cumulative_return, dividend_income, net_inflow,
                    cash_ccy_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (date_str, owner_user_id, total_value, cash_value, stock_value_base,
                 option_value_base, daily_return,
                 cumulative_return - 1, flows["income"], daily_inflow,
                 _json.dumps(cash_by_currency)),
            )
            daily_rows += 1

            # Store Holdings_History — store both native and base-currency values
            for _key, h in holdings.items():
                if h["quantity"] != 0:
                    mv_native = h.get("market_value")
                    cur_rate = h.get("fx_rate") or eur_per_unit(h["currency"], date_str)
                    mv_base = mv_native * cur_rate if mv_native is not None else None
                    conn3.execute(
                        """INSERT OR REPLACE INTO Holdings_History
                           (date, symbol, asset_category, owner_user_id, quantity, market_price,
                            market_value, market_value_native, currency, fx_rate,
                            is_option, strike, expiry, put_call, underlying,
                            price_date, price_source)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (date_str, h["symbol"], h["asset_category"], owner_user_id,
                         h["quantity"], h.get("market_price"),
                         mv_base, mv_native, h["currency"],
                         cur_rate,
                         1 if h["is_option"] else 0,
                         h.get("strike"), h.get("expiry"),
                         h.get("put_call"), h.get("underlying"),
                         h.get("price_date"), h.get("price_source")),
                    )
                    hh_rows += 1

            prev_total_value = total_value
            current_date += timedelta(days=1)

        # --- Store current holdings ---
        # Filter out expired options (expiry < today).
        for h in holdings.values():
            if abs(h["quantity"]) <= 0:
                continue
            # Skip expired options: expiry date is in the past
            if h["is_option"] and h.get("expiry"):
                exp_date = _parse_date(h["expiry"])
                if exp_date and exp_date < Date.today():
                    logger.debug("Skipping expired option: %s expiry %s",
                                 h["symbol"], h["expiry"])
                    continue
            mv_native = h.get("market_value")
            cur_rate = h.get("fx_rate") or 1.0
            mv_base = mv_native * cur_rate if mv_native is not None else None
            conn3.execute(
                """INSERT OR REPLACE INTO Portfolio_Holdings
                   (symbol, asset_category, owner_user_id, quantity, avg_cost, market_price,
                    market_value, market_value_native, currency, fx_rate,
                    is_option, strike, expiry, put_call, underlying,
                    price_date, price_source, price_ticker, price_currency)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (h["symbol"], h["asset_category"], owner_user_id, h["quantity"],
                 h.get("avg_cost"), h.get("market_price"),
                 mv_base, mv_native, h["currency"],
                 cur_rate,
                 1 if h["is_option"] else 0,
                 h.get("strike"), h.get("expiry"),
                 h.get("put_call"), h.get("underlying"),
                 h.get("price_date"), h.get("price_source"),
                 h.get("price_ticker"), h.get("price_currency")),
            )

        conn3.commit()
        # Count actual current holdings (excluding expired options)
        active_count = len([h for h in holdings.values()
                          if abs(h["quantity"]) > 0
                          and not (h["is_option"] and h.get("expiry")
                                   and _parse_date(h["expiry"])
                                   and _parse_date(h["expiry"]) < Date.today())])
        logger.info("Portfolio state built: %d daily rows, %d holdings",
                     daily_rows, active_count)
        return {"daily_rows": daily_rows, "holdings_count": active_count}

    except Exception:
        conn3.rollback()
        raise
    finally:
        conn3.close()
        conn2.close()


def _account_currency(transactions: list[dict]) -> str:
    """The account's base currency: the one the broker converts at exactly 1."""
    counts: dict[str, int] = {}
    for txn in transactions:
        currency = (txn.get("currency") or "").strip()
        if currency and txn.get("fx_rate_to_base") == 1:
            counts[currency] = counts.get(currency, 0) + 1
    return max(counts, key=counts.__getitem__) if counts else "EUR"


def _apply_transaction(
    txn: dict,
    holdings: dict[tuple[str, str], dict],
    cash: dict[str, float],
    flows: dict[str, float],
    broker_fx: dict[str, float],
    to_eur,
    account_currency: str = "EUR",
) -> None:
    """Apply one ledger record to holdings, per-currency cash, and the day's flows.

    *flows* collects the day's external flows (``inflow``: deposits less
    withdrawals, in EUR at the day's ECB rate via *to_eur(amount, currency)*,
    the rate the cash itself is valued at) and net dividend income
    (``income``).  Cash is kept per currency and valued daily.
    """
    def _add_cash(ccy: str, amount: float) -> None:
        cash[ccy] = cash.get(ccy, 0.0) + amount

    activity = txn["activity_type"]
    symbol = (txn.get("symbol") or "").strip()
    asset_cat = (txn.get("asset_category") or "STK").strip()
    currency = txn.get("currency", "") or "EUR"
    qty = txn.get("quantity") or 0
    amount = txn.get("amount") or 0
    commission = txn.get("commission") or 0
    trade_price = txn.get("trade_price")

    # The broker's rate is the fallback for currencies without ECB data.
    if txn.get("fx_rate_to_base"):
        broker_fx[currency] = txn["fx_rate_to_base"]

    key = (symbol, asset_cat)

    if activity == "TRADE":
        if not key[0]:
            return
        is_forex = asset_cat == "CASH"
        if is_forex:
            # Forex trades (e.g. EUR.USD): net_cash is always 0 — the trade
            # exchanges currencies. Parse the pair to adjust both sides.
            sym = symbol  # e.g. "EUR.USD", "USD.JPY"
            if "." in sym:
                base_ccy, quote_ccy = sym.split(".", 1)
                base_qty = qty                  # e.g. -97 for SELL EUR
                quote_amount = txn.get("trade_money") or 0  # e.g. -117.92
                # Adjust both legs
                _add_cash(base_ccy.strip(), base_qty)
                _add_cash(quote_ccy.strip(), -quote_amount)  # opposite sign
            # The commission is charged separately, usually in the account's
            # base currency; imports before it was recorded fall back to that.
            if commission:
                _add_cash((txn.get("commission_currency") or account_currency).strip(), commission)
            return

        is_option = asset_cat == "OPT"
        multiplier = txn.get("multiplier") or 1

        if key not in holdings:
            holdings[key] = {
                "symbol": symbol,
                "asset_category": asset_cat,
                "quantity": 0,
                "total_cost": 0.0,
                "avg_cost": None,
                "market_price": None,
                "market_value": None,
                "currency": currency,
                "is_option": is_option,
                "strike": txn.get("strike"),
                "expiry": txn.get("expiry"),
                "put_call": txn.get("put_call"),
                "underlying": txn.get("underlying_symbol"),
                "multiplier": multiplier,
            }

        h = holdings[key]
        held = h["quantity"]
        if txn.get("buy_sell") == "BUY":
            bought = qty
            if held < 0 and bought > 0:
                # Buying back a short position first; the rest opens a long one.
                covered = min(bought, -held)
                h["total_cost"] -= h["total_cost"] * covered / -held
                h["quantity"] += covered
                bought -= covered
                if abs(h["quantity"]) < 1e-9:
                    h["quantity"] = 0
                    h["total_cost"] = 0.0
            if bought or held >= 0:
                h["quantity"] += bought
                # Cost basis in the holding's currency, commission included.
                if trade_price and bought > 0:
                    h["total_cost"] += bought * trade_price * multiplier + abs(commission) * bought / qty
        else:  # SELL (and the broker's cancellation of a buy)
            sold = abs(qty)
            # A sale stops at zero unless it is marked as opening a short
            # (or the position is short already): a file that starts after
            # shares were bought must not invent a short position.
            may_short = "O" in (txn.get("open_close") or "") or held < 0
            if held > 0:
                closing = min(sold, held)
                if h["total_cost"]:
                    h["total_cost"] -= h["total_cost"] * closing / held
                h["quantity"] -= closing
                sold -= closing
            if sold > 0 and may_short:
                h["quantity"] -= sold
                # A short's cost is what its sale brought in, as a negative amount.
                if trade_price:
                    h["total_cost"] -= sold * trade_price * multiplier - abs(commission) * sold / abs(qty)
            if h["quantity"] == 0 or (h["quantity"] < 0 and not may_short):
                h["quantity"] = 0
                h["total_cost"] = 0

        if h["quantity"] != 0 and h["total_cost"]:
            h["avg_cost"] = h["total_cost"] / h["quantity"]

        # Cash effect
        net_cash = txn.get("net_cash") or 0
        _add_cash(currency, net_cash)

    elif activity in ("DIVIDEND", "PIL_DIVIDEND", "WITHHOLDING_TAX"):
        # Withholding tax is netted against the gross dividend.  Income is
        # converted at the broker's booking rate, matching its statements.
        _add_cash(currency, amount)
        booked = txn.get("fx_rate_to_base")
        flows["income"] += amount * booked if booked else to_eur(amount, currency)

    elif activity == "DEPOSIT_WITHDRAWAL":
        _add_cash(currency, amount)
        flows["inflow"] += to_eur(amount, currency)

    elif activity in ("BROKER_INTEREST", "BOND_INTEREST", "OTHER_FEE", "COMMISSION_ADJ", "OTHER_CASH"):
        _add_cash(currency, amount)

    elif activity == "SPINOFF":
        if not symbol:
            return
        if key not in holdings:
            holdings[key] = {
                "symbol": symbol,
                "asset_category": asset_cat,
                "quantity": 0,
                "total_cost": 0.0,
                "avg_cost": 0.0,
                "market_price": None,
                "market_value": None,
                "currency": currency,
                "is_option": False,
                "strike": None,
                "expiry": None,
                "put_call": None,
                "underlying": None,
                "multiplier": 1,
            }
        holdings[key]["quantity"] += qty
        # Spinoff shares have zero cost basis
        holdings[key]["avg_cost"] = 0.0


def _price_holding(
    h: dict,
    date_str: str,
    market,
) -> tuple[float, str | None, str] | None:
    """Price one holding on *date_str*: ``(price, price date, source)`` or None.

    Stocks use the latest close on or before the date, as traded and in the
    holding's currency (``source`` ``market``, or ``manual`` when the latest
    price is one the owner entered).  Split actions are applied to
    holdings above, so a split-adjusted quote would double-adjust the
    position.  Before a ticker has any stored price the average cost stands
    in (``cost``).  Options are valued with a binomial model on the
    underlying's close (``model``), unless the owner entered a price for the
    contract at least as recent as that close (``manual``).
    """
    if h.get("is_option"):
        symbol = h["symbol"]
        underlying = h.get("underlying") or (symbol[:symbol.index(" ")] if " " in symbol else symbol)
        quote = market.price(underlying, h["currency"], date_str)
        # Option contracts have no market closes here: a series for the
        # contract holds only prices entered by hand.
        entered = market.price(symbol, h["currency"], date_str) if symbol != underlying else None
        if entered is not None and entered[2].source_on(date_str) == "manual" and (quote is None or entered[1] >= quote[1]):
            return entered[0], entered[1], "manual"
        if quote is None:
            return None
        S, price_date, _series = quote
        K = h.get("strike") or 0
        if K == 0:
            return None
        expiry = _parse_date(h.get("expiry"))
        if expiry is None:
            return 0.01, price_date, "model"  # very short time → minimal value
        T = max((expiry - Date.fromisoformat(date_str)).days / 365.0, 0.0)
        if T <= 0:
            return 0.0, price_date, "model"
        opt_type = "put" if h.get("put_call") == "P" else "call"
        return _op.binomial_tree(opt_type, S, K, T, 0.05, 0.20), price_date, "model"

    quote = market.price(h["symbol"], h["currency"], date_str)
    if quote is not None:
        price, price_date, series = quote
        h["price_ticker"] = series.ticker
        h["price_currency"] = series.source_currency
        return price, price_date, series.source_on(date_str)
    # Fall back to average cost if no market price available
    # (common for recently purchased positions where price hasn't been fetched yet)
    avg = h.get("avg_cost")
    if avg is not None and avg > 0:
        return avg, None, "cost"
    return None


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_daily_values(
    db3_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    owner_user_id: str = "",
) -> list[dict]:
    """Return daily portfolio value series."""
    db3_path = db3_path or get_app_db()
    conn = connect_read(db3_path)
    where = ["owner_user_id = ?"]
    params = [owner_user_id]
    if start_date:
        where.append("date >= ?")
        params.append(start_date)
    if end_date:
        where.append("date <= ?")
        params.append(end_date)
    sql = "SELECT * FROM Portfolio_Daily"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY date"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_current_holdings(
    db3_path: str | None = None,
    owner_user_id: str = "",
    db2_path: str | None = None,
) -> list[dict]:
    """Return current holdings snapshot with cash balance.

    With *db2_path*, cash in each currency is valued at the ECB reference
    rate on the valuation date; otherwise at the rate of a holding in the
    same currency.
    """
    db3_path = db3_path or get_app_db()
    conn = connect_read(db3_path)
    today = Date.today().isoformat()

    rows = conn.execute(
        """SELECT * FROM Portfolio_Holdings
           WHERE owner_user_id = ?
             AND ((is_option = 0)
              OR (is_option = 1 AND (expiry IS NULL OR expiry >= ?)))
           ORDER BY COALESCE(market_value, 0) DESC""",
        (owner_user_id, today),
    ).fetchall()

    result = [dict(r) for r in rows]

    # Add per-currency cash balances from Portfolio_Daily (latest row)
    cash_row = conn.execute(
        "SELECT date, cash_balance, cash_ccy_json, total_value FROM Portfolio_Daily WHERE owner_user_id = ? ORDER BY date DESC LIMIT 1",
        (owner_user_id,),
    ).fetchone()
    conn.close()
    market = None
    if db2_path and cash_row:
        from src.portfolio.market_data import MarketData

        market_conn = connect_read(db2_path)
        market = MarketData(market_conn)

    if cash_row:
        total_val = cash_row["total_value"] or 0
        ccy_json = cash_row["cash_ccy_json"] or "{}"
        try:
            import json
            ccy_map = json.loads(ccy_json) if isinstance(ccy_json, str) else (ccy_json or {})
        except (json.JSONDecodeError, TypeError):
            ccy_map = {}

        if ccy_map:
            for ccy, amount in ccy_map.items():
                if abs(amount) < 0.001:
                    continue
                # EUR per unit: the ECB rate on the valuation date, else a
                # holding's rate in the same currency.
                cur_fx = None
                if market is not None:
                    per_eur = market.per_eur(ccy, cash_row["date"])
                    cur_fx = 1.0 / per_eur if per_eur else None
                for r in result:
                    if cur_fx is None and r.get("currency") == ccy and r.get("fx_rate"):
                        cur_fx = r["fx_rate"]
                        break
                fx = cur_fx or 1.0
                result.append({
                    "symbol": f"CASH {ccy}",
                    "asset_category": "CASH",
                    "quantity": amount,
                    "avg_cost": None,
                    "market_price": None,
                    "market_value": amount * fx,
                    "market_value_native": amount,
                    "currency": ccy,
                    "fx_rate": fx,
                    "weight": round(abs(amount * fx) / abs(total_val) * 100, 2) if total_val else None,
                    "is_option": False,
                    "strike": None,
                    "expiry": None,
                    "put_call": None,
                    "underlying": None,
                })
    if market is not None:
        market.conn.close()
    return result


def get_holdings_at_date(
    db3_path: str | None = None,
    date: str | None = None,
    owner_user_id: str = "",
) -> list[dict]:
    """Return holdings snapshot at a specific date."""
    db3_path = db3_path or get_app_db()
    create_tables(db3_path)
    conn = connect_read(db3_path)
    rows = conn.execute("""
        SELECT h.* FROM Holdings_History h
        INNER JOIN (
            SELECT symbol, asset_category, MAX(date) AS max_date
            FROM Holdings_History
            WHERE date <= ? AND owner_user_id = ?
            GROUP BY symbol, asset_category
        ) latest ON h.symbol = latest.symbol
                  AND h.asset_category = latest.asset_category
                  AND h.date = latest.max_date
        WHERE h.quantity != 0 AND h.owner_user_id = ?
    """, (date, owner_user_id, owner_user_id)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_closed_positions(db3_path: str | None = None, owner_user_id: str = "") -> list[dict]:
    """Return positions that were fully closed (sold/expired) for one user."""
    db3_path = db3_path or get_app_db()
    create_tables(db3_path)
    conn = connect_read(db3_path)
    try:
        cur_syms = {r["symbol"] for r in conn.execute(
            "SELECT DISTINCT symbol FROM Portfolio_Holdings WHERE owner_user_id = ?",
            (owner_user_id,),
        ).fetchall()}
        closed_syms = [r["symbol"] for r in conn.execute(
            "SELECT DISTINCT symbol, asset_category FROM Transactions WHERE activity_type = 'TRADE' AND owner_user_id = ?",
            (owner_user_id,),
        ).fetchall() if r["symbol"] not in cur_syms and r["asset_category"] != 'CASH']

        if not closed_syms:
            return []

        placeholders = ",".join("?" for _ in closed_syms)
        rows = conn.execute(f"""
            SELECT
                symbol,
                asset_category,
                description,
                currency,
                SUM(CASE WHEN buy_sell = 'BUY' THEN ABS(quantity) ELSE 0 END) AS total_bought,
                SUM(CASE WHEN buy_sell = 'SELL' THEN ABS(quantity) ELSE 0 END) AS total_sold,
                SUM(CASE WHEN buy_sell = 'SELL' THEN proceeds ELSE 0 END) AS total_proceeds,
                SUM(CASE WHEN buy_sell = 'BUY' THEN ABS(trade_money) ELSE 0 END) AS total_cost,
                MAX(trade_date) AS last_trade_date,
                MIN(trade_date) AS first_trade_date
            FROM Transactions
            WHERE symbol IN ({placeholders})
              AND activity_type = 'TRADE'
              AND owner_user_id = ?
            GROUP BY symbol, asset_category
        """, [*closed_syms, owner_user_id]).fetchall()

        from src.portfolio.assets import security_names

        names = security_names(conn, owner_user_id)
        result = []
        for row in rows:
            r = dict(row)
            realized_pnl = r["total_proceeds"] - r["total_cost"]
            result.append({
                "symbol": r["symbol"],
                "asset_category": r["asset_category"],
                "description": names.get(r["symbol"]) or r["description"],
                "currency": r["currency"],
                "total_bought": round(r["total_bought"] or 0, 6),
                "total_sold": round(r["total_sold"] or 0, 6),
                "realized_pnl": round(realized_pnl, 2),
                "total_cost": round(r["total_cost"] or 0, 2),
                "total_proceeds": round(r["total_proceeds"] or 0, 2),
                "first_trade_date": r["first_trade_date"],
                "last_trade_date": r["last_trade_date"],
            })
        return result
    finally:
        conn.close()


def _lookup_industry(symbol: str, db2_path: str) -> str | None:
    """Look up the industry for a ticker from CompanyInfo in db2."""
    clean = str(symbol).strip()
    candidates = [clean]
    if clean.endswith('.T') or clean.endswith('.JP'):
        base = clean.rsplit('.', 1)[0]
        if len(base) <= 4 and base.isdigit():
            candidates.append(base + '0')
        candidates.append(base)
    elif len(clean) == 5 and clean.isdigit():
        candidates.append(clean[:4])
    conn = connect_read(db2_path)
    try:
        for cand in candidates:
            row = conn.execute(
                "SELECT Company_Industry FROM CompanyInfo WHERE Company_Ticker = ? LIMIT 1",
                (cand,),
            ).fetchone()
            if row and row[0]:
                return row[0]
    finally:
        conn.close()
    return None


def _compute_holding_periods(
    hist_records: list[dict],
) -> dict:
    """Compute longest/latest holding period and count of holding periods.

    ``hist_records`` must be a list of dicts with a ``"date"`` key, sorted
    chronologically.  A new holding period begins whenever there is a gap of
    more than 1 calendar day between consecutive records (Holdings_History
    only stores rows when ``quantity > 0``, so gaps mean the position was
    closed and later re-opened).
    """
    if not hist_records:
        return {
            "longest_holding_days": 0,
            "latest_holding_days": 0,
            "num_holding_periods": 0,
            "held_since": None,
            "held_until": None,
        }

    from datetime import date as _date

    dates = [_date.fromisoformat(r["date"]) for r in hist_records]
    if len(dates) == 1:
        return {
            "longest_holding_days": 1,
            "latest_holding_days": 1,
            "num_holding_periods": 1,
            "held_since": dates[0].isoformat(),
            "held_until": dates[0].isoformat(),
        }

    streaks: list[int] = []
    streak_start = dates[0]
    prev = dates[0]

    for d in dates[1:]:
        if (d - prev).days > 1:
            # Gap > 1 day → previous streak ends
            streaks.append((prev - streak_start).days + 1)
            streak_start = d
        prev = d

    # Close the final streak
    streaks.append((prev - streak_start).days + 1)

    return {
        "longest_holding_days": max(streaks),
        "latest_holding_days": streaks[-1],
        "num_holding_periods": len(streaks),
        # The latest continuous holding period: a full sale and later
        # repurchase starts a new one.
        "held_since": streak_start.isoformat(),
        "held_until": prev.isoformat(),
    }


def _compute_position_stats(
    txns: list[dict],
    display_currency: str = "EUR",
    db2_path: str | None = None,
    split_events: list[tuple[str, float]] | None = None,
) -> dict:
    """Walk a position's records chronologically with average-cost accounting.

    Tracks the position (negative while short), its cost basis, and realized
    P&L across buy/sell periods, so a sale and later repurchase start a new
    period.  A sale stops at zero unless it is marked as opening a short
    (``open_close`` ``O``) or the position is short already, as in the
    ledger; a short's cost basis is negative (what its sale brought in) and
    buying it back realizes the difference.  Commissions count in the cost of
    a purchase and reduce a sale's proceeds.  ``SPINOFF`` records deliver
    shares at no cost, and *split_events* (``[(date, factor), ...]``) change
    the share count at the start of their day.

    Returns:
        cost_basis_native, cost_basis_display, cost_basis_eur,
        realized_pnl_native, realized_pnl_display, realized_pnl_eur,
        current_period_start, first_trade_date, last_trade_date,
        shares_bought_total, shares_sold_total, currency, current_shares
    """
    from src.portfolio.currency import get_rate_at_date_any

    shares = 0.0
    cost = {"native": 0.0, "display": 0.0, "eur": 0.0}
    realized = {"native": 0.0, "display": 0.0, "eur": 0.0}
    current_period_start: str | None = None
    first_trade_date: str | None = None
    last_trade_date: str | None = None
    shares_bought_total = 0.0
    shares_sold_total = 0.0
    currency: str = ""

    def amounts(native: float, td: str, fx: float) -> dict[str, float]:
        if display_currency != currency:
            rate = get_rate_at_date_any(currency, display_currency, td, db2_path)
            display = native * (rate if rate else fx)
        else:
            display = native
        return {"native": native, "display": display, "eur": native * fx}

    def close_out() -> None:
        nonlocal shares, current_period_start
        shares = 0.0
        for key in cost:
            cost[key] = 0.0
        current_period_start = None  # set again when a new position opens

    # Splits take effect at the start of their day, before its trades.
    events: list[tuple[str, int, int, object]] = [
        (str(t["trade_date"])[:10], 1, index, t) for index, t in enumerate(txns)
    ]
    events += [(str(day)[:10], 0, index, factor) for index, (day, factor) in enumerate(split_events or [])]
    events.sort(key=lambda event: event[:3])

    for td, kind, _index, payload in events:
        if kind == 0:
            if shares:
                shares *= float(payload)
            continue
        t = payload
        if first_trade_date is None or td < first_trade_date:
            first_trade_date = td
        if last_trade_date is None or td > last_trade_date:
            last_trade_date = td

        cur = t.get("currency", "")
        if cur:
            currency = cur
        if t.get("activity_type") == "SPINOFF":
            delivered = t.get("quantity") or 0
            if delivered and shares == 0:
                current_period_start = td
            shares += delivered
            continue

        bs = t.get("buy_sell", "")
        qty = abs(t["quantity"] or 0)
        if qty == 0:
            continue
        trade_money = t["trade_money"] or 0
        commission = t["commission"] or 0
        fx = t["fx_rate_to_base"] or 1.0

        if bs == "BUY":
            paid = amounts(abs(trade_money) + abs(commission), td, fx)
            bought = qty
            if shares < 0:
                # Buying back a short position first.
                covered = min(qty, -shares)
                for key in cost:
                    basis = cost[key] * covered / -shares
                    realized[key] += -basis - paid[key] * covered / qty
                    cost[key] -= basis
                shares += covered
                bought -= covered
                if abs(shares) < 1e-9:
                    close_out()
            if bought > 0:
                if shares == 0:
                    current_period_start = td
                shares += bought
                for key in cost:
                    cost[key] += paid[key] * bought / qty
            shares_bought_total += qty

        elif bs == "SELL":
            received = amounts(abs(trade_money) - abs(commission), td, fx)
            may_short = "O" in (t.get("open_close") or "") or shares < 0
            closing = min(qty, shares) if shares > 0 else 0.0
            if closing > 0:
                # Without a short, the whole sale's proceeds count against the
                # shares held (a file that starts after they were bought).
                share_of_sale = closing / qty if may_short else 1.0
                for key in cost:
                    basis = cost[key] * closing / shares
                    realized[key] += received[key] * share_of_sale - basis
                    cost[key] -= basis
                shares -= closing
                shares_sold_total += closing
                if abs(shares) < 1e-9:
                    close_out()
            opened = qty - closing
            if opened > 1e-9 and may_short:
                if shares == 0:
                    current_period_start = td
                shares -= opened
                for key in cost:
                    cost[key] -= received[key] * opened / qty
                shares_sold_total += opened

        elif bs == "BUY (Ca.)":
            # Cancellation: reverse a previous BUY — negative trade_money
            # means it reduces shares and cost basis
            shares -= qty
            shares_bought_total -= qty
            reversed_cost = amounts(trade_money + commission, td, fx)  # may be negative
            for key in cost:
                cost[key] += reversed_cost[key]
            if shares < 0:
                shares = 0.0

    return {
        "cost_basis_native": cost["native"],
        "cost_basis_display": cost["display"],
        "cost_basis_eur": cost["eur"],
        "realized_pnl_native": realized["native"],
        "realized_pnl_display": realized["display"],
        "realized_pnl_eur": realized["eur"],
        "current_period_start": current_period_start or first_trade_date,
        "first_trade_date": first_trade_date or "",
        "last_trade_date": last_trade_date or "",
        "shares_bought_total": shares_bought_total,
        "shares_sold_total": shares_sold_total,
        "currency": currency,
        # Negative while short.
        "current_shares": shares,
    }


def _position_return(value: float, cost_basis: float) -> float:
    """Gain over cost: ``value / cost - 1`` for a long position, the same on the sale proceeds for a short one."""
    if not cost_basis or not value:
        return 0.0
    return (value - cost_basis) / abs(cost_basis)


def get_holding_performance(
    symbol: str,
    db3_path: str | None = None,
    db2_path: str | None = None,
    display_currency: str = "EUR",
    owner_user_id: str = "",
) -> dict | None:
    """Compute performance metrics for a single holding.

    All monetary values are returned in BOTH native currency and the
    requested *display_currency*.  Properly handles sell-and-rebuy
    scenarios via ``_compute_position_stats``.
    """
    import numpy as np

    from src.portfolio.currency import get_rate_at_date_any
    db3_path = db3_path or get_app_db()
    db2_path = db2_path or get_market_db()
    conn = connect_read(db3_path)

    txns = conn.execute(
        "SELECT * FROM Transactions WHERE symbol = ? AND activity_type IN ('TRADE', 'SPINOFF') "
        "AND owner_user_id = ? ORDER BY trade_date",
        (symbol, owner_user_id),
    ).fetchall()
    if not txns:
        conn.close()
        return None

    conn2 = connect_read(db2_path)
    try:
        splits = _position_split_events(conn, conn2, owner_user_id, {symbol}).get(symbol, [])
    finally:
        conn2.close()
    stats = _compute_position_stats(
        [dict(t) for t in txns], display_currency, db2_path, splits,
    )
    currency = stats["currency"]
    current_qty = stats["current_shares"]
    cost_basis_native = stats["cost_basis_native"]
    cost_basis_display = stats["cost_basis_display"]
    cost_basis_eur = stats["cost_basis_eur"]
    realized_pnl_native = stats["realized_pnl_native"]
    realized_pnl_display = stats["realized_pnl_display"]

    # Use Portfolio_Holdings for current market values
    holding = conn.execute(
        "SELECT * FROM Portfolio_Holdings WHERE symbol = ? AND owner_user_id = ?", (symbol, owner_user_id),
    ).fetchone()

    avg_cost = holding["avg_cost"] if holding and holding["avg_cost"] else 0
    current_price = holding["market_price"] if holding and holding["market_price"] else avg_cost
    current_value_eur = holding["market_value"] if holding and holding["market_value"] else (current_qty * current_price)
    current_value_native = holding["market_value_native"] if holding and holding["market_value_native"] is not None else current_qty * current_price

    # P&L (Nat.) — unrealized only
    pnl_native = current_value_native - cost_basis_native if current_qty != 0 else 0.0

    # P&L (Display)
    if display_currency.upper() != "EUR":
        from datetime import date as _date
        today_str = _date.today().isoformat()
        if display_currency.upper() != currency.upper():
            rate_now = get_rate_at_date_any(currency, display_currency, today_str, db2_path)
            current_value_display = round(current_value_native * rate_now, 2) if rate_now and current_value_native else 0
        else:
            current_value_display = current_value_native
        pnl_display = current_value_display - cost_basis_display if current_value_display else 0
    else:
        current_value_display = current_value_eur
        pnl_display = current_value_eur - cost_basis_eur if current_value_eur else 0

    # ── Dividends ──
    div_rows = conn.execute(
        "SELECT activity_type, amount, fx_rate_to_base, trade_date FROM Transactions "
        "WHERE symbol = ? AND activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX') "
        "AND owner_user_id = ?",
        (symbol, owner_user_id),
    ).fetchall()
    div_gross_native = sum(
        abs(r["amount"] or 0) for r in div_rows if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND")
    )
    div_tax_native = sum(
        abs(r["amount"] or 0) for r in div_rows if r["activity_type"] == "WITHHOLDING_TAX"
    )
    div_net_native = div_gross_native - div_tax_native
    div_gross_eur = sum(
        abs(r["amount"] or 0) * (r["fx_rate_to_base"] or 1)
        for r in div_rows if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND")
    )
    div_tax_eur = sum(
        abs(r["amount"] or 0) * (r["fx_rate_to_base"] or 1)
        for r in div_rows if r["activity_type"] == "WITHHOLDING_TAX"
    )

    div_net_display = 0.0
    if display_currency.upper() != currency.upper() and div_rows:
        for r in div_rows:
            amt = abs(r["amount"] or 0)
            sign = 1 if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND") else -1
            dt = r["trade_date"]
            rate = get_rate_at_date_any(currency, display_currency, dt, db2_path)
            div_net_display += amt * sign * (rate if rate else (r["fx_rate_to_base"] or 1))
    elif display_currency.upper() == currency.upper():
        div_net_display = div_net_native
    else:
        div_net_display = div_gross_eur - div_tax_eur

    # ── Returns from Holdings_History ──
    hist = conn.execute(
        "SELECT date, market_value, market_value_native FROM Holdings_History WHERE symbol = ? AND owner_user_id = ? ORDER BY date",
        (symbol, owner_user_id),
    ).fetchall()

    from src.portfolio.assets import security_names

    asset_name = security_names(conn, owner_user_id).get(symbol)
    conn.close()

    values = [h["market_value"] or 0 for h in hist]
    first_val = next((i for i, v in enumerate(values) if v > 0), None)
    daily_returns: list[float] = []
    if first_val is not None:
        for i in range(first_val + 1, len(values)):
            if values[i-1] > 0 and values[i] > 0:
                daily_returns.append(values[i] / values[i-1] - 1)

    # ── Total Return ──
    total_return = _position_return(current_value_eur, cost_basis_eur)
    total_return_native = _position_return(current_value_native, cost_basis_native)
    total_return_display = _position_return(current_value_display, cost_basis_display)

    volatility = 0.0
    if daily_returns and len(daily_returns) >= 2:
        volatility = float(np.std(daily_returns, ddof=1) * np.sqrt(252))
    held_values = [v for v in values if v > 0]
    avg_val = float(np.mean(held_values)) if held_values else 0
    div_yield = (div_gross_eur - div_tax_eur) / avg_val if avg_val > 0 else 0

    # Annualized return: use current_period_start, NOT first_buy
    period_start = stats["current_period_start"]
    annualized_return = 0.0
    annualized_return_native = 0.0
    if period_start:
        from datetime import date as _date
        hold_days = (_date.today() - _date.fromisoformat(period_start)).days
        years = max(hold_days / 365.25, 0.01)
        # A short position can lose more than it was opened for: no annual rate then.
        annualized_return = (1 + total_return) ** (1 / years) - 1 if total_return > -1 else None
        annualized_return_native = (1 + total_return_native) ** (1 / years) - 1 if total_return_native > -1 else None

    return {
        "symbol": symbol,
        "currency": currency,
        "display_currency": display_currency,
        "first_purchase": period_start,
        "last_purchase": stats["last_trade_date"],
        "first_trade": stats["first_trade_date"],
        "last_trade": stats["last_trade_date"],
        "num_buys": len([t for t in txns if t["buy_sell"] == "BUY"]),
        "num_sells": len([t for t in txns if t["buy_sell"] == "SELL"]),
        "shares_bought": round(stats["shares_bought_total"], 2),
        "shares_sold": round(stats["shares_sold_total"], 2),
        "current_shares": round(current_qty, 2),
        "avg_cost": round(avg_cost, 4),
        "current_price": round(current_price, 4),
        "current_value": round(current_value_eur, 2),
        "current_value_native": round(current_value_native, 2),
        "current_value_display": round(current_value_display, 2),
        # ── Cost basis ──
        "cost_basis_native": round(cost_basis_native, 2),
        "cost_basis_display": round(cost_basis_display, 2),
        "cost_basis_eur": round(cost_basis_eur, 2),
        # ── P&L ──
        "pnl_native": round(pnl_native, 2),
        "pnl_display": round(pnl_display, 2),
        # ── Realized P&L ──
        "realized_pnl_native": round(realized_pnl_native, 2),
        "realized_pnl_display": round(realized_pnl_display, 2),
        # ── Dividends ──
        "dividends_native": round(div_net_native, 2),
        "dividends_display": round(div_net_display, 2),
        # ── Returns ──
        "total_return_native": round(total_return_native, 6),
        "total_return_display": round(total_return_display, 6),
        "annualized_return_native": None if annualized_return_native is None else round(annualized_return_native, 6),
        "total_return": round(total_return, 6),
        "annualized_return": None if annualized_return is None else round(annualized_return, 6),
        # ── FX attribution ──
        "fx_return": round(
            ((1 + total_return_display) / (1 + total_return_native) - 1)
            if (1 + total_return_native) > 0 else 0, 6,
        ),
        # ── Misc ──
        "unrealized_pnl": round(current_value_eur - cost_basis_eur if current_qty != 0 else 0, 2),
        "volatility": round(volatility, 6),
        "dividend_income": round(div_gross_eur - div_tax_eur, 2),
        "dividend_gross": round(div_gross_eur, 2),
        "dividend_tax": round(div_tax_eur, 2),
        "dividend_yield": round(div_yield, 6),
        # ── Holding periods ──
        **_compute_holding_periods(hist),
        # ── Metadata ──
        "name": asset_name,
        "industry": _lookup_industry(symbol, db2_path),
    }


def get_all_holdings_performance(
    db3_path: str | None = None,
    db2_path: str | None = None,
    display_currency: str = "EUR",
    owner_user_id: str = "",
) -> list[dict]:
    """Batch version: compute performance for ALL current holdings in one pass.

    Opens one connection per database and reuses them across all symbols.
    Industry lookups are batched.  ~10× faster than calling
    ``get_holding_performance`` per symbol.
    """
    from collections import defaultdict

    import numpy as np

    from src.portfolio.currency import get_fx_series
    db3_path = db3_path or get_app_db()
    db2_path = db2_path or get_market_db()

    holdings = get_current_holdings(db3_path, owner_user_id=owner_user_id, db2_path=db2_path)
    result: list[dict] = []

    conn = connect_read(db3_path)
    conn2 = connect_read(db2_path)
    valuation_row = conn.execute(
        "SELECT MAX(date) FROM Portfolio_Daily WHERE owner_user_id = ?", (owner_user_id,),
    ).fetchone()
    valuation_date = (valuation_row[0] if valuation_row and valuation_row[0] else None) or Date.today().isoformat()

    # ── Pre-load FX series to avoid repeated connection opens ──
    _fx_cache: dict[str, dict[str, float]] = {}
    def _cached_fx_rate(from_ccy, to_ccy, date_str):
        if from_ccy == to_ccy:
            return 1.0
        cache_key = f"{from_ccy}->{to_ccy}"
        if cache_key not in _fx_cache:
            _fx_cache[cache_key] = get_fx_series(from_ccy, to_ccy, db2_path)
        series = _fx_cache[cache_key]
        if not series:
            return None
        if date_str in series:
            return series[date_str]
        candidates = [(d, r) for d, r in series.items() if d <= date_str]
        if candidates:
            candidates.sort(key=lambda x: x[0])
            return candidates[-1][1]
        return None

    try:
        # ── 1. Gather all symbols ──
        symbols = []
        for h in holdings:
            sym = h["symbol"]
            if h["asset_category"] == "CASH" or sym.startswith("CASH"):
                rate = _cached_fx_rate(h.get("currency") or "EUR", display_currency, valuation_date)
                native = h.get("market_value_native") or 0.0
                result.append({
                    **h,
                    "market_value_display": round(native * rate, 2) if rate else h.get("market_value"),
                    "valuation_date": valuation_date,
                    "performance": None,
                })
                continue
            symbols.append(sym)

        if not symbols:
            return result

        # ── 2. Batch-fetch transactions grouped by symbol ──
        placeholders = ",".join("?" for _ in symbols)
        txns_by_sym: dict[str, list] = defaultdict(list)
        txns_rows = conn.execute(
            f"SELECT * FROM Transactions WHERE symbol IN ({placeholders}) AND activity_type IN ('TRADE', 'SPINOFF') "
            "AND owner_user_id = ? ORDER BY trade_date",
            [*symbols, owner_user_id],
        ).fetchall()
        for r in txns_rows:
            txns_by_sym[r["symbol"]].append(dict(r))

        # ── 3. Batch-fetch dividend transactions ──
        div_by_sym: dict[str, list] = defaultdict(list)
        div_rows = conn.execute(
            f"SELECT symbol, activity_type, amount, fx_rate_to_base, trade_date FROM Transactions WHERE symbol IN ({placeholders}) AND activity_type IN ('DIVIDEND','PIL_DIVIDEND','WITHHOLDING_TAX') AND owner_user_id = ?",
            [*symbols, owner_user_id],
        ).fetchall()
        for r in div_rows:
            div_by_sym[r["symbol"]].append(dict(r))

        # ── 4. Names: the asset's definition, else the broker's description ──
        from src.portfolio.assets import security_names

        names = security_names(conn, owner_user_id)
        split_events = _position_split_events(conn, conn2, owner_user_id, set(symbols))

        # ── 5. Batch-fetch holdings history ──
        hist_by_sym: dict[str, list] = defaultdict(list)
        hist_rows = conn.execute(
            f"SELECT symbol, date, market_value, market_value_native, market_price FROM Holdings_History WHERE symbol IN ({placeholders}) AND owner_user_id = ? ORDER BY symbol, date",
            [*symbols, owner_user_id],
        ).fetchall()
        for r in hist_rows:
            hist_by_sym[r["symbol"]].append(dict(r))

        # ── 6. Batch-fetch current holdings ──
        cur_holdings: dict[str, dict] = {}
        cur_rows = conn.execute(
            f"SELECT * FROM Portfolio_Holdings WHERE symbol IN ({placeholders}) AND owner_user_id = ?",
            [*symbols, owner_user_id],
        ).fetchall()
        for r in cur_rows:
            cur_holdings[r["symbol"]] = dict(r)

        # ── 7. Batch company lookup: EDINET code, English name, industry ──
        from src.portfolio.market_data import price_ticker_candidates

        industries: dict[str, str | None] = {}
        companies: dict[str, dict] = {}
        company_columns = {str(row[1]) for row in conn2.execute("PRAGMA table_info(CompanyInfo)")}
        if {"Company_Ticker", "Company_Code"} <= company_columns:
            name_column = "Company_Name" if "Company_Name" in company_columns else "NULL"
            industry_column = "Company_Industry" if "Company_Industry" in company_columns else "NULL"
            for sym in symbols:
                for cand in price_ticker_candidates(sym):
                    row = conn2.execute(
                        f"SELECT Company_Code, {name_column}, {industry_column} FROM CompanyInfo "
                        "WHERE Company_Ticker = ? LIMIT 1",
                        (cand,),
                    ).fetchone()
                    if row and row[0]:
                        companies[sym] = {"edinet_code": row[0], "company_name": row[1]}
                        industries[sym] = row[2]
                        break

        # ── 8. Compute per-symbol performance ──
        for sym in symbols:
            txns = txns_by_sym.get(sym, [])
            if not txns:
                result.append(next((h for h in holdings if h["symbol"] == sym), {}))
                continue

            stats = _compute_position_stats(
                txns, display_currency, db2_path, split_events.get(sym),
            )
            currency = stats["currency"]
            cost_basis_native = stats["cost_basis_native"]
            cost_basis_display = stats["cost_basis_display"]
            cost_basis_eur = stats["cost_basis_eur"]
            realized_pnl_native = stats["realized_pnl_native"]
            realized_pnl_display = stats["realized_pnl_display"]

            # Current state
            holding = cur_holdings.get(sym)
            current_qty = stats["current_shares"]
            avg_cost = holding["avg_cost"] if holding and holding["avg_cost"] else 0
            current_price = holding["market_price"] if holding and holding["market_price"] else avg_cost
            cv_eur = holding["market_value"] if holding and holding["market_value"] else (current_qty * current_price)
            cv_native = holding["market_value_native"] if holding and holding["market_value_native"] is not None else current_qty * current_price

            pnl_native = cv_native - cost_basis_native if current_qty != 0 else 0
            cv_display = cv_eur
            pnl_display = cv_eur - cost_basis_eur
            if display_currency.upper() != currency.upper():
                rate_now = _cached_fx_rate(currency, display_currency, valuation_date)
                cv_display = round(cv_native * rate_now, 2) if rate_now and cv_native else 0
                pnl_display = cv_display - cost_basis_display if cv_display else 0
            elif display_currency.upper() != "EUR":
                cv_display = cv_native
                pnl_display = cv_native - cost_basis_native

            # Dividends
            divs = div_by_sym.get(sym, [])
            div_gross_native = sum(abs(r["amount"] or 0) for r in divs if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND"))
            div_tax_native = sum(abs(r["amount"] or 0) for r in divs if r["activity_type"] == "WITHHOLDING_TAX")
            div_net_native = div_gross_native - div_tax_native
            div_gross_eur = sum(abs(r["amount"] or 0) * (r["fx_rate_to_base"] or 1) for r in divs if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND"))
            div_tax_eur = sum(abs(r["amount"] or 0) * (r["fx_rate_to_base"] or 1) for r in divs if r["activity_type"] == "WITHHOLDING_TAX")
            div_net_display = div_gross_eur - div_tax_eur
            if display_currency.upper() != currency.upper() and divs:
                div_net_display = 0
                for r in divs:
                    amt = abs(r["amount"] or 0)
                    sign = 1 if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND") else -1
                    dt = r["trade_date"]
                    rate = _cached_fx_rate(currency, display_currency, dt)
                    div_net_display += amt * sign * (rate if rate else (r["fx_rate_to_base"] or 1))
            elif display_currency.upper() == currency.upper():
                div_net_display = div_net_native

            # Returns: volatility from weekday price changes in the holding's
            # own currency (value changes would count purchases as returns).
            hist = hist_by_sym.get(sym, [])
            values = [h["market_value"] or 0 for h in hist]
            dr_list = []
            weekday_prices = [
                h["market_price"] for h in hist
                if h.get("market_price") and Date.fromisoformat(h["date"]).weekday() < 5
            ]
            for previous_price, price in zip(weekday_prices[:-1], weekday_prices[1:], strict=False):
                if previous_price > 0 and price != previous_price:
                    dr_list.append(price / previous_price - 1)
                elif previous_price > 0:
                    dr_list.append(0.0)

            total_return = _position_return(cv_eur, cost_basis_eur)
            total_return_native = _position_return(cv_native, cost_basis_native)
            total_return_display_d = _position_return(cv_display, cost_basis_display)

            volatility = float(np.std(dr_list, ddof=1) * np.sqrt(261)) if len(dr_list) >= 2 else 0
            held_values = [v for v in values if v > 0]
            avg_val = float(np.mean(held_values)) if held_values else 0
            div_yield = (div_gross_eur - div_tax_eur) / avg_val if avg_val > 0 else 0

            period_start = stats["current_period_start"]
            annualized_return = 0.0
            annualized_return_native = 0.0
            if period_start:
                hold_days = (Date.fromisoformat(valuation_date) - Date.fromisoformat(period_start)).days
                years = max(hold_days / 365.25, 0.01)
                # A short position can lose more than it was opened for: no annual rate then.
                annualized_return = (1 + total_return) ** (1 / years) - 1 if total_return > -1 else None
                annualized_return_native = (1 + total_return_native) ** (1 / years) - 1 if total_return_native > -1 else None

            fx_ret = ((1 + total_return_display_d) / (1 + total_return_native) - 1) if (1 + total_return_native) > 0 else 0

            hold_periods = _compute_holding_periods(hist)

            perf = {
                "symbol": sym, "currency": currency, "display_currency": display_currency,
                "first_purchase": stats["current_period_start"],
                "last_purchase": stats["last_trade_date"],
                "first_trade": stats["first_trade_date"],
                "last_trade": stats["last_trade_date"],
                "num_buys": len([t for t in txns if t["buy_sell"] == "BUY"]),
                "num_sells": len([t for t in txns if t["buy_sell"] == "SELL"]),
                "shares_bought": round(stats["shares_bought_total"], 2),
                "shares_sold": round(stats["shares_sold_total"], 2),
                "current_shares": round(current_qty, 2),
                "avg_cost": round(avg_cost, 4), "current_price": round(current_price, 4),
                "current_value": round(cv_eur, 2), "current_value_native": round(cv_native, 2),
                "current_value_display": round(cv_display, 2),
                "cost_basis_native": round(cost_basis_native, 2),
                "cost_basis_display": round(cost_basis_display, 2),
                "cost_basis_eur": round(cost_basis_eur, 2),
                "pnl_native": round(pnl_native, 2), "pnl_display": round(pnl_display, 2),
                "realized_pnl_native": round(realized_pnl_native, 2),
                "realized_pnl_display": round(realized_pnl_display, 2),
                "dividends_native": round(div_net_native, 2),
                "dividends_display": round(div_net_display, 2),
                "total_return_native": round(total_return_native, 6),
                "total_return_display": round(total_return_display_d, 6),
                "annualized_return_native": None if annualized_return_native is None else round(annualized_return_native, 6),
                "fx_return": round(fx_ret, 6),
                "unrealized_pnl": round(cv_eur - cost_basis_eur if current_qty != 0 else 0, 2),
                "total_return": round(total_return, 6),
                "annualized_return": None if annualized_return is None else round(annualized_return, 6),
                "volatility": round(volatility, 6),
                "dividend_income": round(div_gross_eur - div_tax_eur, 2),
                "dividend_gross": round(div_gross_eur, 2),
                "dividend_tax": round(div_tax_eur, 2),
                "dividend_yield": round(div_yield, 6),
                "longest_holding_days": hold_periods["longest_holding_days"],
                "latest_holding_days": hold_periods["latest_holding_days"],
                "num_holding_periods": hold_periods["num_holding_periods"],
                "held_since": hold_periods["held_since"],
                "held_until": hold_periods["held_until"],
                "name": (companies.get(sym) or {}).get("company_name") or names.get(sym),
                "broker_description": names.get(sym),
                "industry": industries.get(sym),
                "edinet_code": (companies.get(sym) or {}).get("edinet_code"),
                "total_pnl_display": round(pnl_display + realized_pnl_display + div_net_display, 2),
                "holding_days": hold_periods["latest_holding_days"],
            }
            # Find the matching holding item
            h_item = next((h for h in holdings if h["symbol"] == sym), {})
            result.append({
                **h_item,
                "market_value_display": round(cv_display, 2),
                "valuation_date": valuation_date,
                "performance": perf,
            })

        return result
    finally:
        conn.close()
        conn2.close()
