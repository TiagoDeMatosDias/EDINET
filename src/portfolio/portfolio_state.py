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

from src.orchestrator.common.db_config import get_db2, get_db3
from src.orchestrator.common.sqlite import connect_read, connect_write
from src.portfolio import option_pricing as _op
from src.portfolio.schema import create_tables
from src.utilities.price_provenance import table_columns

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Split-adjusted price helpers
# ---------------------------------------------------------------------------

_split_table_checked: set[str] = set()
# Cache keyed by (connection_id, ticker) to avoid cross-database contamination
# (e.g. when tests switch between different database files).
_split_factor_cache: dict[tuple[int, str], list[tuple[str, float]]] = {}


def _ensure_split_table_exists(db2_path: str) -> None:
    """Ensure the Stock_Splits table exists in db2 (idempotent)."""
    global _split_table_checked
    db_key = str(db2_path)
    if db_key in _split_table_checked:
        return
    try:
        from src.portfolio.split_schema import ensure_split_tables
        from src.utilities.price_provenance import ensure_price_provenance_columns

        migration_conn = sqlite3.connect(db2_path)
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
    splits: list[tuple[str, float]] = []
    for split_date, ratio_from, ratio_to, _method, _event_id in rows:
        date_text = str(split_date)[:10]
        if date_text in seen_dates:
            continue
        seen_dates.add(date_text)
        try:
            ratio = float(ratio_to) / float(ratio_from)
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        if ratio > 0:
            splits.append((date_text, ratio))
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

def build_portfolio_state(
    db3_path: str | None = None,
    db2_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    base_currency: str = "EUR",
    owner_user_id: str = "",
) -> dict:
    """Rebuild portfolio state from scratch.

    Args:
        db3_path: Path to Portfolio.db (default ``get_db3()``).
        db2_path: Path to Standardized.db (default ``get_db2()``).
        start_date: Override earliest date (YYYY-MM-DD). None = auto.
        end_date: Override latest date (YYYY-MM-DD). None = today.
        base_currency: Account base currency for FX conversion (default EUR).

    Returns:
        ``{'daily_rows': N, 'holdings_count': N}``
    """

    db3_path = db3_path or get_db3()
    db2_path = db2_path or get_db2()

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

        # 2. Walk forward
        holdings: dict[tuple[str, str], dict] = {}  # (symbol, asset_category) → holding dict
        cash_balance = 0.0
        cash_by_currency: dict[str, float] = {"EUR": 0.0}  # per-currency cash tracking
        cumulative_return = 1.0
        prev_total_value = 0.0
        fx_rates: dict[str, float] = {}  # currency → latest fxRateToBase
        split_events = _load_confirmed_split_events(conn2)
        txn_index = 0
        daily_rows = 0
        hh_rows = 0

        current_date = first_date
        while current_date <= last_date:
            date_str = current_date.isoformat()
            daily_dividend = 0.0
            daily_inflow = 0.0

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

                _apply_transaction(txn, holdings, cash_balance_ref := [cash_balance],
                                   cash_ccy_ref := [cash_by_currency],
                                   daily_inflow_ref := [daily_inflow],
                                   daily_div_ref := [daily_dividend],
                                   fx_rates)
                cash_balance = cash_balance_ref[0]
                cash_by_currency = cash_ccy_ref[0]
                daily_inflow = daily_inflow_ref[0]
                daily_dividend = daily_div_ref[0]
                txn_index += 1

            # --- Price current holdings ---
            stock_value_native = 0.0
            option_value_native = 0.0
            for _key, h in holdings.items():
                if h["quantity"] == 0:
                    continue

                price = _price_holding(h, date_str, conn2)
                if price is not None:
                    h["market_price"] = price
                    multiplier = h.get("multiplier", 1) or 1
                    value = price * abs(h["quantity"]) * multiplier
                    h["market_value"] = value

                    if h["is_option"]:
                        option_value_native += value
                    else:
                        stock_value_native += value

            # Convert to base currency
            # For proper multi-currency: sum (value * fx_rate[currency])
            stock_value_base = 0.0
            option_value_base = 0.0
            for h in holdings.values():
                if h.get("market_value") and h["quantity"] != 0:
                    fx = fx_rates.get(h["currency"], 1.0)
                    if h["is_option"]:
                        option_value_base += h["market_value"] * fx
                    else:
                        stock_value_base += h["market_value"] * fx

            total_value = cash_balance + stock_value_base + option_value_base

            # Compute daily return and cumulative return (robust Modified Dietz)
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

            # Store in Portfolio_Daily
            import json as _json
            cash_ccy_json = _json.dumps(cash_by_currency) if cash_by_currency else "{}"
            conn3.execute(
                """INSERT OR REPLACE INTO Portfolio_Daily
                   (date, owner_user_id, total_value, cash_balance, stock_value, option_value,
                    daily_return, cumulative_return, dividend_income, net_inflow,
                    cash_ccy_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (date_str, owner_user_id, total_value, cash_balance, stock_value_base,
                 option_value_base, daily_return,
                 cumulative_return - 1, daily_dividend, daily_inflow,
                 cash_ccy_json),
            )
            daily_rows += 1

            # Store Holdings_History — store both native and base-currency values
            for _key, h in holdings.items():
                if h["quantity"] != 0:
                    mv_native = h.get("market_value")
                    cur_rate = fx_rates.get(h["currency"], 1.0)
                    mv_base = mv_native * cur_rate if mv_native is not None else None
                    conn3.execute(
                        """INSERT OR REPLACE INTO Holdings_History
                           (date, symbol, asset_category, owner_user_id, quantity, market_price,
                            market_value, market_value_native, currency, fx_rate,
                            is_option, strike, expiry, put_call, underlying)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (date_str, h["symbol"], h["asset_category"], owner_user_id,
                         h["quantity"], h.get("market_price"),
                         mv_base, mv_native, h["currency"],
                         cur_rate,
                         1 if h["is_option"] else 0,
                         h.get("strike"), h.get("expiry"),
                         h.get("put_call"), h.get("underlying")),
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
            cur_rate = fx_rates.get(h["currency"], 1.0)
            mv_base = mv_native * cur_rate if mv_native is not None else None
            conn3.execute(
                """INSERT OR REPLACE INTO Portfolio_Holdings
                   (symbol, asset_category, owner_user_id, quantity, avg_cost, market_price,
                    market_value, market_value_native, currency, fx_rate,
                    is_option, strike, expiry, put_call, underlying)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (h["symbol"], h["asset_category"], owner_user_id, h["quantity"],
                 h.get("avg_cost"), h.get("market_price"),
                 mv_base, mv_native, h["currency"],
                 cur_rate,
                 1 if h["is_option"] else 0,
                 h.get("strike"), h.get("expiry"),
                 h.get("put_call"), h.get("underlying")),
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


def _apply_transaction(
    txn: dict,
    holdings: dict[tuple[str, str], dict],
    cash_balance: list[float],
    cash_ccy: list[dict[str, float]],
    daily_inflow: list[float],
    daily_div: list[float],
    fx_rates: dict[str, float],
) -> None:
    """Modify holdings, cash, daily_inflow, and daily_div in-place."""
    def _add_cash(ccy: str, amount: float) -> None:
        """Add amount to total cash (EUR) AND per-currency cash tracker."""
        fx = fx_rates.get(ccy, 1.0)
        cash_balance[0] += amount * fx
        ccy_map = cash_ccy[0]
        ccy_map[ccy] = ccy_map.get(ccy, 0.0) + amount

    activity = txn["activity_type"]
    symbol = (txn.get("symbol") or "").strip()
    asset_cat = (txn.get("asset_category") or "STK").strip()
    currency = txn.get("currency", "")
    fx = txn.get("fx_rate_to_base") or 1.0
    qty = txn.get("quantity") or 0
    amount = txn.get("amount") or 0
    commission = txn.get("commission") or 0
    trade_price = txn.get("trade_price")

    # Update FX rate for this currency
    if currency:
        fx_rates[currency] = fx

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
        if txn.get("buy_sell") == "BUY":
            h["quantity"] += qty
            # Update cost basis
            if trade_price and qty > 0:
                h["total_cost"] += qty * trade_price * multiplier + commission * fx
        else:  # SELL
            h["quantity"] -= abs(qty)
            if h["quantity"] <= 0:
                h["total_cost"] = 0
                if h["quantity"] < 0:
                    h["quantity"] = 0

        if h["quantity"] > 0 and h["total_cost"]:
            h["avg_cost"] = h["total_cost"] / h["quantity"]

        # Cash effect
        net_cash = txn.get("net_cash") or 0
        _add_cash(currency, net_cash)

    elif activity == "DIVIDEND":
        _add_cash(currency, amount)
        daily_div[0] += amount * fx

    elif activity == "PIL_DIVIDEND":
        _add_cash(currency, amount)
        daily_div[0] += amount * fx

    elif activity == "WITHHOLDING_TAX":
        _add_cash(currency, amount)
        daily_div[0] += amount * fx  # netted against gross dividend

    elif activity == "DEPOSIT_WITHDRAWAL":
        _add_cash(currency, amount)
        daily_inflow[0] += amount * fx

    elif activity == "BROKER_INTEREST":
        _add_cash(currency, amount)

    elif activity == "OTHER_FEE":
        _add_cash(currency, amount)

    elif activity == "COMMISSION_ADJ":
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
    conn2: sqlite3.Connection,
) -> float | None:
    """Price a single holding for a given date.

    Stocks use the quote on the same (as-traded) basis as the ledger quantity.
    Split actions are applied to holdings above, so using a split-adjusted
    quote here would double-adjust the position.  Options use the same raw
    underlying convention; their strike/multiplier are adjusted with the
    contract action where available.
    """
    if h.get("is_option"):
        # Need underlying price, strike, T, r, sigma
        underlying = h.get("underlying") or h["symbol"][:h["symbol"].index(" ")] if " " in h["symbol"] else h["symbol"]
        S = _get_as_traded_price(conn2, underlying, date_str)
        if S is None:
            return None
        K = h.get("strike") or 0
        if K == 0:
            return None
        expiry = _parse_date(h.get("expiry"))
        if expiry is None:
            return 0.01  # very short time → minimal value
        T = max((expiry - Date.fromisoformat(date_str)).days / 365.0, 0.0)
        if T <= 0:
            return 0.0
        opt_type = "put" if h.get("put_call") == "P" else "call"
        return _op.binomial_tree(opt_type, S, K, T, 0.05, 0.20)
    else:
        price = _get_as_traded_price(conn2, h["symbol"], date_str)
        if price is not None:
            return price
        # Fall back to average cost if no market price available
        # (common for recently purchased positions where price hasn't been fetched yet)
        avg = h.get("avg_cost")
        if avg is not None and avg > 0:
            return avg
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
    db3_path = db3_path or get_db3()
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


def get_current_holdings(db3_path: str | None = None, owner_user_id: str = "") -> list[dict]:
    """Return current holdings snapshot with cash balance."""
    db3_path = db3_path or get_db3()
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
        "SELECT cash_balance, cash_ccy_json, total_value FROM Portfolio_Daily WHERE owner_user_id = ? ORDER BY date DESC LIMIT 1",
        (owner_user_id,),
    ).fetchone()
    conn.close()

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
                fx = 1.0  # cash stored in native currency amount
                # Use latest FX rate from the holdings data
                cur_fx = None
                for r in result:
                    if r.get("currency") == ccy and r.get("fx_rate"):
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
    return result


def get_holdings_at_date(
    db3_path: str | None = None,
    date: str | None = None,
    owner_user_id: str = "",
) -> list[dict]:
    """Return holdings snapshot at a specific date."""
    db3_path = db3_path or get_db3()
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
    db3_path = db3_path or get_db3()
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

        result = []
        for row in rows:
            r = dict(row)
            realized_pnl = r["total_proceeds"] - r["total_cost"]
            result.append({
                "symbol": r["symbol"],
                "asset_category": r["asset_category"],
                "description": r["description"],
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
        }

    from datetime import date as _date

    dates = [_date.fromisoformat(r["date"]) for r in hist_records]
    if len(dates) == 1:
        return {
            "longest_holding_days": 1,
            "latest_holding_days": 1,
            "num_holding_periods": 1,
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
    }


def _compute_position_stats(
    txns: list[dict],
    display_currency: str = "EUR",
    db2_path: str | None = None,
) -> dict:
    """Walk trade transactions chronologically with average-cost accounting.

    Tracks shares, cost basis, and realized P&L across multiple buy/sell
    periods.  Handles the sell-and-rebuy scenario correctly.

    Returns:
        cost_basis_native, cost_basis_display, cost_basis_eur,
        realized_pnl_native, realized_pnl_display,
        current_period_start, first_trade_date, last_trade_date,
        shares_bought_total, shares_sold_total
    """
    from src.portfolio.currency import get_rate_at_date_any

    shares = 0.0
    cost_basis_native = 0.0
    cost_basis_display = 0.0
    cost_basis_eur = 0.0
    realized_pnl_native = 0.0
    realized_pnl_display = 0.0
    realized_pnl_eur = 0.0
    current_period_start: str | None = None
    first_trade_date: str | None = None
    last_trade_date: str | None = None
    shares_bought_total = 0.0
    shares_sold_total = 0.0
    currency: str = ""

    for t in txns:
        td = t["trade_date"]
        if first_trade_date is None or td < first_trade_date:
            first_trade_date = td
        if last_trade_date is None or td > last_trade_date:
            last_trade_date = td

        bs = t.get("buy_sell", "")
        qty = abs(t["quantity"] or 0)
        if qty == 0:
            continue
        trade_money = t["trade_money"] or 0
        commission = t["commission"] or 0
        fx = t["fx_rate_to_base"] or 1.0
        cur = t.get("currency", "")
        if cur:
            currency = cur

        if bs == "BUY":
            total_cost = trade_money + commission  # native
            shares += qty
            shares_bought_total += qty
            cost_basis_native += total_cost
            cost_basis_eur += total_cost * fx

            # Display currency
            if display_currency != currency:
                rate = get_rate_at_date_any(currency, display_currency, td, db2_path)
                cost_basis_display += total_cost * (rate if rate else fx)
            else:
                cost_basis_display += total_cost

            # Track current period start (shares went from 0 → >0)
            if shares == qty:  # just went from 0 to qty
                current_period_start = td

        elif bs == "SELL":
            qty_sell = min(qty, shares)
            if shares > 0:
                avg_cost_native = cost_basis_native / shares
                avg_cost_eur = cost_basis_eur / shares
                avg_cost_display = cost_basis_display / shares

                # Reduce cost basis proportionally
                cost_basis_native -= qty_sell * avg_cost_native
                cost_basis_eur -= qty_sell * avg_cost_eur
                cost_basis_display -= qty_sell * avg_cost_display

                # Compute realized P&L for this sale
                # trade_money for sells is negative (money out), proceeds = -trade_money
                actual_proceeds = -trade_money if trade_money < 0 else trade_money
                cost_of_sold = qty_sell * avg_cost_native
                realized_pnl_native += actual_proceeds - cost_of_sold

                # Display currency realized
                proceeds_display = actual_proceeds
                if display_currency != currency:
                    rate = get_rate_at_date_any(currency, display_currency, td, db2_path)
                    proceeds_display = actual_proceeds * (rate if rate else fx)
                realized_pnl_display += proceeds_display - (qty_sell * avg_cost_display)

                # EUR realized
                realized_pnl_eur += (actual_proceeds * fx) - (qty_sell * avg_cost_eur)

            shares -= qty_sell
            shares_sold_total += qty_sell
            if shares <= 0:
                shares = 0.0
                cost_basis_native = 0.0
                cost_basis_display = 0.0
                cost_basis_eur = 0.0
                current_period_start = None  # will be set on next BUY

        elif bs == "BUY (Ca.)":
            # Cancellation: reverse a previous BUY — negative trade_money
            # means it reduces shares and cost basis
            shares -= qty
            shares_bought_total -= qty
            total_cost = trade_money + commission  # may be negative
            cost_basis_native += total_cost  # adds a negative
            cost_basis_eur += total_cost * fx
            if display_currency != currency:
                rate = get_rate_at_date_any(currency, display_currency, td, db2_path)
                cost_basis_display += total_cost * (rate if rate else fx)
            else:
                cost_basis_display += total_cost
            if shares < 0:
                shares = 0.0

    return {
        "cost_basis_native": cost_basis_native,
        "cost_basis_display": cost_basis_display,
        "cost_basis_eur": cost_basis_eur,
        "realized_pnl_native": realized_pnl_native,
        "realized_pnl_display": realized_pnl_display,
        "realized_pnl_eur": realized_pnl_eur,
        "current_period_start": current_period_start or first_trade_date,
        "first_trade_date": first_trade_date or "",
        "last_trade_date": last_trade_date or "",
        "shares_bought_total": shares_bought_total,
        "shares_sold_total": shares_sold_total,
        "currency": currency,
        "current_shares": max(shares, 0),
    }


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
    db3_path = db3_path or get_db3()
    db2_path = db2_path or get_db2()
    conn = connect_read(db3_path)

    txns = conn.execute(
        "SELECT * FROM Transactions WHERE symbol = ? AND activity_type = 'TRADE' AND owner_user_id = ? ORDER BY trade_date",
        (symbol, owner_user_id),
    ).fetchall()
    if not txns:
        conn.close()
        return None

    stats = _compute_position_stats(
        [dict(t) for t in txns], display_currency, db2_path,
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
    current_value_native = holding["market_value_native"] if holding else current_qty * current_price

    # P&L (Nat.) — unrealized only
    pnl_native = current_value_native - cost_basis_native if current_qty > 0 and cost_basis_native != 0 else 0.0

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
        "WHERE symbol = ? AND activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX')",
        (symbol,),
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

    name_row = conn.execute(
        "SELECT description FROM Transactions WHERE symbol = ? AND activity_type = 'TRADE' "
        "AND description IS NOT NULL AND description != '' ORDER BY trade_date DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    asset_name = name_row[0] if name_row else None
    conn.close()

    values = [h["market_value"] or 0 for h in hist]
    first_val = next((i for i, v in enumerate(values) if v > 0), None)
    daily_returns: list[float] = []
    if first_val is not None:
        for i in range(first_val + 1, len(values)):
            if values[i-1] > 0 and values[i] > 0:
                daily_returns.append(values[i] / values[i-1] - 1)

    # ── Total Return ──
    total_return = 0.0
    if cost_basis_eur > 0 and current_value_eur:
        total_return = current_value_eur / cost_basis_eur - 1
    total_return_native = 0.0
    if cost_basis_native > 0 and current_value_native:
        total_return_native = current_value_native / cost_basis_native - 1
    total_return_display = 0.0
    if cost_basis_display > 0 and current_value_display:
        total_return_display = current_value_display / cost_basis_display - 1

    volatility = 0.0
    if daily_returns and len(daily_returns) >= 2:
        volatility = float(np.std(daily_returns, ddof=1) * np.sqrt(252))
    avg_val = float(np.mean([v for v in values if v > 0])) if values else 0
    div_yield = (div_gross_eur - div_tax_eur) / avg_val if avg_val > 0 else 0

    # Annualized return: use current_period_start, NOT first_buy
    period_start = stats["current_period_start"]
    annualized_return = 0.0
    annualized_return_native = 0.0
    if period_start:
        from datetime import date as _date
        hold_days = (_date.today() - _date.fromisoformat(period_start)).days
        years = max(hold_days / 365.25, 0.01)
        if total_return > -1:
            annualized_return = (1 + total_return) ** (1 / years) - 1
        if total_return_native > -1:
            annualized_return_native = (1 + total_return_native) ** (1 / years) - 1

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
        "annualized_return_native": round(annualized_return_native, 6),
        "total_return": round(total_return, 6),
        "annualized_return": round(annualized_return, 6),
        # ── FX attribution ──
        "fx_return": round(
            ((1 + total_return_display) / (1 + total_return_native) - 1)
            if (1 + total_return_native) > 0 else 0, 6,
        ),
        # ── Misc ──
        "unrealized_pnl": round(current_value_eur - cost_basis_eur if current_qty > 0 else 0, 2),
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
    db3_path = db3_path or get_db3()
    db2_path = db2_path or get_db2()

    holdings = get_current_holdings(db3_path, owner_user_id=owner_user_id)
    result: list[dict] = []

    conn = connect_read(db3_path)
    conn2 = connect_read(db2_path)

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
                result.append({**h, "performance": None})
                continue
            symbols.append(sym)

        if not symbols:
            return result

        # ── 2. Batch-fetch transactions grouped by symbol ──
        placeholders = ",".join("?" for _ in symbols)
        txns_by_sym: dict[str, list] = defaultdict(list)
        txns_rows = conn.execute(
            f"SELECT * FROM Transactions WHERE symbol IN ({placeholders}) AND activity_type = 'TRADE' AND owner_user_id = ? ORDER BY trade_date",
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

        # ── 4. Batch-fetch names (descriptions) ──
        names: dict[str, str | None] = {}
        name_rows = conn.execute(
            f"SELECT symbol, description FROM Transactions WHERE symbol IN ({placeholders}) AND activity_type='TRADE' AND description IS NOT NULL AND description!='' AND owner_user_id = ? ORDER BY trade_date DESC",
            [*symbols, owner_user_id],
        ).fetchall()
        for r in name_rows:
            if r["symbol"] not in names:
                names[r["symbol"]] = r["description"]

        # ── 5. Batch-fetch holdings history ──
        hist_by_sym: dict[str, list] = defaultdict(list)
        hist_rows = conn.execute(
            f"SELECT symbol, date, market_value, market_value_native FROM Holdings_History WHERE symbol IN ({placeholders}) AND owner_user_id = ? ORDER BY symbol, date",
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

        # ── 7. Batch industry lookup ──
        industries: dict[str, str | None] = {}
        for sym in symbols:
            clean = str(sym).strip()
            candidates = [clean]
            if clean.endswith('.T') or clean.endswith('.JP'):
                base = clean.rsplit('.', 1)[0]
                if len(base) <= 4 and base.isdigit():
                    candidates.append(base + '0')
                candidates.append(base)
            elif len(clean) == 5 and clean.isdigit():
                candidates.append(clean[:4])
            found = None
            for cand in candidates:
                row = conn2.execute(
                    "SELECT Company_Industry FROM CompanyInfo WHERE Company_Ticker = ? LIMIT 1",
                    (cand,),
                ).fetchone()
                if row and row[0]:
                    found = row[0]
                    break
            industries[sym] = found

        # ── 8. Compute per-symbol performance ──
        for sym in symbols:
            txns = txns_by_sym.get(sym, [])
            if not txns:
                result.append(next((h for h in holdings if h["symbol"] == sym), {}))
                continue

            stats = _compute_position_stats(
                txns, display_currency, db2_path,
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
            cv_native = holding["market_value_native"] if holding else current_qty * current_price

            pnl_native = cv_native - cost_basis_native if current_qty > 0 and cost_basis_native != 0 else 0
            cv_display = cv_eur
            pnl_display = cv_eur - cost_basis_eur
            if display_currency.upper() != currency.upper():
                from datetime import date as _date
                today_str = _date.today().isoformat()
                rate_now = _cached_fx_rate(currency, display_currency, today_str)
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

            # Returns
            hist = hist_by_sym.get(sym, [])
            values = [h["market_value"] or 0 for h in hist]
            dr_list = []
            first_val = next((i for i, v in enumerate(values) if v > 0), None)
            if first_val is not None:
                for i in range(first_val + 1, len(values)):
                    if values[i - 1] > 0 and values[i] > 0:
                        dr_list.append(values[i] / values[i - 1] - 1)

            total_return = cv_eur / cost_basis_eur - 1 if cost_basis_eur > 0 and cv_eur else 0
            total_return_native = cv_native / cost_basis_native - 1 if cost_basis_native > 0 and cv_native else 0
            total_return_display_d = cv_display / cost_basis_display - 1 if cost_basis_display > 0 and cv_display else 0

            volatility = float(np.std(dr_list, ddof=1) * np.sqrt(252)) if len(dr_list) >= 2 else 0
            avg_val = float(np.mean([v for v in values if v > 0])) if values else 0
            div_yield = (div_gross_eur - div_tax_eur) / avg_val if avg_val > 0 else 0

            period_start = stats["current_period_start"]
            annualized_return = 0.0
            annualized_return_native = 0.0
            if period_start:
                from datetime import date as _date
                hold_days = (_date.today() - _date.fromisoformat(period_start)).days
                years = max(hold_days / 365.25, 0.01)
                if total_return > -1:
                    annualized_return = (1 + total_return) ** (1 / years) - 1
                if total_return_native > -1:
                    annualized_return_native = (1 + total_return_native) ** (1 / years) - 1

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
                "annualized_return_native": round(annualized_return_native, 6),
                "fx_return": round(fx_ret, 6),
                "unrealized_pnl": round(cv_eur - cost_basis_eur if current_qty > 0 else 0, 2),
                "total_return": round(total_return, 6),
                "annualized_return": round(annualized_return, 6),
                "volatility": round(volatility, 6),
                "dividend_income": round(div_gross_eur - div_tax_eur, 2),
                "dividend_gross": round(div_gross_eur, 2),
                "dividend_tax": round(div_tax_eur, 2),
                "dividend_yield": round(div_yield, 6),
                "longest_holding_days": hold_periods["longest_holding_days"],
                "latest_holding_days": hold_periods["latest_holding_days"],
                "num_holding_periods": hold_periods["num_holding_periods"],
                "name": names.get(sym),
                "industry": industries.get(sym),
            }
            # Find the matching holding item
            h_item = next((h for h in holdings if h["symbol"] == sym), {})
            result.append({**h_item, "performance": perf})

        return result
    finally:
        conn.close()
        conn2.close()
