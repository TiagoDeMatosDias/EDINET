"""Walk-forward portfolio state reconstruction.

Rebuilds holdings, daily values, and dividend income from Transactions
(db3) + market prices (db2) + option pricing models.

The algorithm walks day-by-day from the earliest transaction to today,
applying trades, corporate actions, and pricing current holdings at each
step.  Results are stored in ``Portfolio_Daily``, ``Portfolio_Holdings``,
and ``Holdings_History`` tables for fast retrieval by the API.
"""

from __future__ import annotations

import sqlite3
import logging
from collections import defaultdict
from datetime import date as Date, timedelta

from src.orchestrator.common.db_config import get_db2, get_db3
from src.portfolio.schema import create_tables
from src.portfolio import option_pricing as _op

logger = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_portfolio_state(
    db3_path: str | None = None,
    db2_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    base_currency: str = "EUR",
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
    import sys, os

    db3_path = db3_path or get_db3()
    db2_path = db2_path or get_db2()

    create_tables(db3_path)

    conn3 = sqlite3.connect(db3_path)
    conn2 = sqlite3.connect(db2_path)

    try:
        # 0. Clear previous rebuild state so stale entries don't persist
        conn3.execute("DELETE FROM Holdings_History")
        conn3.execute("DELETE FROM Portfolio_Daily")
        conn3.execute("DELETE FROM Portfolio_Holdings")

        # 1. Load transactions sorted by date
        conn3.row_factory = sqlite3.Row
        rows = conn3.execute(
            "SELECT * FROM Transactions ORDER BY trade_date, id"
        ).fetchall()
        transactions = [dict(r) for r in rows]

        if not transactions:
            logger.info("No transactions found — nothing to build")
            return {"daily_rows": 0, "holdings_count": 0}

        # Determine date range
        all_dates = sorted(
            _parse_date(t["trade_date"])
            for t in transactions
            if _parse_date(t["trade_date"])
        )
        if not all_dates:
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
        txn_index = 0
        daily_rows = 0
        hh_rows = 0

        current_date = first_date
        while current_date <= last_date:
            date_str = current_date.isoformat()
            daily_dividend = 0.0
            daily_inflow = 0.0

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
            total_value_native = 0.0

            for key, h in holdings.items():
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
            fx_stock = fx_rates.get("stock_total", 1.0)  # rough; per-currency below
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
                   (date, total_value, cash_balance, stock_value, option_value,
                    daily_return, cumulative_return, dividend_income, net_inflow,
                    cash_ccy_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (date_str, total_value, cash_balance, stock_value_base,
                 option_value_base, daily_return,
                 cumulative_return - 1, daily_dividend, daily_inflow,
                 cash_ccy_json),
            )
            daily_rows += 1

            # Store Holdings_History — store both native and base-currency values
            for key, h in holdings.items():
                if h["quantity"] != 0:
                    mv_native = h.get("market_value")
                    cur_rate = fx_rates.get(h["currency"], 1.0)
                    mv_base = mv_native * cur_rate if mv_native is not None else None
                    conn3.execute(
                        """INSERT OR REPLACE INTO Holdings_History
                           (date, symbol, asset_category, quantity, market_price,
                            market_value, market_value_native, currency, fx_rate,
                            is_option, strike, expiry, put_call, underlying)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (date_str, h["symbol"], h["asset_category"],
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
        today = Date.today().isoformat()
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
                   (symbol, asset_category, quantity, avg_cost, market_price,
                    market_value, market_value_native, currency, fx_rate,
                    is_option, strike, expiry, put_call, underlying)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (h["symbol"], h["asset_category"], h["quantity"],
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
        old_qty = h["quantity"]

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

    Stocks: lookup from Stock_Prices in db2; falls back to average cost.
    Options: compute using binomial tree with underlying price from db2.
    """
    if h.get("is_option"):
        # Need underlying price, strike, T, r, sigma
        underlying = h.get("underlying") or h["symbol"][:h["symbol"].index(" ")] if " " in h["symbol"] else h["symbol"]
        S = _get_price(conn2, underlying, date_str)
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
        price = _get_price(conn2, h["symbol"], date_str)
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
) -> list[dict]:
    """Return daily portfolio value series."""
    db3_path = db3_path or get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row
    where = []
    params = []
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


def get_current_holdings(db3_path: str | None = None) -> list[dict]:
    """Return current holdings snapshot with cash balance.

    Returns stock/option holdings plus a synthetic "CASH" row representing
    the current cash balance (which can be negative for margin accounts).
    Expired options are excluded.
    """
    db3_path = db3_path or get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row
    today = Date.today().isoformat()

    rows = conn.execute(
        """SELECT * FROM Portfolio_Holdings
           WHERE (is_option = 0)
              OR (is_option = 1 AND (expiry IS NULL OR expiry >= ?))
           ORDER BY COALESCE(market_value, 0) DESC""",
        (today,),
    ).fetchall()

    result = [dict(r) for r in rows]

    # Add per-currency cash balances from Portfolio_Daily (latest row)
    cash_row = conn.execute(
        "SELECT cash_balance, cash_ccy_json, total_value FROM Portfolio_Daily ORDER BY date DESC LIMIT 1"
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
) -> list[dict]:
    """Return holdings snapshot at a specific date."""
    db3_path = db3_path or get_db3()
    create_tables(db3_path)
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row
    # Find most recent Holdings_History entry before or on date
    rows = conn.execute("""
        SELECT h.* FROM Holdings_History h
        INNER JOIN (
            SELECT symbol, asset_category, MAX(date) AS max_date
            FROM Holdings_History
            WHERE date <= ?
            GROUP BY symbol, asset_category
        ) latest ON h.symbol = latest.symbol
                  AND h.asset_category = latest.asset_category
                  AND h.date = latest.max_date
        WHERE h.quantity != 0
    """, (date,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_closed_positions(db3_path: str | None = None) -> list[dict]:
    """Return positions that were fully closed (sold/expired) and are no
    longer in the current portfolio.

    Computes realized P&L from buy/sell trades for each symbol that appears
    in ``Transactions`` but not in ``Portfolio_Holdings``.
    """
    db3_path = db3_path or get_db3()
    create_tables(db3_path)
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row
    try:
        cur_syms = {r["symbol"] for r in conn.execute(
            "SELECT DISTINCT symbol FROM Portfolio_Holdings"
        ).fetchall()}
        closed_syms = [r["symbol"] for r in conn.execute(
            "SELECT DISTINCT symbol, asset_category FROM Transactions WHERE activity_type = 'TRADE'"
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
            GROUP BY symbol, asset_category
        """, closed_syms).fetchall()

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


def get_holding_performance(
    symbol: str,
    db3_path: str | None = None,
) -> dict | None:
    """Compute performance metrics for a single holding."""
    import numpy as np
    db3_path = db3_path or get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row

    txns = conn.execute(
        "SELECT * FROM Transactions WHERE symbol = ? AND activity_type = 'TRADE' ORDER BY trade_date",
        (symbol,),
    ).fetchall()
    if not txns:
        conn.close()
        return None

    buys = [t for t in txns if t["buy_sell"] == "BUY"]
    sells = [t for t in txns if t["buy_sell"] == "SELL"]
    first_date = txns[0]["trade_date"]
    currency = txns[0]["currency"] or ""

    total_cost = 0.0
    total_shares_bought = 0.0
    total_shares_sold = 0.0
    for t in buys:
        qty = abs(t["quantity"] or 0)
        total_shares_bought += qty
    for t in sells:
        total_shares_sold += abs(t["quantity"] or 0)

    # Use Portfolio_Holdings for authoritative cost basis (handles cancellations)
    holding = conn.execute(
        "SELECT * FROM Portfolio_Holdings WHERE symbol = ?", (symbol,),
    ).fetchone()

    net_shares = total_shares_bought - total_shares_sold
    avg_cost = holding["avg_cost"] if holding and holding["avg_cost"] else 0
    total_cost = avg_cost * (holding["quantity"] if holding else abs(net_shares))
    current_qty = holding["quantity"] if holding else net_shares
    current_price = holding["market_price"] if holding and holding["market_price"] else avg_cost
    current_value = holding["market_value"] if holding and holding["market_value"] else (current_qty * current_price)
    current_value_native = holding["market_value_native"] if holding else current_qty * current_price
    unrealized_pnl = (current_price - avg_cost) * current_qty if current_qty > 0 and avg_cost else 0

    div_rows = conn.execute(
        "SELECT activity_type, amount, fx_rate_to_base FROM Transactions "
        "WHERE symbol = ? AND activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX')",
        (symbol,),
    ).fetchall()
    div_gross = sum(
        abs(r["amount"] or 0) * (r["fx_rate_to_base"] or 1)
        for r in div_rows if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND")
    )
    div_tax = sum(
        (r["amount"] or 0) * (r["fx_rate_to_base"] or 1)
        for r in div_rows if r["activity_type"] == "WITHHOLDING_TAX"
    )

    hist = conn.execute(
        "SELECT date, market_value FROM Holdings_History WHERE symbol = ? ORDER BY date",
        (symbol,),
    ).fetchall()
    conn.close()

    values = [h["market_value"] or 0 for h in hist]
    first_val = next((i for i, v in enumerate(values) if v > 0), None)
    daily_returns: list[float] = []
    if first_val is not None:
        for i in range(first_val + 1, len(values)):
            if values[i-1] > 0 and values[i] > 0:
                daily_returns.append(values[i] / values[i-1] - 1)

    total_return = 0.0
    annualized_return = 0.0
    volatility = 0.0
    if daily_returns:
        # Total return: native vs native (cost and value in same currency)
        if total_cost > 0 and current_value_native:
            total_return = current_value_native / total_cost - 1
        elif total_cost > 0:
            total_return = current_value / total_cost - 1
        else:
            total_return = 0.0
        if len(daily_returns) >= 2:
            volatility = float(np.std(daily_returns, ddof=1) * np.sqrt(252))

    avg_val = float(np.mean([v for v in values if v > 0])) if values else 0
    div_yield = (div_gross + div_tax) / avg_val if avg_val > 0 else 0

    first_buy = buys[0]["trade_date"] if buys else None
    last_buy = buys[-1]["trade_date"] if buys else None

    # Compute annualized return using actual holding period
    if first_buy and total_return != 0:
        from datetime import date as _date
        hold_days = (_date.today() - _date.fromisoformat(first_buy)).days
        years = max(hold_days / 365.25, 0.01)
        if total_return > -1:
            annualized_return = (1 + total_return) ** (1 / years) - 1

    return {
        "symbol": symbol,
        "currency": currency,
        "first_purchase": first_buy,
        "last_purchase": last_buy,
        "first_trade": first_date,
        "last_trade": txns[-1]["trade_date"],
        "num_buys": len(buys),
        "num_sells": len(sells),
        "shares_bought": round(total_shares_bought, 2),
        "shares_sold": round(total_shares_sold, 2),
        "current_shares": round(current_qty, 2),
        "avg_cost": round(avg_cost, 4),
        "current_price": round(current_price, 4),
        "current_value": round(current_value, 2),
        "current_value_native": round(current_value_native, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "total_return": round(total_return, 6),
        "annualized_return": round(annualized_return, 6),
        "volatility": round(volatility, 6),
        "dividend_income": round(div_gross + div_tax, 2),
        "dividend_gross": round(div_gross, 2),
        "dividend_tax": round(div_tax, 2),
        "dividend_yield": round(div_yield, 6),
    }
