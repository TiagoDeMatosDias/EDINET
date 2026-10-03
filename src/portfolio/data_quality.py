"""What the portfolio figures rest on, and where that data is thin.

The Portfolio page shows this beside its statistics: when the ledger was
last valued, which quotes each holding used and how old they are, holdings
valued at cost, quotes converted from another listing's currency, daily
moves too large to be believable, and the rate and inflation inputs.
"""

from __future__ import annotations

import sqlite3
from datetime import date as Date

from src.orchestrator.common.sqlite import connect_read
from src.portfolio import analytics
from src.portfolio.market_data import STALE_PRICE_DAYS, MarketData, price_ticker_candidates
from src.utilities.stock_prices import tse_code

# A diversified portfolio moving this much in a day usually means a bad quote.
SUSPICIOUS_DAILY_MOVE = 0.08


# Fewer stored closes than this in a full calendar year means weekly data.
DAILY_ROWS_PER_YEAR = 150


def sparse_price_years(conn2: sqlite3.Connection, ticker: str, since: str | None = None) -> list[int]:
    """Complete calendar years in which *ticker* has only weekly (or sparser) closes."""
    try:
        rows = conn2.execute(
            "SELECT substr(Date, 1, 4), COUNT(*), MIN(Date), MAX(Date) FROM Stock_Prices "
            "WHERE Ticker = ? AND Date >= ? GROUP BY 1 ORDER BY 1",
            (ticker, since or "0000"),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    if len(rows) < 2:
        return []
    # The first and last years of a series are usually partial.
    return [int(year) for year, count, _first, _last in rows[1:-1] if count < DAILY_ROWS_PER_YEAR]


def _days_between(start: str | None, end: str | None) -> int | None:
    if not start or not end:
        return None
    return (Date.fromisoformat(end[:10]) - Date.fromisoformat(start[:10])).days


def refresh_ticker(symbol: str, price_ticker: str | None) -> str:
    """The stored ticker to refresh for a holding (Tokyo codes use the five-character form)."""
    if price_ticker:
        return price_ticker
    code = tse_code(symbol)
    return f"{code}0" if code else price_ticker_candidates(symbol)[0]


def portfolio_data_quality(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
    owner_user_id: str = "",
    today: str | None = None,
) -> dict:
    today = today or Date.today().isoformat()
    currency = (display_currency or "EUR").upper()
    conn3 = connect_read(db3_path)
    conn2 = connect_read(db2_path)
    try:
        market = MarketData(conn2)
        valuation = conn3.execute(
            "SELECT MAX(date), MIN(date) FROM Portfolio_Daily WHERE owner_user_id = ?",
            (owner_user_id,),
        ).fetchone()
        valuation_date, first_date = (valuation[0], valuation[1]) if valuation else (None, None)
        last_transaction = conn3.execute(
            "SELECT MAX(trade_date) FROM Transactions WHERE owner_user_id = ?",
            (owner_user_id,),
        ).fetchone()[0]
        total_row = conn3.execute(
            "SELECT total_value FROM Portfolio_Daily WHERE owner_user_id = ? AND date = ?",
            (owner_user_id, valuation_date),
        ).fetchone() if valuation_date else None
        total_value = float(total_row[0] or 0.0) if total_row else 0.0

        columns = {str(row[1]) for row in conn3.execute("PRAGMA table_info(Portfolio_Holdings)")}
        provenance = all(name in columns for name in ("price_date", "price_source", "price_ticker", "price_currency"))
        select = "symbol, asset_category, currency, market_value, market_price, is_option, expiry"
        if provenance:
            select += ", price_date, price_source, price_ticker, price_currency"
        rows = conn3.execute(
            f"SELECT {select} FROM Portfolio_Holdings WHERE owner_user_id = ? AND quantity != 0 "
            "AND asset_category != 'CASH' ORDER BY market_value DESC",
            (owner_user_id,),
        ).fetchall()

        holdings = []
        for row in rows:
            record = dict(row)
            if record.get("is_option") and record.get("expiry") and str(record["expiry"]) < today:
                continue
            symbol = record["symbol"]
            holding_currency = (record.get("currency") or "EUR").upper()
            series = market.price_series(symbol, holding_currency)
            price_date = record.get("price_date") if provenance else (series.on(valuation_date)[1] if series and valuation_date and series.on(valuation_date) else None)
            source = record.get("price_source") if provenance else ("market" if price_date else "cost")
            weight = float(record.get("market_value") or 0.0) / total_value if total_value else None
            stale_days = _days_between(price_date, valuation_date) if source == "market" else None
            latest_stored = series.last_date if series else None
            quote_currency = (record.get("price_currency") if provenance else None) or (series.source_currency if series else None)
            status = "ok"
            if source == "cost":
                status = "cost"
            elif source is None:
                status = "missing"
            elif source == "market" and stale_days is not None and stale_days > STALE_PRICE_DAYS:
                status = "stale"
            weekly_years = sparse_price_years(conn2, series.ticker, first_date) if series else []
            holdings.append({
                "symbol": symbol,
                "weekly_years": weekly_years,
                "currency": holding_currency,
                "weight": weight,
                "price_source": source,
                "price_date": price_date,
                "stale_days": stale_days,
                "price_ticker": (record.get("price_ticker") if provenance else None) or (series.ticker if series else None),
                "quote_currency": quote_currency,
                "converted": bool(quote_currency and quote_currency != holding_currency),
                "latest_stored_price": latest_stored,
                "market_age_days": _days_between(latest_stored, today),
                "newer_price_stored": bool(latest_stored and price_date and latest_stored > price_date),
                "refresh_ticker": refresh_ticker(symbol, (record.get("price_ticker") if provenance else None) or (series.ticker if series else None)),
                "status": status,
            })

        large_moves = [
            {"date": str(row[0]), "return": float(row[1])}
            for row in conn3.execute(
                "SELECT date, daily_return FROM Portfolio_Daily WHERE owner_user_id = ? "
                "AND ABS(daily_return) >= ? ORDER BY ABS(daily_return) DESC LIMIT 8",
                (owner_user_id, SUSPICIOUS_DAILY_MOVE),
            )
        ]

        held_currencies = sorted({holding["currency"] for holding in holdings} | {currency})
        fx = {
            code: market.fx_last_date(code)
            for code in held_currencies if code != "EUR"
        }
        risk_free = analytics.load_risk_free(conn2, currency)
        inflation = analytics.load_inflation(conn2, currency)

        issues: list[dict] = []
        behind = _days_between(valuation_date, today)
        if valuation_date is None:
            issues.append({"level": "error", "code": "not_built", "message": "The portfolio has not been valued yet. Rebuild it after importing activity."})
        else:
            if last_transaction and last_transaction > valuation_date:
                issues.append({"level": "error", "code": "activity_after_valuation", "message": f"Activity dated up to {last_transaction} was imported after the last rebuild. Rebuild to include it."})
            if behind is not None and behind > 3:
                issues.append({"level": "warning", "code": "valuation_behind", "message": f"Values are as of {valuation_date}, {behind} days ago. Rebuild to value the portfolio today."})
        costed = [holding for holding in holdings if holding["status"] in ("cost", "missing")]
        if costed:
            share = sum(holding["weight"] or 0 for holding in costed)
            issues.append({
                "level": "error", "code": "valued_at_cost",
                "message": f"{len(costed)} holding(s) have no stored prices and are valued at cost ({share:.1%} of the portfolio): {', '.join(holding['symbol'] for holding in costed)}.",
                "symbols": [holding["symbol"] for holding in costed],
            })
        stale = [holding for holding in holdings if holding["status"] == "stale"]
        if stale:
            share = sum(holding["weight"] or 0 for holding in stale)
            issues.append({
                "level": "warning" if share < 0.2 else "error", "code": "stale_prices",
                "message": f"{len(stale)} holding(s) use quotes more than {STALE_PRICE_DAYS} days older than the valuation date ({share:.1%} of the portfolio): "
                + ", ".join(f"{holding['symbol']} ({holding['price_date']})" for holding in stale) + ".",
                "symbols": [holding["symbol"] for holding in stale],
            })
        newer = [holding for holding in holdings if holding["newer_price_stored"]]
        if newer:
            issues.append({"level": "info", "code": "newer_prices", "message": f"Newer prices are stored for {len(newer)} holding(s) than the last rebuild used. Rebuild to use them.", "symbols": [holding["symbol"] for holding in newer]})
        weekly = [holding for holding in holdings if holding["weekly_years"]]
        if weekly:
            share = sum(holding["weight"] or 0 for holding in weekly)
            issues.append({
                "level": "warning", "code": "weekly_prices",
                "message": f"{len(weekly)} holding(s) have only weekly prices in some years ({share:.1%} of the portfolio): "
                + ", ".join(f"{holding['symbol']} ({holding['weekly_years'][0]}–{holding['weekly_years'][-1]})" for holding in weekly)
                + ". Daily returns, volatility, and drawdowns are approximate there; refreshing prices fetches daily history.",
                "symbols": [holding["symbol"] for holding in weekly],
            })
        converted = [holding for holding in holdings if holding["converted"]]
        for holding in converted:
            issues.append({"level": "info", "code": "converted_quotes", "message": f"{holding['symbol']} is booked in {holding['currency']} but quoted in {holding['quote_currency']} ({holding['price_ticker']}); prices are converted at ECB reference rates.", "symbols": [holding["symbol"]]})
        if large_moves:
            issues.append({
                "level": "warning", "code": "large_moves",
                "message": f"{len(large_moves)} day(s) moved the whole portfolio by {SUSPICIOUS_DAILY_MOVE:.0%} or more, which usually points to a bad quote or a missing transaction: "
                + ", ".join(f"{move['date']} ({move['return']:+.1%})" for move in large_moves[:4]) + ".",
            })
        missing_fx = [code for code, last in fx.items() if last is None]
        if missing_fx:
            issues.append({"level": "error", "code": "fx_missing", "message": f"No ECB reference rates for {', '.join(missing_fx)}; the broker's last booking rate is used instead."})
        stale_fx = [code for code, last in fx.items() if last and (_days_between(last, today) or 0) > 5]
        if stale_fx:
            issues.append({"level": "warning", "code": "fx_stale", "message": f"Exchange rates for {', '.join(stale_fx)} end {min(fx[code] for code in stale_fx)}. Run Update FX Data."})
        if risk_free.kind == "missing":
            issues.append({"level": "warning", "code": "risk_free_missing", "message": f"No short-term interest rate is stored for {currency}: Sharpe and Sortino ratios assume 0%."})
        elif (_days_between(risk_free.dates[-1], today) or 0) > 45:
            issues.append({"level": "info", "code": "risk_free_stale", "message": f"The {currency} short-term rate ends {risk_free.dates[-1]}."})
        if inflation is None:
            issues.append({"level": "info", "code": "inflation_missing", "message": f"No consumer price index is stored for {currency}; real returns are unavailable."})

        return {
            "today": today,
            "display_currency": currency,
            "valuation_date": valuation_date,
            "first_date": first_date,
            "days_behind": behind,
            "last_transaction": last_transaction,
            "holdings": holdings,
            "large_moves": large_moves,
            "fx": {"source": "ECB euro reference rates", "last_dates": fx},
            "risk_free": {
                "kind": risk_free.kind,
                "ticker": risk_free.ticker,
                "source": analytics.RISK_FREE_SOURCES.get(currency) if risk_free.kind == "series" else None,
                "last_date": risk_free.dates[-1] if risk_free.kind == "series" else None,
                "latest_rate": risk_free.rates[-1] if risk_free.kind == "series" else None,
                "supported": currency in analytics.RISK_FREE_SOURCES,
            },
            "inflation": {
                "ticker": inflation.ticker if inflation else None,
                "last_observation": inflation.months[-1] if inflation else None,
            },
            "issues": issues,
        }
    finally:
        conn3.close()
        conn2.close()


def held_refresh_tickers(db3_path: str, owner_user_id: str = "") -> list[str]:
    """Stored tickers for the open holdings, for a price refresh."""
    conn = connect_read(db3_path)
    try:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(Portfolio_Holdings)")}
        ticker_column = "price_ticker" if "price_ticker" in columns else "NULL"
        rows = conn.execute(
            f"SELECT symbol, {ticker_column}, underlying, is_option FROM Portfolio_Holdings "
            "WHERE owner_user_id = ? AND quantity != 0 AND asset_category != 'CASH'",
            (owner_user_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()
    tickers = []
    for symbol, price_ticker, underlying, is_option in rows:
        tickers.append(refresh_ticker(underlying or symbol, None) if is_option else refresh_ticker(symbol, price_ticker))
    return list(dict.fromkeys(tickers))
