"""Dividend income by payment and by company, for the Portfolio Income tab.

Every dividend, payment in lieu, and withholding-tax record is grouped into
payments: one company, one pay date, one declared amount per share.  The
broker's description states the amount per share ("CASH DIVIDEND USD 1.06 PER
SHARE"), which also gives the exact number of shares paid on; withholding tax
(including the reversals and re-charges brokers book later) joins the payment
with the same company and amount per share.

Amounts are kept in the payment currency and converted to the display
currency on the payment date: the broker's booking rate into the account
currency, then the ECB reference rate.
"""

from __future__ import annotations

import re
from bisect import bisect_right
from collections import defaultdict
from datetime import date as Date
from datetime import timedelta

from src.orchestrator.common.sqlite import connect_read
from src.portfolio.market_data import MarketData, price_ticker_candidates

_PER_SHARE = re.compile(r"(?:CASH DIVIDEND|DIVIDEND|IN LIEU)\D*?([A-Z]{3})\s+(\d+(?:\.\d+)?)", re.IGNORECASE)
# Tax rows are matched to the payment with the same amount per share within this window.
_TAX_MATCH_DAYS = 400


def per_share_amount(description: str | None) -> tuple[str, float] | None:
    """The currency and amount per share a dividend description declares, if any."""
    match = _PER_SHARE.search(description or "")
    if not match:
        return None
    try:
        return match.group(1).upper(), float(match.group(2))
    except ValueError:
        return None


def _days(left: str, right: str) -> int:
    return abs((Date.fromisoformat(left[:10]) - Date.fromisoformat(right[:10])).days)


def _shares_lookup(conn3, owner_user_id: str, symbols: set[str]):
    """Shares held by date from the rebuilt ledger (includes splits and spinoffs)."""
    series: dict[str, tuple[list[str], list[float]]] = {}
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        rows = conn3.execute(
            f"SELECT symbol, date, quantity FROM Holdings_History WHERE owner_user_id = ? "
            f"AND symbol IN ({placeholders}) AND is_option = 0 ORDER BY symbol, date",
            [owner_user_id, *sorted(symbols)],
        ).fetchall()
        grouped: dict[str, list[tuple[str, float]]] = defaultdict(list)
        for symbol, day, quantity in rows:
            grouped[symbol].append((str(day)[:10], float(quantity or 0)))
        series = {symbol: ([day for day, _ in values], [quantity for _, quantity in values]) for symbol, values in grouped.items()}

    def shares_on(symbol: str, day: str) -> float | None:
        dates, quantities = series.get(symbol, ([], []))
        index = bisect_right(dates, day) - 1
        if index < 0:
            return None
        # A holding sold just before the payment still earned it: look back
        # to the most recent day it was held (within ~two months).
        if _days(dates[index], day) > 62:
            return None
        return quantities[index]

    return shares_on


def portfolio_income(db3_path: str, db2_path: str, display_currency: str = "EUR", owner_user_id: str = "") -> dict:
    currency = (display_currency or "EUR").upper()
    conn3 = connect_read(db3_path)
    conn2 = connect_read(db2_path)
    try:
        market = MarketData(conn2)
        rows = [dict(row) for row in conn3.execute(
            "SELECT id, symbol, trade_date, activity_type, amount, currency, fx_rate_to_base, description "
            "FROM Transactions WHERE owner_user_id = ? "
            "AND activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX') "
            "AND COALESCE(symbol, '') != '' ORDER BY trade_date, id",
            (owner_user_id,),
        ).fetchall()]
        valuation = conn3.execute(
            "SELECT MAX(date) FROM Portfolio_Daily WHERE owner_user_id = ?", (owner_user_id,),
        ).fetchone()
        valuation_date = (valuation[0] if valuation and valuation[0] else None) or Date.today().isoformat()
        holdings = {
            row["symbol"]: dict(row)
            for row in conn3.execute(
                "SELECT symbol, quantity, avg_cost, market_price, currency FROM Portfolio_Holdings "
                "WHERE owner_user_id = ? AND quantity != 0 AND is_option = 0",
                (owner_user_id,),
            ).fetchall()
        }
        trade_names = {
            row[0]: row[1]
            for row in conn3.execute(
                "SELECT symbol, description FROM Transactions WHERE owner_user_id = ? AND activity_type = 'TRADE' "
                "AND COALESCE(description, '') != '' ORDER BY trade_date",
                (owner_user_id,),
            ).fetchall()
        }
        symbols = {row["symbol"] for row in rows}
        shares_on = _shares_lookup(conn3, owner_user_id, symbols)

        companies_info = {}
        columns = {str(row[1]) for row in conn2.execute("PRAGMA table_info(CompanyInfo)")}
        if {"Company_Ticker", "Company_Code"} <= columns:
            name_column = "Company_Name" if "Company_Name" in columns else "NULL"
            for symbol in symbols:
                for candidate in price_ticker_candidates(symbol):
                    found = conn2.execute(
                        f"SELECT Company_Code, {name_column} FROM CompanyInfo WHERE Company_Ticker = ? LIMIT 1",
                        (candidate,),
                    ).fetchone()
                    if found and found[0]:
                        companies_info[symbol] = {"edinet_code": found[0], "name": found[1]}
                        break

        def to_display(amount: float, native: str, broker_rate: float | None, day: str) -> float:
            if broker_rate:
                return amount * broker_rate * (market.rate("EUR", currency, day) or 1.0)
            return amount * (market.rate(native or "EUR", currency, day) or 1.0)

        # 1. Dividend payments, keyed by company, date, and amount per share.
        #    Payments in lieu (on shares lent out) join the dividend paid the
        #    same day; reversals join the payment they correct.
        payments: dict[tuple, dict] = {}
        by_symbol: dict[str, list[dict]] = defaultdict(list)

        def new_payment(row: dict, per_share: float | None, kind: str, key: tuple) -> dict:
            payment = {
                "symbol": row["symbol"], "date": row["trade_date"], "currency": row["currency"] or "EUR",
                "per_share": per_share, "type": kind,
                "gross_native": 0.0, "tax_native": 0.0, "in_lieu_native": 0.0, "gross": 0.0, "tax": 0.0,
            }
            payments[key] = payment
            by_symbol[row["symbol"]].append(payment)
            return payment

        def nearest(row: dict, per_share: float | None) -> dict | None:
            candidates = [
                payment for payment in by_symbol.get(row["symbol"], [])
                if (per_share is None or payment["per_share"] is None or abs(payment["per_share"] - per_share) < 1e-9)
                and _days(payment["date"], row["trade_date"]) <= _TAX_MATCH_DAYS
            ]
            return min(candidates, key=lambda item: _days(item["date"], row["trade_date"])) if candidates else None

        declared_rows = [row for row in rows if row["activity_type"] == "DIVIDEND" and float(row["amount"] or 0) > 0 and per_share_amount(row["description"])]
        other_rows = [row for row in rows if row["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND") and row not in declared_rows]
        for row in declared_rows:
            per_share = per_share_amount(row["description"])[1]
            key = (row["symbol"], row["trade_date"], per_share)
            payment = payments.get(key) or new_payment(row, per_share, "Dividend", key)
            amount = float(row["amount"] or 0.0)
            payment["gross_native"] += amount
            payment["gross"] += to_display(amount, row["currency"], row["fx_rate_to_base"], row["trade_date"])
        for row in other_rows:
            amount = float(row["amount"] or 0.0)
            declared = per_share_amount(row["description"])
            same_day = [payment for payment in by_symbol.get(row["symbol"], []) if payment["date"] == row["trade_date"]]
            if same_day:
                payment = same_day[0]
            elif amount < 0:
                payment = nearest(row, declared[1] if declared else None) or new_payment(row, declared[1] if declared else None, "Adjustment", (row["symbol"], row["trade_date"], "adjustment"))
            else:
                key = (row["symbol"], row["trade_date"], declared[1] if declared else None)
                payment = payments.get(key) or new_payment(row, declared[1] if declared else None, "In lieu" if row["activity_type"] == "PIL_DIVIDEND" else "Dividend", key)
            payment["gross_native"] += amount
            if row["activity_type"] == "PIL_DIVIDEND":
                payment["in_lieu_native"] += amount
            payment["gross"] += to_display(amount, row["currency"], row["fx_rate_to_base"], row["trade_date"])

        # 2. Withholding tax joins the payment with the same amount per share.
        for row in rows:
            if row["activity_type"] != "WITHHOLDING_TAX":
                continue
            declared = per_share_amount(row["description"])
            amount = float(row["amount"] or 0.0)
            payment = nearest(row, declared[1] if declared else None) or new_payment(
                row, declared[1] if declared else None, "Tax adjustment", (row["symbol"], row["trade_date"], "tax"),
            )
            payment["tax_native"] += amount
            payment["tax"] += to_display(amount, row["currency"], row["fx_rate_to_base"], row["trade_date"])

        # 3. Shares paid on, and the amount per share where not declared.
        out_payments = []
        for payment in sorted(payments.values(), key=lambda item: (item["date"], item["symbol"])):
            if abs(payment["gross_native"]) < 1e-9 and abs(payment["tax_native"]) < 1e-9:
                continue
            per_share = payment["per_share"]
            shares = payment["gross_native"] / per_share if per_share and payment["gross_native"] else shares_on(payment["symbol"], payment["date"])
            if per_share is None and shares and payment["gross_native"]:
                per_share = payment["gross_native"] / shares
            gross = payment["gross"]
            out_payments.append({
                "date": payment["date"],
                "symbol": payment["symbol"],
                "type": payment["type"],
                "currency": payment["currency"],
                "per_share": per_share,
                "shares": round(shares, 4) if shares else None,
                "gross_native": round(payment["gross_native"], 4),
                "tax_native": round(payment["tax_native"], 4),
                "net_native": round(payment["gross_native"] + payment["tax_native"], 4),
                "in_lieu_native": round(payment["in_lieu_native"], 4),
                "gross": round(gross, 4),
                "tax": round(payment["tax"], 4),
                "net": round(gross + payment["tax"], 4),
                "withholding_rate": round(-payment["tax"] / gross, 6) + 0.0 if gross > 0 else None,
            })

        # 4. Per company.
        total_net = sum(item["net"] for item in out_payments) or 0.0
        year_end = valuation_date[:4]
        ttm_start = (Date.fromisoformat(valuation_date) - timedelta(days=365)).isoformat()
        companies = []
        grouped: dict[str, list[dict]] = defaultdict(list)
        for item in out_payments:
            grouped[item["symbol"]].append(item)
        for symbol, items in grouped.items():
            dividends = [item for item in items if item["type"] in ("Dividend", "In lieu") and item["per_share"] and item["gross_native"] > 0]
            gross = sum(item["gross"] for item in items)
            tax = sum(item["tax"] for item in items)
            annual: dict[str, dict] = {}
            for item in items:
                year = item["date"][:4]
                bucket = annual.setdefault(year, {"year": int(year), "gross": 0.0, "tax": 0.0, "net": 0.0, "per_share": 0.0, "payments": 0})
                bucket["gross"] += item["gross"]
                bucket["tax"] += item["tax"]
                bucket["net"] += item["net"]
                if item in dividends:
                    bucket["per_share"] += item["per_share"]
                    bucket["payments"] += 1
            years = sorted(annual.values(), key=lambda bucket: bucket["year"])
            # A year is complete when it pays as often as the company's usual year
            # and is neither the first year held nor the current one.
            interior = [bucket["payments"] for bucket in (years[1:-1] if len(years) > 2 else years)]
            usual = max(set(interior), key=lambda count: (interior.count(count), count)) if interior else 0
            for index, bucket in enumerate(years):
                bucket["partial"] = index == 0 or str(bucket["year"]) == year_end or bucket["payments"] < usual
                previous = years[index - 1] if index else None
                bucket["per_share_growth"] = (
                    bucket["per_share"] / previous["per_share"] - 1
                    if previous and not bucket["partial"] and not previous["partial"] and previous["per_share"] > 0 else None
                )
            ttm = [item for item in dividends if item["date"] > ttm_start]
            ttm_per_share = sum(item["per_share"] for item in ttm)
            latest = dividends[-1] if dividends else None
            year_ago = None
            if latest:
                earlier = [item for item in dividends if 300 <= _days(item["date"], latest["date"]) <= 430]
                if earlier:
                    year_ago = min(earlier, key=lambda item: abs(_days(item["date"], latest["date"]) - 365))
            held = holdings.get(symbol)
            price = held.get("market_price") if held else None
            cost = held.get("avg_cost") if held else None
            native = items[-1]["currency"]
            same_currency = held is not None and (held.get("currency") or native) == native
            info = companies_info.get(symbol, {})
            companies.append({
                "symbol": symbol,
                "name": info.get("name") or trade_names.get(symbol) or symbol,
                "edinet_code": info.get("edinet_code"),
                "currency": native,
                "is_held": held is not None,
                "shares_held": float(held["quantity"]) if held else None,
                "gross": round(gross, 2),
                "tax": round(tax, 2),
                "net": round(gross + tax, 2),
                "withholding_rate": round(-tax / gross, 6) + 0.0 if gross > 0 else None,
                "share_of_income": round((gross + tax) / total_net, 6) if total_net else None,
                "payments": len(dividends),
                "first_date": items[0]["date"],
                "last_date": items[-1]["date"],
                "frequency": usual,
                "ttm_net": round(sum(item["net"] for item in items if item["date"] > ttm_start), 2),
                "ttm_per_share": round(ttm_per_share, 6) if ttm else None,
                "latest_per_share": latest["per_share"] if latest else None,
                "latest_date": latest["date"] if latest else None,
                "per_share_growth_1y": round(latest["per_share"] / year_ago["per_share"] - 1, 6) if latest and year_ago and year_ago["per_share"] else None,
                "current_yield": round(ttm_per_share / price, 6) if ttm and price and same_currency else None,
                "yield_on_cost": round(ttm_per_share / cost, 6) if ttm and cost and same_currency else None,
                "annual": [{**bucket, "gross": round(bucket["gross"], 2), "tax": round(bucket["tax"], 2), "net": round(bucket["net"], 2)} for bucket in years],
            })
        companies.sort(key=lambda company: company["net"], reverse=True)
        return {
            "currency": currency,
            "valuation_date": valuation_date,
            "total_net": round(total_net, 2),
            "payments": out_payments,
            "companies": companies,
        }
    finally:
        conn3.close()
        conn2.close()
