"""Tags that follow the user's portfolio: one for positions held now, one for positions closed.

Research is keyed by EDINET code. A holding listed in Tokyo maps to its EDINET
company; any other stock or fund (a US share, a European ETF) is keyed by its
portfolio symbol, which is also how its Analysis page and prices find it.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from src.orchestrator.common.sqlite import connect_read

from .storage import ResearchStore

_LOGGER = logging.getLogger(__name__)

OPEN_TAG = "Open position"
CLOSED_TAG = "Closed position"
POSITION_TAGS = (OPEN_TAG, CLOSED_TAG)

_EDINET_CODE = re.compile(r"^E\d{5}$")
# Cash balances and derivatives are not holdings of a company.
_EXCLUDED_CATEGORIES = ("CASH", "OPT", "FOP", "FUT", "WAR", "IOPT")


def is_edinet_code(code: str) -> bool:
    return bool(_EDINET_CODE.match(code or ""))


def _symbols(conn, sql: str, user_id: str) -> set[str]:
    try:
        return {str(row[0]) for row in conn.execute(sql, (user_id,)).fetchall() if row[0]}
    except Exception:  # noqa: BLE001 - an empty or older portfolio database has no rows to tag
        return set()


def portfolio_positions(db3: str, db2: str | None, user_id: str) -> list[dict[str, Any]]:
    """Every stock or fund the user holds now or has held, keyed the way research keys companies.

    A position is open while the rebuilt portfolio still holds a non-zero
    quantity; it is closed once it appears only in the holdings history. When
    several symbols map to one company (two listings), the company is open if
    any of them is.
    """
    excluded = ", ".join(f"'{category}'" for category in _EXCLUDED_CATEGORIES)
    conn = connect_read(db3)
    try:
        current = _symbols(conn, (
            "SELECT symbol FROM Portfolio_Holdings WHERE owner_user_id = ? AND COALESCE(is_option, 0) = 0 "
            f"AND UPPER(COALESCE(asset_category, '')) NOT IN ({excluded}) AND ABS(quantity) > 1e-9"
        ), user_id)
        held = current | _symbols(conn, (
            "SELECT DISTINCT symbol FROM Holdings_History WHERE owner_user_id = ? AND COALESCE(is_option, 0) = 0 "
            f"AND UPPER(COALESCE(asset_category, '')) NOT IN ({excluded}) AND ABS(quantity) > 1e-9"
        ), user_id)
        names: dict[str, str] = {}
        try:
            # A trade names the security ("ALTRIA GROUP INC"); dividends describe the payment, so trades win.
            for symbol, description in conn.execute(
                "SELECT symbol, description FROM Transactions WHERE owner_user_id = ? AND COALESCE(description, '') != '' "
                "ORDER BY activity_type = 'TRADE', trade_date",
                (user_id,),
            ).fetchall():
                if symbol in held:
                    names[str(symbol)] = str(description)
        except Exception:  # noqa: BLE001 - names are a convenience
            pass
    finally:
        conn.close()

    companies = _edinet_codes(db2, held) if db2 else {}
    positions: dict[str, dict[str, Any]] = {}
    for symbol in sorted(held):
        code = companies.get(symbol) or symbol
        entry = positions.setdefault(code, {
            "code": code,
            "symbols": [],
            "name": None if code != symbol else names.get(symbol),
            "is_open": False,
            "listed_in_japan": code != symbol,
        })
        entry["symbols"].append(symbol)
        entry["is_open"] = entry["is_open"] or symbol in current
    return list(positions.values())


def _edinet_codes(db2: str, symbols: set[str]) -> dict[str, str]:
    from src.portfolio.market_data import price_ticker_candidates

    try:
        conn = connect_read(db2)
    except Exception:  # noqa: BLE001 - without market data every holding keeps its symbol
        return {}
    try:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(CompanyInfo)")}
        if not {"Company_Ticker", "Company_Code"} <= columns:
            return {}
        found: dict[str, str] = {}
        for symbol in symbols:
            for candidate in price_ticker_candidates(symbol):
                row = conn.execute("SELECT Company_Code FROM CompanyInfo WHERE Company_Ticker = ? LIMIT 1", (candidate,)).fetchone()
                if row and row[0]:
                    found[symbol] = str(row[0])
                    break
        return found
    finally:
        conn.close()


def sync_position_tags(store: ResearchStore, user_id: str, positions: list[dict[str, Any]]) -> dict[str, Any]:
    """Tag open positions and closed ones, moving a company between the two when its position changes."""
    open_codes = {item["code"] for item in positions if item["is_open"]}
    closed_codes = {item["code"] for item in positions if not item["is_open"]}
    opened, no_longer_open = store.replace_tag_members(user_id, OPEN_TAG, open_codes)
    closed, reopened = store.replace_tag_members(user_id, CLOSED_TAG, closed_codes)
    return {
        "open": len(open_codes),
        "closed": len(closed_codes),
        "newly_open": sorted(opened),
        "newly_closed": sorted(closed),
        "removed": sorted((no_longer_open - closed) | (reopened - opened)),
    }


def sync_from_portfolio(store: ResearchStore, user_id: str) -> dict[str, Any] | None:
    """Re-tag from the configured portfolio database; ``None`` (and no change) if it cannot be read."""
    from src.orchestrator.common.db_config import get_db2, get_db3

    try:
        positions = portfolio_positions(get_db3(), get_db2(), user_id)
    except Exception as exc:  # noqa: BLE001 - never clear tags because the portfolio was unreadable
        _LOGGER.warning("Could not read portfolio positions for tagging: %s", exc)
        return None
    return {**sync_position_tags(store, user_id, positions), "positions": positions}
