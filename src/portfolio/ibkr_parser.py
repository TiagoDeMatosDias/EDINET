"""Parses IBKR FlexQuery XML into normalized internal representations.

Handles three element types across all relevant levels of detail:

- ``<Trade levelOfDetail="EXECUTION">`` — STK, OPT, and CASH trades
- ``<CashTransaction levelOfDetail="DETAIL">`` — dividends, taxes, deposits, etc.
- ``<CorporateAction levelOfDetail="DETAIL">`` — spinoffs (type="SO")

Returns a flat list of dicts suitable for insertion into the Transactions table.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# XML elements we extract
_TRADE_TAG = "Trade"
_CASH_TXN_TAG = "CashTransaction"
_CORP_ACTION_TAG = "CorporateAction"

# levelOfDetail values we accept
_EXECUTION_LOD = "EXECUTION"
_DETAIL_LOD = "DETAIL"

# Mapping from XML CashTransaction type → our activity_type
_CASH_TYPE_MAP: dict[str, str] = {
    "Dividends":                  "DIVIDEND",
    "Withholding Tax":            "WITHHOLDING_TAX",
    "Payment In Lieu Of Dividends": "PIL_DIVIDEND",
    "Deposits/Withdrawals":       "DEPOSIT_WITHDRAWAL",
    "Broker Interest Paid":       "BROKER_INTEREST",
    "Other Fees":                 "OTHER_FEE",
    "Commission Adjustments":     "COMMISSION_ADJ",
}

# Mapping from XML CorporateAction type → activity_type  
_CORP_TYPE_MAP: dict[str, str] = {
    "SO": "SPINOFF",
}


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _extract_date(date_time_str: str) -> str:
    """Extract just the date from IBKR dateTime (e.g. '2024-01-24;075046' → '2024-01-24').
    
    Handles also bare dates like '2024-01-24'.
    """
    if not date_time_str:
        return ""
    return date_time_str.split(";")[0].strip()


def _safe_float(val: Any) -> float | None:
    """Convert a string to float, returning None for empty/invalid."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _safe_str(val: Any) -> str:
    """Return a trimmed string, empty string for None."""
    if val is None:
        return ""
    return str(val).strip()


# ---------------------------------------------------------------------------
# Trade (EXECUTION level) parser
# ---------------------------------------------------------------------------

def _parse_trade(el: ET.Element) -> dict | None:
    """Parse a <Trade> element (EXECUTION level) into a normalized dict."""
    lod = _safe_str(el.get("levelOfDetail"))
    if lod != _EXECUTION_LOD:
        return None

    asset = _safe_str(el.get("assetCategory"))
    if not asset:
        logger.debug("Skipping Trade without assetCategory (transactionID=%s)", 
                      el.get("transactionID", "?"))
        return None

    return {
        "transaction_id":    _safe_str(el.get("transactionID")),
        "trade_id":          _safe_str(el.get("tradeID")),
        "account_id":        _safe_str(el.get("accountId")),
        "activity_type":     "TRADE",
        "asset_category":    asset,
        "symbol":            _safe_str(el.get("symbol")),
        "description":       _safe_str(el.get("description")),
        "isin":              _safe_str(el.get("isin")),
        "conid":             _safe_str(el.get("conid")),
        "currency":          _safe_str(el.get("currency")),
        "trade_date":        _safe_str(el.get("tradeDate")),
        "settle_date":       _extract_date(_safe_str(el.get("settleDateTarget"))),
        "quantity":          _safe_float(el.get("quantity")) or 0,
        "trade_price":       _safe_float(el.get("tradePrice")),
        "trade_money":       _safe_float(el.get("tradeMoney")),
        "amount":            0,
        "proceeds":          _safe_float(el.get("proceeds")),
        "commission":        _safe_float(el.get("ibCommission")) or 0,
        "taxes":             _safe_float(el.get("taxes")) or 0,
        "net_cash":          _safe_float(el.get("netCash")),
        "buy_sell":          _safe_str(el.get("buySell")),
        "fx_rate_to_base":   _safe_float(el.get("fxRateToBase")) or 1.0,
        "strike":            _safe_float(el.get("strike")),
        "expiry":            _safe_str(el.get("expiry")),
        "put_call":          _safe_str(el.get("putCall")),
        "underlying_symbol": _safe_str(el.get("underlyingSymbol")),
        "underlying_conid":  _safe_str(el.get("underlyingConid")),
        "multiplier":        _safe_float(el.get("multiplier")) or 1,
        "action_description": None,
        "action_id":         None,
    }


# ---------------------------------------------------------------------------
# CashTransaction (DETAIL level) parser
# ---------------------------------------------------------------------------

def _parse_cash_transaction(el: ET.Element) -> dict | None:
    """Parse a <CashTransaction> element (DETAIL level) into a normalized dict."""
    lod = _safe_str(el.get("levelOfDetail"))
    if lod != _DETAIL_LOD:
        return None

    xml_type = _safe_str(el.get("type"))
    activity_type = _CASH_TYPE_MAP.get(xml_type)
    if activity_type is None:
        logger.debug("Unrecognized CashTransaction type '%s' (txID=%s)", 
                      xml_type, el.get("transactionID", "?"))
        return None

    return {
        "transaction_id":    _safe_str(el.get("transactionID")),
        "trade_id":          _safe_str(el.get("tradeID")),
        "account_id":        _safe_str(el.get("accountId")),
        "activity_type":     activity_type,
        "asset_category":    _safe_str(el.get("assetCategory")),
        "symbol":            _safe_str(el.get("symbol")),
        "description":       _safe_str(el.get("description")),
        "isin":              _safe_str(el.get("isin")),
        "conid":             _safe_str(el.get("conid")),
        "currency":          _safe_str(el.get("currency")),
        "trade_date":        _extract_date(_safe_str(el.get("dateTime"))),
        "settle_date":       _extract_date(_safe_str(el.get("settleDate"))),
        "quantity":          0,
        "trade_price":       None,
        "trade_money":       None,
        "amount":            _safe_float(el.get("amount")) or 0,
        "proceeds":          None,
        "commission":        0,
        "taxes":             0,
        "net_cash":          None,
        "buy_sell":          None,
        "fx_rate_to_base":   _safe_float(el.get("fxRateToBase")) or 1.0,
        "strike":            None,
        "expiry":            None,
        "put_call":          None,
        "underlying_symbol": None,
        "underlying_conid":  None,
        "multiplier":        1,
        "action_description": _safe_str(el.get("description")),
        "action_id":         None,
    }


# ---------------------------------------------------------------------------
# CorporateAction (DETAIL level) parser
# ---------------------------------------------------------------------------

def _parse_corp_action(el: ET.Element) -> dict | None:
    """Parse a <CorporateAction> element (DETAIL level) into a normalized dict."""
    lod = _safe_str(el.get("levelOfDetail"))
    if lod != _DETAIL_LOD:
        return None

    xml_type = _safe_str(el.get("type"))
    activity_type = _CORP_TYPE_MAP.get(xml_type)
    if activity_type is None:
        logger.debug("Unrecognized CorporateAction type '%s' (txID=%s)",
                      xml_type, el.get("transactionID", "?"))
        return None

    return {
        "transaction_id":    _safe_str(el.get("transactionID")),
        "trade_id":          None,
        "account_id":        _safe_str(el.get("accountId")),
        "activity_type":     activity_type,
        "asset_category":    _safe_str(el.get("assetCategory")),
        "symbol":            _safe_str(el.get("symbol")),
        "description":       _safe_str(el.get("description")),
        "isin":              _safe_str(el.get("isin")),
        "conid":             _safe_str(el.get("conid")),
        "currency":          _safe_str(el.get("currency")),
        "trade_date":        _extract_date(_safe_str(el.get("dateTime"))),
        "settle_date":       None,
        "quantity":          _safe_float(el.get("quantity")) or 0,
        "trade_price":       None,
        "trade_money":       None,
        "amount":            _safe_float(el.get("amount")) or 0,
        "proceeds":          _safe_float(el.get("proceeds")),
        "commission":        0,
        "taxes":             0,
        "net_cash":          None,
        "buy_sell":          None,
        "fx_rate_to_base":   _safe_float(el.get("fxRateToBase")) or 1.0,
        "strike":            None,
        "expiry":            None,
        "put_call":          None,
        "underlying_symbol": None,
        "underlying_conid":  None,
        "multiplier":        _safe_float(el.get("multiplier")) or 1,
        "action_description": _safe_str(el.get("actionDescription")),
        "action_id":         _safe_str(el.get("actionID")),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_ibkr_xml(xml_content: str | bytes) -> dict[str, list[dict]]:
    """Parse IBKR FlexQuery XML into categorized lists: trades, cash_transactions, corp_actions.

    Returns a dict with keys ``trades``, ``cash_transactions``, ``corp_actions``,
    each containing a list of normalized entry dicts.
    """
    root = ET.fromstring(xml_content) if isinstance(xml_content, str) else \
           ET.fromstring(xml_content.decode("utf-8"))

    result: dict[str, list[dict]] = {
        "trades": [],
        "cash_transactions": [],
        "corp_actions": [],
    }

    for trade_el in root.iter(_TRADE_TAG):
        entry = _parse_trade(trade_el)
        if entry:
            result["trades"].append(entry)

    for cash_el in root.iter(_CASH_TXN_TAG):
        entry = _parse_cash_transaction(cash_el)
        if entry:
            result["cash_transactions"].append(entry)

    for corp_el in root.iter(_CORP_ACTION_TAG):
        entry = _parse_corp_action(corp_el)
        if entry:
            result["corp_actions"].append(entry)

    logger.info("Parsed XML: %d trades, %d cash txns, %d corp actions",
                 len(result["trades"]), len(result["cash_transactions"]),
                 len(result["corp_actions"]))
    return result


def parse_ibkr_xml_file(filepath: str) -> dict[str, list[dict]]:
    """Convenience wrapper: parse IBKR XML from a file path."""
    with open(filepath, "r", encoding="utf-8") as f:
        return parse_ibkr_xml(f.read())


def normalize_entries(xml_data: dict[str, list[dict]]) -> list[dict]:
    """Flatten categorized XML data into a single list of normalized dicts."""
    entries = []
    entries.extend(xml_data.get("trades", []))
    entries.extend(xml_data.get("cash_transactions", []))
    entries.extend(xml_data.get("corp_actions", []))
    return entries
