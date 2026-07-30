"""Tests for IBKR FlexQuery parsing using synthetic, non-sensitive data."""

from __future__ import annotations

import pytest

from src.portfolio.ibkr_parser import (
    InvalidPortfolioXML,
    normalize_entries,
    parse_ibkr_xml,
    parse_ibkr_xml_file,
)


@pytest.mark.parametrize(
    "payload",
    [
        "<!DOCTYPE FlexQueryResponse><FlexQueryResponse />",
        "<!ENTITY secret SYSTEM 'file:///etc/passwd'><FlexQueryResponse />",
    ],
)
def test_rejects_dtd_and_entity_declarations(payload: str) -> None:
    with pytest.raises(InvalidPortfolioXML, match="DTD and entity"):
        parse_ibkr_xml(payload)


def test_rejects_non_ibkr_root() -> None:
    with pytest.raises(InvalidPortfolioXML, match="not an IBKR"):
        parse_ibkr_xml("<NotAnIbkrDocument />")


def test_rejects_malformed_or_non_utf8_content() -> None:
    with pytest.raises(InvalidPortfolioXML, match="malformed"):
        parse_ibkr_xml("<FlexQueryResponse>")
    with pytest.raises(InvalidPortfolioXML, match="UTF-8"):
        parse_ibkr_xml(b"\xff\xfe")


def test_empty_flex_response_returns_empty_categories() -> None:
    result = parse_ibkr_xml("<FlexQueryResponse />")
    assert result == {"trades": [], "cash_transactions": [], "corp_actions": []}


def test_missing_fx_rate_defaults_to_one() -> None:
    xml = """
    <FlexQueryResponse><Trade levelOfDetail="EXECUTION" assetCategory="STK"
      transactionID="trade-1" symbol="AAA" currency="EUR"
      tradeDate="2024-01-01" quantity="1" buySell="BUY" /></FlexQueryResponse>
    """
    trade = parse_ibkr_xml(xml)["trades"][0]
    assert trade["fx_rate_to_base"] == 1.0


def test_file_wrapper_parses_all_supported_categories(sample_ibkr_file) -> None:
    result = parse_ibkr_xml_file(str(sample_ibkr_file))

    assert set(result) == {"trades", "cash_transactions", "corp_actions"}
    assert len(result["trades"]) == 3
    assert len(result["cash_transactions"]) == 4
    assert len(result["corp_actions"]) == 1


def test_trade_fields_are_normalized(sample_ibkr_content: str) -> None:
    trades = parse_ibkr_xml(sample_ibkr_content)["trades"]
    stock = next(entry for entry in trades if entry["symbol"] == "AAA")
    option = next(entry for entry in trades if entry["asset_category"] == "OPT")

    assert stock["activity_type"] == "TRADE"
    assert stock["buy_sell"] == "BUY"
    assert stock["quantity"] == 10
    assert stock["commission"] == -1
    assert stock["settle_date"] == "2024-01-04"
    assert option["strike"] == 120
    assert option["expiry"] == "2028-01-21"
    assert option["put_call"] == "C"
    assert option["underlying_symbol"] == "AAA"
    assert option["multiplier"] == 100


def test_cash_types_and_timestamp_are_normalized(sample_ibkr_content: str) -> None:
    cash = parse_ibkr_xml(sample_ibkr_content)["cash_transactions"]
    by_type = {entry["activity_type"]: entry for entry in cash}

    assert set(by_type) == {
        "DEPOSIT_WITHDRAWAL",
        "DIVIDEND",
        "WITHHOLDING_TAX",
        "BROKER_INTEREST",
    }
    assert by_type["DIVIDEND"]["amount"] == 20
    assert by_type["WITHHOLDING_TAX"]["amount"] == -3
    assert by_type["DIVIDEND"]["trade_date"] == "2024-01-08"


def test_spinoff_fields_are_normalized(sample_ibkr_content: str) -> None:
    actions = parse_ibkr_xml(sample_ibkr_content)["corp_actions"]

    assert actions == [
        {
            "transaction_id": "spinoff-1",
            "trade_id": None,
            "account_id": "TEST-ACCOUNT",
            "activity_type": "SPINOFF",
            "asset_category": "STK",
            "symbol": "SPIN",
            "description": "Synthetic spinoff",
            "isin": "",
            "conid": "",
            "currency": "USD",
            "trade_date": "2024-01-10",
            "settle_date": None,
            "quantity": 2.0,
            "trade_price": None,
            "trade_money": None,
            "amount": 0,
            "proceeds": None,
            "commission": 0,
            "taxes": 0,
            "net_cash": None,
            "buy_sell": None,
            "fx_rate_to_base": 0.9,
            "strike": None,
            "expiry": None,
            "put_call": None,
            "underlying_symbol": None,
            "underlying_conid": None,
            "multiplier": 1,
            "action_description": "AAA spins off SPIN",
            "action_id": "action-1",
        }
    ]


def test_unsupported_detail_levels_and_types_are_ignored(
    sample_ibkr_content: str,
) -> None:
    entries = normalize_entries(parse_ibkr_xml(sample_ibkr_content))
    transaction_ids = {entry["transaction_id"] for entry in entries}

    assert "ignored-order" not in transaction_ids
    assert "ignored-cash" not in transaction_ids


def test_normalized_entries_have_unique_ids_and_iso_dates(
    sample_ibkr_content: str,
) -> None:
    entries = normalize_entries(parse_ibkr_xml(sample_ibkr_content))
    ids = [entry["transaction_id"] for entry in entries]

    assert len(entries) == 8
    assert len(ids) == len(set(ids))
    assert all(entry["activity_type"] for entry in entries)
    assert all(entry["currency"] for entry in entries)
    assert all(len(entry["trade_date"]) == 10 for entry in entries)
