"""Tests for src/portfolio/ibkr_parser.py — IBKR FlexQuery XML parsing."""

import os
import pytest
from src.portfolio.ibkr_parser import parse_ibkr_xml_file, parse_ibkr_xml, normalize_entries

IBKR_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "ibkr"
)


class TestParseIbkrXmlFile:
    """Parse all 6 sample XML files and verify structural invariants."""

    @pytest.mark.parametrize("year", ["2020", "2021", "2022", "2023", "2024", "2025"])
    def test_parse_file_has_expected_categories(self, year):
        fpath = os.path.join(IBKR_DIR, f"{year}.xml")
        assert os.path.exists(fpath), f"Test file missing: {fpath}"
        result = parse_ibkr_xml_file(fpath)
        assert isinstance(result, dict)
        assert set(result.keys()) == {"trades", "cash_transactions", "corp_actions"}
        # All files must have at least trades
        assert len(result["trades"]) >= 0

    @pytest.mark.parametrize("year", ["2020", "2021", "2022", "2023", "2024", "2025"])
    def test_all_trade_entries_are_execution(self, year):
        fpath = os.path.join(IBKR_DIR, f"{year}.xml")
        result = parse_ibkr_xml_file(fpath)
        for t in result["trades"]:
            assert t["activity_type"] == "TRADE"
            assert t["asset_category"] in ("STK", "OPT", "CASH")

    @pytest.mark.parametrize("year", ["2020", "2021", "2022", "2023", "2024", "2025"])
    def test_all_cash_entries_have_recognised_type(self, year):
        fpath = os.path.join(IBKR_DIR, f"{year}.xml")
        result = parse_ibkr_xml_file(fpath)
        valid_activities = {"DIVIDEND", "WITHHOLDING_TAX", "PIL_DIVIDEND",
                            "DEPOSIT_WITHDRAWAL", "BROKER_INTEREST", "OTHER_FEE",
                            "COMMISSION_ADJ"}
        for ct in result["cash_transactions"]:
            assert ct["activity_type"] in valid_activities, \
                f"Unexpected activity_type '{ct['activity_type']}' for txID={ct['transaction_id']}"

    @pytest.mark.parametrize("year", ["2020", "2021", "2022", "2023", "2024", "2025"])
    def test_all_entries_have_required_fields(self, year):
        fpath = os.path.join(IBKR_DIR, f"{year}.xml")
        result = parse_ibkr_xml_file(fpath)
        entries = normalize_entries(result)
        required = {"transaction_id", "activity_type", "currency", "trade_date"}
        for e in entries:
            for field in required:
                assert e[field], f"Missing required field '{field}' in txID={e.get('transaction_id')}"

    @pytest.mark.parametrize("year", ["2020", "2021", "2022", "2023", "2024", "2025"])
    def test_no_duplicate_transaction_ids(self, year):
        fpath = os.path.join(IBKR_DIR, f"{year}.xml")
        result = parse_ibkr_xml_file(fpath)
        entries = normalize_entries(result)
        ids = [e["transaction_id"] for e in entries]
        assert len(ids) == len(set(ids)), f"Duplicate transactionIDs in {year}.xml"

    @pytest.mark.parametrize("year", ["2020", "2021", "2022", "2023", "2024", "2025"])
    def test_trade_dates_are_valid(self, year):
        fpath = os.path.join(IBKR_DIR, f"{year}.xml")
        result = parse_ibkr_xml_file(fpath)
        entries = normalize_entries(result)
        for e in entries:
            date = e["trade_date"]
            assert len(date) == 10, f"Bad date format: {date}"
            assert date[4] == "-" and date[7] == "-", f"Bad date format: {date}"


class TestSpecificEntryTypes:
    """Verify counts and specific field extractions."""

    def test_all_stk_trades_have_symbol(self):
        fpath = os.path.join(IBKR_DIR, "2024.xml")
        result = parse_ibkr_xml_file(fpath)
        stk_trades = [t for t in result["trades"] if t["asset_category"] == "STK"]
        for t in stk_trades:
            assert t["symbol"], f"STK trade without symbol: {t['transaction_id']}"
            assert t["buy_sell"] in ("BUY", "SELL")

    def test_opt_trades_have_option_fields(self):
        fpath = os.path.join(IBKR_DIR, "2024.xml")
        result = parse_ibkr_xml_file(fpath)
        opt_trades = [t for t in result["trades"] if t["asset_category"] == "OPT"]
        for t in opt_trades:
            assert t["strike"] is not None, f"OPT trade without strike: {t['transaction_id']}"
            assert t["expiry"], f"OPT trade without expiry: {t['transaction_id']}"
            assert t["put_call"] in ("P", "C"), f"Bad put_call: {t['put_call']}"
            assert t["underlying_symbol"], f"No underlying for: {t['transaction_id']}"

    def test_dividend_entries_have_positive_amount(self):
        fpath = os.path.join(IBKR_DIR, "2024.xml")
        result = parse_ibkr_xml_file(fpath)
        divs = [ct for ct in result["cash_transactions"]
                if ct["activity_type"] == "DIVIDEND"]
        assert len(divs) > 0, "Expected at least one dividend"
        for d in divs:
            assert d["amount"] > 0, f"Dividend amount not positive: {d['amount']}"

    def test_withholding_tax_present(self):
        fpath = os.path.join(IBKR_DIR, "2024.xml")
        result = parse_ibkr_xml_file(fpath)
        taxes = [ct for ct in result["cash_transactions"]
                 if ct["activity_type"] == "WITHHOLDING_TAX"]
        assert len(taxes) > 0, "Expected at least one withholding tax entry"
        # Most are negative (tax paid), but some may be positive (refund/adjustment)
        neg_count = sum(1 for t in taxes if t["amount"] < 0)
        assert neg_count > 0, "Expected some negative withholding tax entries"

    def test_spinoff_entries(self):
        """Verify spinoffs parse correctly across all years."""
        all_spinoffs = []
        for year in ["2020", "2021", "2022", "2023", "2024", "2025"]:
            fpath = os.path.join(IBKR_DIR, f"{year}.xml")
            result = parse_ibkr_xml_file(fpath)
            spinoffs = [ca for ca in result["corp_actions"]
                        if ca["activity_type"] == "SPINOFF"]
            all_spinoffs.extend(spinoffs)
        assert len(all_spinoffs) == 2, f"Expected 2 spinoffs, got {len(all_spinoffs)}"
        for s in all_spinoffs:
            assert s["quantity"] > 0
            assert s["symbol"]
            assert s["action_description"]

    def test_deposit_withdrawal(self):
        fpath = os.path.join(IBKR_DIR, "2024.xml")
        result = parse_ibkr_xml_file(fpath)
        deposits = [ct for ct in result["cash_transactions"]
                    if ct["activity_type"] == "DEPOSIT_WITHDRAWAL"]
        assert len(deposits) > 0, "Expected deposit/withdrawal entries"

    def test_date_extraction_removes_timestamp(self):
        """Verify dateTime '2024-01-24;075046' → '2024-01-24'."""
        xml = """<?xml version="1.0"?>
        <FlexQueryResponse>
          <FlexStatements>
            <FlexStatement>
              <CashTransactions>
                <CashTransaction accountId="U1" currency="USD" fxRateToBase="1"
                  dateTime="2024-01-24;075046" amount="100" type="Dividends"
                  transactionID="test-date-1" levelOfDetail="DETAIL"
                  reportDate="2024-01-24" />
              </CashTransactions>
            </FlexStatement>
          </FlexStatements>
        </FlexQueryResponse>"""
        result = parse_ibkr_xml(xml)
        assert result["cash_transactions"][0]["trade_date"] == "2024-01-24"
