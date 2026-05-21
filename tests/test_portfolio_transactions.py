"""Tests for src/portfolio/transactions.py — CRUD and deduplication."""

import os
import sqlite3
import tempfile
import pytest
from src.portfolio.schema import create_tables
from src.portfolio.transactions import (
    insert_entries,
    get_transactions,
    get_unique_symbols,
    get_date_range,
    get_activity_summary,
    delete_by_source,
)


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    create_tables(path)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


def _make_entry(**overrides):
    """Build a minimal valid transaction entry."""
    base = {
        "transaction_id": "tx-1",
        "trade_id": None,
        "account_id": "U123",
        "activity_type": "TRADE",
        "asset_category": "STK",
        "symbol": "VWCE",
        "description": "Test",
        "isin": None,
        "conid": None,
        "currency": "EUR",
        "trade_date": "2024-01-15",
        "settle_date": None,
        "quantity": 10,
        "trade_price": 100.0,
        "trade_money": 1000.0,
        "amount": 0,
        "proceeds": -1000.0,
        "commission": 3.0,
        "taxes": 0.0,
        "net_cash": -1003.0,
        "buy_sell": "BUY",
        "fx_rate_to_base": 1.0,
        "strike": None,
        "expiry": None,
        "put_call": None,
        "underlying_symbol": None,
        "underlying_conid": None,
        "multiplier": 1,
        "action_description": None,
        "action_id": None,
    }
    base.update(overrides)
    return base


class TestInsert:
    def test_insert_single(self, db_path):
        result = insert_entries(db_path, [_make_entry()], source_file="test.xml")
        assert result["inserted"] == 1
        assert result["skipped"] == 0
        assert result["by_activity"] == {"TRADE": 1}

    def test_dedup_on_transaction_id(self, db_path):
        e1 = _make_entry(transaction_id="tx-1", trade_date="2024-01-15")
        result1 = insert_entries(db_path, [e1])
        assert result1["inserted"] == 1
        # Re-insert same transaction
        result2 = insert_entries(db_path, [e1])
        assert result2["inserted"] == 0
        assert result2["skipped"] == 1

    def test_multiple_activity_types(self, db_path):
        entries = [
            _make_entry(transaction_id="tx-1", activity_type="TRADE"),
            _make_entry(transaction_id="tx-2", activity_type="DIVIDEND",
                        trade_price=None, trade_money=None, quantity=0, amount=50.0),
            _make_entry(transaction_id="tx-3", activity_type="DEPOSIT_WITHDRAWAL",
                        trade_price=None, trade_money=None, quantity=0, amount=1000.0),
        ]
        result = insert_entries(db_path, entries)
        assert result["inserted"] == 3
        assert result["by_activity"]["TRADE"] == 1
        assert result["by_activity"]["DIVIDEND"] == 1
        assert result["by_activity"]["DEPOSIT_WITHDRAWAL"] == 1

    def test_empty_entries(self, db_path):
        result = insert_entries(db_path, [])
        assert result["inserted"] == 0
        assert result["skipped"] == 0

    def test_source_file_stored(self, db_path):
        insert_entries(db_path, [_make_entry()], source_file="2024.xml")
        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT source_file FROM Transactions LIMIT 1").fetchone()
        conn.close()
        assert row[0] == "2024.xml"


class TestQueries:
    def test_get_transactions_by_symbol(self, db_path):
        e1 = _make_entry(transaction_id="tx-1", symbol="VWCE", trade_date="2024-01-15")
        e2 = _make_entry(transaction_id="tx-2", symbol="JXN", trade_date="2024-02-01")
        insert_entries(db_path, [e1, e2])
        rows = get_transactions(db_path, symbol="VWCE")
        assert len(rows) == 1
        assert rows[0]["symbol"] == "VWCE"

    def test_get_transactions_by_date_range(self, db_path):
        e1 = _make_entry(transaction_id="tx-1", trade_date="2024-01-01")
        e2 = _make_entry(transaction_id="tx-2", trade_date="2024-06-15")
        e3 = _make_entry(transaction_id="tx-3", trade_date="2024-12-31")
        insert_entries(db_path, [e1, e2, e3])
        rows = get_transactions(db_path, start_date="2024-06-01", end_date="2024-12-31")
        assert len(rows) == 2

    def test_get_unique_symbols(self, db_path):
        entries = [
            _make_entry(transaction_id="tx-1", symbol="VWCE"),
            _make_entry(transaction_id="tx-2", symbol="JXN"),
            _make_entry(transaction_id="tx-3", symbol="VWCE"),
        ]
        insert_entries(db_path, entries)
        symbols = get_unique_symbols(db_path)
        assert len(symbols) == 2
        syms = {s["symbol"] for s in symbols}
        assert "VWCE" in syms
        assert "JXN" in syms

    def test_get_date_range(self, db_path):
        entries = [
            _make_entry(transaction_id="tx-1", trade_date="2023-05-01"),
            _make_entry(transaction_id="tx-2", trade_date="2024-12-31"),
        ]
        insert_entries(db_path, entries)
        r = get_date_range(db_path)
        assert r["min_date"] == "2023-05-01"
        assert r["max_date"] == "2024-12-31"

    def test_get_activity_summary(self, db_path):
        entries = [
            _make_entry(transaction_id="tx-1", activity_type="TRADE"),
            _make_entry(transaction_id="tx-2", activity_type="DIVIDEND",
                        trade_price=None, trade_money=None, quantity=0, amount=50.0),
        ]
        insert_entries(db_path, entries)
        summary = get_activity_summary(db_path)
        assert summary["TRADE"] == 1
        assert summary["DIVIDEND"] == 1


class TestDelete:
    def test_delete_by_source(self, db_path):
        e1 = _make_entry(transaction_id="tx-1")
        e2 = _make_entry(transaction_id="tx-2")
        insert_entries(db_path, [e1], source_file="a.xml")
        insert_entries(db_path, [e2], source_file="b.xml")
        deleted = delete_by_source(db_path, "a.xml")
        assert deleted == 1
        remaining = get_transactions(db_path)
        assert len(remaining) == 1
        assert remaining[0]["source_file"] == "b.xml"
