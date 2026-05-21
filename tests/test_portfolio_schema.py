"""Tests for src/portfolio/schema.py — DDL creation and Pydantic validation."""

import sqlite3
import os
import tempfile
import pytest
from src.portfolio.schema import create_tables, TransactionEntry, UploadResponse, HoldingItem, PerformanceResponse


class TestCreateTables:
    """Table creation and basic integrity checks."""

    @pytest.fixture
    def db_path(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_create_tables_idempotent(self, db_path):
        create_tables(db_path)
        create_tables(db_path)  # second call must not error

    def test_all_tables_exist(self, db_path):
        create_tables(db_path)
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor]
        conn.close()
        for t in ("Transactions", "Portfolio_Daily", "Portfolio_Holdings",
                  "Holdings_History", "Portfolio_Metrics"):
            assert t in tables, f"Table {t} missing"

    def test_transactions_unique_txn_id(self, db_path):
        create_tables(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO Transactions (transaction_id, activity_type, currency, trade_date) "
            "VALUES (?,?,?,?)",
            ("txn-1", "TRADE", "USD", "2024-01-01"),
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO Transactions (transaction_id, activity_type, currency, trade_date) "
                "VALUES (?,?,?,?)",
                ("txn-1", "TRADE", "USD", "2024-01-02"),
            )
        conn.close()

    def test_portfolio_daily_primary_key(self, db_path):
        create_tables(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute("INSERT INTO Portfolio_Daily (date) VALUES ('2024-01-01')")
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO Portfolio_Daily (date) VALUES ('2024-01-01')")
        conn.close()

    def test_holdings_primary_key(self, db_path):
        create_tables(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO Portfolio_Holdings (symbol, asset_category, quantity, currency) "
            "VALUES (?,?,?,?)",
            ("VWCE", "STK", 10, "EUR"),
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO Portfolio_Holdings (symbol, asset_category, quantity, currency) "
                "VALUES (?,?,?,?)",
                ("VWCE", "STK", 20, "EUR"),
            )
        conn.close()


class TestPydanticModels:
    """Validation of request/response model shapes."""

    def test_transaction_entry_valid(self):
        e = TransactionEntry(
            transaction_id="t1",
            activity_type="TRADE",
            currency="USD",
            trade_date="2024-01-15",
        )
        assert e.transaction_id == "t1"
        assert e.activity_type == "TRADE"

    def test_transaction_entry_missing_required(self):
        with pytest.raises(Exception):
            TransactionEntry(activity_type="TRADE", currency="USD", trade_date="2024-01-01")

    def test_upload_response_defaults(self):
        r = UploadResponse(source_file="test.xml", total_entries=0, inserted=0, skipped=0)
        assert r.by_activity == {}
        assert r.new_tickers_fetched == []

    def test_holding_item_weight_optional(self):
        h = HoldingItem(symbol="VWCE", asset_category="STK", quantity=10, currency="EUR")
        assert h.weight is None

    def test_performance_response_optional_fields(self):
        p = PerformanceResponse(start_date="2024-01-01", end_date="2024-12-31",
                                base_currency="EUR", total_dividend_income=100.0)
        assert p.sharpe_ratio is None
        assert p.benchmark is None
