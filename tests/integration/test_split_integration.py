"""Integration tests for end-to-end split handling.

Verifies that confirmed splits produce correct adjusted prices through
the portfolio rebuild pipeline.
"""

from __future__ import annotations

import gc
import tempfile
from pathlib import Path

import sqlite3

from src.portfolio.split_schema import ensure_split_tables
from src.portfolio.split_detection import run_split_detection
from src.portfolio.portfolio_state import (
    build_portfolio_state,
    _get_adjusted_price,
    _get_price,
    _invalidate_split_cache,
)
from tests.factories import create_market_database, add_split_test_data


class TestEndToEndSplitPipeline:
    """Full pipeline: detect → confirm → rebuild → verify."""

    def test_detect_and_confirm_flow(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db2_path = Path(tmp) / "Standardized.db"
            db3_path = Path(tmp) / "Portfolio.db"

            create_market_database(db2_path)
            add_split_test_data(db2_path)

            # Create minimal Portfolio.db with Transactions
            conn3 = sqlite3.connect(str(db3_path))
            conn3.executescript(
                """
                CREATE TABLE IF NOT EXISTS Transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT NOT NULL,
                    trade_id TEXT,
                    account_id TEXT,
                    owner_user_id TEXT NOT NULL DEFAULT '',
                    activity_type TEXT NOT NULL,
                    asset_category TEXT,
                    symbol TEXT,
                    description TEXT,
                    isin TEXT,
                    conid TEXT,
                    currency TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    settle_date TEXT,
                    quantity REAL DEFAULT 0,
                    trade_price REAL,
                    trade_money REAL,
                    amount REAL DEFAULT 0,
                    proceeds REAL,
                    commission REAL DEFAULT 0,
                    taxes REAL DEFAULT 0,
                    net_cash REAL,
                    buy_sell TEXT,
                    fx_rate_to_base REAL,
                    strike REAL,
                    expiry TEXT,
                    put_call TEXT,
                    underlying_symbol TEXT,
                    underlying_conid TEXT,
                    multiplier REAL DEFAULT 1,
                    action_description TEXT,
                    action_id TEXT,
                    source_file TEXT,
                    imported_at TEXT DEFAULT (datetime('now')),
                    notes TEXT
                );
                """
            )
            conn3.execute(
                """INSERT INTO Transactions
                   (transaction_id, owner_user_id, activity_type, asset_category,
                    symbol, currency, trade_date, quantity, trade_price,
                    trade_money, buy_sell, fx_rate_to_base, net_cash)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "txn-1", "", "TRADE", "STK", "AAA", "USD",
                    "2024-01-15", 10, 95.0, 950.0,
                    "BUY", 0.9, -950.0,
                ),
            )
            conn3.execute(
                """INSERT INTO Transactions
                   (transaction_id, owner_user_id, activity_type,
                    currency, trade_date, amount, fx_rate_to_base)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    "dep-1", "", "DEPOSIT_WITHDRAWAL",
                    "EUR", "2024-01-15", 10000.0, 1.0,
                ),
            )
            conn3.commit()
            conn3.close()

            # Run split detection
            results = run_split_detection(
                str(db2_path), tickers=["AAA"], mode="full",
            )
            assert results["tickers_scanned"] >= 1

            # Confirm the split was stored (or verify existing confirmed one)
            conn2 = sqlite3.connect(str(db2_path))
            conn2.row_factory = sqlite3.Row
            splits = conn2.execute(
                "SELECT * FROM Stock_Splits WHERE ticker = 'AAA' AND confirmation = 'confirmed'"
            ).fetchall()
            conn2.close()
            assert len(splits) >= 1, "Should have at least one confirmed split for AAA"

            # Build portfolio state — should use adjusted prices
            _invalidate_split_cache()
            result = build_portfolio_state(
                db3_path=str(db3_path),
                db2_path=str(db2_path),
                owner_user_id="",
            )
            assert result["daily_rows"] > 0

            # Verify Portfolio_Daily has sensible values (no -50% daily return)
            conn3 = sqlite3.connect(str(db3_path))
            conn3.row_factory = sqlite3.Row
            daily = conn3.execute(
                "SELECT date, daily_return, cumulative_return, total_value "
                "FROM Portfolio_Daily WHERE owner_user_id = '' "
                "AND date >= '2024-06-13' AND date <= '2024-06-18' "
                "ORDER BY date"
            ).fetchall()
            conn3.close()

            # The split date (June 15, 2024) is a Saturday.
            # The market price moves from pre-split (100 → adjusted 50)
            # to post-split (50).  With split adjustment, there should be
            # no massive single-day return around the split.
            for row in daily:
                dr = row["daily_return"] or 0
                # No single day should show a -50% return
                assert dr > -0.30, (
                    f"Daily return on {row['date']} is {dr:.4f} — "
                    f"split adjustment should prevent massive drops"
                )

    def test_adjusted_vs_raw_price_history(self):
        """Verify that adjusted prices correct the pre-split prices."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db2_path = Path(tmp) / "Standardized.db"
            create_market_database(db2_path)
            add_split_test_data(db2_path)
            ensure_split_tables(str(db2_path))
            _invalidate_split_cache()

            conn = sqlite3.connect(str(db2_path))
            conn.row_factory = sqlite3.Row

            # Pre-split date
            raw_pre = _get_price(conn, "AAA", "2024-06-14")
            adj_pre = _get_adjusted_price(conn, "AAA", "2024-06-14")

            # Post-split date
            raw_post = _get_price(conn, "AAA", "2024-06-17")
            adj_post = _get_adjusted_price(conn, "AAA", "2024-06-17")

            conn.close()

            # Raw prices show ~50% drop
            assert raw_pre is not None
            assert raw_post is not None
            raw_drop = (raw_post - raw_pre) / raw_pre
            assert raw_drop < -0.30, (
                f"Raw prices should show a large drop, got {raw_drop:.2%}"
            )

            # Adjusted prices should be continuous (~0% single-day gap)
            adj_drop = (adj_post - adj_pre) / adj_pre if adj_pre else 0
            assert abs(adj_drop) < 0.10, (
                f"Adjusted prices should be near-continuous, got {adj_drop:.2%}"
            )

            # Pre-split adjusted = pre-split raw / 2
            expected_adj = raw_pre / 2.0
            assert abs(adj_pre - expected_adj) < 0.01

    def test_no_splits_is_noop(self):
        """Without splits, adjusted prices equal raw prices."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db2_path = Path(tmp) / "Standardized.db"
            create_market_database(db2_path)
            # Do NOT add split data

            conn = sqlite3.connect(str(db2_path))
            conn.row_factory = sqlite3.Row

            raw = _get_price(conn, "AAA", "2024-06-14")
            adj = _get_adjusted_price(conn, "AAA", "2024-06-14")

            conn.close()

            assert raw is not None
            assert adj == raw, (
                f"Without splits, adjusted ({adj}) should equal raw ({raw})"
            )
