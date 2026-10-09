"""Unit tests for split detection and price adjustment logic."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from src.portfolio.portfolio_state import (
    _get_adjusted_price,
    _get_price,
    _invalidate_split_cache,
    _load_split_factors,
)
from src.portfolio.split_detection import (
    _round_split_ratio,
    detect_splits_by_price_heuristic,
    run_split_detection,
    verify_split_with_share_metrics,
)
from src.portfolio.split_schema import ensure_split_tables
from tests.factories import add_split_test_data, create_market_database

# ── helpers ─────────────────────────────────────────────────────────────────


def _make_in_memory_db() -> sqlite3.Connection:
    """Create an in-memory db2 with Stock_Prices and a ticker with a split."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE Stock_Prices (
            Date TEXT NOT NULL,
            Ticker TEXT NOT NULL,
            Currency TEXT NOT NULL,
            Price REAL NOT NULL,
            PRIMARY KEY (Date, Ticker, Currency)
        );
        CREATE TABLE CompanyInfo (
            Company_Code TEXT PRIMARY KEY,
            Company_Ticker TEXT NOT NULL,
            Company_Name TEXT NOT NULL
        );
        CREATE TABLE FinancialStatements (
            docID TEXT PRIMARY KEY,
            Company_Code TEXT NOT NULL,
            periodEnd TEXT NOT NULL
        );
        CREATE TABLE ShareMetrics (
            docID TEXT PRIMARY KEY,
            "Number of issued shares as of filing date" REAL
        );
    """
    )
    return conn


def _seed_split_prices(conn: sqlite3.Connection) -> None:
    """Insert a clean 2:1 split simulation.

    Price drops from 104 to 52 on consecutive trading days (June 13→14),
    which is exactly a 50% drop → 2:1 split.
    """
    rows = [
        ("2024-06-10", "SPLITCO", "USD", 100.0),
        ("2024-06-11", "SPLITCO", "USD", 101.0),
        ("2024-06-12", "SPLITCO", "USD", 102.0),
        ("2024-06-13", "SPLITCO", "USD", 104.0),
        ("2024-06-14", "SPLITCO", "USD", 52.0),  # 50% drop → 2:1 split
        ("2024-06-17", "SPLITCO", "USD", 53.0),
        ("2024-06-18", "SPLITCO", "USD", 54.0),
    ]
    conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", rows)
    conn.commit()


def _seed_share_metrics(conn: sqlite3.Connection) -> None:
    """Insert CompanyInfo, FinancialStatements, ShareMetrics for SPLITCO."""
    conn.execute(
        "INSERT INTO CompanyInfo VALUES ('E99999', 'SPLITCO', 'Split Test Co')"
    )
    conn.execute(
        "INSERT INTO FinancialStatements VALUES ('DOC-2023', 'E99999', '2024-03-31')"
    )
    conn.execute(
        "INSERT INTO FinancialStatements VALUES ('DOC-2024', 'E99999', '2025-03-31')"
    )
    conn.execute(
        "INSERT INTO ShareMetrics VALUES ('DOC-2023', 5_000_000)"
    )
    conn.execute(
        "INSERT INTO ShareMetrics VALUES ('DOC-2024', 10_000_000)"
    )
    conn.commit()


def _seed_no_share_metrics(conn: sqlite3.Connection) -> None:
    """Insert CompanyInfo + FinancialStatements without ShareMetrics."""
    conn.execute(
        "INSERT INTO CompanyInfo VALUES ('E99999', 'SPLITCO', 'Split Test Co')"
    )
    conn.execute(
        "INSERT INTO FinancialStatements VALUES ('DOC-2023', 'E99999', '2024-03-31')"
    )
    conn.execute(
        "INSERT INTO FinancialStatements VALUES ('DOC-2024', 'E99999', '2025-03-31')"
    )
    conn.commit()


# ── ratio rounding ──────────────────────────────────────────────────────────


def test_round_split_ratio_forward():
    assert _round_split_ratio(2.0) == (1, 2)
    assert _round_split_ratio(3.05) == (1, 3)
    assert _round_split_ratio(10.0) == (1, 10)


def test_round_split_ratio_reverse():
    assert _round_split_ratio(0.5) == (2, 1)
    assert _round_split_ratio(0.1) == (10, 1)


def test_round_split_ratio_no_split():
    assert _round_split_ratio(1.0) == (1, 1)


# ── price heuristic ─────────────────────────────────────────────────────────


class TestPriceHeuristic:
    def test_detects_forward_split(self):
        conn = _make_in_memory_db()
        _seed_split_prices(conn)
        candidates = detect_splits_by_price_heuristic(conn, "SPLITCO")
        assert len(candidates) == 1
        c = candidates[0]
        assert c["split_date"] == "2024-06-14"
        assert c["ratio_from"] == 1
        assert c["ratio_to"] == 2

    def test_no_split_on_small_drop(self):
        conn = _make_in_memory_db()
        # 25% drop — below the default 40% threshold
        rows = [
            ("2024-06-13", "NODROP", "USD", 100.0),
            ("2024-06-14", "NODROP", "USD", 75.0),
        ]
        conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", rows)
        conn.commit()
        candidates = detect_splits_by_price_heuristic(conn, "NODROP")
        assert len(candidates) == 0

    def test_reverse_split_detection(self):
        conn = _make_in_memory_db()
        # Price spike → reverse split
        rows = [
            ("2024-06-13", "REVCO", "USD", 10.0),
            ("2024-06-14", "REVCO", "USD", 100.0),  # 10x spike
        ]
        conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", rows)
        conn.commit()
        candidates = detect_splits_by_price_heuristic(conn, "REVCO")
        assert len(candidates) == 1
        c = candidates[0]
        assert c["ratio_from"] == 10
        assert c["ratio_to"] == 1

    def test_fractional_reverse_split_preserves_ratio_to(self):
        conn = _make_in_memory_db()
        rows = [
            ("2024-06-13", "REVFRAC", "USD", 100.0),
            ("2024-06-14", "REVFRAC", "USD", 250.0),  # 5:2 reverse split
        ]
        conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", rows)
        conn.commit()

        candidates = detect_splits_by_price_heuristic(conn, "REVFRAC")
        assert len(candidates) == 1
        assert candidates[0]["ratio_from"] == 5
        assert candidates[0]["ratio_to"] == 2

    def test_skips_large_gaps(self):
        conn = _make_in_memory_db()
        rows = [
            ("2024-01-01", "GAPCO", "USD", 100.0),
            # 30-day gap → skip
            ("2024-01-31", "GAPCO", "USD", 50.0),
        ]
        conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", rows)
        conn.commit()
        candidates = detect_splits_by_price_heuristic(conn, "GAPCO")
        # Gap > 10 days should be skipped
        assert len(candidates) == 0

    def test_empty_ticker(self):
        conn = _make_in_memory_db()
        candidates = detect_splits_by_price_heuristic(conn, "NOEXIST")
        assert candidates == []

    def test_zero_price_is_skipped_not_divided_by(self):
        # A zero print used to crash the whole step with ZeroDivisionError.
        conn = _make_in_memory_db()
        conn.executemany(
            "INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)",
            [
                ("2024-06-10", "ZERO", "JPY", 100.0),
                ("2024-06-11", "ZERO", "JPY", 0.0),
                ("2024-06-12", "ZERO", "JPY", -1.0),
                ("2024-06-13", "ZERO", "JPY", 101.0),
            ],
        )
        assert detect_splits_by_price_heuristic(conn, "ZERO") == []

    def test_split_across_a_bad_print_is_still_found(self):
        conn = _make_in_memory_db()
        conn.executemany(
            "INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)",
            [
                ("2024-06-10", "BADPRINT", "JPY", 100.0),
                ("2024-06-11", "BADPRINT", "JPY", 0.0),
                ("2024-06-12", "BADPRINT", "JPY", 50.0),
            ],
        )
        [candidate] = detect_splits_by_price_heuristic(conn, "BADPRINT")
        assert (candidate["split_date"], candidate["ratio_from"], candidate["ratio_to"]) == ("2024-06-12", 1, 2)


# ── ShareMetrics cross-validation ───────────────────────────────────────────


class TestShareMetricsVerification:
    def test_confirms_matching_ratio(self):
        conn = _make_in_memory_db()
        _seed_split_prices(conn)
        _seed_share_metrics(conn)
        verdict = verify_split_with_share_metrics(
            conn, "SPLITCO", "2024-06-14", 1, 2,
        )
        assert verdict["confirmation"] == "confirmed"
        assert verdict["confirmed_by"] == "share_metrics"
        assert verdict["share_count_before"] == 5_000_000
        assert verdict["share_count_after"] == 10_000_000
        assert abs(verdict["share_count_ratio"] - 2.0) < 0.001

    def test_reverse_split_report_price_cross_check_uses_reciprocal(self):
        conn = _make_in_memory_db()
        _seed_share_metrics(conn)
        conn.execute("ALTER TABLE FinancialStatements ADD COLUMN SharePrice REAL")
        conn.execute(
            "UPDATE FinancialStatements SET SharePrice = CASE "
            "WHEN docID = 'DOC-2023' THEN 100 ELSE 250 END"
        )
        conn.execute(
            'UPDATE ShareMetrics SET "Number of issued shares as of filing date" = ? '
            "WHERE docID = 'DOC-2024'",
            (2_000_000,),
        )
        conn.commit()

        verdict = verify_split_with_share_metrics(
            conn, "SPLITCO", "2024-06-14", 5, 2,
        )
        assert verdict["confirmation"] == "confirmed"
        assert verdict["share_count_ratio"] == pytest.approx(0.4)

    def test_parses_comma_separated_share_counts(self):
        conn = _make_in_memory_db()
        _seed_split_prices(conn)
        _seed_share_metrics(conn)
        conn.execute(
            'UPDATE ShareMetrics SET "Number of issued shares as of filing date" = ? '
            "WHERE docID = 'DOC-2023'",
            ("5,000,000",),
        )
        conn.execute(
            'UPDATE ShareMetrics SET "Number of issued shares as of filing date" = ? '
            "WHERE docID = 'DOC-2024'",
            ("10,000,000",),
        )
        conn.commit()

        verdict = verify_split_with_share_metrics(
            conn, "SPLITCO", "2024-06-14", 1, 2,
        )
        assert verdict["confirmation"] == "confirmed"
        assert verdict["share_count_before"] == 5_000_000
        assert verdict["share_count_after"] == 10_000_000

    def test_pending_on_missing_share_metrics(self):
        conn = _make_in_memory_db()
        _seed_split_prices(conn)
        _seed_no_share_metrics(conn)
        verdict = verify_split_with_share_metrics(
            conn, "SPLITCO", "2024-06-14", 1, 2,
        )
        assert verdict["confirmation"] == "pending"

    def test_rejects_far_mismatch(self):
        conn = _make_in_memory_db()
        _seed_split_prices(conn)
        _seed_share_metrics(conn)
        # Claim a 10:1 split — but ShareMetrics shows 2:1
        verdict = verify_split_with_share_metrics(
            conn, "SPLITCO", "2024-06-14", 1, 10,
        )
        assert verdict["confirmation"] == "rejected"

    def test_a_lasting_fall_with_an_unmoved_share_count_is_rejected(self):
        conn = _make_in_memory_db()
        _seed_split_prices(conn)
        _seed_share_metrics(conn)
        conn.execute("UPDATE ShareMetrics SET \"Number of issued shares as of filing date\" = 5_000_000 WHERE docID = 'DOC-2024'")
        conn.execute("INSERT INTO FinancialStatements VALUES ('DOC-2025', 'E99999', '2026-03-31')")
        conn.execute("INSERT INTO ShareMetrics VALUES ('DOC-2025', 7_142_857)")
        conn.commit()
        # A 30 % fall the heuristic read as 10-for-7. A split with its record
        # date at the year end shows only a year later: still pending.
        verdict = verify_split_with_share_metrics(conn, "SPLITCO", "2024-06-14", 7, 10)
        assert verdict["confirmation"] == "pending"
        # The count never moved: a price move, not a split.
        conn.execute("UPDATE ShareMetrics SET \"Number of issued shares as of filing date\" = 5_000_000 WHERE docID = 'DOC-2025'")
        conn.commit()
        verdict = verify_split_with_share_metrics(conn, "SPLITCO", "2024-06-14", 7, 10)
        assert verdict["confirmation"] == "rejected"
        assert "did not move" in verdict["detail"]

    def test_two_splits_in_one_year_keep_the_candidate_pending(self):
        conn = _make_in_memory_db()
        _seed_split_prices(conn)
        _seed_share_metrics(conn)
        # The reports show 2:1, which a 1:2 candidate explains only with a
        # second split; the first split stays for review instead.
        verdict = verify_split_with_share_metrics(
            conn, "SPLITCO", "2024-06-14", 4, 1,
        )
        assert verdict["confirmation"] == "rejected"
        conn.execute('UPDATE ShareMetrics SET "Number of issued shares as of filing date" = 60000000 WHERE docID = \'DOC-2024\'')
        conn.commit()
        verdict = verify_split_with_share_metrics(
            conn, "SPLITCO", "2024-06-14", 1, 2,
        )
        assert verdict["confirmation"] == "pending"

    def test_a_price_move_that_does_not_last_is_not_a_split(self):
        from src.portfolio.split_detection import _reject_if_price_returned

        conn = _make_in_memory_db()
        _seed_split_prices(conn)
        # An illiquid ¥20 stock ticks to ¥10 for a day.
        conn.executemany(
            "INSERT INTO Stock_Prices VALUES (?, 'TICKCO', 'JPY', ?)",
            [("2024-06-10", 20.0), ("2024-06-11", 20.0), ("2024-06-12", 10.0), ("2024-06-13", 20.0), ("2024-06-14", 20.0)],
        )
        pending = {"confirmation": "pending", "detail": "Could not find ShareMetrics straddling the split date"}
        assert _reject_if_price_returned(conn, "TICKCO", "2024-06-12", 1, 2, pending)["confirmation"] == "rejected"
        assert _reject_if_price_returned(conn, "SPLITCO", "2024-06-14", 1, 2, pending)["confirmation"] == "pending"

    def test_pending_on_unknown_ticker(self):
        conn = _make_in_memory_db()
        verdict = verify_split_with_share_metrics(
            conn, "NOEXIST", "2024-06-14", 1, 2,
        )
        assert verdict["confirmation"] == "pending"


# ── adjustment factor math ──────────────────────────────────────────────────


def _create_stock_splits_in_conn(conn: sqlite3.Connection) -> None:
    """Create the Stock_Splits table in the given connection (for in-memory tests)."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS Stock_Splits (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker            TEXT NOT NULL,
            split_date        TEXT NOT NULL,
            ratio_from        REAL NOT NULL,
            ratio_to          REAL NOT NULL,
            detection_method  TEXT NOT NULL DEFAULT 'price_heuristic',
            confirmation      TEXT NOT NULL DEFAULT 'pending',
            confirmed_by      TEXT,
            source_detail     TEXT,
            share_count_before REAL,
            share_count_after  REAL,
            share_count_ratio  REAL,
            price_basis       TEXT NOT NULL DEFAULT 'raw',
            created_at        TEXT DEFAULT (datetime('now')),
            updated_at        TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


class TestAdjustmentFactors:
    def test_no_splits_returns_empty(self):
        conn = sqlite3.connect(":memory:")
        _create_stock_splits_in_conn(conn)
        factors = _load_split_factors(conn, "NOSPLIT")
        assert factors == []

    def test_single_split_factor(self):
        conn = sqlite3.connect(":memory:")
        _create_stock_splits_in_conn(conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, confirmation) "
            "VALUES ('T', '2024-06-15', 1, 2, 'confirmed')"
        )
        conn.commit()

        factors = _load_split_factors(conn, "T")
        assert len(factors) == 1
        split_date, cum_factor = factors[0]
        assert split_date == "2024-06-15"
        assert abs(cum_factor - 0.5) < 0.001

    def test_two_splits_cumulative(self):
        conn = sqlite3.connect(":memory:")
        _create_stock_splits_in_conn(conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, confirmation) "
            "VALUES "
            "('T', '2023-06-15', 1, 2, 'confirmed'),"  # first 2:1
            "('T', '2024-06-15', 1, 3, 'confirmed')"   # then 3:1
        )
        conn.commit()

        factors = _load_split_factors(conn, "T")
        assert len(factors) == 2
        # First split: cumulative = 1 / (2 * 3) = 1/6
        assert abs(factors[0][1] - (1.0 / 6.0)) < 0.001
        # Second split: cumulative = 1 / 3
        assert abs(factors[1][1] - (1.0 / 3.0)) < 0.001


# ── adjusted price ──────────────────────────────────────────────────────────


class TestAdjustedPrice:
    def test_adjusted_equals_raw_when_no_splits(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE Stock_Prices "
            "(Date TEXT, Ticker TEXT, Currency TEXT, Price REAL)"
        )
        conn.execute(
            "INSERT INTO Stock_Prices VALUES ('2024-06-15', 'X', 'USD', 50.0)"
        )
        conn.commit()

        _create_stock_splits_in_conn(conn)
        _invalidate_split_cache()

        raw = _get_price(conn, "X", "2024-06-15")
        adj = _get_adjusted_price(conn, "X", "2024-06-15")
        assert raw == 50.0
        assert adj == raw

    def test_pre_split_price_adjusted(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE Stock_Prices "
            "(Date TEXT, Ticker TEXT, Currency TEXT, Price REAL)"
        )
        conn.execute(
            "INSERT INTO Stock_Prices VALUES ('2024-06-14', 'X', 'USD', 100.0)"
        )
        conn.execute(
            "INSERT INTO Stock_Prices VALUES ('2024-06-17', 'X', 'USD', 50.0)"
        )
        conn.commit()

        _create_stock_splits_in_conn(conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, confirmation) "
            "VALUES ('X', '2024-06-15', 1, 2, 'confirmed')"
        )
        conn.commit()
        _invalidate_split_cache()

        # Pre-split: 100 / 2 = 50
        adj = _get_adjusted_price(conn, "X", "2024-06-14")
        assert abs(adj - 50.0) < 0.001

        # Post-split: unchanged
        adj2 = _get_adjusted_price(conn, "X", "2024-06-17")
        assert abs(adj2 - 50.0) < 0.001

    def test_forward_fill_with_adjustment(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE Stock_Prices "
            "(Date TEXT, Ticker TEXT, Currency TEXT, Price REAL)"
        )
        # Pre-split price on June 10
        conn.execute(
            "INSERT INTO Stock_Prices VALUES ('2024-06-10', 'X', 'USD', 100.0)"
        )
        conn.commit()

        _create_stock_splits_in_conn(conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, confirmation) "
            "VALUES ('X', '2024-06-15', 1, 2, 'confirmed')"
        )
        conn.commit()
        _invalidate_split_cache()

        # June 20: forward-fill from June 10, but with split adjustment
        adj = _get_adjusted_price(conn, "X", "2024-06-20")
        assert abs(adj - 50.0) < 0.001

    def test_cache_invalidation(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE Stock_Prices "
            "(Date TEXT, Ticker TEXT, Currency TEXT, Price REAL)"
        )
        conn.execute(
            "INSERT INTO Stock_Prices VALUES ('2024-06-14', 'X', 'USD', 100.0)"
        )
        conn.commit()

        _create_stock_splits_in_conn(conn)
        _invalidate_split_cache()

        # Before split is inserted
        adj1 = _get_adjusted_price(conn, "X", "2024-06-14")
        assert adj1 == 100.0  # no adjustment

        # Insert a confirmed split
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, confirmation) "
            "VALUES ('X', '2024-06-15', 1, 2, 'confirmed')"
        )
        conn.commit()

        # Without invalidation, cache still returns raw
        adj2 = _get_adjusted_price(conn, "X", "2024-06-14")
        assert adj2 == 100.0  # stale cache

        # After invalidation
        _invalidate_split_cache()
        adj3 = _get_adjusted_price(conn, "X", "2024-06-14")
        assert abs(adj3 - 50.0) < 0.001


# ── end-to-end detection → Stock_Splits table ───────────────────────────────


class TestRunSplitDetection:
    def test_full_mode_populates_table(self):
        import gc
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_market_database(db_path)
            add_split_test_data(db_path)

            results = run_split_detection(
                str(db_path), tickers=["AAA"], mode="full",
            )
            # Should detect the 2:1 split and confirm via ShareMetrics
            assert results["tickers_scanned"] >= 1

            # Verify the table was populated
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM Stock_Splits WHERE ticker = 'AAA'"
            ).fetchall()
            conn.close()
            gc.collect()
            assert len(rows) >= 1

    def test_all_tickers_skip_rate_and_index_series(self):
        import gc
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_market_database(db_path)
            add_split_test_data(db_path)
            conn = sqlite3.connect(db_path)
            conn.executemany(
                "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price) VALUES (?, ?, ?, ?)",
                [
                    ("2024-01-04", "RiskFree_EUR", "EUR", 0.5),
                    ("2024-01-05", "RiskFree_EUR", "EUR", 0.0),
                    ("2024-01-08", "RiskFree_EUR", "EUR", -0.2),
                    ("2024-01-04", "Inflation_JPY", "JPY", 100.0),
                    ("2024-01-05", "Inflation_JPY", "JPY", 40.0),
                ],
            )
            conn.commit()
            conn.close()
            seen: list[str] = []

            results = run_split_detection(
                str(db_path), mode="full", progress=lambda done, total, message: seen.append(message),
            )

            gc.collect()
            assert not any("RiskFree_" in message or "Inflation_" in message for message in seen)
            assert len(seen) == results["tickers_scanned"] > 0

    def test_incremental_skips_already_known(self):
        import gc
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_market_database(db_path)
            add_split_test_data(db_path)

            # First run
            run_split_detection(str(db_path), tickers=["AAA"], mode="full")
            gc.collect()
            # Second run should find nothing new
            results2 = run_split_detection(
                str(db_path), tickers=["AAA"], mode="full",
            )
            gc.collect()
            assert results2["already_known"] >= results2.get("new_pending", 0)


# ── schema idempotency ──────────────────────────────────────────────────────


class TestSchema:
    def test_ensure_split_tables_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            # First call
            ensure_split_tables(str(db_path))
            # Second call — should not raise
            ensure_split_tables(str(db_path))

            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='Stock_Splits'"
            ).fetchall()
            conn.close()
            assert len(rows) == 1


class TestSchemaUniqueConstraint:
    def test_duplicate_split_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            ensure_split_tables(str(db_path))
            conn = sqlite3.connect(str(db_path))
            conn.execute(
                "INSERT INTO Stock_Splits "
                "(ticker, split_date, ratio_from, ratio_to) "
                "VALUES ('X', '2024-06-15', 1, 2)"
            )
            conn.commit()
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO Stock_Splits "
                    "(ticker, split_date, ratio_from, ratio_to) "
                    "VALUES ('X', '2024-06-15', 1, 2)"
                )
            conn.close()
