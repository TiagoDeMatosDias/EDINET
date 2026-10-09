"""Unit tests for the provider retro-adjustment / price-basis reconciliation.

When price updates are infrequent, the provider (Yahoo) returns split-adjusted
prices for the newly fetched range while cached rows are on the raw (as-traded)
basis.  This produces a spurious discontinuity at the fetch boundary and a
mis-dated split.  The fix restores the fetched rows back to the raw basis using
the provider's authoritative split events, and records the split at its true
date so read-time adjustment applies it.  Genuine (correctly-dated) splits and
raw Stooq data are left untouched.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pandas as pd
import pytest

from src.portfolio.portfolio_state import _invalidate_split_cache, _load_split_factors
from src.portfolio.split_schema import ensure_split_tables
from src.utilities.stock_prices import (
    _create_prices_table,
    _extract_split_events,
    _parse_yahoo_chart_payload,
    _reconcile_splits,
    _record_provider_splits,
    _replace_ticker_rows,
    _split_restore_factor,
    load_ticker_data,
    reconcile_ticker_price_basis,
    repair_squeeze_out_prices,
)


def _make_prices_conn() -> sqlite3.Connection:
    """An in-memory db with the Stock_Prices table."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _create_prices_table(conn, "Stock_Prices")
    return conn


def _insert(conn, ticker, date, price):
    conn.execute(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price) "
        "VALUES (?, ?, 'JPY', ?)",
        (date, ticker, price),
    )
    conn.commit()


def _create_stock_splits_in_conn(conn: sqlite3.Connection) -> None:
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


# ---------------------------------------------------------------------------
# _extract_split_events
# ---------------------------------------------------------------------------


class TestExtractSplitEvents:
    def test_extracts_split_with_ratio(self):
        result = {
            "events": {
                "splits": {
                    "1782691200": {
                        "date": 1782691200,
                        "numerator": 5.0,
                        "denominator": 1.0,
                        "splitRatio": "5:1",
                    }
                }
            }
        }
        events = _extract_split_events(result)
        assert len(events) == 1
        assert events[0]["ratio_from"] == 1.0
        assert events[0]["ratio_to"] == 5.0
        # 1782691200 ≈ 2026-06-29 in Asia/Tokyo
        assert events[0]["split_date"].startswith("2026-06-2")

    def test_no_events(self):
        assert _extract_split_events({}) == []
        assert _extract_split_events({"events": {"dividends": {}}}) == []

    def test_ignores_malformed(self):
        result = {
            "events": {
                "splits": {
                    "1": {"numerator": "bad", "denominator": 0},
                    "2": {},
                }
            }
        }
        assert _extract_split_events(result) == []


# ---------------------------------------------------------------------------
# _split_restore_factor
# ---------------------------------------------------------------------------


class TestSplitRestoreFactor:
    def test_single_forward_split(self):
        splits = [{"split_date": "2026-06-29", "ratio_from": 1, "ratio_to": 5}]
        assert _split_restore_factor(splits, "2026-02-13") == 5.0
        assert _split_restore_factor(splits, "2026-07-01") == 1.0  # after the split
        # A price dated exactly on the split date is already post-split
        assert _split_restore_factor(splits, "2026-06-29") == 1.0

    def test_multiple_forward_splits(self):
        splits = [
            {"split_date": "2024-06-15", "ratio_from": 1, "ratio_to": 2},
            {"split_date": "2026-06-29", "ratio_from": 1, "ratio_to": 5},
        ]
        # Before both → ×2×5
        assert _split_restore_factor(splits, "2024-01-01") == 10.0
        # Between the two → ×5 only
        assert _split_restore_factor(splits, "2025-01-01") == 5.0
        # After both → ×1
        assert _split_restore_factor(splits, "2027-01-01") == 1.0

    def test_reverse_split_restores(self):
        # A 5:1 reverse split (price ×5) is restored by ×(1/5) = 0.2
        splits = [{"split_date": "2017-09-27", "ratio_from": 5, "ratio_to": 1}]
        assert _split_restore_factor(splits, "2015-01-01") == 0.2
        # After the reverse split: no restore
        assert _split_restore_factor(splits, "2018-01-01") == 1.0


# ---------------------------------------------------------------------------
# _load_split_factors respects price_basis
# ---------------------------------------------------------------------------


class TestPriceBasisFiltering:
    def test_adjusted_split_not_applied(self):
        conn = sqlite3.connect(":memory:")
        ensure_split_tables(None, conn=conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, confirmation, price_basis) "
            "VALUES ('X', '2024-06-15', 1, 2, 'confirmed', 'adjusted')"
        )
        conn.commit()
        _invalidate_split_cache()
        assert _load_split_factors(conn, "X") == []

    def test_raw_split_applied(self):
        conn = sqlite3.connect(":memory:")
        ensure_split_tables(None, conn=conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, confirmation, price_basis) "
            "VALUES ('X', '2024-06-15', 1, 2, 'confirmed', 'raw')"
        )
        conn.commit()
        _invalidate_split_cache()
        factors = _load_split_factors(conn, "X")
        assert len(factors) == 1
        assert abs(factors[0][1] - 0.5) < 0.001


# ---------------------------------------------------------------------------
# _record_provider_splits and _reconcile_splits
# ---------------------------------------------------------------------------


class TestReconcileSplits:
    def test_record_provider_split_dedup(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        ensure_split_tables(None, conn=conn)
        events = [{"split_date": "2026-06-29", "ratio_from": 1, "ratio_to": 5}]

        _record_provider_splits(conn, "31100", events)
        _record_provider_splits(conn, "31100", events)  # dedup on (ticker, date)

        rows = conn.execute(
            "SELECT * FROM Stock_Splits WHERE ticker = '31100'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["ratio_to"] == 5
        assert rows[0]["price_basis"] == "raw"
        assert rows[0]["confirmation"] == "confirmed"
        assert rows[0]["confirmed_by"] == "provider"

    def test_reconcile_removes_stale_heuristic_split(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        ensure_split_tables(None, conn=conn)
        # A stale heuristic split at the wrong boundary date
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, detection_method, "
            "confirmation, price_basis) "
            "VALUES ('31100', '2026-02-13', 1, 5, 'price_heuristic', "
            "'rejected', 'raw')"
        )
        # A manual split with a different ratio — must be preserved
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, detection_method, "
            "confirmation, price_basis) "
            "VALUES ('31100', '2021-03-15', 1, 2, 'manual', 'confirmed', 'raw')"
        )
        conn.commit()

        events = [{"split_date": "2026-06-29", "ratio_from": 1, "ratio_to": 5}]
        _reconcile_splits(conn, "31100", events)

        rows = conn.execute(
            "SELECT detection_method, split_date, price_basis "
            "FROM Stock_Splits WHERE ticker = '31100' ORDER BY split_date"
        ).fetchall()
        assert [(r["detection_method"], r["split_date"], r["price_basis"]) for r in rows] == [
            ("manual", "2021-03-15", "raw"),
            ("provider", "2026-06-29", "raw"),
        ]


# ---------------------------------------------------------------------------
# load_ticker_data: restore fetched Yahoo rows to raw
# ---------------------------------------------------------------------------


class TestBoundaryReconciliation:
    def test_incremental_update_after_split_restores_to_raw(self):
        """A long-gap update that returns adjusted prices must restore the
        fetched rows to the raw basis and record the split at its true date."""
        conn = _make_prices_conn()
        # Cached pre-split (raw) prices through 2026-02-12
        _insert(conn, "31100", "2026-02-10", 20460.0)
        _insert(conn, "31100", "2026-02-12", 20460.0)

        # Incremental fetch (from 2026-02-13): provider now returns ADJUSTED
        # prices (20460 / 5 ≈ 4092), i.e. a ~5x discontinuity vs the cache.
        incremental = pd.DataFrame(
            {
                "Date": ["2026-02-13", "2026-02-16"],
                "Close": [4092.0, 4142.0],
            }
        )
        events = [{"split_date": "2026-06-29", "ratio_from": 1, "ratio_to": 5}]

        with patch("src.utilities.stock_prices._load_provider_history") as mock:
            mock.side_effect = [("Yahoo Finance chart", incremental, events)]
            ok = load_ticker_data("31100", "Stock_Prices", conn)
            conn.commit()

        assert ok is True
        # Only the incremental fetch is made (cached rows untouched, no re-fetch)
        assert mock.call_count == 1

        # Fetched rows restored to raw (×5, since they're before the June split)
        rows = conn.execute(
            "SELECT Date, Price FROM Stock_Prices WHERE Ticker = '31100' ORDER BY Date"
        ).fetchall()
        assert [r["Price"] for r in rows] == pytest.approx(
            [20460.0, 20460.0, 20460.0, 20710.0]
        )

        # Split recorded at the authoritative date, raw basis (read-time adjusts)
        split = conn.execute(
            "SELECT * FROM Stock_Splits WHERE ticker = '31100'"
        ).fetchone()
        assert split is not None
        assert split["split_date"] == "2026-06-29"
        assert split["price_basis"] == "raw"

    def test_continuous_update_no_restore(self):
        """Normal small moves (or Stooq raw data with no events) are unchanged."""
        conn = _make_prices_conn()
        _insert(conn, "7203", "2026-07-01", 1200.0)

        incremental = pd.DataFrame(
            {
                "Date": ["2026-07-02", "2026-07-03"],
                "Close": [1210.0, 1225.0],
            }
        )
        events: list[dict] = []

        with patch("src.utilities.stock_prices._load_provider_history") as mock:
            mock.side_effect = [("Stooq", incremental, events)]
            ok = load_ticker_data("7203", "Stock_Prices", conn)
            conn.commit()

        assert ok is True
        assert mock.call_count == 1
        rows = conn.execute(
            "SELECT Date, Price FROM Stock_Prices WHERE Ticker = '7203' ORDER BY Date"
        ).fetchall()
        assert [r["Price"] for r in rows] == pytest.approx([1200.0, 1210.0, 1225.0])

    def test_initial_fetch_restores_and_records_splits(self):
        """The first (full) Yahoo fetch is restored to raw and its historical
        splits are recorded at their true dates."""
        conn = _make_prices_conn()
        full = pd.DataFrame(
            {
                "Date": ["2020-01-02", "2024-06-14", "2024-06-17"],
                "Close": [100.0, 50.0, 51.0],
            }
        )
        events = [{"split_date": "2024-06-15", "ratio_from": 1, "ratio_to": 2}]

        with patch("src.utilities.stock_prices._load_provider_history") as mock:
            mock.side_effect = [("Yahoo Finance chart", full, events)]
            ok = load_ticker_data("9999", "Stock_Prices", conn)
            conn.commit()

        assert ok is True
        # Pre-split rows restored ×2 → raw; post-split unchanged
        rows = conn.execute(
            "SELECT Date, Price FROM Stock_Prices WHERE Ticker = '9999' ORDER BY Date"
        ).fetchall()
        assert [r["Price"] for r in rows] == pytest.approx([200.0, 100.0, 51.0])

        split = conn.execute(
            "SELECT * FROM Stock_Splits WHERE ticker = '9999'"
        ).fetchone()
        assert split is not None
        assert split["split_date"] == "2024-06-15"
        assert split["price_basis"] == "raw"

    def test_replace_ticker_rows(self):
        conn = _make_prices_conn()
        _insert(conn, "X", "2020-01-01", 100.0)
        _insert(conn, "X", "2020-01-02", 101.0)

        full = pd.DataFrame({"Date": ["2020-01-01", "2020-01-02"], "Close": [50.0, 50.5]})
        n = _replace_ticker_rows(conn, "Stock_Prices", "X", full, "JPY")
        conn.commit()

        assert n == 2
        rows = conn.execute(
            "SELECT Price FROM Stock_Prices WHERE Ticker = 'X' ORDER BY Date"
        ).fetchall()
        assert [r["Price"] for r in rows] == pytest.approx([50.0, 50.5])


# ---------------------------------------------------------------------------
# reconcile_ticker_price_basis (existing-data backfill)
# ---------------------------------------------------------------------------


class TestReconcileTickerPriceBasis:
    def test_fixes_interior_boundary_artifact(self):
        """A mis-dated heuristic split triggers restoration of the fetched rows
        to raw and recording of the provider split at its true date."""
        conn = _make_prices_conn()
        # Cached raw rows
        _insert(conn, "31100", "2020-01-02", 20460.0)
        _insert(conn, "31100", "2026-02-12", 20460.0)
        # Fetched provider-adjusted rows (before the true split in June)
        _insert(conn, "31100", "2026-02-13", 4038.0)
        _insert(conn, "31100", "2026-02-16", 4142.0)

        # The mis-dated heuristic split at the boundary (rejected)
        ensure_split_tables(None, conn=conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, detection_method, "
            "confirmation) VALUES ('31100', '2026-02-13', 1, 5, "
            "'price_heuristic', 'rejected')"
        )
        conn.commit()

        events = [{"split_date": "2026-06-29", "ratio_from": 1, "ratio_to": 5}]
        with patch("src.utilities.stock_prices._load_provider_history") as mock:
            mock.return_value = ("Yahoo Finance chart", pd.DataFrame(), events)
            result = reconcile_ticker_price_basis(conn, "Stock_Prices", "31100")

        assert result["status"] == "reconciled"

        # Fetched rows (≥ Feb 13) restored to raw ×5; cached rows untouched
        rows = conn.execute(
            "SELECT Date, Price FROM Stock_Prices WHERE Ticker = '31100' ORDER BY Date"
        ).fetchall()
        assert [r["Price"] for r in rows] == pytest.approx(
            [20460.0, 20460.0, 20190.0, 20710.0]
        )

        # Stale heuristic split removed; provider split recorded at true date
        splits = conn.execute(
            "SELECT split_date, detection_method, price_basis "
            "FROM Stock_Splits WHERE ticker = '31100'"
        ).fetchall()
        assert [(s["split_date"], s["detection_method"], s["price_basis"]) for s in splits] == [
            ("2026-06-29", "provider", "raw"),
        ]

    def test_correctly_dated_split_is_left_alone(self):
        """A heuristic split whose date matches the provider event is a genuine
        split — it must not be touched."""
        conn = _make_prices_conn()
        _insert(conn, "14910", "2007-09-10", 100.0)
        _insert(conn, "14910", "2007-09-12", 50.0)
        _insert(conn, "14910", "2007-09-13", 51.0)

        ensure_split_tables(None, conn=conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, detection_method, "
            "confirmation) VALUES ('14910', '2007-09-11', 1, 2, "
            "'price_heuristic', 'confirmed')"
        )
        conn.commit()

        # Provider event at the SAME date as the heuristic split
        events = [{"split_date": "2007-09-11", "ratio_from": 1, "ratio_to": 2}]
        with patch("src.utilities.stock_prices._load_provider_history") as mock:
            mock.return_value = ("Yahoo Finance chart", pd.DataFrame(), events)
            result = reconcile_ticker_price_basis(conn, "Stock_Prices", "14910")

        assert result["status"] == "already_correct"
        # Prices untouched
        rows = conn.execute(
            "SELECT Price FROM Stock_Prices WHERE Ticker = '14910' ORDER BY Date"
        ).fetchall()
        assert [r["Price"] for r in rows] == pytest.approx([100.0, 50.0, 51.0])
        # Heuristic split still present (not removed, not duplicated)
        splits = conn.execute(
            "SELECT split_date, detection_method FROM Stock_Splits WHERE ticker = '14910'"
        ).fetchall()
        assert [(s["split_date"], s["detection_method"]) for s in splits] == [
            ("2007-09-11", "price_heuristic"),
        ]

    def test_ratio_mismatch_still_reconciled(self):
        """An artifact where the heuristic rounded the boundary ratio to 1:8 but
        the provider's true split is 1:10 must still be reconciled (matched by
        date, not ratio)."""
        conn = _make_prices_conn()
        _insert(conn, "60850", "2026-02-12", 1015.0)   # cached raw
        _insert(conn, "60850", "2026-02-13", 131.5)    # fetched ÷10
        _insert(conn, "60850", "2026-04-23", 150.0)    # fetched, post-split

        ensure_split_tables(None, conn=conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, detection_method, "
            "confirmation) VALUES ('60850', '2026-02-13', 1, 8, "
            "'price_heuristic', 'pending')"
        )
        conn.commit()

        # Provider's true split is 1:10 on 2026-04-22 (different ratio AND date)
        events = [{"split_date": "2026-04-22", "ratio_from": 1, "ratio_to": 10}]
        with patch("src.utilities.stock_prices._load_provider_history") as mock:
            mock.return_value = ("Yahoo Finance chart", pd.DataFrame(), events)
            result = reconcile_ticker_price_basis(conn, "Stock_Prices", "60850")

        assert result["status"] == "reconciled"

        # Fetched rows (≥ Feb 13, before Apr 22) restored ×10 to raw
        rows = conn.execute(
            "SELECT Date, Price FROM Stock_Prices WHERE Ticker = '60850' ORDER BY Date"
        ).fetchall()
        assert [r["Price"] for r in rows] == pytest.approx([1015.0, 1315.0, 150.0])

        # Mis-dated 1:8 heuristic removed; true 1:10 split recorded at its date
        splits = conn.execute(
            "SELECT split_date, ratio_from, ratio_to, detection_method "
            "FROM Stock_Splits WHERE ticker = '60850'"
        ).fetchall()
        assert [(s["split_date"], s["ratio_from"], s["ratio_to"], s["detection_method"])
                for s in splits] == [("2026-04-22", 1.0, 10.0, "provider")]

    def test_no_discontinuity_is_not_touched(self):
        """A mis-dated heuristic split that does NOT sit on an actual >40% price
        move must not trigger a restore (avoids modifying genuine data)."""
        conn = _make_prices_conn()
        # Continuous prices at the heuristic's date (no boundary)
        _insert(conn, "79460", "2017-09-26", 1566.0)
        _insert(conn, "79460", "2017-09-27", 1563.0)
        _insert(conn, "79460", "2017-09-28", 1540.0)

        ensure_split_tables(None, conn=conn)
        conn.execute(
            "INSERT INTO Stock_Splits "
            "(ticker, split_date, ratio_from, ratio_to, detection_method, "
            "confirmation) VALUES ('79460', '2017-09-27', 10, 1, "
            "'price_heuristic', 'rejected')"
        )
        conn.commit()

        # Provider has a split at a later date (mis-dated), but the prices here
        # are continuous → must be skipped.
        events = [{"split_date": "2017-09-27", "ratio_from": 10, "ratio_to": 1}]
        with patch("src.utilities.stock_prices._load_provider_history") as mock:
            mock.return_value = ("Yahoo Finance chart", pd.DataFrame(), events)
            result = reconcile_ticker_price_basis(conn, "Stock_Prices", "79460")

        assert result["status"] == "already_correct"
        rows = conn.execute(
            "SELECT Price FROM Stock_Prices WHERE Ticker = '79460' ORDER BY Date"
        ).fetchall()
        # Prices untouched
        assert [r["Price"] for r in rows] == pytest.approx([1566.0, 1563.0, 1540.0])

    def test_no_heuristic_splits_returns_status(self):
        """A ticker with no heuristic split records is not reconciled."""
        conn = _make_prices_conn()
        _insert(conn, "X", "2020-01-02", 100.0)
        _insert(conn, "X", "2020-01-03", 101.0)

        events = [{"split_date": "2024-06-15", "ratio_from": 1, "ratio_to": 2}]
        with patch("src.utilities.stock_prices._load_provider_history") as mock:
            mock.return_value = ("Yahoo Finance chart", pd.DataFrame(), events)
            result = reconcile_ticker_price_basis(conn, "Stock_Prices", "X")

        assert result["status"] == "no_heuristic_splits"


# ---------------------------------------------------------------------------
# Squeeze-outs: a consolidation of millions to one before a delisting
# ---------------------------------------------------------------------------


def _tokyo_timestamp(day: str) -> int:
    return int(pd.Timestamp(f"{day} 15:00", tz="Asia/Tokyo").timestamp())


def test_a_squeeze_out_is_taken_back_out_of_a_yahoo_series():
    # Yahoo multiplies every close before a 1,440,960-to-1 consolidation by
    # 1,440,960 and calls it a split.
    days = ["2026-09-10", "2026-09-11", "2026-09-16"]
    payload = {"chart": {"result": [{
        "meta": {"dataGranularity": "1d", "currency": "JPY"},
        "timestamp": [_tokyo_timestamp(day) for day in days],
        "indicators": {"quote": [{"close": [1309.0 * 1440960, 1309.0 * 1440960, 1.0]}]},
        "events": {"splits": {"1": {"date": _tokyo_timestamp("2026-09-16"), "numerator": 1.0, "denominator": 1440960.0}}},
    }]}}
    prices, splits = _parse_yahoo_chart_payload(payload)
    assert prices["Close"].round(6).tolist() == [1309.0, 1309.0]
    assert splits == []
    assert prices.attrs["currency"] == "JPY"


def test_stored_closes_inflated_by_a_squeeze_out_are_restored():
    conn = _make_prices_conn()
    ensure_split_tables(conn=conn)
    rows = [(f"2026-0{month}-{day:02d}", 1309.0) for month in (6, 7, 8) for day in (1, 15)]
    rows += [("2026-09-09", 1309.0), ("2026-09-10", 1309.0 * 1440960), ("2026-09-16", 1309.0 * 1440960)]
    conn.executemany("INSERT INTO Stock_Prices (Date, Ticker, Currency, Price) VALUES (?, '21800', 'JPY', ?)", rows)
    conn.execute(
        "INSERT INTO Stock_Splits (ticker, split_date, ratio_from, ratio_to, confirmation, detection_method) "
        "VALUES ('21800', '2026-09-16', 1440960, 1, 'confirmed', 'provider')"
    )
    assert repair_squeeze_out_prices(conn, "Stock_Prices", "21800") == 2
    stored = conn.execute("SELECT Date, Price FROM Stock_Prices WHERE Date >= '2026-09-09' ORDER BY Date").fetchall()
    assert [(row[0], round(row[1], 6)) for row in stored] == [("2026-09-09", 1309.0), ("2026-09-10", 1309.0)]
    assert repair_squeeze_out_prices(conn, "Stock_Prices", "21800") == 0
    # The consolidation never adjusts prices as a split would.
    _invalidate_split_cache()
    assert _load_split_factors(conn, "21800") == []


def test_unknown_basis_closes_take_a_confirmed_split_only_when_they_show_it():
    from src.utilities.price_provenance import refresh_split_adjusted_prices

    conn = _make_prices_conn()
    ensure_split_tables(conn=conn)
    # 17230 was imported as traded: the 4-for-1 split on 30 March is still a drop.
    shown = [("2026-03-26", 9570.0), ("2026-03-27", 9250.0), ("2026-03-30", 2192.0), ("2026-03-31", 2211.0)]
    # Another import was already adjusted for its split: no drop.
    hidden = [("2026-03-26", 2392.5), ("2026-03-27", 2312.5), ("2026-03-30", 2192.0), ("2026-03-31", 2211.0)]
    for ticker, rows in (("17230", shown), ("99990", hidden)):
        conn.executemany(
            "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price, Price_Basis) VALUES (?, ?, 'JPY', ?, 'unknown')",
            [(day, ticker, price) for day, price in rows],
        )
        conn.execute(
            "INSERT INTO Stock_Splits (ticker, split_date, ratio_from, ratio_to, confirmation, detection_method) "
            "VALUES (?, '2026-03-30', 1, 4, 'confirmed', 'provider')",
            (ticker,),
        )
    # 31930: the import stops on 10 July at ¥2,789; Yahoo took over on
    # 15 July with closes already halved for the split on 30 July.
    conn.executemany(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price, Price_Basis) VALUES (?, '31930', 'JPY', ?, ?)",
        [("2026-07-09", 2810.0, "unknown"), ("2026-07-10", 2789.0, "unknown"), ("2026-07-15", 1431.5, "adjusted"),
         ("2026-07-16", 1440.0, "adjusted"), ("2026-07-29", 1474.5, "adjusted"), ("2026-07-30", 1470.0, "adjusted")],
    )
    conn.execute(
        "INSERT INTO Stock_Splits (ticker, split_date, ratio_from, ratio_to, confirmation, detection_method) "
        "VALUES ('31930', '2026-07-30', 1, 2, 'confirmed', 'provider')"
    )
    refresh_split_adjusted_prices(conn)
    adjusted = {
        (row[0], row[1]): row[2]
        for row in conn.execute("SELECT Ticker, Date, Adjusted_Price FROM Stock_Prices")
    }
    assert adjusted[("17230", "2026-03-27")] == pytest.approx(2312.5)
    assert adjusted[("17230", "2026-03-31")] is None
    assert adjusted[("99990", "2026-03-27")] is None
    assert adjusted[("31930", "2026-07-10")] == pytest.approx(1394.5)
    assert adjusted[("31930", "2026-07-15")] == pytest.approx(1431.5)


def test_adjusted_closes_that_still_step_at_a_split_take_it():
    from src.utilities.price_provenance import refresh_split_adjusted_prices

    conn = _make_prices_conn()
    ensure_split_tables(conn=conn)
    # 79110: daily quotes fetched each evening, already adjusted for every
    # earlier split, then a 2-for-1 split on 29 September. The quotes fetched
    # before it stay on the old shares; the provider adjusted later fetches.
    # Its history to 14 July was fetched again afterwards: already halved.
    conn.executemany(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price, Price_Basis, Retrieved_At) VALUES (?, '79110', 'JPY', ?, 'adjusted', '2026-10-09T09:00:00+00:00')",
        [("2026-07-13", 2574.0), ("2026-07-14", 2454.5)],
    )
    conn.executemany(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price, Price_Basis, Retrieved_At) VALUES (?, '79110', 'JPY', ?, 'adjusted', ?)",
        [("2026-09-22", 4890.0, "2026-09-23T09:00:00+00:00"), ("2026-09-24", 4921.0, "2026-09-25T09:00:00+00:00"),
         ("2026-09-25", 2459.0, "2026-09-29T09:00:00+00:00"), ("2026-09-28", 2421.5, "2026-09-29T09:00:00+00:00"),
         ("2026-09-29", 2339.5, "2026-10-03T09:00:00+00:00"), ("2026-09-30", 2362.5, "2026-10-03T09:00:00+00:00"),
         ("2026-10-01", 2417.5, "2026-10-03T09:00:00+00:00"), ("2026-10-02", 2384.5, "2026-10-07T09:00:00+00:00"),
         ("2026-10-05", 2469.0, "2026-10-07T09:00:00+00:00"), ("2026-10-06", 2438.0, "2026-10-07T09:00:00+00:00")],
    )
    # 99990: the same split, but its whole history was fetched afterwards.
    conn.executemany(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price, Price_Basis, Retrieved_At) VALUES (?, '99990', 'JPY', ?, 'adjusted', '2026-10-09T09:00:00+00:00')",
        [("2026-09-24", 2460.5), ("2026-09-25", 2459.0), ("2026-09-29", 2339.5), ("2026-10-01", 2417.5)],
    )
    for ticker in ("79110", "99990"):
        conn.execute(
            "INSERT INTO Stock_Splits (ticker, split_date, ratio_from, ratio_to, confirmation, detection_method) "
            "VALUES (?, '2026-09-29', 1, 2, 'confirmed', 'provider')",
            (ticker,),
        )
    # 82270: a provider listed a 3-for-1 split on 19 February but served its
    # whole history, fetched months later, still at the price as traded.
    conn.executemany(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price, Price_Basis, Retrieved_At) VALUES (?, '82270', 'JPY', ?, 'adjusted', '2026-10-09T09:00:00+00:00')",
        [("2026-02-13", 11050.0), ("2026-02-16", 11030.0), ("2026-02-17", 11040.0), ("2026-02-18", 3713.3), ("2026-02-19", 3628.0), ("2026-02-20", 3537.0)],
    )
    conn.execute(
        "INSERT INTO Stock_Splits (ticker, split_date, ratio_from, ratio_to, confirmation, detection_method) "
        "VALUES ('82270', '2026-02-19', 1, 3, 'confirmed', 'provider')"
    )
    refresh_split_adjusted_prices(conn)
    adjusted = {(row[0], row[1]): row[2] for row in conn.execute("SELECT Ticker, Date, Adjusted_Price FROM Stock_Prices")}
    assert adjusted[("79110", "2026-09-24")] == pytest.approx(2460.5)
    assert adjusted[("79110", "2026-09-22")] == pytest.approx(2445.0)
    assert adjusted[("79110", "2026-09-25")] == pytest.approx(2459.0)
    assert adjusted[("79110", "2026-07-14")] == pytest.approx(2454.5)
    assert adjusted[("99990", "2026-09-24")] == pytest.approx(2460.5)
    assert adjusted[("82270", "2026-02-17")] == pytest.approx(3680.0)
    assert adjusted[("82270", "2026-02-18")] == pytest.approx(3713.3)
    # Refreshing again changes nothing: the step is read from the stored closes.
    assert refresh_split_adjusted_prices(conn) == 0


def test_one_split_recorded_twice_days_apart_adjusts_once():
    from src.utilities.price_provenance import refresh_split_adjusted_prices

    conn = _make_prices_conn()
    ensure_split_tables(conn=conn)
    # 79460: the price heuristic dated a 5-for-1 split at the step on 2 March,
    # the provider at 5 March; both were confirmed.
    conn.executemany(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price, Price_Basis) VALUES (?, '79460', 'JPY', ?, 'raw')",
        [("2026-02-26", 3845.0), ("2026-02-27", 3735.0), ("2026-03-02", 746.0), ("2026-03-03", 676.0)],
    )
    conn.executemany(
        "INSERT INTO Stock_Splits (ticker, split_date, ratio_from, ratio_to, confirmation, detection_method) VALUES ('79460', ?, 1, 5, 'confirmed', ?)",
        [("2026-03-02", "price_heuristic"), ("2026-03-05", "provider")],
    )
    refresh_split_adjusted_prices(conn)
    adjusted = dict(conn.execute("SELECT Date, Adjusted_Price FROM Stock_Prices WHERE Ticker = '79460'").fetchall())
    assert adjusted["2026-02-27"] == pytest.approx(747.0)
    assert adjusted["2026-03-02"] == pytest.approx(746.0)
    _invalidate_split_cache()
    assert [(day, round(factor, 6)) for day, factor in _load_split_factors(conn, "79460")] == [("2026-03-02", 0.2)]
