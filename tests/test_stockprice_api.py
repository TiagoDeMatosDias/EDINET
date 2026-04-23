import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.utilities.stock_prices import _create_prices_table, import_stock_prices_csv, load_ticker_data


class TestImportStockPricesCsv(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "prices.db")
        self.csv_path = os.path.join(self.tmpdir.name, "prices.csv")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_import_db_table_format_with_ticker_currency_columns(self):
        df = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Ticker": ["7203", "6758"],
                "Currency": ["JPY", "JPY"],
                "Price": [1000.5, 2000.0],
            }
        )
        df.to_csv(self.csv_path, index=False)

        inserted = import_stock_prices_csv(
            self.db_path,
            "stock_prices",
            self.csv_path,
            default_ticker="",
            default_currency="",
            date_column="Date",
            price_column="Price",
            ticker_column="Ticker",
            currency_column="Currency",
        )

        self.assertEqual(inserted, 2)

        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT Date, Ticker, Currency, Price FROM stock_prices ORDER BY Date"
            ).fetchall()
            self.assertEqual(rows[0], ("2026-01-01", "7203", "JPY", 1000.5))
            self.assertEqual(rows[1], ("2026-01-02", "6758", "JPY", 2000.0))
        finally:
            conn.close()

    def test_defaults_fill_blank_ticker_currency(self):
        df = pd.DataFrame(
            {
                "Date": ["2026-01-01"],
                "Ticker": [""],
                "Currency": [""],
                "Price": [100.0],
            }
        )
        df.to_csv(self.csv_path, index=False)

        inserted = import_stock_prices_csv(
            self.db_path,
            "stock_prices",
            self.csv_path,
            default_ticker="TPX",
            default_currency="JPY",
            date_column="Date",
            price_column="Price",
            ticker_column="Ticker",
            currency_column="Currency",
        )

        self.assertEqual(inserted, 1)

        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT Date, Ticker, Currency, Price FROM stock_prices"
            ).fetchone()
            self.assertEqual(row, ("2026-01-01", "TPX", "JPY", 100.0))
        finally:
            conn.close()

    def test_import_auto_detects_backup_csv_columns_when_pipeline_defaults_are_stale(self):
        df = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Ticker": ["7203", "6758"],
                "Currency": ["JPY", "JPY"],
                "Price": [1000.5, 2000.0],
            }
        )
        df.to_csv(self.csv_path, index=False)

        inserted = import_stock_prices_csv(
            self.db_path,
            "stock_prices",
            self.csv_path,
            default_ticker="",
            default_currency="JPY",
            date_column="Date",
            price_column="Close",
            ticker_column="",
            currency_column="",
        )

        self.assertEqual(inserted, 2)

        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT Date, Ticker, Currency, Price FROM stock_prices ORDER BY Date"
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    ("2026-01-01", "7203", "JPY", 1000.5),
                    ("2026-01-02", "6758", "JPY", 2000.0),
                ],
            )
        finally:
            conn.close()

    def test_load_ticker_data_prefers_stooq_history(self):
        db_path = os.path.join(self.tmpdir.name, "history.db")
        history = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Close": [810.0, 825.5],
            }
        )

        with patch("src.utilities.stock_prices._fetch_stooq_history", return_value=history) as fetch_stooq, patch(
            "src.utilities.stock_prices._fetch_yahoo_history"
        ) as fetch_yahoo:
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                ok = load_ticker_data("13010", "stock_prices", conn)
                conn.commit()
                rows = conn.execute(
                    "SELECT Date, Ticker, Currency, Price FROM stock_prices ORDER BY Date"
                ).fetchall()
            finally:
                conn.close()

        self.assertTrue(ok)
        self.assertEqual(fetch_stooq.call_args.args, ("1301.jp",))
        self.assertEqual(fetch_stooq.call_args.kwargs, {"start_date": None})
        fetch_yahoo.assert_not_called()
        self.assertEqual(
            rows,
            [
                ("2026-01-01", "13010", "JPY", 810.0),
                ("2026-01-02", "13010", "JPY", 825.5),
            ],
        )

    def test_load_ticker_data_falls_back_to_yahoo_when_stooq_fails(self):
        db_path = os.path.join(self.tmpdir.name, "fallback-history.db")
        history = pd.DataFrame(
            {
                ("Close", "1301.T"): [810.0, 825.5],
                ("Volume", "1301.T"): [1000, 1100],
            },
            index=pd.to_datetime(["2026-01-01", "2026-01-02"]),
        )
        history.index.name = "Date"

        with patch("src.utilities.stock_prices._fetch_stooq_history", side_effect=RuntimeError("blocked")) as fetch_stooq, patch(
            "src.utilities.stock_prices._fetch_yahoo_history", return_value=history
        ) as fetch_yahoo:
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                ok = load_ticker_data("13010", "stock_prices", conn)
                conn.commit()
                rows = conn.execute(
                    "SELECT Date, Ticker, Currency, Price FROM stock_prices ORDER BY Date"
                ).fetchall()
            finally:
                conn.close()

        self.assertTrue(ok)
        self.assertEqual(fetch_stooq.call_args.args, ("1301.jp",))
        self.assertEqual(fetch_yahoo.call_args.args, ("1301.T",))
        self.assertEqual(
            rows,
            [
                ("2026-01-01", "13010", "JPY", 810.0),
                ("2026-01-02", "13010", "JPY", 825.5),
            ],
        )

    def test_load_ticker_data_returns_false_when_all_providers_fail(self):
        db_path = os.path.join(self.tmpdir.name, "invalid-history.db")
        bad_history = pd.DataFrame({"Volume": [1000]}, index=pd.to_datetime(["2026-01-01"]))
        bad_history.index.name = "Date"

        with patch("src.utilities.stock_prices._fetch_stooq_history", side_effect=RuntimeError("blocked")), patch(
            "src.utilities.stock_prices._fetch_yahoo_history", return_value=bad_history
        ):
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                ok = load_ticker_data("13010", "stock_prices", conn)
            finally:
                conn.close()

        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
