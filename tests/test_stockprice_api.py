import os
import sqlite3
import tempfile
import unittest

import pandas as pd

from src.stockprice_api import import_stock_prices_csv


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


if __name__ == "__main__":
    unittest.main()
