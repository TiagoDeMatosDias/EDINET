"""
Tests for src/data_processing.py

Strategy
--------
* Pure-logic methods (_adjust_string) are tested by bypassing __init__ via
  object.__new__ so there is no dependency on .env or the config files.
* Methods that only need a database (parse_edinet_taxonomy, _create_table,
  _insert_data) receive an in-memory SQLite connection directly, so the real
  SQL paths are exercised without any file-system side-effects.
"""
import os
import sys
import json
import sqlite3
import tempfile
import textwrap
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.data_processing import data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_data_instance():
    """Return a data() instance with __init__ bypassed (no .env needed)."""
    instance = object.__new__(data)
    instance.DB_PATH = ":memory:"
    return instance


# ---------------------------------------------------------------------------
# _adjust_string
# ---------------------------------------------------------------------------

class TestAdjustString(unittest.TestCase):

    def setUp(self):
        self.d = _make_data_instance()

    def test_matching_prefix_is_replaced(self):
        result = self.d._adjust_string("jppfs_cor_Revenue", "jppfs_cor_", "jppfs_cor:")
        self.assertEqual(result, "jppfs_cor:Revenue")

    def test_non_matching_prefix_unchanged(self):
        result = self.d._adjust_string("jpcrp_cor_OtherItem", "jppfs_cor_", "jppfs_cor:")
        self.assertEqual(result, "jpcrp_cor_OtherItem")

    def test_only_first_occurrence_is_replaced(self):
        result = self.d._adjust_string("jppfs_cor_jppfs_cor_X", "jppfs_cor_", "jppfs_cor:")
        # Leading prefix replaced once; inner occurrence stays
        self.assertEqual(result, "jppfs_cor:jppfs_cor_X")

    def test_none_input_returns_none(self):
        result = self.d._adjust_string(None, "jppfs_cor_", "jppfs_cor:")
        self.assertIsNone(result)

    def test_empty_string_unchanged(self):
        result = self.d._adjust_string("", "jppfs_cor_", "jppfs_cor:")
        self.assertEqual(result, "")


# ---------------------------------------------------------------------------
# _create_table and _insert_data  (in-memory SQLite)
# ---------------------------------------------------------------------------

class TestCreateTableAndInsertData(unittest.TestCase):

    def setUp(self):
        self.d = _make_data_instance()
        self.conn = sqlite3.connect(":memory:")

    def tearDown(self):
        self.conn.close()

    def test_create_table_produces_correct_columns(self):
        self.d._create_table(self.conn, "mytable", ["Id", "Name", "Type"])
        cursor = self.conn.execute("PRAGMA table_info(mytable)")
        cols = [row[1] for row in cursor.fetchall()]
        self.assertEqual(cols, ["Id", "Name", "Type"])

    def test_create_table_is_idempotent(self):
        """Calling twice should not raise (IF NOT EXISTS)."""
        self.d._create_table(self.conn, "mytable", ["Id", "Name"])
        self.d._create_table(self.conn, "mytable", ["Id", "Name"])  # no exception

    def test_insert_data_roundtrip(self):
        self.d._create_table(self.conn, "items", ["Id", "Val"])
        rows = [("id1", "v1"), ("id2", "v2")]
        self.d._insert_data(self.conn, "items", rows)
        self.conn.commit()
        result = self.conn.execute("SELECT Id, Val FROM items").fetchall()
        self.assertEqual(result, rows)

    def test_insert_data_empty_list_is_safe(self):
        self.d._create_table(self.conn, "empty_table", ["Id"])
        self.d._insert_data(self.conn, "empty_table", [])  # should not raise
        count = self.conn.execute("SELECT COUNT(*) FROM empty_table").fetchone()[0]
        self.assertEqual(count, 0)


# ---------------------------------------------------------------------------
# parse_edinet_taxonomy  (minimal real XSD + in-memory SQLite)
# ---------------------------------------------------------------------------

_MINIMAL_XSD = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
               xmlns:xbrli="http://www.xbrl.org/2003/instance">
      <!-- Balance Sheet asset (instant, debit) -->
      <xs:element name="CashAndDeposits"
                  id="jppfs_cor_CashAndDeposits"
                  abstract="false"
                  xbrli:periodType="instant"
                  xbrli:balance="debit"/>
      <!-- Balance Sheet liability (instant, credit) -->
      <xs:element name="LongTermLoan"
                  id="jppfs_cor_LongTermLoan"
                  abstract="false"
                  xbrli:periodType="instant"
                  xbrli:balance="credit"/>
      <!-- Income Statement income (duration, credit) -->
      <xs:element name="Revenue"
                  id="jppfs_cor_Revenue"
                  abstract="false"
                  xbrli:periodType="duration"
                  xbrli:balance="credit"/>
      <!-- Income Statement expense (duration, debit) -->
      <xs:element name="OperatingExpenses"
                  id="jppfs_cor_OperatingExpenses"
                  abstract="false"
                  xbrli:periodType="duration"
                  xbrli:balance="debit"/>
      <!-- Cashflow (duration, no balance) -->
      <xs:element name="CashFlowFromOperations"
                  id="jppfs_cor_CashFlowFromOperations"
                  abstract="false"
                  xbrli:periodType="duration"/>
      <!-- Abstract element – should get statement = "Other Statement" -->
      <xs:element name="AbstractParent"
                  id="jppfs_cor_AbstractParent"
                  abstract="true"
                  xbrli:periodType="instant"/>
    </xs:schema>
""")


class TestParseEdinetTaxonomy(unittest.TestCase):

    def setUp(self):
        self.d = _make_data_instance()
        self.conn = sqlite3.connect(":memory:")

        # Write minimal XSD to a temp file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".xsd", delete=False, encoding="utf-8"
        ) as f:
            f.write(_MINIMAL_XSD)
            self.xsd_path = f.name

    def tearDown(self):
        self.conn.close()
        os.remove(self.xsd_path)

    def _rows(self):
        """Return all rows from the taxonomy table as a list of dicts."""
        cur = self.conn.execute("SELECT Id, Name, Statement, Type FROM taxonomy")
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def test_table_created_with_correct_columns(self):
        self.d.parse_edinet_taxonomy(self.xsd_path, "taxonomy", connection=self.conn)
        cur = self.conn.execute("PRAGMA table_info(taxonomy)")
        cols = [row[1] for row in cur.fetchall()]
        self.assertEqual(cols, ["Id", "Name", "Statement", "Type"])

    def test_correct_row_count(self):
        """6 elements in XSD, all have id + name → 6 rows expected."""
        self.d.parse_edinet_taxonomy(self.xsd_path, "taxonomy", connection=self.conn)
        rows = self._rows()
        self.assertEqual(len(rows), 6)

    def test_id_prefix_adjusted(self):
        """jppfs_cor_ prefix should be converted to jppfs_cor:"""
        self.d.parse_edinet_taxonomy(self.xsd_path, "taxonomy", connection=self.conn)
        ids = {r["Id"] for r in self._rows()}
        self.assertIn("jppfs_cor:CashAndDeposits", ids)
        self.assertNotIn("jppfs_cor_CashAndDeposits", ids)

    def test_balance_sheet_asset_classification(self):
        self.d.parse_edinet_taxonomy(self.xsd_path, "taxonomy", connection=self.conn)
        rows = {r["Name"]: r for r in self._rows()}
        self.assertEqual(rows["CashAndDeposits"]["Statement"], "Balance Sheet")
        self.assertEqual(rows["CashAndDeposits"]["Type"], "Asset")

    def test_balance_sheet_liability_classification(self):
        self.d.parse_edinet_taxonomy(self.xsd_path, "taxonomy", connection=self.conn)
        rows = {r["Name"]: r for r in self._rows()}
        self.assertEqual(rows["LongTermLoan"]["Statement"], "Balance Sheet")
        self.assertEqual(rows["LongTermLoan"]["Type"], "Liability")

    def test_income_statement_income_classification(self):
        self.d.parse_edinet_taxonomy(self.xsd_path, "taxonomy", connection=self.conn)
        rows = {r["Name"]: r for r in self._rows()}
        self.assertEqual(rows["Revenue"]["Statement"], "Income Statement")
        self.assertEqual(rows["Revenue"]["Type"], "Income")

    def test_income_statement_expense_classification(self):
        self.d.parse_edinet_taxonomy(self.xsd_path, "taxonomy", connection=self.conn)
        rows = {r["Name"]: r for r in self._rows()}
        self.assertEqual(rows["OperatingExpenses"]["Statement"], "Income Statement")
        self.assertEqual(rows["OperatingExpenses"]["Type"], "Expense")

    def test_cashflow_classification(self):
        self.d.parse_edinet_taxonomy(self.xsd_path, "taxonomy", connection=self.conn)
        rows = {r["Name"]: r for r in self._rows()}
        self.assertEqual(rows["CashFlowFromOperations"]["Statement"], "Cashflow Statement")
        self.assertEqual(rows["CashFlowFromOperations"]["Type"], "Other")

    def test_abstract_element_is_other_statement(self):
        self.d.parse_edinet_taxonomy(self.xsd_path, "taxonomy", connection=self.conn)
        rows = {r["Name"]: r for r in self._rows()}
        self.assertEqual(rows["AbstractParent"]["Statement"], "Other Statement")


class TestGenerateFinancialStatements(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.source_db = os.path.join(self.tmpdir.name, "source.db")
        self.target_db = os.path.join(self.tmpdir.name, "target.db")
        self.mappings_file = os.path.join(self.tmpdir.name, "mappings.json")

        source_conn = sqlite3.connect(self.source_db)
        source_conn.executescript(
            """
            CREATE TABLE Standard_Data (
                AccountingTerm TEXT,
                Period TEXT,
                Amount TEXT,
                docID TEXT,
                edinetCode TEXT,
                docTypeCode TEXT,
                periodStart TEXT,
                periodEnd TEXT
            );
            """
        )
        rows = [
            ("jppfs_cor:NetSales", "CurrentYearDuration", "1000", "DOC1", "E00001", "120", "2024-01-01", "2024-12-31"),
            ("jppfs_cor:OperatingIncome", "CurrentYearDuration", "150", "DOC1", "E00001", "120", "2024-01-01", "2024-12-31"),
            ("jppfs_cor:CashAndDeposits", "CurrentYearInstant", "250", "DOC1", "E00001", "120", "2024-01-01", "2024-12-31"),
            ("jppfs_cor:CurrentAssets", "CurrentYearInstant", "800", "DOC1", "E00001", "120", "2024-01-01", "2024-12-31"),
            ("jpcrp_cor:NumberOfEmployees", "CurrentYearInstant", "42", "DOC1", "E00001", "120", "2024-01-01", "2024-12-31"),
            ("jpcrp_cor:DescriptionOfBusinessTextBlock", "FilingDateInstant", "Makes parts", "DOC1", "E00001", "120", "2024-01-01", "2024-12-31"),
            ("jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults", "CurrentYearInstant", "500", "DOC1", "E00001", "120", "2024-01-01", "2024-12-31"),
        ]
        source_conn.executemany(
            "INSERT INTO Standard_Data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        source_conn.commit()
        source_conn.close()

        target_conn = sqlite3.connect(self.target_db)
        target_conn.executescript(
            """
            CREATE TABLE companyInfo (
                EdinetCode TEXT,
                Company_Ticker TEXT
            );
            CREATE TABLE stock_prices (
                Date TEXT,
                Ticker TEXT,
                Currency TEXT,
                Price REAL
            );
            """
        )
        target_conn.executemany(
            "INSERT INTO companyInfo (EdinetCode, Company_Ticker) VALUES (?, ?)",
            [("E00001", "7203")],
        )
        target_conn.executemany(
            "INSERT INTO stock_prices (Date, Ticker, Currency, Price) VALUES (?, ?, ?, ?)",
            [
                ("2024-06-30", "7203", "JPY", 98.0),
                ("2024-12-31", "7203", "JPY", 123.0),
                ("2025-01-15", "7203", "JPY", 130.0),
            ],
        )
        target_conn.commit()
        target_conn.close()

        mappings = {
            "Mappings": [
                {
                    "Name": "SharesOutstanding",
                    "Table": "FinancialStatements",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults"],
                },
                {
                    "Name": "netSales",
                    "Table": "IncomeStatement",
                    "periods": ["CurrentYearDuration"],
                    "Terms": ["jppfs_cor:NetSales"],
                },
                {
                    "Name": "cash",
                    "Table": "BalanceSheet",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jppfs_cor:CashAndDeposits"],
                },
            ]
        }
        with open(self.mappings_file, "w", encoding="utf-8") as f:
            json.dump(mappings, f)

        self.d = _make_data_instance()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_generates_tables_and_rows(self):
        self.d.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            overwrite=False,
            batch_size=10,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            fs = conn.execute(
                "SELECT docID, edinetCode, SharesOutstanding, SharePrice, DescriptionOfBusiness_EN FROM FinancialStatements"
            ).fetchall()
            inc = conn.execute(
                "SELECT docID, netSales FROM IncomeStatement"
            ).fetchall()
            bal = conn.execute(
                "SELECT docID, cash FROM BalanceSheet"
            ).fetchall()

            self.assertEqual(fs, [("DOC1", "E00001", 500.0, 123.0, None)])
            self.assertEqual(inc, [("DOC1", 1000.0)])
            self.assertEqual(bal, [("DOC1", 250.0)])
        finally:
            conn.close()

    def test_rerun_is_idempotent(self):
        self.d.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            overwrite=False,
            batch_size=10,
        )
        self.d.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            overwrite=False,
            batch_size=10,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            counts = {
                "FinancialStatements": conn.execute("SELECT COUNT(*) FROM FinancialStatements").fetchone()[0],
                "IncomeStatement": conn.execute("SELECT COUNT(*) FROM IncomeStatement").fetchone()[0],
                "BalanceSheet": conn.execute("SELECT COUNT(*) FROM BalanceSheet").fetchone()[0],
                "CashflowStatement": conn.execute("SELECT COUNT(*) FROM CashflowStatement").fetchone()[0],
            }
            self.assertEqual(counts["FinancialStatements"], 1)
            self.assertEqual(counts["IncomeStatement"], 1)
            self.assertEqual(counts["BalanceSheet"], 1)
            self.assertEqual(counts["CashflowStatement"], 1)
        finally:
            conn.close()

    def test_rerun_backfills_new_mapped_columns(self):
        self.d.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            overwrite=False,
            batch_size=10,
        )

        expanded_mappings = {
            "Mappings": [
                {
                    "Name": "SharesOutstanding",
                    "Table": "FinancialStatements",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults"],
                },
                {
                    "Name": "NumberOfEmployees",
                    "Table": "FinancialStatements",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jpcrp_cor:NumberOfEmployees"],
                },
                {
                    "Name": "DescriptionOfBusiness",
                    "Table": "FinancialStatements",
                    "periods": ["FilingDateInstant"],
                    "Terms": ["jpcrp_cor:DescriptionOfBusinessTextBlock"],
                },
                {
                    "Name": "netSales",
                    "Table": "IncomeStatement",
                    "periods": ["CurrentYearDuration"],
                    "Terms": ["jppfs_cor:NetSales"],
                },
                {
                    "Name": "operatingIncome",
                    "Table": "IncomeStatement",
                    "periods": ["CurrentYearDuration"],
                    "Terms": ["jppfs_cor:OperatingIncome"],
                },
                {
                    "Name": "cash",
                    "Table": "BalanceSheet",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jppfs_cor:CashAndDeposits"],
                },
                {
                    "Name": "currentAssets",
                    "Table": "BalanceSheet",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jppfs_cor:CurrentAssets"],
                },
            ]
        }
        with open(self.mappings_file, "w", encoding="utf-8") as f:
            json.dump(expanded_mappings, f)

        self.d.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            overwrite=False,
            batch_size=10,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            fs_cols = {row[1] for row in conn.execute("PRAGMA table_info(FinancialStatements)").fetchall()}
            self.assertIn("NumberOfEmployees", fs_cols)
            self.assertIn("DescriptionOfBusiness", fs_cols)
            self.assertIn("DescriptionOfBusiness_EN", fs_cols)

            fs = conn.execute(
                "SELECT NumberOfEmployees, DescriptionOfBusiness FROM FinancialStatements WHERE docID = ?",
                ("DOC1",),
            ).fetchone()
            inc = conn.execute(
                "SELECT operatingIncome FROM IncomeStatement WHERE docID = ?",
                ("DOC1",),
            ).fetchone()
            bal = conn.execute(
                "SELECT currentAssets FROM BalanceSheet WHERE docID = ?",
                ("DOC1",),
            ).fetchone()

            self.assertEqual(fs, (42.0, "Makes parts"))
            self.assertEqual(inc, (150.0,))
            self.assertEqual(bal, (800.0,))
        finally:
            conn.close()

    def test_populate_business_descriptions_en_updates_translation_column(self):
        expanded_mappings = {
            "Mappings": [
                {
                    "Name": "SharesOutstanding",
                    "Table": "FinancialStatements",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults"],
                },
                {
                    "Name": "DescriptionOfBusiness",
                    "Table": "FinancialStatements",
                    "periods": ["FilingDateInstant"],
                    "Terms": ["jpcrp_cor:DescriptionOfBusinessTextBlock"],
                },
                {
                    "Name": "netSales",
                    "Table": "IncomeStatement",
                    "periods": ["CurrentYearDuration"],
                    "Terms": ["jppfs_cor:NetSales"],
                },
                {
                    "Name": "cash",
                    "Table": "BalanceSheet",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jppfs_cor:CashAndDeposits"],
                },
            ]
        }
        with open(self.mappings_file, "w", encoding="utf-8") as f:
            json.dump(expanded_mappings, f)

        self.d.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            overwrite=False,
            batch_size=10,
        )

        with patch(
            "src.description_translation.load_translation_providers",
            return_value=([object()], {"chunk_char_limit": 120, "row_delay_seconds": 0.0}),
        ), patch(
            "src.description_translation.translate_text_with_providers",
            return_value=("Makes parts in English.", "StubProvider"),
        ):
            result = self.d.populate_business_descriptions_en(
                target_database=self.target_db,
                providers_config="ignored.json",
                batch_size=10,
            )

        conn = sqlite3.connect(self.target_db)
        try:
            translated_value = conn.execute(
                "SELECT DescriptionOfBusiness_EN FROM FinancialStatements WHERE docID = ?",
                ("DOC1",),
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(translated_value, "Makes parts in English.")
        self.assertEqual(result["translated_rows"], 1)
        self.assertEqual(result["failed_rows"], 0)
        self.assertEqual(result["provider_usage"], {"StubProvider": 1})

    def test_populate_business_descriptions_en_skips_existing_translations(self):
        expanded_mappings = {
            "Mappings": [
                {
                    "Name": "SharesOutstanding",
                    "Table": "FinancialStatements",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults"],
                },
                {
                    "Name": "DescriptionOfBusiness",
                    "Table": "FinancialStatements",
                    "periods": ["FilingDateInstant"],
                    "Terms": ["jpcrp_cor:DescriptionOfBusinessTextBlock"],
                },
            ]
        }
        with open(self.mappings_file, "w", encoding="utf-8") as f:
            json.dump(expanded_mappings, f)

        self.d.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            overwrite=False,
            batch_size=10,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            conn.execute(
                "UPDATE FinancialStatements SET DescriptionOfBusiness_EN = ? WHERE docID = ?",
                ("Already translated.", "DOC1"),
            )
            conn.commit()
        finally:
            conn.close()

        with patch(
            "src.description_translation.load_translation_providers",
            return_value=([object()], {"chunk_char_limit": 120, "row_delay_seconds": 0.0}),
        ), patch(
            "src.description_translation.translate_text_with_providers",
        ) as mock_translate:
            result = self.d.populate_business_descriptions_en(
                target_database=self.target_db,
                providers_config="ignored.json",
                batch_size=10,
            )

        mock_translate.assert_not_called()
        self.assertEqual(result["processed_rows"], 0)
        self.assertEqual(result["translated_rows"], 0)
        self.assertEqual(result["failed_rows"], 0)
        self.assertEqual(result["existing_translation_rows"], 1)
        self.assertFalse(result["stopped_early"])

    def test_populate_business_descriptions_en_stops_when_providers_are_exhausted(self):
        from src.description_translation import TranslationError

        expanded_mappings = {
            "Mappings": [
                {
                    "Name": "SharesOutstanding",
                    "Table": "FinancialStatements",
                    "periods": ["CurrentYearInstant"],
                    "Terms": ["jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults"],
                },
                {
                    "Name": "DescriptionOfBusiness",
                    "Table": "FinancialStatements",
                    "periods": ["FilingDateInstant"],
                    "Terms": ["jpcrp_cor:DescriptionOfBusinessTextBlock"],
                },
            ]
        }
        with open(self.mappings_file, "w", encoding="utf-8") as f:
            json.dump(expanded_mappings, f)

        self.d.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            overwrite=False,
            batch_size=10,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            conn.execute(
                "INSERT INTO FinancialStatements (docID, DescriptionOfBusiness) VALUES (?, ?)",
                ("DOC2", "Builds robots"),
            )
            conn.commit()
        finally:
            conn.close()

        def _exhaust_providers(_text, providers, **_kwargs):
            providers.clear()
            raise TranslationError("All translation providers are unavailable for the remainder of this run.")

        with patch(
            "src.description_translation.load_translation_providers",
            return_value=([object(), object()], {"chunk_char_limit": 120, "row_delay_seconds": 0.0}),
        ), patch(
            "src.description_translation.translate_text_with_providers",
            side_effect=_exhaust_providers,
        ) as mock_translate, self.assertLogs("src.data_processing", level="WARNING") as logs:
            result = self.d.populate_business_descriptions_en(
                target_database=self.target_db,
                providers_config="ignored.json",
                batch_size=10,
            )

        conn = sqlite3.connect(self.target_db)
        try:
            target_values = conn.execute(
                "SELECT docID, DescriptionOfBusiness_EN FROM FinancialStatements ORDER BY docID"
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(mock_translate.call_count, 1)
        self.assertEqual(result["processed_rows"], 1)
        self.assertEqual(result["translated_rows"], 0)
        self.assertEqual(result["failed_rows"], 1)
        self.assertTrue(result["stopped_early"])
        self.assertIn("All translation providers are unavailable", result["stop_reason"])
        self.assertTrue(any("Stopping Populate Business Descriptions EN early" in message for message in logs.output))
        self.assertEqual(target_values, [("DOC1", None), ("DOC2", None)])

    def test_shareprice_falls_back_to_source_db(self):
        # Remove lookup tables from target DB
        target_conn = sqlite3.connect(self.target_db)
        target_conn.executescript(
            """
            DROP TABLE IF EXISTS companyInfo;
            DROP TABLE IF EXISTS stock_prices;
            """
        )
        target_conn.commit()
        target_conn.close()

        # Create lookup tables in source DB (fallback path)
        source_conn = sqlite3.connect(self.source_db)
        source_conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS companyInfo (
                EdinetCode TEXT,
                Company_Ticker TEXT
            );
            CREATE TABLE IF NOT EXISTS stock_prices (
                Date TEXT,
                Ticker TEXT,
                Currency TEXT,
                Price REAL
            );
            """
        )
        source_conn.executemany(
            "INSERT INTO companyInfo (EdinetCode, Company_Ticker) VALUES (?, ?)",
            [("E00001", "7203")],
        )
        source_conn.executemany(
            "INSERT INTO stock_prices (Date, Ticker, Currency, Price) VALUES (?, ?, ?, ?)",
            [
                ("2024-12-30", "7203", "JPY", 122.0),
                ("2024-12-31", "7203", "JPY", 123.0),
            ],
        )
        source_conn.commit()
        source_conn.close()

        # Intentionally use different case for company table name
        self.d.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            company_table="CompanyInfo",
            prices_table="stock_prices",
            overwrite=False,
            batch_size=10,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            fs = conn.execute(
                "SELECT docID, SharePrice FROM FinancialStatements"
            ).fetchall()
            self.assertEqual(fs, [("DOC1", 123.0)])
        finally:
            conn.close()


class TestGenerateRatios(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "ratios.db")
        self.formulas_file = os.path.join(self.tmpdir.name, "ratios_formulas.json")

        conn = sqlite3.connect(self.db_path)
        conn.executescript(
            """
            CREATE TABLE FinancialStatements (
                docID TEXT PRIMARY KEY,
                SharesOutstanding REAL,
                SharePrice REAL
            );
            CREATE TABLE IncomeStatement (
                docID TEXT PRIMARY KEY,
                netIncome REAL,
                netSales REAL
            );
            CREATE TABLE BalanceSheet (
                docID TEXT PRIMARY KEY,
                currentAssets REAL,
                currentLiabilities REAL,
                totalAssets REAL,
                shareholdersEquity REAL
            );
            CREATE TABLE CashflowStatement (
                docID TEXT PRIMARY KEY,
                operatingCashflow REAL
            );
            """
        )
        conn.execute(
            "INSERT INTO FinancialStatements (docID, SharesOutstanding, SharePrice) VALUES (?, ?, ?)",
            ("DOC1", 500.0, 100.0),
        )
        conn.execute(
            "INSERT INTO IncomeStatement (docID, netIncome, netSales) VALUES (?, ?, ?)",
            ("DOC1", 50.0, 1000.0),
        )
        conn.execute(
            "INSERT INTO BalanceSheet (docID, currentAssets, currentLiabilities, totalAssets, shareholdersEquity) VALUES (?, ?, ?, ?, ?)",
            ("DOC1", 400.0, 200.0, 1200.0, 700.0),
        )
        conn.execute(
            "INSERT INTO CashflowStatement (docID, operatingCashflow) VALUES (?, ?)",
            ("DOC1", 10.0),
        )
        conn.commit()
        conn.close()

        self.d = _make_data_instance()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_generate_ratios_creates_tables_and_computes_formulas(self):
        formulas = {
            "Quality": [
                {"Column": "CurrentRatio", "Formula": "BalanceSheet.currentAssets / BalanceSheet.currentLiabilities"},
                {"Column": "ReturnOnAssets", "Formula": "IncomeStatement.netIncome / BalanceSheet.totalAssets"},
            ],
            "PerShare": [
                {"Column": "EPS", "Formula": "IncomeStatement.netIncome / FinancialStatements.SharesOutstanding"},
            ],
            "Valuation": [
                {"Column": "PERatio", "Formula": "FinancialStatements.SharePrice / PerShare.EPS"},
            ],
        }
        with open(self.formulas_file, "w", encoding="utf-8") as f:
            json.dump(formulas, f)

        self.d.generate_ratios(
            source_database=self.db_path,
            target_database=self.db_path,
            formulas_config=self.formulas_file,
            overwrite=False,
            batch_size=10,
        )

        conn = sqlite3.connect(self.db_path)
        try:
            per_share = conn.execute("SELECT docID, EPS FROM PerShare").fetchall()
            valuation = conn.execute("SELECT docID, PERatio FROM Valuation").fetchall()
            quality = conn.execute("SELECT docID, CurrentRatio, ReturnOnAssets FROM Quality").fetchall()

            self.assertEqual(per_share, [("DOC1", 0.1)])
            self.assertEqual(valuation, [("DOC1", 1000.0)])
            self.assertEqual(quality, [("DOC1", 2.0, 50.0 / 1200.0)])
        finally:
            conn.close()

    def test_generate_ratios_logs_cyclic_dependencies_and_executes_independent_formulas(self):
        formulas = {
            "Quality": [
                {"Column": "CurrentRatio", "Formula": "BalanceSheet.currentAssets / BalanceSheet.currentLiabilities"},
            ],
            "PerShare": [
                {"Column": "A", "Formula": "Valuation.B + 1"},
            ],
            "Valuation": [
                {"Column": "B", "Formula": "PerShare.A + 1"},
            ],
        }
        with open(self.formulas_file, "w", encoding="utf-8") as f:
            json.dump(formulas, f)

        with self.assertLogs("src.data_processing", level="WARNING") as logs:
            self.d.generate_ratios(
                source_database=self.db_path,
                target_database=self.db_path,
                formulas_config=self.formulas_file,
                overwrite=False,
                batch_size=10,
            )

        log_text = "\n".join(logs.output)
        self.assertIn("cyclic/unresolved", log_text)

        conn = sqlite3.connect(self.db_path)
        try:
            quality = conn.execute("SELECT docID, CurrentRatio FROM Quality").fetchall()
            per_share = conn.execute("SELECT docID, A FROM PerShare").fetchall()
            valuation = conn.execute("SELECT docID, B FROM Valuation").fetchall()

            self.assertEqual(quality, [("DOC1", 2.0)])
            self.assertEqual(per_share, [("DOC1", None)])
            self.assertEqual(valuation, [("DOC1", None)])
        finally:
            conn.close()


class TestGenerateHistoricalRatios(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "historical.db")
        conn = sqlite3.connect(self.db_path)
        conn.executescript(
            """
            CREATE TABLE FinancialStatements (
                docID TEXT PRIMARY KEY,
                edinetCode TEXT,
                periodEnd TEXT,
                SharePrice REAL
            );
            CREATE TABLE PerShare (
                docID TEXT PRIMARY KEY,
                EPS REAL
            );
            CREATE TABLE Quality (
                docID TEXT PRIMARY KEY,
                CurrentRatio REAL
            );
            CREATE TABLE Valuation (
                docID TEXT PRIMARY KEY,
                PERatio REAL
            );
            """
        )

        conn.executemany(
            "INSERT INTO FinancialStatements (docID, edinetCode, periodEnd, SharePrice) VALUES (?, ?, ?, ?)",
            [
                ("D1", "E1", "2022-12-31", 10.0),
                ("D2", "E1", "2023-12-31", 12.0),
                ("D3", "E2", "2023-12-31", 11.0),
            ],
        )
        conn.executemany(
            "INSERT INTO PerShare (docID, EPS) VALUES (?, ?)",
            [("D1", 1.0), ("D2", 3.0), ("D3", 2.0)],
        )
        conn.executemany(
            "INSERT INTO Quality (docID, CurrentRatio) VALUES (?, ?)",
            [("D1", 1.5), ("D2", 2.0), ("D3", 1.8)],
        )
        conn.executemany(
            "INSERT INTO Valuation (docID, PERatio) VALUES (?, ?)",
            [("D1", 10.0), ("D2", 12.0), ("D3", 11.0)],
        )
        conn.commit()
        conn.close()

        self.d = _make_data_instance()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_generate_historical_ratios_creates_tables_and_metrics(self):
        self.d.generate_historical_ratios(
            source_database=self.db_path,
            target_database=self.db_path,
            overwrite=False,
        )

        conn = sqlite3.connect(self.db_path)
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(Pershare_Historical)").fetchall()]
            self.assertIn("docID", cols)
            self.assertIn("EPS_1Year_Average", cols)
            self.assertIn("EPS_2Year_Average", cols)
            self.assertIn("EPS_StdDev", cols)
            self.assertIn("EPS_ZScore_IntraCompany", cols)
            self.assertIn("EPS_ZScore_AllCompanies", cols)
            self.assertIn("EPS_1Year_Growth", cols)
            self.assertIn("EPS_2Year_Growth", cols)
            self.assertIn("EPS_3Year_Growth", cols)
            self.assertIn("SharePrice_1Year_Average", cols)
            self.assertIn("SharePrice_1Year_Growth", cols)

            rows = conn.execute(
                "SELECT docID, EPS_1Year_Average, EPS_2Year_Average, EPS_1Year_Growth, EPS_2Year_Growth FROM Pershare_Historical ORDER BY docID"
            ).fetchall()
            self.assertEqual(len(rows), 3)

            # E1 chronology: D1=1.0, D2=3.0 => D2 2-year rolling avg = 2.0
            d2 = [r for r in rows if r[0] == "D2"][0]
            self.assertEqual(d2[1], 3.0)
            self.assertEqual(d2[2], 2.0)

            # D2 (E1, 2023): 1-year CAGR = (3.0/1.0)^(1/1) - 1 = 2.0
            self.assertAlmostEqual(d2[3], 2.0, places=6)
            # D2 (E1, 2023): no 2-year prior for E1 => 2-year CAGR is NULL
            self.assertIsNone(d2[4])

            # D1 (E1, 2022): no prior year => 1-year growth is NULL
            d1 = [r for r in rows if r[0] == "D1"][0]
            self.assertIsNone(d1[3])

            # D3 (E2, 2023): only one year for E2 => growth is NULL
            d3 = [r for r in rows if r[0] == "D3"][0]
            self.assertIsNone(d3[3])

            price_rows = conn.execute(
                "SELECT docID, SharePrice, SharePrice_1Year_Average, SharePrice_1Year_Growth "
                "FROM Pershare_Historical ORDER BY docID"
            ).fetchall()
            p_map = {doc: (p, p_avg, p_g) for doc, p, p_avg, p_g in price_rows}
            # D2 (E1, 2023): SharePrice 10 -> 12 => 1-year growth = 0.2
            self.assertEqual(p_map["D2"][0], 12.0)
            self.assertEqual(p_map["D2"][1], 12.0)
            self.assertAlmostEqual(p_map["D2"][2], 0.2, places=6)
            # First observation per company has no 1-year growth baseline
            self.assertIsNone(p_map["D1"][2])
            self.assertIsNone(p_map["D3"][2])

            # Cross-sectional all-companies z-score at 2023-12-31:
            # D2(E1)=3.0, D3(E2)=2.0 => mean=2.5, std(sample)=~0.7071
            z_rows = conn.execute(
                "SELECT docID, EPS_ZScore_AllCompanies FROM Pershare_Historical ORDER BY docID"
            ).fetchall()
            z_map = {doc: z for doc, z in z_rows}
            self.assertAlmostEqual(z_map["D2"], 0.70710678, places=5)
            self.assertAlmostEqual(z_map["D3"], -0.70710678, places=5)
        finally:
            conn.close()

    def test_generate_historical_ratios_overwrite_rebuilds(self):
        self.d.generate_historical_ratios(
            source_database=self.db_path,
            target_database=self.db_path,
            overwrite=False,
        )
        self.d.generate_historical_ratios(
            source_database=self.db_path,
            target_database=self.db_path,
            overwrite=True,
        )

        conn = sqlite3.connect(self.db_path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM Quality_Historical").fetchone()[0]
            self.assertEqual(count, 3)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
