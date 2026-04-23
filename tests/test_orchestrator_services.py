import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from src.orchestrator.common import ratios as ratio_services
from src.orchestrator.generate_financial_statements import service as financial_statement_services
from src.orchestrator.populate_business_descriptions_en import service as description_services


class TestGenerateFinancialStatementsPlaceholder(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.source_db = os.path.join(self.tmpdir.name, "source.db")
        self.target_db = os.path.join(self.tmpdir.name, "target.db")
        self.mappings_file = os.path.join(self.tmpdir.name, "mappings.json")

        conn = sqlite3.connect(self.source_db)
        conn.executescript(
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
        conn.executemany(
            "INSERT INTO Standard_Data (AccountingTerm, Period, Amount, docID, edinetCode, docTypeCode, periodStart, periodEnd) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("jppfs_cor:NetSales", "CurrentYearDuration", "1000", "DOC1", "E00001", "120", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:CashAndDeposits", "CurrentYearInstant", "250", "DOC1", "E00001", "120", "2024-01-01", "2024-12-31"),
            ],
        )
        conn.commit()
        conn.close()

        with open(self.mappings_file, "w", encoding="utf-8") as handle:
            json.dump({"Mappings": []}, handle)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_scaffolds_tables_and_seeds_document_rows(self):
        result = financial_statement_services.generate_financial_statements(
            source_database=self.source_db,
            source_table="Standard_Data",
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            overwrite=False,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            fs_rows = conn.execute(
                "SELECT docID, edinetCode, docTypeCode, periodStart, periodEnd FROM FinancialStatements"
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(result["status"], "placeholder")
        self.assertEqual(result["documents_seeded"], 1)
        self.assertTrue({"FinancialStatements", "IncomeStatement", "BalanceSheet", "CashflowStatement", "statement_line_items"}.issubset(tables))
        self.assertEqual(fs_rows, [("DOC1", "E00001", "120", "2024-01-01", "2024-12-31")])

    def test_refresh_statement_hierarchy_is_non_destructive_placeholder(self):
        conn = sqlite3.connect(self.target_db)
        try:
            conn.execute(
                """
                CREATE TABLE statement_line_items (
                    statement_family TEXT NOT NULL,
                    concept_qname TEXT NOT NULL,
                    column_name TEXT,
                    display_label TEXT,
                    concept_name TEXT,
                    taxonomy_release_id INTEGER,
                    role_uri TEXT,
                    presentation_parent_qname TEXT,
                    parent_column_name TEXT,
                    line_order REAL,
                    line_depth INTEGER,
                    period_key TEXT,
                    value_type TEXT,
                    is_abstract INTEGER,
                    is_required_metric INTEGER,
                    PRIMARY KEY (statement_family, concept_qname)
                )
                """
            )
            conn.execute(
                "INSERT INTO statement_line_items (statement_family, concept_qname) VALUES (?, ?)",
                ("IncomeStatement", "jppfs_cor:NetSales"),
            )
            conn.commit()
        finally:
            conn.close()

        result = financial_statement_services.refresh_statement_hierarchy(
            target_database=self.target_db,
            mappings_config=self.mappings_file,
            max_line_depth=5,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            rows = conn.execute("SELECT statement_family, concept_qname FROM statement_line_items").fetchall()
        finally:
            conn.close()

        self.assertEqual(result["status"], "placeholder")
        self.assertEqual(result["rows_retained"], 1)
        self.assertEqual(rows, [("IncomeStatement", "jppfs_cor:NetSales")])


class TestPopulateBusinessDescriptionsService(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.target_db = os.path.join(self.tmpdir.name, "target.db")
        conn = sqlite3.connect(self.target_db)
        conn.execute(
            """
            CREATE TABLE FinancialStatements (
                docID TEXT PRIMARY KEY,
                edinetCode TEXT,
                periodEnd TEXT,
                DescriptionOfBusiness TEXT,
                DescriptionOfBusiness_EN TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO FinancialStatements (docID, edinetCode, periodEnd, DescriptionOfBusiness, DescriptionOfBusiness_EN) VALUES (?, ?, ?, ?, ?)",
            ("DOC1", "E00001", "2024-12-31", "Makes parts", None),
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_translation_updates_seeded_placeholder_table(self):
        with patch(
            "src.orchestrator.populate_business_descriptions_en.description_translation.load_translation_providers",
            return_value=([object()], {"chunk_char_limit": 120, "row_delay_seconds": 0.0}),
        ), patch(
            "src.orchestrator.populate_business_descriptions_en.description_translation.translate_text_with_providers",
            return_value=("Makes parts in English.", "StubProvider"),
        ):
            result = description_services.populate_business_descriptions_en(
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
        self.assertEqual(result["provider_usage"], {"StubProvider": 1})


class TestGenerateRatiosPlaceholder(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "ratios.db")
        self.formulas_file = os.path.join(self.tmpdir.name, "ratios_formulas.json")

        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "CREATE TABLE FinancialStatements (docID TEXT PRIMARY KEY, SharePrice REAL)"
        )
        conn.executemany(
            "INSERT INTO FinancialStatements (docID, SharePrice) VALUES (?, ?)",
            [("DOC1", 100.0), ("DOC2", 110.0)],
        )
        conn.commit()
        conn.close()

        with open(self.formulas_file, "w", encoding="utf-8") as handle:
            json.dump({"PerShare": [{"Column": "EPS", "Formula": "1"}]}, handle)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_scaffolds_ratio_tables_without_formula_execution(self):
        result = ratio_services.generate_ratios(
            source_database=self.db_path,
            target_database=self.db_path,
            formulas_config=self.formulas_file,
            overwrite=False,
        )

        conn = sqlite3.connect(self.db_path)
        try:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            per_share_rows = conn.execute("SELECT docID FROM PerShare ORDER BY docID").fetchall()
            per_share_cols = [row[1] for row in conn.execute("PRAGMA table_info(PerShare)").fetchall()]
        finally:
            conn.close()

        self.assertEqual(result["status"], "placeholder")
        self.assertEqual(result["documents_seeded"], 2)
        self.assertTrue({"PerShare", "Valuation", "Quality"}.issubset(tables))
        self.assertEqual(per_share_rows, [("DOC1",), ("DOC2",)])
        self.assertEqual(per_share_cols, ["docID"])


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

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_generate_historical_ratios_creates_tables_and_metrics(self):
        ratio_services.generate_historical_ratios(
            source_database=self.db_path,
            target_database=self.db_path,
            overwrite=False,
        )

        conn = sqlite3.connect(self.db_path)
        try:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(Pershare_Historical)").fetchall()]
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

            d2 = [row for row in rows if row[0] == "D2"][0]
            self.assertEqual(d2[1], 3.0)
            self.assertEqual(d2[2], 2.0)
            self.assertAlmostEqual(d2[3], 2.0, places=6)
            self.assertIsNone(d2[4])

            d1 = [row for row in rows if row[0] == "D1"][0]
            self.assertIsNone(d1[3])

            d3 = [row for row in rows if row[0] == "D3"][0]
            self.assertIsNone(d3[3])

            price_rows = conn.execute(
                "SELECT docID, SharePrice, SharePrice_1Year_Average, SharePrice_1Year_Growth FROM Pershare_Historical ORDER BY docID"
            ).fetchall()
            price_map = {doc_id: (price, price_avg, price_growth) for doc_id, price, price_avg, price_growth in price_rows}
            self.assertEqual(price_map["D2"][0], 12.0)
            self.assertEqual(price_map["D2"][1], 12.0)
            self.assertAlmostEqual(price_map["D2"][2], 0.2, places=6)
            self.assertIsNone(price_map["D1"][2])
            self.assertIsNone(price_map["D3"][2])

            z_rows = conn.execute(
                "SELECT docID, EPS_ZScore_AllCompanies FROM Pershare_Historical ORDER BY docID"
            ).fetchall()
            z_map = {doc_id: z_value for doc_id, z_value in z_rows}
            self.assertAlmostEqual(z_map["D2"], 0.70710678, places=5)
            self.assertAlmostEqual(z_map["D3"], -0.70710678, places=5)
        finally:
            conn.close()

    def test_generate_historical_ratios_overwrite_rebuilds(self):
        ratio_services.generate_historical_ratios(
            source_database=self.db_path,
            target_database=self.db_path,
            overwrite=False,
        )
        ratio_services.generate_historical_ratios(
            source_database=self.db_path,
            target_database=self.db_path,
            overwrite=True,
        )

        conn = sqlite3.connect(self.db_path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM Quality_Historical").fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(count, 3)