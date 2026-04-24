import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from src.orchestrator.common import ratios as ratio_services
from src.orchestrator.generate_financial_statements import service as financial_statement_services
from src.orchestrator.populate_business_descriptions_en import service as description_services


class TestGenerateFinancialStatementsService(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.source_db = os.path.join(self.tmpdir.name, "source.db")
        self.target_db = os.path.join(self.tmpdir.name, "target.db")

        conn = sqlite3.connect(self.source_db)
        conn.executescript(
            """
            CREATE TABLE financialData_full (
                AccountingTerm TEXT,
                Period TEXT,
                Amount TEXT,
                docID TEXT,
                edinetCode TEXT,
                docTypeCode TEXT,
                submitDateTime TEXT,
                periodStart TEXT,
                periodEnd TEXT
            );
            """
        )
        conn.commit()
        conn.close()

        conn = sqlite3.connect(self.target_db)
        conn.executescript(
            """
            CREATE TABLE Taxonomy (
                release_id TEXT NOT NULL,
                statement_family TEXT NOT NULL,
                value_type TEXT NOT NULL,
                level INTEGER NOT NULL,
                concept_qname TEXT NOT NULL,
                parent_concept_qname TEXT,
                primary_label_en TEXT NOT NULL,
                PRIMARY KEY (release_id, concept_qname)
            );
            """
        )
        conn.executemany(
            "INSERT INTO Taxonomy (release_id, statement_family, value_type, level, concept_qname, parent_concept_qname, primary_label_en) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("2024-01-31", "IncomeStatement", "number", 1, "jppfs_cor:NetSales", None, "Net Sales"),
                ("2024-01-31", "IncomeStatement", "number", 1, "jppfs_cor:NonOperatingIncome", None, "Non-operating income"),
                ("2024-01-31", "IncomeStatement", "number", 1, "jppfs_cor:NonOperatingIncomeEDU", None, "Non-operating income"),
                ("2024-01-31", "IncomeStatement", "number", 1, "jppfs_cor:NonOperatingIncomeMED", None, "Non-operating income"),
                ("2024-01-31", "BalanceSheet", "number", 1, "jppfs_cor:CashAndDeposits", None, "Cash and Deposits"),
                ("2024-01-31", "CashflowStatement", "number", 1, "jppfs_cor:OperatingCashflow", None, "Operating Cashflow"),
                ("2024-01-31", "ShareMetrics", "number", 0, "jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults", None, "Total Number of Issued Shares, Summary of Business Results"),
                ("2024-01-31", "ShareMetrics", "number", 0, "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults", None, "Basic earnings (loss) per share"),
                ("2024-01-31", "ShareMetrics", "number", 0, "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults", None, "Dividend Paid Per Share, Summary of Business Results"),
                ("2024-01-31", "ShareMetrics", "number", 0, "jpcrp_cor:TotalShareholderReturn", None, "Total Shareholder Return"),
                ("2024-06-30", "IncomeStatement", "number", 1, "jppfs_cor:NetSales", None, "Net Sales"),
                ("2024-06-30", "IncomeStatement", "number", 1, "jppfs_cor:OperatingIncome", None, "Operating Income"),
                ("2024-06-30", "BalanceSheet", "number", 1, "jppfs_cor:CashAndDeposits", None, "Cash and Deposits"),
                ("2024-06-30", "CashflowStatement", "number", 1, "jppfs_cor:OperatingCashflow", None, "Operating Cashflow"),
                ("2024-06-30", "ShareMetrics", "number", 0, "jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults", None, "Total Number of Issued Shares, Summary of Business Results"),
                ("2024-06-30", "ShareMetrics", "number", 0, "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults", None, "Basic earnings (loss) per share"),
                ("2024-06-30", "ShareMetrics", "number", 0, "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults", None, "Dividend Paid Per Share, Summary of Business Results"),
                ("2024-06-30", "ShareMetrics", "number", 0, "jpcrp_cor:TotalShareholderReturn", None, "Total Shareholder Return"),
            ],
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _insert_source_rows(self, rows):
        conn = sqlite3.connect(self.source_db)
        conn.executemany(
            "INSERT INTO financialData_full (AccountingTerm, Period, Amount, docID, edinetCode, docTypeCode, submitDateTime, periodStart, periodEnd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
        conn.close()

    def test_generates_dynamic_statement_tables_from_taxonomy(self):
        self._insert_source_rows(
            [
                ("jppfs_cor:NetSales", "CurrentYearDuration", "1000", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:NonOperatingIncome", "CurrentYearDuration", "10", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:NonOperatingIncomeEDU", "CurrentYearDuration", "20", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:NonOperatingIncomeMED", "FilingDateInstant", "30", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:CashAndDeposits", "CurrentYearInstant", "250", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:CashAndDeposits", "Prior1YearInstant", "999", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
            ]
        )

        result = financial_statement_services.generate_financial_statements(
            source_database=self.source_db,
            target_database=self.target_db,
            granularity_level=1,
            overwrite=False,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            fs_rows = conn.execute(
                "SELECT docID, edinetCode, docTypeCode, submitDateTime, periodStart, periodEnd, release_id FROM FinancialStatements"
            ).fetchall()
            income_row = conn.execute(
                'SELECT docID, [Net Sales], [Non-operating income] FROM IncomeStatement WHERE docID = ?',
                ("DOC1",),
            ).fetchone()
            balance_row = conn.execute(
                'SELECT docID, [Cash and Deposits] FROM BalanceSheet WHERE docID = ?',
                ("DOC1",),
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["documents_processed"], 1)
        self.assertEqual(result["granularity_level"], 1)
        self.assertTrue({"FinancialStatements", "IncomeStatement", "BalanceSheet", "CashflowStatement"}.issubset(tables))
        self.assertEqual(
            fs_rows,
            [("DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31", "2024-01-31")],
        )
        self.assertEqual(income_row, ("DOC1", 1000.0, 60.0))
        self.assertEqual(balance_row, ("DOC1", 250.0))

    def test_appends_new_documents_and_adds_new_taxonomy_columns(self):
        self._insert_source_rows(
            [
                ("jppfs_cor:NetSales", "CurrentYearDuration", "1000", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:CashAndDeposits", "CurrentYearInstant", "250", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
            ]
        )
        financial_statement_services.generate_financial_statements(
            source_database=self.source_db,
            target_database=self.target_db,
            granularity_level=1,
            overwrite=False,
        )

        self._insert_source_rows(
            [
                ("jppfs_cor:NetSales", "CurrentYearDuration", "1200", "DOC2", "E00002", "120", "2024-08-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:OperatingIncome", "CurrentYearDuration", "300", "DOC2", "E00002", "120", "2024-08-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:CashAndDeposits", "CurrentYearInstant", "400", "DOC2", "E00002", "120", "2024-08-10T09:00:00", "2024-01-01", "2024-12-31"),
            ]
        )

        result = financial_statement_services.generate_financial_statements(
            source_database=self.source_db,
            target_database=self.target_db,
            granularity_level=1,
            overwrite=False,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            income_columns = [row[1] for row in conn.execute("PRAGMA table_info(IncomeStatement)").fetchall()]
            income_rows = conn.execute(
                'SELECT docID, [Net Sales], [Operating Income] FROM IncomeStatement ORDER BY docID'
            ).fetchall()
            fs_rows = conn.execute(
                'SELECT docID, release_id FROM FinancialStatements ORDER BY docID'
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(result["documents_processed"], 1)
        self.assertIn("Operating Income", income_columns)
        self.assertEqual(
            income_rows,
            [("DOC1", 1000.0, None), ("DOC2", 1200.0, 300.0)],
        )
        self.assertEqual(fs_rows, [("DOC1", "2024-01-31"), ("DOC2", "2024-06-30")])

    def test_processes_multiple_document_batches(self):
        rows = []
        for index in range(3):
            doc_id = f"DOC{index + 1}"
            rows.extend(
                [
                    ("jppfs_cor:NetSales", "CurrentYearDuration", str(1000 + index), doc_id, f"E0000{index + 1}", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                    ("jppfs_cor:CashAndDeposits", "CurrentYearInstant", str(250 + index), doc_id, f"E0000{index + 1}", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ]
            )
        self._insert_source_rows(rows)

        with patch.object(financial_statement_services, "_DOCUMENT_BATCH_SIZE", 2):
            result = financial_statement_services.generate_financial_statements(
                source_database=self.source_db,
                target_database=self.target_db,
                granularity_level=1,
                overwrite=False,
            )

        conn = sqlite3.connect(self.target_db)
        try:
            fs_count = conn.execute("SELECT COUNT(*) FROM FinancialStatements").fetchone()[0]
            income_rows = conn.execute(
                'SELECT docID, [Net Sales] FROM IncomeStatement ORDER BY docID'
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(result["documents_processed"], 3)
        self.assertEqual(fs_count, 3)
        self.assertEqual(
            income_rows,
            [("DOC1", 1000.0), ("DOC2", 1001.0), ("DOC3", 1002.0)],
        )

    def test_processes_multiple_releases_within_single_batch(self):
        self._insert_source_rows(
            [
                ("jppfs_cor:NetSales", "CurrentYearDuration", "1000", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:NonOperatingIncome", "CurrentYearDuration", "10", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:NetSales", "CurrentYearDuration", "1200", "DOC2", "E00002", "120", "2024-08-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jppfs_cor:OperatingIncome", "CurrentYearDuration", "300", "DOC2", "E00002", "120", "2024-08-10T09:00:00", "2024-01-01", "2024-12-31"),
            ]
        )

        result = financial_statement_services.generate_financial_statements(
            source_database=self.source_db,
            target_database=self.target_db,
            granularity_level=1,
            overwrite=False,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            fs_rows = conn.execute(
                "SELECT docID, release_id FROM FinancialStatements ORDER BY docID"
            ).fetchall()
            income_rows = conn.execute(
                'SELECT docID, [Net Sales], [Non-operating income], [Operating Income] FROM IncomeStatement ORDER BY docID'
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(result["documents_processed"], 2)
        self.assertEqual(fs_rows, [("DOC1", "2024-01-31"), ("DOC2", "2024-06-30")])
        self.assertEqual(
            income_rows,
            [("DOC1", 1000.0, 10.0, None), ("DOC2", 1200.0, None, 300.0)],
        )

    def test_generates_share_metrics_with_context_priority(self):
        self._insert_source_rows(
            [
                ("jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults", "CurrentYearInstant_NonConsolidatedMember", "90", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults", "CurrentYearInstant", "100", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults", "CurrentYearDuration", "70.82", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults", "CurrentYearDuration_NonConsolidatedMember", "12", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
                ("jpcrp_cor:TotalShareholderReturn", "FilingDateInstant", "1.5", "DOC1", "E00001", "120", "2024-05-10T09:00:00", "2024-01-01", "2024-12-31"),
            ]
        )

        result = financial_statement_services.generate_financial_statements(
            source_database=self.source_db,
            target_database=self.target_db,
            granularity_level=1,
            overwrite=False,
        )

        conn = sqlite3.connect(self.target_db)
        try:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            share_metrics_row = conn.execute(
                'SELECT docID, [Total Number of Issued Shares, Summary of Business Results], [Basic earnings (loss) per share], [Dividend Paid Per Share, Summary of Business Results], [Total Shareholder Return] FROM ShareMetrics WHERE docID = ?',
                ("DOC1",),
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["documents_processed"], 1)
        self.assertIn("ShareMetrics", tables)
        self.assertEqual(share_metrics_row, ("DOC1", 100.0, 70.82, 12.0, 1.5))


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