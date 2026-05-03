import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from src.orchestrator.common import ratios as ratio_services
from src.orchestrator.generate_ratios.generate_ratios import generate_ratios
from src.orchestrator.generate_financial_statements import service as financial_statement_services
from src.orchestrator.generate_rolling_metrics import service as rolling_metrics_services


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


class TestGenerateRollingMetricsService(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.database = os.path.join(self.tmpdir.name, "rolling.db")
        self.rolling_metrics_config = os.path.join(self.tmpdir.name, "rolling_metrics.json")

        with open(self.rolling_metrics_config, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "IncomeStatement": ["Revenue"],
                },
                handle,
            )

        conn = sqlite3.connect(self.database)
        conn.executescript(
            """
            CREATE TABLE FinancialStatements (
                docID TEXT PRIMARY KEY,
                edinetCode TEXT,
                periodEnd TEXT
            );

            CREATE TABLE IncomeStatement (
                docID TEXT PRIMARY KEY,
                Revenue REAL
            );

            CREATE TABLE CompanyInfo (
                edinetCode TEXT PRIMARY KEY,
                CompanyName TEXT
            );
            """
        )
        conn.executemany(
            "INSERT INTO FinancialStatements (docID, edinetCode, periodEnd) VALUES (?, ?, ?)",
            [
                ("A1", "E00001", "2020-03-31"),
                ("A2", "E00001", "2021-03-31"),
                ("A3", "E00001", "2022-03-31"),
                ("A4", "E00001", "2023-03-31"),
                ("B1", "E00002", "2020-03-31"),
                ("B2", "E00002", "2021-03-31"),
                ("B3", "E00002", "2022-03-31"),
                ("B4", "E00002", "2023-03-31"),
            ],
        )
        conn.executemany(
            "INSERT INTO IncomeStatement (docID, Revenue) VALUES (?, ?)",
            [
                ("A1", 100.0),
                ("A2", 110.0),
                ("A3", 121.0),
                ("A4", 133.1),
                ("B1", 200.0),
                ("B2", 220.0),
                ("B3", 242.0),
                ("B4", 266.2),
            ],
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _run_generate(self, overwrite=True):
        with patch.object(
            rolling_metrics_services,
            "ROLLING_METRICS_CONFIG_PATH",
            self.rolling_metrics_config,
        ):
            return rolling_metrics_services.generate_rolling_metrics(
                source_database=self.database,
                target_database=self.database,
                overwrite=overwrite,
            )

    def test_discovers_only_docid_primary_key_tables(self):
        conn = sqlite3.connect(self.database)
        try:
            tables = rolling_metrics_services.list_docid_primary_key_tables(
                conn,
                schema_name="main",
                excluded_tables=["FinancialStatements"],
            )
        finally:
            conn.close()

        self.assertEqual(tables, ["IncomeStatement"])

    def test_generates_rolling_table_with_expected_columns_and_values(self):
        result = self._run_generate(overwrite=True)

        conn = sqlite3.connect(self.database)
        try:
            table_names = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            columns = [
                row[1]
                for row in conn.execute("PRAGMA table_info(IncomeStatement_Rolling)").fetchall()
            ]
            row_a4 = conn.execute(
                """
                SELECT
                    docID,
                    Revenue_Average_3_Year,
                    Revenue_Growth_3_Year
                FROM IncomeStatement_Rolling
                WHERE docID = ?
                """,
                ("A4",),
            ).fetchone()
            row_b4 = conn.execute(
                """
                SELECT
                    docID,
                    Revenue_Average_3_Year,
                    Revenue_Growth_3_Year
                FROM IncomeStatement_Rolling
                WHERE docID = ?
                """,
                ("B4",),
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["status"], "completed")
        self.assertIn("IncomeStatement_Rolling", result["tables_processed"])
        self.assertIn("IncomeStatement_Rolling", table_names)
        self.assertIn("docID", columns)
        self.assertIn("Revenue_Average_3_Year", columns)
        self.assertIn("Revenue_Average_5_Year", columns)
        self.assertIn("Revenue_Average_10_Year", columns)
        self.assertIn("Revenue_Growth_3_Year", columns)
        self.assertIn("Revenue_Growth_5_Year", columns)
        self.assertIn("Revenue_Growth_10_Year", columns)

        self.assertEqual(row_a4[0], "A4")
        self.assertAlmostEqual(row_a4[1], (110.0 + 121.0 + 133.1) / 3.0, places=6)
        self.assertAlmostEqual(row_a4[2], 0.1, places=6)

        self.assertEqual(row_b4[0], "B4")
        self.assertAlmostEqual(row_b4[1], (220.0 + 242.0 + 266.2) / 3.0, places=6)
        self.assertAlmostEqual(row_b4[2], 0.1, places=6)

    def test_incremental_mode_recomputes_only_impacted_company(self):
        self._run_generate(overwrite=True)

        conn = sqlite3.connect(self.database)
        try:
            before_a4 = conn.execute(
                "SELECT Revenue_Average_3_Year FROM IncomeStatement_Rolling WHERE docID = ?",
                ("A4",),
            ).fetchone()[0]
            before_b4 = conn.execute(
                "SELECT Revenue_Average_3_Year FROM IncomeStatement_Rolling WHERE docID = ?",
                ("B4",),
            ).fetchone()[0]

            conn.execute(
                "UPDATE IncomeStatement SET Revenue = ? WHERE docID = ?",
                (111.0, "A2"),
            )
            conn.commit()
        finally:
            conn.close()

        self._run_generate(overwrite=False)

        conn = sqlite3.connect(self.database)
        try:
            after_a4 = conn.execute(
                "SELECT Revenue_Average_3_Year FROM IncomeStatement_Rolling WHERE docID = ?",
                ("A4",),
            ).fetchone()[0]
            after_b4 = conn.execute(
                "SELECT Revenue_Average_3_Year FROM IncomeStatement_Rolling WHERE docID = ?",
                ("B4",),
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertNotEqual(before_a4, after_a4)
        self.assertAlmostEqual(after_a4, (111.0 + 121.0 + 133.1) / 3.0, places=6)
        self.assertEqual(before_b4, after_b4)

    def test_sql_prefilter_skips_rows_with_all_numeric_metrics_null(self):
        conn = sqlite3.connect(self.database)
        try:
            conn.executescript(
                """
                CREATE TABLE MixedMetrics (
                    docID TEXT PRIMARY KEY,
                    MetricA REAL,
                    MetricB REAL,
                    Comment TEXT
                );
                """
            )
            conn.executemany(
                "INSERT INTO MixedMetrics (docID, MetricA, MetricB, Comment) VALUES (?, ?, ?, ?)",
                [
                    ("A1", None, None, "all null metrics"),
                    ("A2", 5.0, None, "has numeric data"),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        with open(self.rolling_metrics_config, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "MixedMetrics": ["MetricA", "MetricB"],
                },
                handle,
            )

        self._run_generate(overwrite=True)

        conn = sqlite3.connect(self.database)
        try:
            rows = conn.execute(
                "SELECT docID FROM MixedMetrics_Rolling ORDER BY docID"
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(rows, [("A2",)])

    def test_unlisted_tables_are_not_processed(self):
        conn = sqlite3.connect(self.database)
        try:
            conn.executescript(
                """
                CREATE TABLE ExtraTable (
                    docID TEXT PRIMARY KEY,
                    Value REAL
                );
                """
            )
            conn.executemany(
                "INSERT INTO ExtraTable (docID, Value) VALUES (?, ?)",
                [
                    ("A1", 1.0),
                    ("A2", 2.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        result = self._run_generate(overwrite=True)

        conn = sqlite3.connect(self.database)
        try:
            table_names = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        finally:
            conn.close()

        self.assertIn("IncomeStatement_Rolling", table_names)
        self.assertNotIn("ExtraTable_Rolling", table_names)
        self.assertNotIn("ExtraTable_Rolling", result["tables_processed"])


class TestGenerateRatios(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "ratios.db")
        self.formulas_file = os.path.join(self.tmpdir.name, "ratios_formulas.json")

        with open(self.formulas_file, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "ratios": {
                        "Financial_Ratios": [
                            {
                                "Return on Assets": {
                                    "inputs": [
                                        {
                                            "name": "Assets",
                                            "Table": "BalanceSheet",
                                            "Columns": ["Assets"],
                                            "Aggregation": "sum",
                                        },
                                        {
                                            "name": "NetIncome",
                                            "Table": "IncomeStatement",
                                            "Columns": ["Net income (loss)", "Profit (loss)"],
                                            "Aggregation": "sum",
                                        },
                                    ],
                                    "formula": "NetIncome / Assets",
                                    "skip_nulls": True,
                                }
                            },
                            {
                                "Net Margin": {
                                    "inputs": [
                                        {
                                            "name": "Revenue",
                                            "Table": "IncomeStatement",
                                            "Columns": ["Net sales"],
                                            "Aggregation": "sum",
                                        },
                                        {
                                            "name": "NetIncome",
                                            "Table": "IncomeStatement",
                                            "Columns": ["Net income (loss)", "Profit (loss)"],
                                            "Aggregation": "sum",
                                        },
                                    ],
                                    "formula": "NetIncome / Revenue",
                                    "skip_nulls": True,
                                }
                            },
                        ],
                        "PerShare_Metrics": [
                            {
                                "Sales Per Share": {
                                    "inputs": [
                                        {
                                            "name": "Revenue",
                                            "Table": "IncomeStatement",
                                            "Columns": ["Net sales"],
                                            "Aggregation": "sum",
                                        },
                                        {
                                            "name": "SharesOutstanding",
                                            "Table": "ShareMetrics",
                                            "Columns": [
                                                "Total number of issued shares",
                                                "Number of issued shares as of filing date",
                                                "Number of issued shares as of fiscal year end",
                                            ],
                                            "Aggregation": "FirstNonNull",
                                        },
                                    ],
                                    "formula": "Revenue / SharesOutstanding",
                                    "skip_nulls": True,
                                }
                            }
                        ],
                    }
                },
                handle,
            )

        conn = sqlite3.connect(self.db_path)
        conn.executescript(
            """
            CREATE TABLE FinancialStatements (
                docID TEXT PRIMARY KEY,
                SharePrice REAL
            );
            CREATE TABLE IncomeStatement (
                docID TEXT PRIMARY KEY,
                "Net sales" REAL,
                "Net income (loss)" REAL,
                "Profit (loss)" REAL
            );
            CREATE TABLE BalanceSheet (
                docID TEXT PRIMARY KEY,
                "Assets" REAL
            );
            CREATE TABLE ShareMetrics (
                docID TEXT PRIMARY KEY,
                "Total number of issued shares" REAL,
                "Number of issued shares as of filing date" REAL,
                "Number of issued shares as of fiscal year end" REAL
            );
            """
        )
        conn.executemany(
            "INSERT INTO FinancialStatements (docID, SharePrice) VALUES (?, ?)",
            [("DOC1", 100.0), ("DOC2", 110.0), ("DOC3", 120.0)],
        )
        conn.executemany(
            "INSERT INTO IncomeStatement (docID, \"Net sales\", \"Net income (loss)\", \"Profit (loss)\") VALUES (?, ?, ?, ?)",
            [
                ("DOC1", 50.0, 10.0, None),
                ("DOC2", 40.0, None, 8.0),
                ("DOC3", None, 6.0, None),
            ],
        )
        conn.executemany(
            "INSERT INTO BalanceSheet (docID, \"Assets\") VALUES (?, ?)",
            [("DOC1", 100.0), ("DOC2", 80.0), ("DOC3", 60.0)],
        )
        conn.executemany(
            "INSERT INTO ShareMetrics (docID, \"Total number of issued shares\", \"Number of issued shares as of filing date\", \"Number of issued shares as of fiscal year end\") VALUES (?, ?, ?, ?)",
            [
                ("DOC1", 5.0, 7.0, 9.0),
                ("DOC2", None, 4.0, 6.0),
                ("DOC3", None, None, 3.0),
            ],
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_creates_dynamic_ratio_tables_and_computes_values(self):
        with patch(
            "src.orchestrator.generate_ratios.generate_ratios.RATIO_DEFINITIONS_PATH",
            self.formulas_file,
        ):
            result = generate_ratios(
                database=self.db_path,
                overwrite=False,
            )

        conn = sqlite3.connect(self.db_path)
        try:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            financial_ratio_rows = conn.execute(
                "SELECT docID, \"Return on Assets\", \"Net Margin\" FROM Financial_Ratios ORDER BY docID"
            ).fetchall()
            financial_ratio_cols = [
                row[1] for row in conn.execute("PRAGMA table_info(Financial_Ratios)").fetchall()
            ]
            per_share_rows = conn.execute(
                "SELECT docID, \"Sales Per Share\" FROM PerShare_Metrics ORDER BY docID"
            ).fetchall()
            per_share_cols = [
                row[1] for row in conn.execute("PRAGMA table_info(PerShare_Metrics)").fetchall()
            ]
        finally:
            conn.close()

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["documents_seeded"], 3)
        self.assertEqual(result["ratio_count"], 3)
        self.assertEqual(result["tables"], ["Financial_Ratios", "PerShare_Metrics"])
        self.assertTrue({"Financial_Ratios", "PerShare_Metrics"}.issubset(tables))
        self.assertEqual(
            financial_ratio_cols,
            ["docID", "Return on Assets", "Net Margin"],
        )
        self.assertEqual(
            per_share_cols,
            ["docID", "Sales Per Share"],
        )
        self.assertEqual(
            financial_ratio_rows,
            [
                ("DOC1", 0.1, 0.2),
                ("DOC2", 0.1, 0.2),
                ("DOC3", 0.1, None),
            ],
        )
        self.assertEqual(
            per_share_rows,
            [
                ("DOC1", 10.0),
                ("DOC2", 10.0),
                ("DOC3", None),
            ],
        )