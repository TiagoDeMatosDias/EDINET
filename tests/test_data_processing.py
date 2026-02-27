"""
Tests for src/data_processing.py

Strategy
--------
* Pure-logic methods (evaluate_expression, _adjust_string) are tested by
  bypassing __init__ via object.__new__ so there is no dependency on .env or
  the config files.
* Methods that only need a database (parse_edinet_taxonomy, _create_table,
  _insert_data) receive an in-memory SQLite connection directly, so the real
  SQL paths are exercised without any file-system side-effects.
* The copy_table_to_Standard pipeline is tested by mocking its four
  sub-methods and verifying each is called once in the correct order –
  the sub-methods themselves are already tested individually.
"""
import os
import sys
import sqlite3
import tempfile
import textwrap
import unittest
from unittest.mock import MagicMock, call, patch

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
    instance.FINANCIAL_RATIOS_CONFIG_PATH = ""
    return instance


# ---------------------------------------------------------------------------
# evaluate_expression
# ---------------------------------------------------------------------------

class TestEvaluateExpression(unittest.TestCase):

    def setUp(self):
        self.d = _make_data_instance()
        self.df = pd.DataFrame({
            "a": [1.0, 2.0, 3.0],
            "b": [4.0, 5.0, 6.0],
            "c": [0.0, np.nan, 7.0],
        })

    def test_value_literal(self):
        result = self.d.evaluate_expression(self.df, {"value": 42})
        self.assertEqual(result, 42)

    def test_column_reference(self):
        result = self.d.evaluate_expression(self.df, {"column": "a"})
        pd.testing.assert_series_equal(result, self.df["a"])

    def test_fillna_with_scalar(self):
        expr = {"column": "c", "fillna": -1.0}
        result = self.d.evaluate_expression(self.df, expr)
        expected = pd.Series([0.0, -1.0, 7.0])
        pd.testing.assert_series_equal(result.reset_index(drop=True), expected,
                                       check_names=False)

    def test_fillna_with_fallback_column(self):
        expr = {"column": "c", "fillna": {"column": "a"}}
        result = self.d.evaluate_expression(self.df, expr)
        # c[0]=0.0, c[1]=NaN→a[1]=2.0, c[2]=7.0
        expected = pd.Series([0.0, 2.0, 7.0])
        pd.testing.assert_series_equal(result.reset_index(drop=True), expected,
                                       check_names=False)

    def test_addition(self):
        expr = {"operator": "+", "operands": [{"column": "a"}, {"column": "b"}]}
        result = self.d.evaluate_expression(self.df, expr)
        expected = pd.Series([5.0, 7.0, 9.0])
        pd.testing.assert_series_equal(result.reset_index(drop=True), expected)

    def test_subtraction(self):
        expr = {"operator": "-", "operands": [{"column": "b"}, {"column": "a"}]}
        result = self.d.evaluate_expression(self.df, expr)
        expected = pd.Series([3.0, 3.0, 3.0])
        pd.testing.assert_series_equal(result.reset_index(drop=True), expected)

    def test_multiplication(self):
        expr = {"operator": "*", "operands": [{"column": "a"}, {"value": 3}]}
        result = self.d.evaluate_expression(self.df, expr)
        expected = pd.Series([3.0, 6.0, 9.0])
        pd.testing.assert_series_equal(result.reset_index(drop=True), expected,
                                       check_names=False)

    def test_division(self):
        expr = {"operator": "/", "operands": [{"column": "b"}, {"column": "a"}]}
        result = self.d.evaluate_expression(self.df, expr)
        expected = self.df["b"] / self.df["a"]
        pd.testing.assert_series_equal(result.reset_index(drop=True),
                                       expected.reset_index(drop=True))

    def test_nested_expression(self):
        """(a + b) / value(2)"""
        expr = {
            "operator": "/",
            "operands": [
                {"operator": "+", "operands": [{"column": "a"}, {"column": "b"}]},
                {"value": 2},
            ],
        }
        result = self.d.evaluate_expression(self.df, expr)
        expected = (self.df["a"] + self.df["b"]) / 2
        pd.testing.assert_series_equal(result.reset_index(drop=True),
                                       expected.reset_index(drop=True))

    def test_unknown_operator_returns_none(self):
        expr = {"operator": "^", "operands": [{"column": "a"}, {"value": 2}]}
        result = self.d.evaluate_expression(self.df, expr)
        self.assertIsNone(result)


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


# ---------------------------------------------------------------------------
# copy_table_to_Standard  –  pipeline unit test (sub-methods mocked)
# ---------------------------------------------------------------------------

class TestCopyTableToStandard(unittest.TestCase):

    def setUp(self):
        self.d = _make_data_instance()

    def _run_with_mocks(self, table_exists=False, overwrite=False):
        """Patch all sub-methods and run copy_table_to_Standard."""
        conn = MagicMock()
        with patch.object(self.d, "copy_table") as mock_copy, \
             patch.object(self.d, "rename_columns_to_Standard") as mock_rename, \
             patch.object(self.d, "Filter_for_Relevant") as mock_filter, \
             patch.object(self.d, "delete_table") as mock_delete, \
             patch.object(self.d, "_table_exists", return_value=table_exists):

            self.d.copy_table_to_Standard(
                "src_tbl", "dst_tbl", conn=conn, overwrite=overwrite,
            )

            return mock_copy, mock_rename, mock_filter, mock_delete, conn

    def test_each_step_called_once(self):
        mc, mr, mf, md, _ = self._run_with_mocks(table_exists=False)
        self.assertEqual(mc.call_count, 1)
        self.assertEqual(mr.call_count, 1)
        self.assertEqual(mf.call_count, 1)
        self.assertEqual(md.call_count, 1)

    def test_source_table_passed_to_copy(self):
        mc, _, _, _, _ = self._run_with_mocks(table_exists=False)
        args = mc.call_args
        self.assertEqual(args[0][1], "src_tbl")   # second positional arg is source

    def test_target_table_passed_to_filter(self):
        _, _, mf, _, _ = self._run_with_mocks(table_exists=False)
        args = mf.call_args
        # second positional arg is the output_table
        self.assertEqual(args[0][1], "dst_tbl")

    def test_same_temp_table_used_throughout(self):
        """rename, filter, and delete must all act on the same temp table."""
        conn = MagicMock()
        captured = {}

        def _capture_copy(c, src, tmp):
            captured["tmp"] = tmp

        with patch.object(self.d, "copy_table", side_effect=_capture_copy), \
             patch.object(self.d, "rename_columns_to_Standard") as mock_rename, \
             patch.object(self.d, "Filter_for_Relevant") as mock_filter, \
             patch.object(self.d, "delete_table") as mock_delete, \
             patch.object(self.d, "_table_exists", return_value=False):

            self.d.copy_table_to_Standard("src_tbl", "dst_tbl", conn=conn)

        tmp = captured["tmp"]
        self.assertEqual(mock_rename.call_args[0][1], tmp)   # rename gets temp table
        self.assertEqual(mock_filter.call_args[0][0], tmp)   # filter reads from temp table
        self.assertEqual(mock_delete.call_args[0][0], tmp)   # delete drops temp table

    def test_incremental_inserts_only_new_docids(self):
        """When the target table exists, the incremental INSERT path is used."""
        mc, mr, mf, md, conn = self._run_with_mocks(table_exists=True)
        # Filter is called with the filtered temp name, not the target
        filter_output = mf.call_args[0][1]
        self.assertTrue(filter_output.startswith("_tmp_filtered_"))
        # delete_table called twice: once for filteredTemp, once for tempCopy
        self.assertEqual(md.call_count, 2)
        # An INSERT INTO ... SELECT was executed on the connection's cursor
        cursor = conn.cursor()
        cursor.execute.assert_called()

    def test_overwrite_deletes_target_first(self):
        """When overwrite=True, delete_table is called with the target name."""
        _, _, _, md, _ = self._run_with_mocks(table_exists=False, overwrite=True)
        # First delete_table call should be for the target table
        first_call_args = md.call_args_list[0]
        self.assertEqual(first_call_args[0][0], "dst_tbl")


if __name__ == "__main__":
    unittest.main()
