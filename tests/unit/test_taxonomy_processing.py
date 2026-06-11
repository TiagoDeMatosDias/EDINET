import base64
import io
import os
import sqlite3
import tempfile
import textwrap
import unittest
import zipfile
from unittest.mock import patch

from src.orchestrator.parse_taxonomy import taxonomy_processing


def _build_taxonomy_archive_bytes() -> bytes:
    concept_xsd = textwrap.dedent(
        """\
        <?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                   xmlns:xbrli="http://www.xbrl.org/2003/instance"
                   targetNamespace="http://example.com/jppfs">
          <xs:element name="BalanceSheetAbstract"
                      id="jppfs_cor_BalanceSheetAbstract"
                      abstract="true"
                      xbrli:periodType="instant" />
          <xs:element name="AssetsAbstract"
                      id="jppfs_cor_AssetsAbstract"
                      abstract="true"
                      xbrli:periodType="instant" />
          <xs:element name="CashAndDeposits"
                      id="jppfs_cor_CashAndDeposits"
                      abstract="false"
                      xbrli:periodType="instant"
                      xbrli:balance="debit" />
        </xs:schema>
        """
    )
    role_xsd = textwrap.dedent(
        """\
        <?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                   xmlns:link="http://www.xbrl.org/2003/linkbase">
                    <link:roleType roleURI="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_BalanceSheet" id="role_bs_std">
            <link:definition>Balance Sheet</link:definition>
          </link:roleType>
                    <link:roleType roleURI="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_BalanceSheetCustom" id="role_bs_custom">
                        <link:definition>Balance Sheet Custom</link:definition>
                    </link:roleType>
        </xs:schema>
        """
    )
    label_xml = textwrap.dedent(
        """\
        <?xml version="1.0" encoding="UTF-8"?>
        <link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"
                       xmlns:xlink="http://www.w3.org/1999/xlink">
          <link:labelLink xlink:type="extended">
            <link:loc xlink:type="locator"
                      xlink:label="loc_cash"
                      xlink:href="../jppfs_cor_2024-11-01.xsd#jppfs_cor_CashAndDeposits" />
            <link:label xlink:type="resource"
                        xlink:label="lab_cash"
                        xlink:role="http://www.xbrl.org/2003/role/label"
                        xml:lang="ja">Cash and Deposits</link:label>
            <link:labelArc xlink:type="arc"
                           xlink:from="loc_cash"
                           xlink:to="lab_cash" />
          </link:labelLink>
        </link:linkbase>
        """
    )
    presentation_xml = textwrap.dedent(
        """\
        <?xml version="1.0" encoding="UTF-8"?>
        <link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"
                       xmlns:xlink="http://www.w3.org/1999/xlink">
                    <link:presentationLink xlink:type="extended" xlink:role="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_BalanceSheet">
            <link:loc xlink:type="locator"
                                            xlink:label="bs"
                      xlink:href="../jppfs_cor_2024-11-01.xsd#jppfs_cor_BalanceSheetAbstract" />
            <link:loc xlink:type="locator"
                                            xlink:label="assets"
                                            xlink:href="../jppfs_cor_2024-11-01.xsd#jppfs_cor_AssetsAbstract" />
                        <link:loc xlink:type="locator"
                                            xlink:label="cash"
                      xlink:href="../jppfs_cor_2024-11-01.xsd#jppfs_cor_CashAndDeposits" />
            <link:presentationArc xlink:type="arc"
                                  xlink:arcrole="http://www.xbrl.org/2003/arcrole/parent-child"
                                                                    xlink:from="bs"
                                                                    xlink:to="assets"
                                  order="1" />
                        <link:presentationArc xlink:type="arc"
                                                                    xlink:arcrole="http://www.xbrl.org/2003/arcrole/parent-child"
                                                                    xlink:from="assets"
                                                                    xlink:to="cash"
                                                                    order="2" />
                    </link:presentationLink>
                    <link:presentationLink xlink:type="extended" xlink:role="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_BalanceSheetCustom">
                        <link:loc xlink:type="locator"
                                            xlink:label="custom_bs"
                                            xlink:href="../jppfs_cor_2024-11-01.xsd#jppfs_cor_BalanceSheetAbstract" />
                        <link:loc xlink:type="locator"
                                            xlink:label="custom_cash"
                                            xlink:href="../jppfs_cor_2024-11-01.xsd#jppfs_cor_CashAndDeposits" />
                        <link:presentationArc xlink:type="arc"
                                                                    xlink:arcrole="http://www.xbrl.org/2003/arcrole/parent-child"
                                                                    xlink:from="custom_bs"
                                                                    xlink:to="custom_cash"
                                                                    order="1" />
          </link:presentationLink>
        </link:linkbase>
        """
    )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("taxonomy/jppfs/2024-11-01/jppfs_cor_2024-11-01.xsd", concept_xsd)
        archive.writestr("taxonomy/jppfs/2024-11-01/jppfs_rt_2024-11-01.xsd", role_xsd)
        archive.writestr("taxonomy/jppfs/2024-11-01/label/jppfs_2024-11-01_lab.xml", label_xml)
        archive.writestr("taxonomy/jppfs/2024-11-01/r/jppfs_2024-11-01_pre.xml", presentation_xml)
    return buffer.getvalue()


class _FakeResponse:
    def __init__(self, *, text=None, json_payload=None):
        self.text = text or ""
        self._json_payload = json_payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._json_payload


class _FakeSession:
    def __init__(self, html_text, archive_bytes):
        self._html_text = html_text
        self._archive_bytes = archive_bytes

    def get(self, _url, timeout=30):
        return _FakeResponse(text=self._html_text)

    def post(self, _url, data=None, headers=None, timeout=60):
        del data, headers, timeout
        payload = {
            "gxProps": [
                {
                    "TXTSCRIPT": {
                        "Caption": (
                            'download("data:application/zip;base64,'
                            + base64.b64encode(self._archive_bytes).decode("ascii")
                            + '")'
                        )
                    }
                }
            ]
        }
        return _FakeResponse(json_payload=payload)

    def close(self):
        return None


class TestTaxonomyProcessing(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "taxonomy.db")
        self.download_dir = os.path.join(self.tmpdir.name, "downloads")
        self.archive_bytes = _build_taxonomy_archive_bytes()
        self.html_text = textwrap.dedent(
            """\
            <html>
              <body>
                <input type="hidden" id="GXState" value='{"GX_AJAX_IV":"GXIV","vPGMNAME":"WEEE0020","vPGMDESC":"EDINET TAXONOMY&CODE LIST","gxhash_vPGMNAME":"hash1","gxhash_vPGMDESC":"hash2"}' />
                <table>
                  <tr>
                    <td>EDINET Taxonomy 2025</td>
                    <td><a onclick="onDownload('ALL_20241101.zip');return false;">Download</a></td>
                  </tr>
                  <tr>
                    <td>Japan GAAP</td>
                    <td><a onclick="onDownload('JPPFS_20241101.zip');return false;">Download</a></td>
                  </tr>
                </table>
              </body>
            </html>
            """
        )

    def test_pick_primary_label_prefers_standard_xbrl_label_role(self):
        label = taxonomy_processing._pick_primary_label(
            [
                {
                    "language": "en",
                    "label_text": "Accounts payable",
                    "label_role": "http://disclosure.edinet-fsa.go.jp/jppfs/fnd/role/label",
                    "source_file": "custom.xml",
                },
                {
                    "language": "en",
                    "label_text": "Accounts payable - other",
                    "label_role": "http://www.xbrl.org/2003/role/label",
                    "source_file": "standard.xml",
                },
                {
                    "language": "en",
                    "label_text": "Accounts payable-other",
                    "label_role": "http://www.xbrl.org/2003/role/verboseLabel",
                    "source_file": "verbose.xml",
                },
            ],
            "en",
        )

        self.assertEqual(label, "Accounts payable - other")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_classify_statement_family_recognises_compact_edinet_role_uris(self):
        self.assertEqual(
            taxonomy_processing._classify_statement_family(
                "http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_ConsolidatedStatementOfIncome",
                role_uri="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_ConsolidatedStatementOfIncome",
                namespace_prefix="jppfs_cor",
            ),
            "IncomeStatement",
        )
        self.assertEqual(
            taxonomy_processing._classify_statement_family(
                "http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_ConsolidatedBalanceSheet",
                role_uri="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_ConsolidatedBalanceSheet",
                namespace_prefix="jppfs_cor",
            ),
            "BalanceSheet",
        )
        self.assertEqual(
            taxonomy_processing._classify_statement_family(
                "http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_ConsolidatedStatementOfCashFlows",
                role_uri="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_ConsolidatedStatementOfCashFlows",
                namespace_prefix="jppfs_cor",
            ),
            "CashflowStatement",
        )

    def test_classify_statement_family_treats_jpcrp_roles_as_disclosure(self):
        self.assertEqual(
            taxonomy_processing._classify_statement_family(
                "http://disclosure.edinet-fsa.go.jp/role/jpcrp/rol_std_NotesBalanceSheet",
                role_uri="http://disclosure.edinet-fsa.go.jp/role/jpcrp/rol_std_NotesBalanceSheet",
                namespace_prefix="jpcrp_cor",
            ),
            "Disclosure",
        )

    def test_is_standard_statement_role_only_accepts_standard_edinet_roles(self):
        self.assertTrue(
            taxonomy_processing._is_standard_statement_role(
                "http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_BalanceSheet",
                "BalanceSheet",
            )
        )
        self.assertFalse(
            taxonomy_processing._is_standard_statement_role(
                "http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_BalanceSheetCustom",
                "BalanceSheet",
            )
        )
        self.assertTrue(
            taxonomy_processing._is_standard_statement_role(
                "role://balance-sheet",
                "BalanceSheet",
            )
        )

    def test_select_primary_statement_role_candidate_prefers_core_balance_sheet_role(self):
        candidate = taxonomy_processing._select_primary_statement_role_candidate(
            [
                {
                    "statement_family": "BalanceSheet",
                    "role_uri": "http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_BalanceSheetBank",
                    "concept_qnames": {"jppfs_cor:AssetsAbstract", "jppfs_cor:Loans"},
                    "arcs": [{"child_concept_qname": "jppfs_cor:Loans"}],
                },
                {
                    "statement_family": "BalanceSheet",
                    "role_uri": "http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_BalanceSheet",
                    "concept_qnames": {"jppfs_cor:AssetsAbstract"},
                    "arcs": [{"child_concept_qname": "jppfs_cor:AssetsAbstract"}],
                },
            ]
        )

        self.assertIsNotNone(candidate)
        self.assertEqual(
            candidate["role_uri"],
            "http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_std_BalanceSheet",
        )

    def test_register_primary_candidate_prefers_parented_path(self):
        primary_metadata: dict[str, dict] = {}
        taxonomy_processing._register_primary_candidate(
            primary_metadata,
            "jppfs_cor:CurrentAssets",
            {
                "statement_family_default": "BalanceSheet",
                "primary_role_uri": "role://balance-sheet",
                "primary_parent_concept_qname": None,
                "primary_line_order": 1.0,
                "primary_line_depth": 0,
            },
        )
        taxonomy_processing._register_primary_candidate(
            primary_metadata,
            "jppfs_cor:CurrentAssets",
            {
                "statement_family_default": "BalanceSheet",
                "primary_role_uri": "role://balance-sheet",
                "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                "primary_line_order": 2.0,
                "primary_line_depth": 3,
            },
        )

        self.assertEqual(
            primary_metadata["jppfs_cor:CurrentAssets"]["primary_parent_concept_qname"],
            "jppfs_cor:AssetsAbstract",
        )

    def test_validate_taxonomy_rows_rejects_non_adjacent_parent_levels(self):
        with self.assertRaises(ValueError):
            taxonomy_processing._validate_taxonomy_rows(
                [
                    {
                        "release_id": "2024-11-01",
                        "statement_family": "BalanceSheet",
                        "value_type": "string",
                        "level": 0,
                        "concept_qname": "jppfs_cor:AssetsAbstract",
                        "parent_concept_qname": None,
                        "primary_label_en": "Assets",
                    },
                    {
                        "release_id": "2024-11-01",
                        "statement_family": "BalanceSheet",
                        "value_type": "number",
                        "level": 2,
                        "concept_qname": "jppfs_cor:CashAndDeposits",
                        "parent_concept_qname": "jppfs_cor:AssetsAbstract",
                        "primary_label_en": "Cash and Deposits",
                    },
                ]
            )

    def test_ensure_taxonomy_schema_accepts_share_metrics_rows(self):
        conn = sqlite3.connect(":memory:")
        try:
            taxonomy_processing._ensure_taxonomy_schema(conn)
            conn.execute(
                """
                INSERT INTO Taxonomy (
                    release_id,
                    statement_family,
                    value_type,
                    level,
                    concept_qname,
                    parent_concept_qname,
                    primary_label_en
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2024-11-01",
                    "ShareMetrics",
                    "number",
                    0,
                    "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults",
                    None,
                    "Dividend Paid Per Share",
                ),
            )
            inserted = conn.execute(
                "SELECT statement_family, level, parent_concept_qname FROM Taxonomy"
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(inserted, [("ShareMetrics", 0, None)])

    def test_build_taxonomy_level_rows_adds_share_metrics_as_level_zero_roots(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jpcrp_cor",
            {
                "jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults": {
                    "statement_family_default": None,
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Total Number of Issued Shares, Summary of Business Results",
                    "data_type": "xbrli:sharesItemType",
                    "is_abstract": 0,
                },
                "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults": {
                    "statement_family_default": None,
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Dividend Paid Per Share, Summary of Business Results",
                    "data_type": "xbrli:perShareItemType",
                    "is_abstract": 0,
                },
                "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults": {
                    "statement_family_default": None,
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Basic earnings (loss) per share",
                    "data_type": "xbrli:perShareItemType",
                    "is_abstract": 0,
                },
                "jpcrp_cor:TotalShareholderReturn": {
                    "statement_family_default": None,
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Total Shareholder Return",
                    "data_type": "xbrli:percentItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertEqual(
            set(rows_by_qname),
            {
                "jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults",
                "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults",
                "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults",
                "jpcrp_cor:TotalShareholderReturn",
            },
        )
        self.assertEqual(
            rows_by_qname["jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults"],
            {
                "release_id": "1",
                "statement_family": "ShareMetrics",
                "value_type": "number",
                "concept_qname": "jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults",
                "primary_label_en": "Total Number of Issued Shares, Summary of Business Results",
                "parent_concept_qname": None,
                "level": 0,
            },
        )
        self.assertEqual(rows_by_qname["jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults"]["statement_family"], "ShareMetrics")
        self.assertEqual(rows_by_qname["jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults"]["level"], 0)
        self.assertEqual(rows_by_qname["jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults"]["statement_family"], "ShareMetrics")
        self.assertEqual(rows_by_qname["jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults"]["primary_label_en"], "Basic earnings (loss) per share")
        self.assertEqual(rows_by_qname["jpcrp_cor:TotalShareholderReturn"]["parent_concept_qname"], None)

    def test_build_taxonomy_level_rows_compresses_statement_root_wrappers(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:BalanceSheetAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Balance Sheet",
                    "data_type": None,
                },
                "jppfs_cor:BalanceSheetLineItems": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Balance Sheet Line Items",
                    "data_type": None,
                },
                "jppfs_cor:BalanceSheetHeading": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetLineItems",
                    "primary_label_en": "Balance Sheet Heading",
                    "data_type": None,
                },
                "jppfs_cor:BalanceSheetTable": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetHeading",
                    "primary_label_en": "Balance Sheet Table",
                    "data_type": None,
                },
                "jppfs_cor:AssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetTable",
                    "primary_label_en": "Assets",
                    "data_type": None,
                },
                "jppfs_cor:ConsolidatedOrNonConsolidatedAxis": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Consolidated Or Non-Consolidated Axis",
                    "data_type": None,
                    "substitution_group": "xbrldt:dimensionItem",
                },
                "jppfs_cor:CurrentAssets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Current Assets",
                    "data_type": "xbrli:monetaryItemType",
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertNotIn("jppfs_cor:BalanceSheetAbstract", rows_by_qname)
        self.assertNotIn("jppfs_cor:BalanceSheetLineItems", rows_by_qname)
        self.assertNotIn("jppfs_cor:BalanceSheetHeading", rows_by_qname)
        self.assertNotIn("jppfs_cor:BalanceSheetTable", rows_by_qname)
        self.assertNotIn("jppfs_cor:ConsolidatedOrNonConsolidatedAxis", rows_by_qname)
        self.assertEqual(rows_by_qname["jppfs_cor:AssetsAbstract"]["parent_concept_qname"], None)
        self.assertEqual(rows_by_qname["jppfs_cor:AssetsAbstract"]["level"], 0)
        self.assertEqual(rows_by_qname["jppfs_cor:AssetsAbstract"]["value_type"], "string")
        self.assertEqual(
            rows_by_qname["jppfs_cor:CurrentAssets"]["parent_concept_qname"],
            "jppfs_cor:AssetsAbstract",
        )
        self.assertEqual(rows_by_qname["jppfs_cor:CurrentAssets"]["level"], 1)
        self.assertEqual(rows_by_qname["jppfs_cor:CurrentAssets"]["value_type"], "number")

    def test_build_taxonomy_level_rows_collapses_same_label_abstract_value_pairs(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:BalanceSheetAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Balance Sheet",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:BalanceSheetLineItems": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Balance Sheet Line Items",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:AssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetLineItems",
                    "primary_label_en": "Assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:Assets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:CurrentAssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Current Assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentAssets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Current Assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:CashAndDeposits": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssets",
                    "primary_label_en": "Cash and Deposits",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertNotIn("jppfs_cor:AssetsAbstract", rows_by_qname)
        self.assertNotIn("jppfs_cor:CurrentAssetsAbstract", rows_by_qname)
        self.assertEqual(rows_by_qname["jppfs_cor:Assets"]["parent_concept_qname"], None)
        self.assertEqual(rows_by_qname["jppfs_cor:Assets"]["level"], 0)
        self.assertEqual(
            rows_by_qname["jppfs_cor:CurrentAssets"]["parent_concept_qname"],
            "jppfs_cor:Assets",
        )
        self.assertEqual(rows_by_qname["jppfs_cor:CurrentAssets"]["level"], 1)
        self.assertEqual(
            rows_by_qname["jppfs_cor:CashAndDeposits"]["parent_concept_qname"],
            "jppfs_cor:CurrentAssets",
        )
        self.assertEqual(rows_by_qname["jppfs_cor:CashAndDeposits"]["level"], 2)
        self.assertEqual(rows_by_qname["jppfs_cor:CashAndDeposits"]["value_type"], "number")

    def test_build_taxonomy_level_rows_collapses_variant_abstract_parent_with_same_label(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:IncomeStatementAbstract": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Income Statement",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:OperatingExpensesAbstractCMD": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:IncomeStatementAbstract",
                    "primary_label_en": "Operating expenses",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:OperatingExpensesCMD": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:OperatingExpensesAbstractCMD",
                    "primary_label_en": "Operating expenses",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:SalariesAndAllowancesCMD": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:OperatingExpensesCMD",
                    "primary_label_en": "Salaries and allowances",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertNotIn("jppfs_cor:OperatingExpensesAbstractCMD", rows_by_qname)
        self.assertEqual(rows_by_qname["jppfs_cor:OperatingExpensesCMD"]["parent_concept_qname"], None)
        self.assertEqual(rows_by_qname["jppfs_cor:OperatingExpensesCMD"]["level"], 0)
        self.assertEqual(
            rows_by_qname["jppfs_cor:SalariesAndAllowancesCMD"]["parent_concept_qname"],
            "jppfs_cor:OperatingExpensesCMD",
        )
        self.assertEqual(rows_by_qname["jppfs_cor:SalariesAndAllowancesCMD"]["level"], 1)

    def test_build_taxonomy_level_rows_standardizes_industry_specific_parentage(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:BalanceSheetAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Balance sheet",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:AssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:Assets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:CurrentAssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentAssets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:CashAndDeposits": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Cash and deposits",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:CashAndDepositsAssetsINS": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Cash and deposits",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertEqual(rows_by_qname["jppfs_cor:CashAndDeposits"]["parent_concept_qname"], "jppfs_cor:CurrentAssets")
        self.assertEqual(rows_by_qname["jppfs_cor:CashAndDeposits"]["level"], 2)
        self.assertEqual(
            rows_by_qname["jppfs_cor:CashAndDepositsAssetsINS"]["parent_concept_qname"],
            "jppfs_cor:CurrentAssets",
        )
        self.assertEqual(rows_by_qname["jppfs_cor:CashAndDepositsAssetsINS"]["level"], 2)

    def test_build_taxonomy_level_rows_uses_qualifier_suffix_generic_anchor(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:BalanceSheetAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Balance sheet",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:AssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:Assets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:CurrentAssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentAssets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:MarketableSecuritiesCA": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Marketable securities",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:MarketableSecuritiesAssetsBNK": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Marketable securities",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertEqual(rows_by_qname["jppfs_cor:MarketableSecuritiesCA"]["parent_concept_qname"], "jppfs_cor:CurrentAssets")
        self.assertEqual(rows_by_qname["jppfs_cor:MarketableSecuritiesCA"]["level"], 2)
        self.assertEqual(
            rows_by_qname["jppfs_cor:MarketableSecuritiesAssetsBNK"]["parent_concept_qname"],
            "jppfs_cor:CurrentAssets",
        )
        self.assertEqual(rows_by_qname["jppfs_cor:MarketableSecuritiesAssetsBNK"]["level"], 2)

    def test_build_taxonomy_level_rows_falls_back_to_current_assets_for_direct_asset_receivable(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:BalanceSheetAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Balance sheet",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:AssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:Assets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:CurrentAssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentAssets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AgencyAccountsReceivableAssetsINS": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:AssetsAbstract",
                    "primary_label_en": "Agency accounts receivable",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertEqual(
            rows_by_qname["jppfs_cor:AgencyAccountsReceivableAssetsINS"]["parent_concept_qname"],
            "jppfs_cor:CurrentAssets",
        )
        self.assertEqual(rows_by_qname["jppfs_cor:AgencyAccountsReceivableAssetsINS"]["level"], 2)

    def test_build_taxonomy_level_rows_normalizes_family_labels_without_flattening_distinct_anchors(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:BalanceSheetAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Balance sheet",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentAssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentAssets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:CurrentLiabilitiesAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Current liabilities",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentLiabilities": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentLiabilitiesAbstract",
                    "primary_label_en": "Current liabilities",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:PropertyPlantAndEquipmentAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Property, plant and equipment",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:PropertyPlantAndEquipment": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:PropertyPlantAndEquipmentAbstract",
                    "primary_label_en": "Property, plant and equipment",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AccountsReceivableEDU": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Accounts receivable",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AccountsReceivableCustomerCACMD": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Accounts receivable - customer",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AccountsReceivableFormAgentsCAWAT": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Accounts receivable form agents",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AccountsReceivableFromCompletedConstructionContractsCNS": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Accounts receivable from completed construction contracts",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AccountsReceivableTrade": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Accounts receivable - trade",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AccountsPayableOther": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentLiabilitiesAbstract",
                    "primary_label_en": "Accounts payable - other",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AccountsPayableOtherBusinessCLWAT": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentLiabilitiesAbstract",
                    "primary_label_en": "Accounts payable - other business",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AdvancesReceived": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentLiabilitiesAbstract",
                    "primary_label_en": "Advances received",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:AdvancesReceivedForHighwayManagementHWY": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentLiabilitiesAbstract",
                    "primary_label_en": "Advances received for highway management",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:Vehicles": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:PropertyPlantAndEquipmentAbstract",
                    "primary_label_en": "Vehicles",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:VehiclesAndVesselsMED": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:PropertyPlantAndEquipmentAbstract",
                    "primary_label_en": "Vehicles and vessels",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:IncomeStatementAbstract": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Income statement",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:OperatingExpenses": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:IncomeStatementAbstract",
                    "primary_label_en": "Operating expenses",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:OperatingExpensesINS": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:IncomeStatementAbstract",
                    "primary_label_en": "Ordinary expenses",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertEqual(
            rows_by_qname["jppfs_cor:AccountsReceivableCustomerCACMD"]["primary_label_en"],
            "Accounts receivable",
        )
        self.assertEqual(
            rows_by_qname["jppfs_cor:AccountsReceivableFormAgentsCAWAT"]["primary_label_en"],
            "Accounts receivable",
        )
        self.assertEqual(
            rows_by_qname["jppfs_cor:AccountsReceivableFromCompletedConstructionContractsCNS"]["primary_label_en"],
            "Accounts receivable",
        )
        self.assertEqual(
            rows_by_qname["jppfs_cor:AccountsReceivableTrade"]["primary_label_en"],
            "Accounts receivable - trade",
        )
        self.assertEqual(
            rows_by_qname["jppfs_cor:AccountsPayableOtherBusinessCLWAT"]["primary_label_en"],
            "Accounts payable - other",
        )
        self.assertEqual(
            rows_by_qname["jppfs_cor:AdvancesReceivedForHighwayManagementHWY"]["primary_label_en"],
            "Advances received",
        )
        self.assertEqual(
            rows_by_qname["jppfs_cor:VehiclesAndVesselsMED"]["primary_label_en"],
            "Vehicles and vessels",
        )
        self.assertEqual(
            rows_by_qname["jppfs_cor:OperatingExpensesINS"]["primary_label_en"],
            "Operating expenses",
        )

    def test_build_taxonomy_level_rows_keeps_non_generic_child_under_variant_parent(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:IncomeStatementAbstract": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Income statement",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:OperatingExpensesAbstract": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:IncomeStatementAbstract",
                    "primary_label_en": "Operating expenses",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:OperatingExpenses": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:OperatingExpensesAbstract",
                    "primary_label_en": "Operating expenses",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:OperatingExpensesAbstractCMD": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:IncomeStatementAbstract",
                    "primary_label_en": "Operating expenses",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:OperatingExpensesCMD": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:OperatingExpensesAbstractCMD",
                    "primary_label_en": "Operating expenses",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:SalariesAndAllowancesCMD": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:OperatingExpensesCMD",
                    "primary_label_en": "Salaries and allowances",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertEqual(rows_by_qname["jppfs_cor:OperatingExpensesCMD"]["parent_concept_qname"], None)
        self.assertEqual(
            rows_by_qname["jppfs_cor:SalariesAndAllowancesCMD"]["parent_concept_qname"],
            "jppfs_cor:OperatingExpensesCMD",
        )

    def test_build_taxonomy_level_rows_skips_same_label_numeric_ancestor(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:BalanceSheetAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Balance sheet",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentAssetsAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentAssets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssetsAbstract",
                    "primary_label_en": "Current assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:ContractAssets": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentAssets",
                    "primary_label_en": "Contract assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:ContractAssetsNet": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:ContractAssets",
                    "primary_label_en": "Contract assets",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertEqual(rows_by_qname["jppfs_cor:CurrentAssets"]["level"], 0)
        self.assertEqual(rows_by_qname["jppfs_cor:ContractAssets"]["parent_concept_qname"], "jppfs_cor:CurrentAssets")
        self.assertEqual(rows_by_qname["jppfs_cor:ContractAssets"]["level"], 1)
        self.assertEqual(
            rows_by_qname["jppfs_cor:ContractAssetsNet"]["parent_concept_qname"],
            "jppfs_cor:CurrentAssets",
        )
        self.assertEqual(rows_by_qname["jppfs_cor:ContractAssetsNet"]["level"], 1)

    def test_build_taxonomy_level_rows_collapses_same_label_abstract_sibling(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:StatementOfIncomeLineItems": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Statement of income line items",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:GeneralAndAdministrativeExpensesAbstractWAT": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:StatementOfIncomeLineItems",
                    "primary_label_en": "General and administrative expenses",
                    "data_type": "xbrli:stringItemType",
                    "is_abstract": 1,
                },
                "jppfs_cor:GeneralAndAdministrativeExpensesWAT": {
                    "statement_family_default": "IncomeStatement",
                    "primary_parent_concept_qname": "jppfs_cor:StatementOfIncomeLineItems",
                    "primary_label_en": "General and administrative expenses",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertNotIn("jppfs_cor:GeneralAndAdministrativeExpensesAbstractWAT", rows_by_qname)
        self.assertEqual(
            rows_by_qname["jppfs_cor:GeneralAndAdministrativeExpensesWAT"]["parent_concept_qname"],
            None,
        )
        self.assertEqual(rows_by_qname["jppfs_cor:GeneralAndAdministrativeExpensesWAT"]["level"], 0)

    def test_build_taxonomy_level_rows_prunes_orphan_string_heading(self):
        rows = taxonomy_processing._build_taxonomy_level_rows(
            1,
            "jppfs_cor",
            {
                "jppfs_cor:BalanceSheetAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": None,
                    "primary_label_en": "Balance sheet",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:LiabilitiesAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:BalanceSheetAbstract",
                    "primary_label_en": "Liabilities",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:Liabilities": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:LiabilitiesAbstract",
                    "primary_label_en": "Liabilities",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:CurrentLiabilitiesAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:LiabilitiesAbstract",
                    "primary_label_en": "Current liabilities",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:CurrentLiabilities": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentLiabilitiesAbstract",
                    "primary_label_en": "Current liabilities",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:NoncurrentLiabilitiesAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:LiabilitiesAbstract",
                    "primary_label_en": "Non-current liabilities",
                    "data_type": None,
                    "is_abstract": 1,
                },
                "jppfs_cor:NoncurrentLiabilities": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:NoncurrentLiabilitiesAbstract",
                    "primary_label_en": "Non-current liabilities",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:ReservesUnderTheSpecialLawsAbstract2": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:LiabilitiesAbstract",
                    "primary_label_en": "Provisions",
                    "data_type": "xbrli:stringItemType",
                    "is_abstract": 1,
                },
                "jppfs_cor:ProvisionCLAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentLiabilitiesAbstract",
                    "primary_label_en": "Provisions",
                    "data_type": "xbrli:stringItemType",
                    "is_abstract": 1,
                },
                "jppfs_cor:ProvisionCL": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:CurrentLiabilitiesAbstract",
                    "primary_label_en": "Provisions",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
                "jppfs_cor:ProvisionNCLAbstract": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:NoncurrentLiabilitiesAbstract",
                    "primary_label_en": "Provisions",
                    "data_type": "xbrli:stringItemType",
                    "is_abstract": 1,
                },
                "jppfs_cor:ProvisionNCL": {
                    "statement_family_default": "BalanceSheet",
                    "primary_parent_concept_qname": "jppfs_cor:NoncurrentLiabilitiesAbstract",
                    "primary_label_en": "Provisions",
                    "data_type": "xbrli:monetaryItemType",
                    "is_abstract": 0,
                },
            },
        )

        rows_by_qname = {row["concept_qname"]: row for row in rows}
        self.assertNotIn("jppfs_cor:ReservesUnderTheSpecialLawsAbstract2", rows_by_qname)
        self.assertEqual(rows_by_qname["jppfs_cor:ProvisionCL"]["parent_concept_qname"], "jppfs_cor:CurrentLiabilities")
        self.assertEqual(rows_by_qname["jppfs_cor:ProvisionNCL"]["parent_concept_qname"], "jppfs_cor:NoncurrentLiabilities")

    def test_select_listing_entries_defaults_to_all_historical_releases(self):
        listing = [
            taxonomy_processing.TaxonomyListingEntry(
                archive_name="JPPFS_20231101.zip",
                namespace_prefix="jppfs_cor",
                taxonomy_date="2023-11-01",
                release_label="EDINET Taxonomy 2024",
                release_year=2024,
                source_page_url="https://example.test",
            ),
            taxonomy_processing.TaxonomyListingEntry(
                archive_name="JPPFS_20241101.zip",
                namespace_prefix="jppfs_cor",
                taxonomy_date="2024-11-01",
                release_label="EDINET Taxonomy 2025",
                release_year=2025,
                source_page_url="https://example.test",
            ),
        ]

        selected = taxonomy_processing._select_listing_entries(
            listing,
            namespaces=["jppfs_cor"],
        )

        self.assertEqual(
            [entry.archive_name for entry in selected],
            ["JPPFS_20231101.zip", "JPPFS_20241101.zip"],
        )

    def test_sync_taxonomy_releases_parses_archive_into_single_taxonomy_table(self):
        fake_session_factory = lambda: _FakeSession(self.html_text, self.archive_bytes)

        with patch(
            "src.orchestrator.parse_taxonomy.taxonomy_processing.requests.get",
            return_value=_FakeResponse(text=self.html_text),
        ), patch(
            "src.orchestrator.parse_taxonomy.taxonomy_processing.requests.Session",
            side_effect=fake_session_factory,
        ):
            stats = taxonomy_processing.sync_taxonomy_releases(
                target_database=self.db_path,
                release_selection="latest",
                namespaces=["jppfs_cor"],
                download_dir=self.download_dir,
            )

        conn = sqlite3.connect(self.db_path)
        try:
            tables = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND lower(name) LIKE 'taxonomy%' ORDER BY name"
                ).fetchall()
            ]
            columns = [
                row[1]
                for row in conn.execute("PRAGMA table_info(Taxonomy)").fetchall()
            ]
            taxonomy_rows = conn.execute(
                """
                SELECT release_id, statement_family, value_type, level, concept_qname, parent_concept_qname, primary_label_en
                FROM Taxonomy
                ORDER BY level, concept_qname
                """,
            ).fetchall()
            hierarchy_violations = conn.execute(
                """
                SELECT COUNT(*)
                FROM Taxonomy child
                LEFT JOIN Taxonomy parent
                  ON parent.release_id = child.release_id
                 AND parent.concept_qname = child.parent_concept_qname
                WHERE child.parent_concept_qname IS NOT NULL
                  AND (
                    parent.concept_qname IS NULL
                    OR child.level != parent.level + 1
                  )
                """,
            ).fetchone()
            release_rows = taxonomy_processing.load_release_rows(conn)
        finally:
            conn.close()

        self.assertEqual(stats["releases_processed"], 1)
        self.assertEqual(stats["archives_processed"], 1)
        self.assertEqual(stats["taxonomy_rows"], 2)
        self.assertEqual(tables, ["Taxonomy"])
        self.assertEqual(
            columns,
            [
                "release_id",
                "statement_family",
                "value_type",
                "level",
                "concept_qname",
                "parent_concept_qname",
                "primary_label_en",
            ],
        )
        self.assertEqual(
            taxonomy_rows,
            [
                (
                    "2024-11-01",
                    "BalanceSheet",
                    "string",
                    0,
                    "jppfs_cor:AssetsAbstract",
                    None,
                    "Assets",
                ),
                (
                    "2024-11-01",
                    "BalanceSheet",
                    "number",
                    1,
                    "jppfs_cor:CashAndDeposits",
                    "jppfs_cor:AssetsAbstract",
                    "Cash and Deposits",
                ),
            ],
        )
        self.assertEqual(
            release_rows,
            [
                {
                    "release_id": "2024-11-01",
                    "release_key": "2024-11-01",
                    "release_label": "2024-11-01",
                    "release_year": 2024,
                    "taxonomy_date": "2024-11-01",
                    "valid_from": "2024-11-01",
                    "valid_to": None,
                }
            ],
        )
        self.assertEqual(hierarchy_violations, (0,))