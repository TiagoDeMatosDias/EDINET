"""Tests for generating standardized statements from the filing catalog."""

from __future__ import annotations

import io
import sqlite3
import zipfile

import src.orchestrator.generate_financial_statements.generate_financial_statements as handler_module
from src.filings.catalog import FilingCatalog
from src.filings.ingest import ingest_content
from src.orchestrator.generate_financial_statements.service import generate_financial_statements

_XBRL = b"""<?xml version='1.0'?>
<xbrli:xbrl xmlns:xbrli='http://www.xbrl.org/2003/instance'
 xmlns:jp='https://example.test/jppfs'>
  <xbrli:context id='CurrentYearDuration'>
    <xbrli:entity><xbrli:identifier scheme='x'>E12345</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-04-01</xbrli:startDate><xbrli:endDate>2025-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id='CurrentYearInstant'>
    <xbrli:entity><xbrli:identifier scheme='x'>E12345</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2025-03-31</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:unit id='JPY'><xbrli:measure>iso4217:JPY</xbrli:measure></xbrli:unit>
  <jp:NetSales contextRef='CurrentYearDuration' unitRef='JPY'>1000</jp:NetSales>
  <jp:CashAndDeposits contextRef='CurrentYearInstant' unitRef='JPY'>250</jp:CashAndDeposits>
</xbrli:xbrl>"""

_NON_CONSOLIDATED_XBRL = b"""<?xml version='1.0'?>
<xbrli:xbrl xmlns:xbrli='http://www.xbrl.org/2003/instance'
 xmlns:jp='https://example.test/jppfs'>
  <xbrli:context id='CurrentYearDuration_NonConsolidatedMember'>
    <xbrli:entity><xbrli:identifier scheme='x'>E12345</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-04-01</xbrli:startDate><xbrli:endDate>2025-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id='CurrentYearInstant_NonConsolidatedMember'>
    <xbrli:entity><xbrli:identifier scheme='x'>E12345</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2025-03-31</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:unit id='JPY'><xbrli:measure>iso4217:JPY</xbrli:measure></xbrli:unit>
  <jp:NetSales contextRef='CurrentYearDuration_NonConsolidatedMember' unitRef='JPY'>900</jp:NetSales>
  <jp:CashAndDeposits contextRef='CurrentYearInstant_NonConsolidatedMember' unitRef='JPY'>250</jp:CashAndDeposits>
</xbrli:xbrl>"""


def _zip_bytes(content: bytes) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("PublicDoc/report.xbrl", content)
    return output.getvalue()


def _create_taxonomy(path):
    with sqlite3.connect(path) as conn:
        conn.execute(
            """CREATE TABLE Taxonomy (
                release_id TEXT NOT NULL,
                statement_family TEXT NOT NULL,
                value_type TEXT NOT NULL,
                level INTEGER NOT NULL,
                concept_qname TEXT NOT NULL,
                parent_concept_qname TEXT,
                primary_label_en TEXT NOT NULL,
                column_concept_qname TEXT,
                display_order REAL,
                PRIMARY KEY (release_id, concept_qname)
            )"""
        )
        conn.executemany(
            """INSERT INTO Taxonomy(
                release_id, statement_family, value_type, level,
                concept_qname, parent_concept_qname, primary_label_en,
                column_concept_qname
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    "2024-01-31",
                    "IncomeStatement",
                    "number",
                    1,
                    "jppfs_cor:NetSales",
                    "jppfs_cor:IncomeStatement",
                    "Net Sales",
                    "jppfs_cor:NetSales",
                ),
                (
                    "2024-01-31",
                    "BalanceSheet",
                    "number",
                    1,
                    "jppfs_cor:CashAndDeposits",
                    "jppfs_cor:BalanceSheet",
                    "Cash and Deposits",
                    "jppfs_cor:CashAndDeposits",
                ),
            ],
        )
        conn.commit()


def test_filings_source_generates_standardized_statements(tmp_path):
    filings_path = tmp_path / "Filings.db"
    target_path = tmp_path / "Standardized.db"
    catalog = FilingCatalog(filings_path)
    ingest_content(
        _zip_bytes(_XBRL),
        "S100FILINGS",
        catalog,
        {
            "edinet_code": "E12345",
            "submitted_at": "2025-06-01T09:00:00",
            "period_start": "2024-04-01",
            "period_end": "2025-03-31",
            "doc_type_code": "120",
        },
    )
    _create_taxonomy(target_path)

    result = generate_financial_statements(
        source_database=str(filings_path),
        target_database=str(target_path),
        granularity_level=1,
        source_mode="filings",
    )

    with sqlite3.connect(target_path) as conn:
        financial_row = conn.execute(
            """SELECT docID, Company_Code, docTypeCode, Currency, Data_Source,
                      periodStart, periodEnd, release_id
                 FROM FinancialStatements"""
        ).fetchone()
        income_row = conn.execute(
            'SELECT docID, [Net Sales] FROM IncomeStatement'
        ).fetchone()
        balance_row = conn.execute(
            'SELECT docID, [Cash and Deposits] FROM BalanceSheet'
        ).fetchone()

    assert result["documents_processed"] == 1
    assert financial_row == (
        "S100FILINGS",
        "E12345",
        "120",
        "JPY",
        "Edinet",
        "2024-04-01",
        "2025-03-31",
        "2024-01-31",
    )
    assert income_row == ("S100FILINGS", 1000.0)
    assert balance_row == ("S100FILINGS", 250.0)


def test_filings_source_falls_back_to_non_consolidated_contexts(tmp_path):
    filings_path = tmp_path / "Filings.db"
    target_path = tmp_path / "Standardized.db"
    catalog = FilingCatalog(filings_path)
    ingest_content(
        _zip_bytes(_NON_CONSOLIDATED_XBRL),
        "S100NONCON",
        catalog,
        {
            "edinet_code": "E12345",
            "submitted_at": "2025-06-01T09:00:00",
            "period_start": "2024-04-01",
            "period_end": "2025-03-31",
            "doc_type_code": "120",
        },
    )
    _create_taxonomy(target_path)

    result = generate_financial_statements(
        source_database=str(filings_path),
        target_database=str(target_path),
        granularity_level=1,
        source_mode="filings",
    )

    with sqlite3.connect(target_path) as conn:
        income_row = conn.execute(
            'SELECT docID, [Net Sales] FROM IncomeStatement'
        ).fetchone()
        balance_row = conn.execute(
            'SELECT docID, [Cash and Deposits] FROM BalanceSheet'
        ).fetchone()

    assert result["documents_processed"] == 1
    assert income_row == ("S100NONCON", 900.0)
    assert balance_row == ("S100NONCON", 250.0)


def test_handler_selects_filings_database_for_filings_mode(monkeypatch, tmp_path):
    calls = {}

    def fake_generate(**kwargs):
        calls.update(kwargs)
        return {"status": "completed"}

    monkeypatch.setattr(handler_module, "get_db1", lambda: str(tmp_path / "Base.db"))
    monkeypatch.setattr(handler_module, "get_db2", lambda: str(tmp_path / "Standardized.db"))
    monkeypatch.setattr(handler_module, "get_filings_db", lambda: str(tmp_path / "configured.db"))
    monkeypatch.setenv("EDINET_FILINGS_DB", str(tmp_path / "Filings.db"))
    monkeypatch.setattr(
        handler_module.financial_statement_services,
        "generate_financial_statements",
        fake_generate,
    )

    result = handler_module.run_generate_financial_statements(
        {
            "generate_financial_statements_config": {
                "Source_Mode": "filings",
                "Granularity_level": 2,
            }
        }
    )

    assert result == {"status": "completed"}
    assert calls["source_database"] == str(tmp_path / "Filings.db")
    assert calls["target_database"] == str(tmp_path / "Standardized.db")
    assert calls["source_mode"] == "filings"
    assert calls["granularity_level"] == 2


def _share_metrics_frame(facts):
    import pandas as pd

    from src.orchestrator.generate_financial_statements.service import _build_statement_batch_frames

    columns = {
        "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults": "Basic earnings (loss) per share",
        "jpcrp_cor:NetAssetsPerShareSummaryOfBusinessResults": "Net assets per share",
        "jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults": "Price-earnings ratio",
        "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults": "Dividend paid per share",
    }
    mapping = pd.DataFrame(
        [("ShareMetrics", concept, column, "r1") for concept, column in columns.items()],
        columns=["statement_family", "concept_qname", "column_name", "release_id"],
    )
    metadata = pd.DataFrame([("D1", "r1")], columns=["docID", "release_id"])
    facts = pd.DataFrame(facts, columns=["docID", "context_id", "concept_qname", "value"])
    return _build_statement_batch_frames(metadata, facts, mapping)["ShareMetrics"].iloc[0]


def test_an_ifrs_filers_per_share_figures_are_its_consolidated_ones():
    # Toyota's report for March 2026: the Japanese GAAP summary concepts carry
    # the parent company's figures, the IFRS ones the group's.
    row = _share_metrics_frame([
        ("D1", "CurrentYearDuration", "jpcrp_cor:BasicEarningsLossPerShareIFRSSummaryOfBusinessResults", 295.25),
        ("D1", "CurrentYearDuration_NonConsolidatedMember", "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults", 260.28),
        ("D1", "CurrentYearInstant", "jpcrp_cor:EquityToAssetRatioIFRSSummaryOfBusinessResults", 3062.82),
        ("D1", "CurrentYearInstant_NonConsolidatedMember", "jpcrp_cor:NetAssetsPerShareSummaryOfBusinessResults", 1815.72),
        ("D1", "CurrentYearDuration", "jpcrp_cor:PriceEarningsRatioIFRSSummaryOfBusinessResults", 10.7),
        ("D1", "CurrentYearDuration_NonConsolidatedMember", "jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults", 12.2),
        ("D1", "CurrentYearDuration_NonConsolidatedMember", "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults", 95.0),
    ])
    assert (row["Basic earnings (loss) per share"], row["Net assets per share"], row["Price-earnings ratio"]) == (295.25, 3062.82, 10.7)
    # Dividends are the parent company's to pay.
    assert row["Dividend paid per share"] == 95.0


def test_a_consolidated_filers_missing_figure_is_not_filled_from_the_parent():
    import math

    # A year of ¥1m consolidated profit: the consolidated summary gives EPS of
    # ¥0.02 and no P/E; the parent's P/E of 47.6 describes another company.
    row = _share_metrics_frame([
        ("D1", "CurrentYearDuration", "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults", 0.02),
        ("D1", "CurrentYearDuration_NonConsolidatedMember", "jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults", 47.6),
    ])
    assert row["Basic earnings (loss) per share"] == 0.02
    assert "Price-earnings ratio" not in row or math.isnan(row["Price-earnings ratio"])
    # A filer without subsidiaries reports everything for itself.
    row = _share_metrics_frame([
        ("D1", "CurrentYearDuration_NonConsolidatedMember", "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults", 4.01),
        ("D1", "CurrentYearDuration_NonConsolidatedMember", "jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults", 47.6),
    ])
    assert (row["Basic earnings (loss) per share"], row["Price-earnings ratio"]) == (4.01, 47.6)



def _slip_rows(reports):
    import pandas as pd

    from src.orchestrator.generate_financial_statements import slips

    columns = ["company", "doc", slips.EPS, slips.PER, slips.BPS, slips.SHARE_COUNTS[1], "profit", "net_assets", "stored"]
    rows = pd.DataFrame(reports, columns=columns)
    rows["shares"] = rows[slips.SHARE_COUNTS[1]]
    rows["period_end"] = rows.doc.map(lambda doc: f"20{doc[1:3]}-12-31")
    return rows


def test_a_share_count_filed_in_thousands_is_corrected():
    from src.orchestrator.generate_financial_statements.slips import (
        SHARE_COUNTS,
        find_decimal_slips,
    )

    # 7,094 issued shares beside EPS of ¥21 on ¥149m profit: 7,094 thousand.
    rows = _slip_rows([
        ("A", "D18", 21.06, None, 50.45, 7094, 149_456_000, 363_701_000, None),
        ("A", "D19", 40.30, 62.23, 136.9, 7_627_000, 297_894_000, 1_049_199_000, 2508.0),
        ("A", "D20", 45.00, 50.0, 150.0, 7_700_000, 346_500_000, 1_155_000_000, 2250.0),
        ("A", "D21", 50.00, 40.0, 170.0, 7_700_000, 385_000_000, 1_309_000_000, 2000.0),
    ])
    assert find_decimal_slips(rows, {}) == [
        ("D18", SHARE_COUNTS[1], 7094.0, 7_094_000.0, "share count 0.001 times the count the report's EPS and book value imply; scaled"),
    ]


def test_a_pe_tagged_a_hundred_times_over_is_corrected_but_an_unadjusted_price_is_not():
    from src.orchestrator.generate_financial_statements.slips import PER, find_decimal_slips

    # P/E tagged 5,140.7 for ¥1,151 over EPS of ¥22.39 (51.4).
    rows = _slip_rows([
        ("B", "D21", 5.68, 328.7, 950.0, 38_315_000, 217_629_200, 36_399_250_000, 1869.0),
        ("B", "D22", 63.29, 27.9, 1000.0, 38_315_000, 2_424_956_350, 38_315_000_000, 1767.0),
        ("B", "D23", 22.39, 5140.7, 980.29, 38_315_000, 857_872_850, 37_560_000_000, 1151.0),
    ])
    assert [(doc, column, round(corrected, 2)) for doc, column, _filed, corrected, _reason in find_decimal_slips(rows, {})] == [("D23", PER, 51.41)]
    # A report's P/E × EPS (¥2,003) matches the stored ¥2,000 before a later
    # 10-for-1 split's factor: the price series was left unadjusted, the P/E is right.
    rows = _slip_rows([("C", "D22", 910.57, 2.2, 4385.27, 172_500, 157_073_000, 756_483_000, 2000.0)])
    assert find_decimal_slips(rows, {"D22": (0.1, 0.1)}) == []


def test_the_as_filed_history_shows_a_corrected_slip_as_filed():
    import sqlite3

    from src.security_analysis.history import _show_filed_slips

    conn = sqlite3.connect(":memory:")
    conn.execute('CREATE TABLE ShareMetrics_Corrections ("docID" TEXT, "column_name" TEXT, "filed" REAL, "corrected" REAL, "reason" TEXT)')
    conn.execute("INSERT INTO ShareMetrics_Corrections VALUES ('D23', 'Price-earnings ratio', 5140.7, 51.41, 'P/E')")
    rows = [{"field": "Price-earnings ratio", "values": [27.9, 51.41]}]
    _show_filed_slips(conn, rows, "ShareMetrics", [{"docID": "D22"}, {"docID": "D23"}])
    assert rows[0]["values"] == [27.9, 51.41]
    assert rows[0]["reported_values"] == [27.9, 5140.7]



def test_only_the_slipped_count_of_a_report_is_corrected():
    import pandas as pd

    from src.orchestrator.generate_financial_statements.slips import (
        SHARE_COUNTS,
        find_decimal_slips,
    )

    # The year-end count (15.17 million) is what profit over EPS and net
    # assets over book value per share imply; the filing-date count of
    # 1,513,900 dropped a digit, and the report gives the count right
    # beside it.
    rows = pd.DataFrame([
        ("G", "D22", "2022-08-31", -6.09, None, 210.15, 15_171_800, 15_171_800, 15_171_800, -88_400_000, 3_188_000_000, None),
        ("G", "D23", "2023-08-31", -1.88, None, 210.99, 15_173_900, 15_173_900, 1_513_900, -27_800_000, 3_201_000_000, None),
        ("G", "D24", "2024-08-31", -21.05, None, 193.37, 15_202_100, 15_202_100, 15_202_100, -320_000_000, 2_939_000_000, None),
    ], columns=["company", "doc", "period_end", "Basic earnings (loss) per share", "Price-earnings ratio", "Net assets per share", *SHARE_COUNTS, "profit", "net_assets", "stored"])
    rows["shares"] = rows[SHARE_COUNTS[0]]
    assert [(doc, column, corrected) for doc, column, _filed, corrected, _reason in find_decimal_slips(rows, {})] == [("D23", SHARE_COUNTS[2], 15_173_900.0)]
    # A 10-for-1 split restated in the report before it took effect: the
    # year-end count is the old one, the reports after carry the new count.
    split = pd.DataFrame([
        ("S", "D22", "2022-03-31", 100.0, None, 1000.0, 1_000_000, 1_000_000, 1_000_000, 100_000_000, 1_000_000_000, None),
        ("S", "D23", "2023-03-31", 10.0, None, 100.0, 1_000_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000, None),
        ("S", "D24", "2024-03-31", 11.0, None, 110.0, 10_000_000, 10_000_000, 10_000_000, 110_000_000, 1_100_000_000, None),
    ], columns=rows.columns[:-1])
    split["shares"] = split[SHARE_COUNTS[0]]
    assert find_decimal_slips(split, {}) == []
