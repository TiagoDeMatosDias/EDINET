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
