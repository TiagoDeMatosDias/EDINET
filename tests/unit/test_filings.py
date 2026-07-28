"""Bounded archive, XBRL parsing, catalog, and provider-token tests."""

from __future__ import annotations

import io
import zipfile

import pytest

from src.filings.acquisition import EdinetDownloadClient
from src.filings.archive import (
    ArchiveMemberNotFoundError,
    UnsafeArchiveError,
    archive_zip,
    extract_zip_member,
)
from src.filings.catalog import FilingCatalog
from src.filings.ingest import ingest_archive
from src.filings.quality import assess_facts
from src.orchestrator.common.sqlite import connect_write

XBRL = b"""<?xml version='1.0'?>
<xbrli:xbrl xmlns:xbrli='http://www.xbrl.org/2003/instance'
 xmlns:jp='https://example.test/jp'>
  <xbrli:context id='C1'><xbrli:entity><xbrli:identifier scheme='x'>E123</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-04-01</xbrli:startDate><xbrli:endDate>2025-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:unit id='JPY'><xbrli:measure>iso4217:JPY</xbrli:measure></xbrli:unit>
  <jp:Revenue contextRef='C1' unitRef='JPY' decimals='0'>1234</jp:Revenue>
</xbrli:xbrl>"""


def _zip_bytes(*entries: tuple[str, bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in entries:
            archive.writestr(name, content)
    return output.getvalue()


def test_type1_archive_is_stored_and_indexed(tmp_path):
    content = _zip_bytes(
        ("PublicDoc/report.xbrl", XBRL),
        ("PublicDoc/report.htm", b"<html><h1>Overview</h1><p>Revenue increased.</p></html>"),
    )
    archive, digest, size = archive_zip(content, "S100TEST", tmp_path / "archive")
    catalog = FilingCatalog(tmp_path / "Filings.db")
    fact_count = ingest_archive(
        archive,
        "S100TEST",
        catalog,
        {"edinet_code": "E12345", "submitted_at": "2025-06-01"},
    )

    assert archive.exists()
    assert digest
    assert size == len(content)
    assert fact_count == 1
    filing = catalog.get_filing("S100TEST")
    assert filing["status"] == "parsed"
    assert "archive_content" not in filing.keys()
    assert len(catalog.list_company("E12345")) == 1
    assert catalog.list_facts("S100TEST")[0]["concept"] == "Revenue"
    assert catalog.list_sections("S100TEST")[0]["title"] == "Overview"
    assert catalog.artifact_content_summary()["content_count"] == 0
    html_artifact = next(
        item for item in catalog.list_artifacts("S100TEST")
        if item["member_path"].endswith("report.htm")
    )
    loaded = catalog.get_artifact_content(html_artifact["artifact_id"])
    assert loaded is not None
    assert loaded["content"] == b"<html><h1>Overview</h1><p>Revenue increased.</p></html>"


def test_coverage_summary_counts_unique_filings_companies_and_archives(tmp_path):
    catalog = FilingCatalog(tmp_path / "Filings.db")

    def filing(doc_id: str, company: str, status: str, archive_hash: str) -> dict:
        return {
            "doc_id": doc_id,
            "edinet_code": company,
            "submitter_name": company,
            "period_start": "2024-04-01",
            "period_end": "2025-03-31",
            "submitted_at": "2025-06-01T00:00:00Z",
            "form_code": "030000",
            "doc_type_code": "1",
            "xbrl_flag": "1",
            "csv_flag": "0",
            "archive_path": "",
            "archive_content": b"archive",
            "archive_sha256": archive_hash,
            "archive_size": 7,
            "status": status,
            "parse_error": None,
            "created_at": "2025-06-01T00:00:00Z",
            "updated_at": "2025-06-01T00:00:00Z",
        }

    catalog.upsert_filing(filing("S100ONE", "E00001", "parsed", "archive-a"))
    catalog.upsert_filing(filing("S100TWO", "E00001", "parsed", "archive-a"))
    catalog.upsert_filing(filing("S100THREE", "E00002", "error", "archive-b"))

    conn = connect_write(catalog.path)
    try:
        conn.execute(
            "INSERT INTO quality_issues(issue_id, doc_id, severity, code, message, fact_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("issue-1", "S100THREE", "warning", "test", "Test issue", None, "2025-06-01T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    assert catalog.coverage_summary() == {
        "unique_filings": 3,
        "unique_companies": 2,
        "unique_archives": 2,
        "parsed_filings": 2,
        "error_filings": 1,
        "filings_with_issues": 1,
    }


def test_only_structural_numeric_xbrl_facts_are_indexed(tmp_path):
    xbrl = XBRL.replace(
        b"</xbrli:xbrl>",
        b"<jp:NumericLookingText contextRef='C1'>123</jp:NumericLookingText></xbrli:xbrl>",
    )
    archive, _, _ = archive_zip(
        _zip_bytes(("PublicDoc/report.xbrl", xbrl)),
        "S100NUMERIC",
        tmp_path / "archive",
    )
    catalog = FilingCatalog(tmp_path / "Filings.db")

    assert ingest_archive(archive, "S100NUMERIC", catalog) == 1
    facts = catalog.list_facts("S100NUMERIC")
    assert [fact["concept"] for fact in facts] == ["Revenue"]

    from src.filings.xbrl import XbrlParser

    parsed = XbrlParser().parse(xbrl, "S100NUMERIC", "A1")
    text_fact = next(fact for fact in parsed.facts if fact.concept == "NumericLookingText")
    assert text_fact.numeric_value is None

    conn = connect_write(catalog.path)
    try:
        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'xbrl_facts'"
            )
        }
    finally:
        conn.close()
    assert "sqlite_autoindex_xbrl_facts_2" not in indexes


def test_clear_artifact_content_preserves_archive_fallback(tmp_path):
    content = _zip_bytes(("PublicDoc/report.txt", b"report"))
    archive, _, _ = archive_zip(content, "S100COMPACT", tmp_path / "archive")
    catalog = FilingCatalog(tmp_path / "Filings.db")
    ingest_archive(archive, "S100COMPACT", catalog)
    artifact = catalog.list_artifacts("S100COMPACT")[0]

    conn = connect_write(catalog.path)
    try:
        conn.execute(
            "UPDATE artifacts SET content = ? WHERE artifact_id = ?",
            (b"legacy extracted bytes", artifact["artifact_id"]),
        )
        conn.commit()
    finally:
        conn.close()

    summary = catalog.artifact_content_summary()
    assert summary["content_count"] == 1
    assert summary["safely_clearable_count"] == 1
    assert catalog.clear_artifact_content() == 1
    assert catalog.artifact_content_summary()["content_count"] == 0
    loaded = catalog.get_artifact_content(artifact["artifact_id"])
    assert loaded is not None
    assert loaded["content"] == b"report"


def test_extract_zip_member_rejects_missing_member():
    content = _zip_bytes(("PublicDoc/report.txt", b"report"))

    with pytest.raises(ArchiveMemberNotFoundError):
        extract_zip_member(content, "PublicDoc/missing.txt")


def test_archive_rejects_path_traversal(tmp_path):
    content = _zip_bytes(("../outside.txt", b"escape"))
    with pytest.raises(UnsafeArchiveError):
        archive_zip(content, "S100BAD", tmp_path / "archive")
    dotted = _zip_bytes(("./outside.txt", b"escape"))
    with pytest.raises(UnsafeArchiveError):
        archive_zip(dotted, "S100BAD2", tmp_path / "archive")


def test_archive_rejects_duplicate_members(tmp_path):
    content = _zip_bytes(("PublicDoc/report.xbrl", XBRL), ("PublicDoc/report.xbrl", XBRL))
    with pytest.raises(UnsafeArchiveError, match="duplicate"):
        archive_zip(content, "S100DUP", tmp_path / "archive")


def test_provider_token_is_only_constructed_for_acquisition(monkeypatch):
    monkeypatch.setenv("EDINET_API_TOKEN", "provider-secret")
    client = EdinetDownloadClient.from_environment()
    assert client.provider_token == "provider-secret"


def test_quality_checks_are_explainable():
    issues = assess_facts(
        "S1",
        [{"fact_id": "f1", "context_id": "C1", "value_text": "Narrative", "numeric_value": None, "is_nil": 0}],
    )
    assert issues[0]["code"] == "non_numeric_value"
    assert issues[0]["doc_id"] == "S1"


def test_inline_xbrl_fact_is_normalized():
    inline = b"""<html xmlns:ix='http://www.xbrl.org/2013/inlineXBRL' xmlns:xbrli='http://www.xbrl.org/2003/instance' xmlns:jp='https://example.test/jp'>
    <xbrli:context id='C1'><xbrli:entity><xbrli:identifier>E1</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2025-03-31</xbrli:instant></xbrli:period></xbrli:context>
    <ix:nonFraction name='jp:Revenue' contextRef='C1' unitRef='JPY' scale='3'>12</ix:nonFraction></html>"""
    from src.filings.xbrl import XbrlParser

    parsed = XbrlParser().parse(inline, "S1", "A1")
    assert parsed.facts[0].concept == "Revenue"
    assert parsed.facts[0].numeric_value == 12_000
