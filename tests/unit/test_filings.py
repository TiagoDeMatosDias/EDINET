"""Bounded archive, XBRL parsing, catalog, and provider-token tests."""

from __future__ import annotations

import io
import zipfile

import pytest

from src.filings.acquisition import EdinetDownloadClient
from src.filings.archive import UnsafeArchiveError, archive_zip
from src.filings.catalog import FilingCatalog
from src.filings.ingest import ingest_archive
from src.filings.quality import assess_facts

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
    assert len(catalog.list_company("E12345")) == 1
    assert catalog.list_facts("S100TEST")[0]["concept"] == "Revenue"
    assert catalog.list_sections("S100TEST")[0]["title"] == "Overview"


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
