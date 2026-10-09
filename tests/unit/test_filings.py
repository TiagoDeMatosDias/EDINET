"""Bounded archive, XBRL parsing, catalog, and provider-token tests."""

from __future__ import annotations

import io
import sqlite3
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


def test_coverage_summary_counts_filings_companies_forms_and_dates(tmp_path):
    catalog = FilingCatalog(tmp_path / "Filings.db")

    def filing(doc_id: str, company: str, status: str, archive_hash: str, submitted: str = "2025-06-01T00:00:00Z", form: str = "030000") -> dict:
        return {
            "doc_id": doc_id,
            "edinet_code": company,
            "submitter_name": company,
            "period_start": "2024-04-01",
            "period_end": "2025-03-31",
            "submitted_at": submitted,
            "form_code": form,
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

    catalog.upsert_filing(filing("S100ONE", "E00001", "parsed", "archive-a", "2024-06-01T00:00:00Z"))
    catalog.upsert_filing(filing("S100TWO", "E00001", "parsed", "archive-a", "2025-06-01T00:00:00Z", "043A00"))
    catalog.upsert_filing(filing("S100THREE", "E00002", "error", "archive-b", "2026-06-01T00:00:00Z"))

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
        "filings_with_issues": 1,
        "first_submitted": "2024-06-01T00:00:00Z",
        "last_submitted": "2026-06-01T00:00:00Z",
    }
    assert [dict(row) for row in catalog.form_coverage()] == [
        {"form_code": "030000", "filings": 2, "companies": 2},
        {"form_code": "043A00", "filings": 1, "companies": 1},
    ]
    assert [row["doc_id"] for row in catalog.list_recent()] == ["S100THREE", "S100TWO", "S100ONE"]
    assert [row["doc_id"] for row in catalog.list_recent(form_codes=["030000"])] == ["S100THREE", "S100ONE"]
    assert [row["doc_id"] for row in catalog.list_recent("E00001", form_codes=["030000"])] == ["S100ONE"]


def test_coverage_summary_reads_no_column_stored_after_the_archive_blob(tmp_path, monkeypatch):
    # Columns after archive_content force SQLite to page through every archive;
    # on a full catalog that turned the landing page into a minutes-long scan.
    import src.filings.catalog as catalog_module

    catalog = FilingCatalog(tmp_path / "Filings.db")
    conn = connect_write(catalog.path)
    try:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(filings)")]
    finally:
        conn.close()
    after_blob = set(columns[columns.index("archive_content") + 1:])
    read: set[str] = set()

    def recording_connect(*args, **kwargs):
        connection = connect_write(*args, **kwargs)

        def authorizer(action, table, column, *_):
            if action == sqlite3.SQLITE_READ and table == "filings":
                read.add(column)
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorizer)
        return connection

    monkeypatch.setattr(catalog_module, "connect_write", recording_connect)
    catalog.coverage_summary()
    catalog.form_coverage()

    assert read and not read & after_blob


def test_listed_facts_carry_context_periods(tmp_path):
    xbrl = b"""<?xml version='1.0'?>
<xbrli:xbrl xmlns:xbrli='http://www.xbrl.org/2003/instance'
 xmlns:jp='https://example.test/jp'>
  <xbrli:context id='CurrentYearDuration'><xbrli:entity><xbrli:identifier scheme='x'>E123</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-04-01</xbrli:startDate><xbrli:endDate>2025-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id='CurrentYearInstant_NonConsolidatedMember'><xbrli:entity><xbrli:identifier scheme='x'>E123</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2025-03-31</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:unit id='JPY'><xbrli:measure>iso4217:JPY</xbrli:measure></xbrli:unit>
  <jp:Assets contextRef='CurrentYearInstant_NonConsolidatedMember' unitRef='JPY' decimals='0'>900</jp:Assets>
  <jp:Revenue contextRef='CurrentYearDuration' unitRef='JPY' decimals='0'>1234</jp:Revenue>
</xbrli:xbrl>"""
    archive, _, _ = archive_zip(
        _zip_bytes(("PublicDoc/report.xbrl", xbrl)),
        "S100PERIODS",
        tmp_path / "archive",
    )
    catalog = FilingCatalog(tmp_path / "Filings.db")
    ingest_archive(archive, "S100PERIODS", catalog)

    facts = {row["concept"]: dict(row) for row in catalog.list_facts("S100PERIODS")}

    assert facts["Revenue"]["period_start"] == "2024-04-01"
    assert facts["Revenue"]["period_end"] == "2025-03-31"
    assert facts["Revenue"]["instant"] is None
    assert facts["Assets"]["instant"] == "2025-03-31"
    assert [dict(row)["concept"] for row in catalog.list_facts("S100PERIODS", "Assets")] == ["Assets"]
    assert {row["concept"] for row in catalog.statement_facts("S100PERIODS")} == {"Assets", "Revenue"}


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
    with pytest.warns(UserWarning, match="Duplicate name"):
        content = _zip_bytes(
            ("PublicDoc/report.xbrl", XBRL),
            ("PublicDoc/report.xbrl", XBRL),
        )
    with pytest.raises(UnsafeArchiveError, match="duplicate"):
        archive_zip(content, "S100DUP", tmp_path / "archive")


def test_acquisition_client_reads_the_api_key_setting():
    from src.settings import set_setting, unset_setting

    set_setting("edinet.api_key", "provider-secret")
    try:
        client = EdinetDownloadClient.from_settings()
    finally:
        unset_setting("edinet.api_key")
    assert client.provider_token == "provider-secret"


def test_type1_download_reuses_thread_session(monkeypatch):
    import src.filings.acquisition as acquisition

    sessions = []

    class FakeResponse:
        status_code = 200
        headers = {"Content-Length": "2"}

        def __init__(self):
            self.closed = False

        def iter_content(self, *, chunk_size):
            assert chunk_size == 1024 * 1024
            yield b"PK"

        def close(self):
            self.closed = True

    class FakeSession:
        def __init__(self):
            self.calls = []
            self.closed = False
            sessions.append(self)

        def get(self, url, *, params, timeout, stream):
            self.calls.append((url, params, timeout, stream))
            return FakeResponse()

        def close(self):
            self.closed = True

    monkeypatch.setattr(acquisition.requests, "Session", FakeSession)
    client = EdinetDownloadClient("provider-secret", base_url="https://example.test")

    assert client.download_type1("S100ONE") == b"PK"
    assert client.download_type1("S100TWO") == b"PK"
    client.close()

    assert len(sessions) == 1
    assert len(sessions[0].calls) == 2
    assert all(call[3] is True for call in sessions[0].calls)
    assert sessions[0].closed is True


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


def _filing_row(doc_id: str, company: str, content: bytes | None) -> dict:
    return {
        "doc_id": doc_id,
        "edinet_code": company,
        "submitter_name": "テスト会社",
        "period_start": "2024-04-01",
        "period_end": "2025-03-31",
        "submitted_at": f"2025-06-01T00:00:{doc_id[-1]}Z",
        "form_code": "030000",
        "doc_type_code": "1",
        "xbrl_flag": "1",
        "csv_flag": "0",
        "archive_path": "",
        "archive_content": content,
        "archive_sha256": "digest",
        "archive_size": len(content) if content else 0,
        "status": "parsed",
        "parse_error": None,
        "created_at": "2025-06-01T00:00:00Z",
        "updated_at": "2025-06-01T00:00:00Z",
    }


def test_build_filings_bundle_combines_retained_archives(tmp_path):
    from src.filings import api as filings_api

    catalog = FilingCatalog(tmp_path / "Filings.db")
    catalog.upsert_filing(_filing_row("S100ONE", "E12345", _zip_bytes(("PublicDoc/a.txt", b"one"))))
    catalog.upsert_filing(_filing_row("S100TWO", "E12345", _zip_bytes(("PublicDoc/b.txt", b"two"))))
    catalog.upsert_filing(_filing_row("S100SKIP", "E12345", None))
    catalog.upsert_filing(_filing_row("S100OTHER", "E99999", _zip_bytes(("PublicDoc/c.txt", b"other"))))

    destination = tmp_path / "bundle.zip"
    summary = filings_api.build_filings_bundle(catalog, "E12345", destination)
    assert summary == {"filings": 3, "bundled": 2, "skipped": 1}

    with zipfile.ZipFile(destination) as bundle:
        names = bundle.namelist()
        assert "S100ONE.zip" in names
        assert "S100TWO.zip" in names
        assert "S100SKIP.zip" not in names
        assert "S100OTHER.zip" not in names
        assert "manifest.csv" in names
        for name in ("S100ONE.zip", "S100TWO.zip"):
            inner = zipfile.ZipFile(io.BytesIO(bundle.read(name)))
            assert inner.namelist()[0] == "PublicDoc/a.txt" or inner.namelist()[0] == "PublicDoc/b.txt"
        manifest = bundle.read("manifest.csv").decode("utf-8-sig")
    manifest_lines = manifest.splitlines()
    assert manifest_lines[0].startswith("doc_id,period_end,submitted_at")
    by_doc = {line.split(",")[0]: line for line in manifest_lines[1:]}
    assert by_doc["S100ONE"].endswith(",1")
    assert by_doc["S100SKIP"].endswith(",0")


def test_build_filings_bundle_rejects_unknown_company(tmp_path):
    from fastapi import HTTPException

    from src.filings import api as filings_api

    catalog = FilingCatalog(tmp_path / "Filings.db")
    with pytest.raises(HTTPException) as exc_info:
        filings_api.build_filings_bundle(catalog, "E99999", tmp_path / "bundle.zip")
    assert exc_info.value.status_code == 404


def test_export_company_filings_requires_authentication():
    from types import SimpleNamespace

    from fastapi import HTTPException

    from src.filings import api as filings_api

    request = SimpleNamespace(state=SimpleNamespace(user=None))
    with pytest.raises(HTTPException) as exc_info:
        filings_api.export_company_filings(request, "E12345")
    assert exc_info.value.status_code == 401


def test_report_files_are_named_by_their_own_headings():
    from src.filings.report_files import describe_report_files

    def page(*headings: str) -> bytes:
        return ("<html><body>" + "".join(f"<p>{heading}</p>" for heading in headings) + "</body></html>").encode()

    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("XBRL/PublicDoc/0105310_honbun_x.htm", page("２【財務諸表等】", "（１）【財務諸表】", "①【貸借対照表】"))
        archive.writestr("XBRL/PublicDoc/0101010_honbun_x.htm", page("第一部【企業情報】", "第1【企業の概況】", "1【主要な経営指標等の推移】"))
        archive.writestr("XBRL/PublicDoc/0000000_header_x.htm", page("【表紙】", "【提出書類】"))
        archive.writestr("XBRL/PublicDoc/0102012_honbun_x.htm", page("５【重要な契約等】", "６【研究開発活動】"))
        archive.writestr("XBRL/PublicDoc/0109010_honbun_x.htm", page("【ファンドの状況】"))
        archive.writestr("XBRL/AuditDoc/jpaud-aar-cn-001_x.htm", page("独立監査人の監査報告書"))
    members = [
        {"artifact_id": name, "member_path": f"XBRL/{folder}/{name}.htm", "size_bytes": 1}
        for folder, name in (
            ("PublicDoc", "0105310_honbun_x"),
            ("PublicDoc", "0101010_honbun_x"),
            ("PublicDoc", "0000000_header_x"),
            ("PublicDoc", "0102012_honbun_x"),
            ("PublicDoc", "0109010_honbun_x"),
            ("AuditDoc", "jpaud-aar-cn-001_x"),
        )
    ]

    files = describe_report_files(stream.getvalue(), members)

    assert [(item["label"], item["group"]) for item in files] == [
        ("Cover page", "cover"),
        ("Company overview", "business"),
        ("Material contracts · Research and development", "business"),
        ("Balance sheet (parent company)", "financials"),
        ("ファンドの状況", "business"),
        ("Auditor's report (non-consolidated)", "audit"),
    ]
    assert files[1]["heading"] == "企業の概況"
    assert files[3]["heading"] == "貸借対照表"
    assert describe_report_files(None, members[:1])[0]["label"] == "Section 0105310"
