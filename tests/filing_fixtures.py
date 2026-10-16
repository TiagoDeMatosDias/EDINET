"""Filing catalogs for tests: the old single-file filings.db and filled split catalogs.

``create_legacy_filings_db`` writes the schema every filings.db had before the
split (archives inside ``filings``, extracted bytes in ``artifacts.content``),
so tests can read it through the legacy adapter and split it.
"""

from __future__ import annotations

import hashlib
import io
import sqlite3
import zipfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

LEGACY_SCHEMA = """
CREATE TABLE filings (
    doc_id TEXT PRIMARY KEY, edinet_code TEXT, submitter_name TEXT, period_start TEXT, period_end TEXT,
    submitted_at TEXT, form_code TEXT, doc_type_code TEXT, xbrl_flag TEXT, csv_flag TEXT, archive_path TEXT,
    archive_content BLOB, archive_sha256 TEXT, archive_size INTEGER, status TEXT NOT NULL, parse_error TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX idx_filings_company_date ON filings(edinet_code, submitted_at DESC);
CREATE INDEX idx_filings_submitted ON filings(submitted_at DESC);
CREATE TABLE artifacts (
    artifact_id TEXT PRIMARY KEY, doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    member_path TEXT NOT NULL, kind TEXT NOT NULL, media_type TEXT NOT NULL, size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL, content BLOB, UNIQUE(doc_id, member_path)
);
CREATE INDEX idx_artifacts_doc ON artifacts(doc_id);
CREATE TABLE xbrl_contexts (
    context_id TEXT NOT NULL, doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    entity_identifier TEXT, period_start TEXT, period_end TEXT, instant TEXT, PRIMARY KEY(doc_id, context_id)
);
CREATE TABLE xbrl_units (
    unit_id TEXT NOT NULL, doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE, measure TEXT,
    PRIMARY KEY(doc_id, unit_id)
);
CREATE TABLE xbrl_facts (
    fact_id TEXT PRIMARY KEY, doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE, artifact_id TEXT,
    concept TEXT NOT NULL, namespace_uri TEXT, context_id TEXT, unit_id TEXT, value_text TEXT, numeric_value REAL,
    decimals TEXT, is_nil INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_facts_doc_concept ON xbrl_facts(doc_id, concept);
CREATE TABLE quality_issues (
    issue_id TEXT PRIMARY KEY, doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    severity TEXT NOT NULL, code TEXT NOT NULL, message TEXT NOT NULL, fact_id TEXT, created_at TEXT NOT NULL
);
CREATE INDEX idx_quality_doc ON quality_issues(doc_id, severity);
CREATE TABLE parse_runs (
    parse_run_id TEXT PRIMARY KEY, doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    parser_version TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'started', started_at TEXT NOT NULL,
    completed_at TEXT, fact_count INTEGER, section_count INTEGER, warning_count INTEGER DEFAULT 0, error_message TEXT
);
CREATE INDEX idx_parse_runs_doc ON parse_runs(doc_id, started_at DESC);
CREATE TABLE data_watermarks (
    source_name TEXT PRIMARY KEY, source_version TEXT, max_available_at TEXT, row_count INTEGER, refreshed_at TEXT NOT NULL
);
CREATE TABLE filing_translations (
    source_hash TEXT PRIMARY KEY, source_text TEXT NOT NULL, translated_text TEXT NOT NULL,
    translator_version INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL
);
CREATE TABLE metric_catalog (
    metric_id TEXT PRIMARY KEY, display_name TEXT NOT NULL, value_kind TEXT NOT NULL, unit_family TEXT,
    source_table TEXT, source_column TEXT, concept_qname TEXT, formula_json TEXT, formula_version TEXT,
    valid_from TEXT, valid_to TEXT
);
CREATE TABLE observations (
    observation_id TEXT PRIMARY KEY, company_code TEXT NOT NULL, metric_id TEXT NOT NULL, value_numeric REAL,
    value_text TEXT, unit TEXT, currency TEXT, period_start TEXT, period_end TEXT, available_at TEXT NOT NULL,
    doc_id TEXT, source_kind TEXT NOT NULL, calculation_version TEXT NOT NULL
);
CREATE INDEX idx_obs_company_metric ON observations(company_code, metric_id, period_end);
CREATE TABLE observation_sources (
    observation_id TEXT NOT NULL, fact_id TEXT, source_fact_hash TEXT, extraction_rule TEXT,
    selection_reason TEXT, confidence REAL, PRIMARY KEY(observation_id, fact_id)
);
CREATE TABLE observation_dependencies (
    observation_id TEXT NOT NULL, input_observation_id TEXT NOT NULL, role TEXT, transform_json TEXT,
    PRIMARY KEY(observation_id, input_observation_id)
);
"""

STAMP = "2026-01-01T00:00:00+00:00"


def zip_bytes(*entries: tuple[str, bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in entries:
            archive.writestr(name, content)
    return output.getvalue()


def report_zip(doc_id: str, text: str = "report") -> bytes:
    """A small archive with one text member, distinct per filing."""
    return zip_bytes((f"PublicDoc/{doc_id}.txt", f"{doc_id} {text}".encode()))


def filing_row(
    doc_id: str,
    edinet_code: str = "E00001",
    submitted_at: str = "2025-06-20T09:00:00",
    *,
    content: bytes | None = b"",
    form_code: str = "030000",
    status: str = "parsed",
    **extra: Any,
) -> dict[str, Any]:
    """Values for ``FilingCatalog.upsert_filing`` (``content=b""`` makes a per-filing archive)."""
    archive = report_zip(doc_id) if content == b"" else content
    row: dict[str, Any] = {
        "doc_id": doc_id,
        "edinet_code": edinet_code,
        "submitter_name": f"Company {edinet_code}",
        "period_start": "2024-04-01",
        "period_end": "2025-03-31",
        "submitted_at": submitted_at,
        "form_code": form_code,
        "doc_type_code": "120",
        "xbrl_flag": "1",
        "csv_flag": "0",
        "archive_content": archive,
        "archive_sha256": hashlib.sha256(archive).hexdigest() if archive else None,
        "archive_size": len(archive) if archive else None,
        "status": status,
        "parse_error": None,
        "created_at": STAMP,
        "updated_at": STAMP,
    }
    row.update(extra)
    return row


def create_legacy_filings_db(path: Path, filings: Iterable[dict[str, Any]] = (), *, facts_per_filing: int = 2) -> Path:
    """Write an old single-file filings.db holding ``filings`` (rows from ``filing_row``).

    Each filing gets one artifact (with legacy extracted ``content``), a context,
    a unit, ``facts_per_filing`` facts, a quality issue and a parse run, and the
    cross-filing tables get a row or two, so a split has something in every table.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(LEGACY_SCHEMA)
        for values in filings:
            doc_id = values["doc_id"]
            row = dict(values)
            row.setdefault("archive_path", "")
            columns = [
                "doc_id", "edinet_code", "submitter_name", "period_start", "period_end", "submitted_at", "form_code",
                "doc_type_code", "xbrl_flag", "csv_flag", "archive_path", "archive_content", "archive_sha256",
                "archive_size", "status", "parse_error", "created_at", "updated_at",
            ]
            conn.execute(
                f"INSERT INTO filings({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
                [row.get(column) for column in columns],
            )
            member = f"PublicDoc/{doc_id}.txt".encode()
            conn.execute(
                "INSERT INTO artifacts VALUES (?, ?, ?, 'text', 'text/plain', ?, ?, ?)",
                (f"{doc_id}-A1", doc_id, member.decode(), len(member), hashlib.sha256(member).hexdigest(), b"legacy bytes"),
            )
            conn.execute("INSERT INTO xbrl_contexts VALUES ('C1', ?, 'E1', '2024-04-01', '2025-03-31', NULL)", (doc_id,))
            conn.execute("INSERT INTO xbrl_units VALUES ('JPY', ?, 'iso4217:JPY')", (doc_id,))
            for number in range(facts_per_filing):
                conn.execute(
                    "INSERT INTO xbrl_facts VALUES (?, ?, ?, ?, NULL, 'C1', 'JPY', ?, ?, '0', 0)",
                    (f"{doc_id}-F{number}", doc_id, f"{doc_id}-A1", f"Concept{number}", str(number * 100), number * 100.0),
                )
            conn.execute(
                "INSERT INTO quality_issues VALUES (?, ?, 'warning', 'test', 'Test issue', NULL, ?)",
                (f"{doc_id}-Q1", doc_id, STAMP),
            )
            conn.execute(
                "INSERT INTO parse_runs VALUES (?, ?, 'test-1', 'completed', ?, ?, ?, 0, 0, NULL)",
                (f"{doc_id}-R1", doc_id, STAMP, STAMP, facts_per_filing),
            )
        conn.execute("INSERT INTO filing_translations VALUES ('hash-1', '売上高', 'Net sales', 3, ?)", (STAMP,))
        conn.execute("INSERT INTO data_watermarks VALUES ('edinet', 'v1', ?, 1, ?)", (STAMP, STAMP))
        conn.execute(
            "CREATE TABLE Bond_Documents (doc_id TEXT PRIMARY KEY, edinet_code TEXT, archive BLOB, updated_at TEXT)"
        )
        conn.execute("CREATE INDEX idx_bond_documents_company ON Bond_Documents(edinet_code)")
        conn.execute("INSERT INTO Bond_Documents VALUES ('S100BOND', 'E00001', ?, ?)", (b"bond archive", STAMP))
        conn.commit()
    finally:
        conn.close()
    return path


def legacy_copy(catalog: Any, path: Path) -> Path:
    """The old single-file filings.db holding the same filings and rows as a split ``catalog``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(LEGACY_SCHEMA)
        for row in catalog.shards():
            conn.execute("ATTACH DATABASE ? AS shard", (row["file"],))
            conn.execute(
                "INSERT INTO filings (doc_id, edinet_code, submitter_name, period_start, period_end, submitted_at, "
                "form_code, doc_type_code, xbrl_flag, csv_flag, archive_path, archive_content, archive_sha256, "
                "archive_size, status, parse_error, created_at, updated_at) "
                "SELECT f.doc_id, f.edinet_code, f.submitter_name, f.period_start, f.period_end, f.submitted_at, "
                "f.form_code, f.doc_type_code, f.xbrl_flag, f.csv_flag, '', a.content, f.archive_sha256, "
                "f.archive_size, f.status, f.parse_error, f.created_at, f.updated_at "
                "FROM shard.filings f LEFT JOIN shard.filing_archives a ON a.doc_id = f.doc_id"
            )
            conn.execute(
                "INSERT INTO artifacts (artifact_id, doc_id, member_path, kind, media_type, size_bytes, sha256) "
                "SELECT artifact_id, doc_id, member_path, kind, media_type, size_bytes, sha256 FROM shard.artifacts"
            )
            for table in ("xbrl_contexts", "xbrl_units", "xbrl_facts", "quality_issues", "parse_runs"):
                conn.execute(f"INSERT INTO {table} SELECT * FROM shard.{table}")
            conn.commit()
            conn.execute("DETACH DATABASE shard")
        conn.commit()
    finally:
        conn.close()
    return path
