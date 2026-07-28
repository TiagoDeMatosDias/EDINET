"""Catalog an EDINET archive and build its compact XBRL index.

The compressed provider ZIP is retained in ``Filings.db``. Extracted member
bytes are used transiently for parsing and are not duplicated in the catalog.
Only numeric/analytical XBRL facts are persisted; narrative members are
extracted from the retained ZIP on demand.
"""

from __future__ import annotations

import io
import os
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .archive import DEFAULT_ARCHIVE_POLICY, ArchivePolicy, validate_zip_in_memory
from .catalog import FilingCatalog
from .quality import assess_facts
from .xbrl import XbrlParser, artifact_kind, sha256


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _artifact_id(doc_id: str, member_path: str) -> str:
    import uuid

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}:artifact:{member_path}"))


def ingest_content(
    zip_bytes: bytes,
    doc_id: str,
    catalog: FilingCatalog,
    metadata: dict[str, Any] | None = None,
    *,
    policy: ArchivePolicy = DEFAULT_ARCHIVE_POLICY,
) -> int:
    """Validate, index, and store one ZIP in the catalog database.

    Args:
        zip_bytes: Raw ZIP file content (validated in memory, never written to disk).
        doc_id: EDINET document ID.
        catalog: FilingCatalog instance for the target database.
        metadata: Optional dict with edinet_code, submitter_name, etc.
        policy: Validation policy (size limits, member counts).

    Returns:
        Number of XBRL facts indexed.
    """
    infos = validate_zip_in_memory(zip_bytes, policy)
    metadata = metadata or {}
    # If no company metadata was provided, try to look it up from Base.db
    if not metadata.get("edinet_code") and not metadata.get("submitter_name"):
        try:
            from src.orchestrator.common.db_config import get_db1
            from src.orchestrator.common.sqlite import connect_read

            db1 = get_db1()
            if os.path.exists(db1):
                conn = connect_read(db1)
                row = conn.execute(
                    "SELECT edinetCode, filerName, submitDateTime, periodStart, periodEnd, formCode "
                    "FROM DocumentList WHERE docID = ?",
                    (doc_id,),
                ).fetchone()
                if row:
                    metadata.setdefault("edinet_code", row["edinetCode"] or "")
                    metadata.setdefault("submitter_name", row["filerName"] or "")
                    metadata.setdefault("submitted_at", row["submitDateTime"] or "")
                    metadata.setdefault("period_start", row["periodStart"] or "")
                    metadata.setdefault("period_end", row["periodEnd"] or "")
                    metadata.setdefault("form_code", row["formCode"] or "")
                conn.close()
        except (OSError, sqlite3.Error, ValueError):
            pass
    now = _now()
    catalog.upsert_filing(
        {
            "doc_id": doc_id,
            "edinet_code": metadata.get("edinet_code"),
            "submitter_name": metadata.get("submitter_name"),
            "period_start": metadata.get("period_start"),
            "period_end": metadata.get("period_end"),
            "submitted_at": metadata.get("submitted_at"),
            "form_code": metadata.get("form_code"),
            "doc_type_code": metadata.get("doc_type_code", "1"),
            "xbrl_flag": metadata.get("xbrl_flag", "1"),
            "csv_flag": metadata.get("csv_flag", "0"),
            "archive_path": "",
            "archive_content": zip_bytes,
            "archive_sha256": sha256(zip_bytes),
            "archive_size": len(zip_bytes),
            "status": "parsing",
            "parse_error": None,
            "created_at": metadata.get("created_at", now),
            "updated_at": now,
        }
    )
    artifacts: list[dict[str, Any]] = []
    contexts: list[dict[str, Any]] = []
    units: list[dict[str, Any]] = []
    facts: list[dict[str, Any]] = []
    parser = XbrlParser()
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zipped:
            for info in infos:
                if info.is_dir():
                    continue
                content = zipped.read(info)
                kind, media_type = artifact_kind(info.filename)
                artifact_id = _artifact_id(doc_id, info.filename)
                artifacts.append(
                    {
                        "artifact_id": artifact_id,
                        "doc_id": doc_id,
                        "member_path": info.filename.replace("\\", "/"),
                        "kind": kind,
                        "media_type": media_type,
                        "size_bytes": len(content),
                        "sha256": sha256(content),
                    }
                )
                if kind == "xbrl" and len(content) <= 25 * 1024 * 1024:
                    parsed = parser.parse(content, doc_id, artifact_id, numeric_only=True)
                    contexts.extend(parsed.contexts)
                    units.extend(parsed.units)
                    facts.extend(
                        {
                            "fact_id": fact.fact_id,
                            "doc_id": doc_id,
                            "artifact_id": artifact_id,
                            "concept": fact.concept,
                            "namespace_uri": fact.namespace_uri,
                            "context_id": fact.context_id,
                            "unit_id": fact.unit_id,
                            "value_text": fact.value_text,
                            "numeric_value": fact.numeric_value,
                            "decimals": fact.decimals,
                            "is_nil": fact.is_nil,
                        }
                        for fact in parsed.facts
                        if fact.numeric_value is not None and not fact.is_nil
                    )
        catalog.replace_parsed_content(doc_id, artifacts, contexts, units, facts, [])
        catalog.replace_quality_issues(doc_id, assess_facts(doc_id, facts))
        catalog.set_status(doc_id, "parsed", None, _now())
    except Exception as exc:
        catalog.set_status(doc_id, "error", str(exc)[:500], _now())
        raise
    return len(facts)


# Legacy function retained for callers that already have a file on disk
def ingest_archive(
    archive_path: str | Path,
    doc_id: str,
    catalog: FilingCatalog,
    metadata: dict[str, Any] | None = None,
    *,
    policy: ArchivePolicy = DEFAULT_ARCHIVE_POLICY,
) -> int:
    """Read a ZIP from disk and ingest it. Prefer ``ingest_content`` for new code."""
    archive = Path(archive_path).expanduser().resolve(strict=True)
    content = archive.read_bytes()
    return ingest_content(content, doc_id, catalog, metadata, policy=policy)
