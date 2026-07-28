#!/usr/bin/env python3
"""Build a compact filing catalog from retained ZIP archives.

The source database is never modified.  The rebuilt catalog keeps filing
metadata, compressed archives, artifact metadata, contexts, units, and
numeric XBRL facts.  Materialized narrative sections, the FTS copy of those
sections, nonnumeric facts, nil facts, and their per-fact quality rows are
intentionally omitted because the source ZIP remains available on demand.

Run without ``--apply`` to inspect the expected reduction.  ``--apply``
requires an output path that does not already exist; build the output on a
different drive when the source volume does not have enough free space.
"""

from __future__ import annotations

import argparse
import io
import os
import shutil
import sqlite3
import sys
import zipfile
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.filings.archive import DEFAULT_ARCHIVE_POLICY, validate_zip_in_memory  # noqa: E402
from src.filings.catalog import FilingCatalog  # noqa: E402
from src.filings.quality import assess_facts  # noqa: E402
from src.filings.xbrl import XbrlParser  # noqa: E402
from src.orchestrator.common.db_config import get_filings_db  # noqa: E402
from src.orchestrator.common.sqlite import connect_read, connect_write  # noqa: E402

_ARCHIVE_LIMIT = 25 * 1024 * 1024
_FILINGS_COLUMNS = (
    "doc_id, edinet_code, submitter_name, period_start, period_end, submitted_at, "
    "form_code, doc_type_code, xbrl_flag, csv_flag, archive_path, archive_content, "
    "archive_sha256, archive_size, status, parse_error, created_at, updated_at"
)
_ARTIFACT_COLUMNS = (
    "artifact_id, doc_id, member_path, kind, media_type, size_bytes, sha256"
)
_ARTIFACT_DESTINATION_COLUMNS = f"{_ARTIFACT_COLUMNS}, content"
_FACT_COLUMNS = (
    "fact_id, doc_id, artifact_id, concept, namespace_uri, context_id, unit_id, "
    "value_text, numeric_value, decimals, is_nil"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild a compact Filings.db from retained compressed archives."
    )
    parser.add_argument(
        "--source",
        default=os.getenv("EDINET_FILINGS_DB") or get_filings_db(),
        help="Source Filings.db (default: configured filings database)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="New database path; it must not already exist",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Build the output database; without this flag the command is a dry run.",
    )
    parser.add_argument(
        "--no-vacuum",
        action="store_true",
        help="Skip the final VACUUM; the rebuilt database is still checkpointed.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250,
        help="Rows per copy batch (default: 250)",
    )
    return parser


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone() is not None


def _summary(path: Path) -> dict[str, int]:
    conn = connect_read(path)
    try:
        filing = conn.execute(
            """SELECT COUNT(*) AS filing_count,
                      COALESCE(SUM(archive_size), 0) AS archive_bytes,
                      SUM(CASE WHEN archive_content IS NOT NULL THEN 1 ELSE 0 END)
                          AS archived_count
                 FROM filings"""
        ).fetchone()
        facts = conn.execute(
            """SELECT COUNT(*) AS fact_count,
                      SUM(CASE WHEN numeric_value IS NOT NULL AND is_nil = 0 THEN 1 ELSE 0 END)
                          AS numeric_fact_count,
                      SUM(CASE WHEN numeric_value IS NULL AND value_text IS NOT NULL THEN 1 ELSE 0 END)
                          AS nonnumeric_fact_count,
                      COALESCE(SUM(CASE WHEN numeric_value IS NULL AND value_text IS NOT NULL
                                       THEN length(CAST(value_text AS BLOB)) ELSE 0 END), 0)
                          AS nonnumeric_text_bytes
                 FROM xbrl_facts"""
        ).fetchone()
        sections = (0, 0)
        if _table_exists(conn, "sections"):
            sections = conn.execute(
                """SELECT COUNT(*),
                          COALESCE(SUM(length(CAST(text AS BLOB))), 0)
                     FROM sections"""
            ).fetchone()
        return {
            "filing_count": int(filing["filing_count"] or 0),
            "archived_count": int(filing["archived_count"] or 0),
            "archive_bytes": int(filing["archive_bytes"] or 0),
            "fact_count": int(facts["fact_count"] or 0),
            "numeric_fact_count": int(facts["numeric_fact_count"] or 0),
            "nonnumeric_fact_count": int(facts["nonnumeric_fact_count"] or 0),
            "nonnumeric_text_bytes": int(facts["nonnumeric_text_bytes"] or 0),
            "section_count": int(sections[0] or 0),
            "section_text_bytes": int(sections[1] or 0),
        }
    finally:
        conn.close()


def _print_summary(summary: dict[str, int]) -> None:
    print(f"Filings: {summary['filing_count']:,}")
    print(f"Retained archives: {summary['archived_count']:,}")
    print(f"Archive bytes: {summary['archive_bytes']:,}")
    print(f"Source facts: {summary['fact_count']:,}")
    print(f"Numeric facts to retain: {summary['numeric_fact_count']:,}")
    print(f"Nonnumeric facts to omit: {summary['nonnumeric_fact_count']:,}")
    print(f"Nonnumeric text to omit: {summary['nonnumeric_text_bytes']:,} bytes")
    print(f"Materialized sections to omit: {summary['section_count']:,}")
    print(f"Materialized section text to omit: {summary['section_text_bytes']:,} bytes")


def _copy_batches(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    select_sql: str,
    insert_sql: str,
    *,
    batch_size: int,
) -> int:
    cursor = source.execute(select_sql)
    copied = 0
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        destination.executemany(insert_sql, [tuple(row) for row in rows])
        destination.commit()
        copied += len(rows)
    return copied


def _copy_catalog_rows(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    *,
    batch_size: int,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    counts["filings"] = _copy_batches(
        source,
        destination,
        f"SELECT {_FILINGS_COLUMNS} FROM filings ORDER BY doc_id",
        f"INSERT INTO filings ({_FILINGS_COLUMNS}) VALUES ({','.join('?' for _ in _FILINGS_COLUMNS.split(', '))})",
        batch_size=batch_size,
    )
    counts["artifacts"] = _copy_batches(
        source,
        destination,
        f"SELECT {_ARTIFACT_COLUMNS} FROM artifacts ORDER BY doc_id, member_path",
        f"INSERT INTO artifacts ({_ARTIFACT_DESTINATION_COLUMNS}) VALUES ({','.join('?' for _ in _ARTIFACT_COLUMNS.split(', '))}, NULL)",
        batch_size=batch_size,
    )
    counts["contexts"] = _copy_batches(
        source,
        destination,
        "SELECT context_id, doc_id, entity_identifier, period_start, period_end, instant "
        "FROM xbrl_contexts ORDER BY doc_id, context_id",
        "INSERT OR IGNORE INTO xbrl_contexts "
        "(context_id, doc_id, entity_identifier, period_start, period_end, instant) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        batch_size=batch_size,
    )
    counts["units"] = _copy_batches(
        source,
        destination,
        "SELECT unit_id, doc_id, measure FROM xbrl_units ORDER BY doc_id, unit_id",
        "INSERT OR IGNORE INTO xbrl_units (unit_id, doc_id, measure) VALUES (?, ?, ?)",
        batch_size=batch_size,
    )
    return counts


def _copy_small_tables(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    *,
    batch_size: int,
) -> dict[str, int]:
    """Copy small derived/catalog tables whose schemas are unchanged."""
    counts: dict[str, int] = {}
    table_names = (
        "data_watermarks",
        "filing_translations",
        "metric_catalog",
        "observations",
        "observation_dependencies",
        "parse_runs",
    )
    for table_name in table_names:
        if not _table_exists(source, table_name) or not _table_exists(destination, table_name):
            continue
        source_columns = [row[1] for row in source.execute(f"PRAGMA table_info({table_name})")]
        destination_columns = {row[1] for row in destination.execute(f"PRAGMA table_info({table_name})")}
        columns = [column for column in source_columns if column in destination_columns]
        quoted = ", ".join(f'"{column}"' for column in columns)
        placeholders = ", ".join("?" for _ in columns)
        counts[table_name] = _copy_batches(
            source,
            destination,
            f'SELECT {quoted} FROM "{table_name}"',
            f'INSERT OR REPLACE INTO "{table_name}" ({quoted}) VALUES ({placeholders})',
            batch_size=batch_size,
        )
    return counts


def _copy_observation_sources(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    *,
    batch_size: int,
) -> int:
    """Copy provenance links only when their source fact survived filtering."""
    table_name = "observation_sources"
    if not _table_exists(source, table_name) or not _table_exists(destination, table_name):
        return 0
    columns = [row[1] for row in source.execute(f"PRAGMA table_info({table_name})")]
    destination_columns = {row[1] for row in destination.execute(f"PRAGMA table_info({table_name})")}
    columns = [column for column in columns if column in destination_columns]
    quoted = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = f'INSERT OR REPLACE INTO "{table_name}" ({quoted}) VALUES ({placeholders})'
    copied = 0
    cursor = source.execute(f'SELECT {quoted} FROM "{table_name}"')
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        fact_ids = list({row["fact_id"] for row in rows if row["fact_id"]})
        surviving: set[str] = set()
        for start in range(0, len(fact_ids), 500):
            chunk = fact_ids[start : start + 500]
            marks = ", ".join("?" for _ in chunk)
            surviving.update(
                row[0]
                for row in destination.execute(
                    f"SELECT fact_id FROM xbrl_facts WHERE fact_id IN ({marks})",
                    chunk,
                )
            )
        kept = [
            tuple(row)
            for row in rows
            if not row["fact_id"] or row["fact_id"] in surviving
        ]
        if kept:
            destination.executemany(insert_sql, kept)
            destination.commit()
            copied += len(kept)
    return copied


def _fact_row(doc_id: str, artifact_id: str, fact: Any) -> dict[str, Any]:
    return {
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


def _source_numeric_facts(
    source: sqlite3.Connection,
    doc_id: str,
) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in source.execute(
            f"SELECT {_FACT_COLUMNS} FROM xbrl_facts "
            "WHERE doc_id = ? AND numeric_value IS NOT NULL AND is_nil = 0",
            (doc_id,),
        )
    ]


def _rebuild_facts(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    *,
    batch_size: int,
) -> int:
    """Reparse XBRL members so numeric filtering uses XBRL structure."""
    parser = XbrlParser()
    fact_insert = (
        "INSERT OR IGNORE INTO xbrl_facts "
        f"({_FACT_COLUMNS}) VALUES ({','.join(':' + column for column in _FACT_COLUMNS.split(', '))})"
    )
    quality_insert = (
        "INSERT OR IGNORE INTO quality_issues "
        "(issue_id, doc_id, severity, code, message, fact_id, created_at) "
        "VALUES (:issue_id, :doc_id, :severity, :code, :message, :fact_id, :created_at)"
    )
    total = 0
    filings = source.execute(
        "SELECT doc_id, archive_content FROM filings "
        "WHERE archive_content IS NOT NULL ORDER BY doc_id"
    )
    for index, filing in enumerate(filings, start=1):
        doc_id = str(filing["doc_id"])
        facts: list[dict[str, Any]] = []
        try:
            archive_content = bytes(filing["archive_content"])
            infos = validate_zip_in_memory(archive_content, DEFAULT_ARCHIVE_POLICY)
            info_by_path = {
                info.filename.replace("\\", "/"): info
                for info in infos
                if not info.is_dir()
            }
            artifacts = source.execute(
                "SELECT artifact_id, member_path FROM artifacts "
                "WHERE doc_id = ? AND kind = 'xbrl' ORDER BY member_path",
                (doc_id,),
            )
            with zipfile.ZipFile(io.BytesIO(archive_content)) as archive:
                for artifact in artifacts:
                    member_path = str(artifact["member_path"]).replace("\\", "/")
                    info = info_by_path.get(member_path)
                    if info is None or info.file_size > _ARCHIVE_LIMIT:
                        continue
                    parsed = parser.parse(
                        archive.read(info),
                        doc_id,
                        str(artifact["artifact_id"]),
                        numeric_only=True,
                    )
                    facts.extend(
                        _fact_row(doc_id, str(artifact["artifact_id"]), fact)
                        for fact in parsed.facts
                        if fact.numeric_value is not None and not fact.is_nil
                    )
        except (OSError, ValueError, zipfile.BadZipFile) as exc:
            # Preserve already-indexed numeric facts if a legacy archive is
            # unreadable during migration; the ZIP remains available for a
            # later manual repair.
            print(f"Warning: {doc_id}: reparsing failed ({exc}); using existing numeric facts")
            facts = [
                dict(zip(_FACT_COLUMNS.split(", "), row, strict=True))
                for row in _source_numeric_facts(source, doc_id)
            ]

        if facts:
            destination.executemany(fact_insert, facts)
            issues = assess_facts(doc_id, facts)
            if issues:
                destination.executemany(quality_insert, issues)
            destination.commit()
            total += len(facts)
        if index % 100 == 0 or index == 1:
            print(f"Rebuilt numeric facts for {index:,} filings ({total:,} facts)")
    return total


def _finalize(path: Path, *, vacuum: bool) -> None:
    conn = connect_write(path)
    try:
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        if vacuum:
            conn.execute("VACUUM")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.commit()
    finally:
        conn.close()


def _estimated_output_bytes(summary: dict[str, int]) -> int:
    # Archives dominate the rebuilt file; this intentionally includes a
    # generous per-fact allowance for relational columns and indexes.
    return summary["archive_bytes"] + summary["numeric_fact_count"] * 4096 + 4 * 1024 * 1024 * 1024


def main() -> int:
    args = _parser().parse_args()
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be positive")
    source_path = Path(args.source).expanduser().resolve(strict=True)
    output_path = Path(args.output).expanduser().resolve()
    if source_path == output_path:
        raise SystemExit("--output must be different from --source")

    summary = _summary(source_path)
    _print_summary(summary)
    if not args.apply:
        print("Dry run; pass --apply to build the compact database.")
        return 0
    if output_path.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {output_path}")
    partial_path = output_path.with_name(output_path.name + ".partial")
    if partial_path.exists():
        raise SystemExit(f"Refusing to overwrite existing partial output: {partial_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    required = _estimated_output_bytes(summary)
    free = shutil.disk_usage(output_path.parent).free
    if free < required:
        raise SystemExit(
            f"Not enough free space for the rebuilt database: estimated at least "
            f"{required:,} bytes, available {free:,} bytes"
        )

    try:
        FilingCatalog(partial_path, include_narrative_index=False)
        source = connect_read(source_path)
        destination = connect_write(partial_path)
        try:
            counts = _copy_catalog_rows(source, destination, batch_size=args.batch_size)
            print("Copied catalog rows: " + ", ".join(f"{key}={value:,}" for key, value in counts.items()))
            copied_facts = _rebuild_facts(source, destination, batch_size=args.batch_size)
            print(f"Retained {copied_facts:,} numeric facts.")
            small_counts = _copy_small_tables(source, destination, batch_size=args.batch_size)
            destination.execute(
                "UPDATE parse_runs SET fact_count = "
                "(SELECT COUNT(*) FROM xbrl_facts WHERE xbrl_facts.doc_id = parse_runs.doc_id), "
                "section_count = 0"
            )
            destination.commit()
            small_counts["observation_sources"] = _copy_observation_sources(
                source,
                destination,
                batch_size=args.batch_size,
            )
            if small_counts:
                print("Copied derived rows: " + ", ".join(f"{key}={value:,}" for key, value in small_counts.items()))
        finally:
            source.close()
            destination.close()
        _finalize(partial_path, vacuum=not args.no_vacuum)
        sidecars = [
            partial_path.with_name(partial_path.name + suffix)
            for suffix in ("-wal", "-shm")
        ]
        if any(sidecar.exists() for sidecar in sidecars):
            raise RuntimeError("Rebuilt database still has SQLite sidecar files; refusing to finalize")
        os.replace(partial_path, output_path)
    except Exception:
        print(f"Rebuild failed; partial output was left at {partial_path}", file=sys.stderr)
        raise

    print(f"Compact database written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
