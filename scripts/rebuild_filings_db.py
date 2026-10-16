#!/usr/bin/env python3
"""Rebuild yearly filing files from their own retained ZIP archives.

Each yearly file (``filings/filings_YYYY.db``) is rebuilt into a new file next
to it (``filings_YYYY.rebuilt.db``, or in ``--output-folder``); the existing
file is never modified. The new file keeps the filing metadata, the compressed
archives (each checked against its SHA-256), artifact metadata, contexts,
units and parse runs, and re-parses every archive's XBRL so only structural
numeric facts are kept, with their quality issues assessed again.

Run without ``--apply`` to see what would be rebuilt. ``--switch`` (with the
server stopped) then points the index at the new file and re-reads its filing
rows; the old file stays where it was until you delete it.

The old single-file ``filings.db`` must be split first (``main.py filings split``).
"""

from __future__ import annotations

import argparse
import hashlib
import io
import shutil
import sqlite3
import sys
import zipfile
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.filings.archive import DEFAULT_ARCHIVE_POLICY, validate_zip_in_memory  # noqa: E402
from src.filings.quality import assess_facts  # noqa: E402
from src.filings.storage import (  # noqa: E402
    LAYOUT_SPLIT,
    METADATA_COLUMNS,
    FilingStorage,
    FilingStorageError,
    initialize_shard,
    shard_info,
    validate_shard_id,
)
from src.filings.xbrl import XbrlParser  # noqa: E402
from src.orchestrator.common.db_config import get_filings_db  # noqa: E402
from src.orchestrator.common.sqlite import connect_read, connect_write, initialize_managed_database  # noqa: E402

_ARCHIVE_LIMIT = 25 * 1024 * 1024
_REBUILT_SUFFIX = ".rebuilt.db"
_FACT_COLUMNS = (
    "fact_id, doc_id, artifact_id, concept, namespace_uri, context_id, unit_id, "
    "value_text, numeric_value, decimals, is_nil"
)
#: Tables copied unchanged from the old yearly file, with their columns.
_COPIED_TABLES: tuple[tuple[str, str], ...] = (
    ("filings", ", ".join(METADATA_COLUMNS)),
    ("artifacts", "artifact_id, doc_id, member_path, kind, media_type, size_bytes, sha256"),
    ("xbrl_contexts", "context_id, doc_id, entity_identifier, period_start, period_end, instant"),
    ("xbrl_units", "unit_id, doc_id, measure"),
    (
        "parse_runs",
        "parse_run_id, doc_id, parser_version, status, started_at, completed_at, fact_count, "
        "section_count, warning_count, error_message",
    ),
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rebuild yearly filing files from their retained archives.")
    parser.add_argument("--index", default=get_filings_db(), help="The filing index (default: the configured filings.db)")
    parser.add_argument("--shard", action="append", help="A year to rebuild (repeatable); default: every year")
    parser.add_argument("--output-folder", help="Where the rebuilt files go (default: next to each yearly file)")
    parser.add_argument("--apply", action="store_true", help="Build the new files; without this the command is a dry run.")
    parser.add_argument(
        "--switch",
        action="store_true",
        help="After building, point the index at each new file (server stopped); the old file is kept.",
    )
    parser.add_argument("--no-vacuum", action="store_true", help="Skip the final VACUUM of each new file.")
    parser.add_argument("--batch-size", type=int, default=250, help="Rows per copy batch (default: 250)")
    return parser


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
    """Copy filings, artifact metadata, contexts, units and parse runs, then every archive (SHA-256 checked)."""
    counts: dict[str, int] = {}
    for table, columns in _COPIED_TABLES:
        marks = ", ".join("?" for _ in columns.split(", "))
        counts[table] = _copy_batches(
            source,
            destination,
            f"SELECT {columns} FROM {table} ORDER BY rowid",
            f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({marks})",
            batch_size=batch_size,
        )
    counts["filing_archives"] = 0
    bad: list[str] = []
    cursor = source.execute("SELECT doc_id, sha256, size, content FROM filing_archives ORDER BY doc_id")
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        kept = []
        for row in rows:
            content = bytes(row["content"])
            if hashlib.sha256(content).hexdigest() != row["sha256"] or len(content) != row["size"]:
                bad.append(str(row["doc_id"]))
                continue
            kept.append((row["doc_id"], row["sha256"], row["size"], content))
        destination.executemany("INSERT INTO filing_archives (doc_id, sha256, size, content) VALUES (?, ?, ?, ?)", kept)
        destination.commit()
        counts["filing_archives"] += len(kept)
    if bad:
        # A damaged archive is left out; the filing stays and can be downloaded again.
        print(f"Warning: {len(bad)} archive(s) did not match their SHA-256 and were left out: {', '.join(bad[:20])}")
    counts["damaged_archives"] = len(bad)
    return counts


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


def _source_numeric_facts(source: sqlite3.Connection, doc_id: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in source.execute(
            f"SELECT {_FACT_COLUMNS} FROM xbrl_facts WHERE doc_id = ? AND numeric_value IS NOT NULL AND is_nil = 0",
            (doc_id,),
        )
    ]


def _rebuild_facts(source: sqlite3.Connection, destination: sqlite3.Connection) -> int:
    """Re-parse each copied archive's XBRL members so numeric filtering uses XBRL structure."""
    parser = XbrlParser()
    fact_insert = (
        f"INSERT OR IGNORE INTO xbrl_facts ({_FACT_COLUMNS}) "
        f"VALUES ({', '.join(':' + column for column in _FACT_COLUMNS.split(', '))})"
    )
    quality_insert = (
        "INSERT OR IGNORE INTO quality_issues (issue_id, doc_id, severity, code, message, fact_id, created_at) "
        "VALUES (:issue_id, :doc_id, :severity, :code, :message, :fact_id, :created_at)"
    )
    total = 0
    doc_ids = [str(row[0]) for row in destination.execute("SELECT doc_id FROM filings ORDER BY doc_id")]
    for number, doc_id in enumerate(doc_ids, start=1):
        row = destination.execute("SELECT content FROM filing_archives WHERE doc_id = ?", (doc_id,)).fetchone()
        facts: list[dict[str, Any]] = []
        try:
            if row is None:
                raise ValueError("no archive")
            archive_content = bytes(row[0])
            infos = validate_zip_in_memory(archive_content, DEFAULT_ARCHIVE_POLICY)
            info_by_path = {info.filename.replace("\\", "/"): info for info in infos if not info.is_dir()}
            artifacts = destination.execute(
                "SELECT artifact_id, member_path FROM artifacts WHERE doc_id = ? AND kind = 'xbrl' ORDER BY member_path",
                (doc_id,),
            ).fetchall()
            with zipfile.ZipFile(io.BytesIO(archive_content)) as archive:
                for artifact in artifacts:
                    info = info_by_path.get(str(artifact["member_path"]).replace("\\", "/"))
                    if info is None or info.file_size > _ARCHIVE_LIMIT:
                        continue
                    parsed = parser.parse(archive.read(info), doc_id, str(artifact["artifact_id"]), numeric_only=True)
                    facts.extend(
                        _fact_row(doc_id, str(artifact["artifact_id"]), fact)
                        for fact in parsed.facts
                        if fact.numeric_value is not None and not fact.is_nil
                    )
        except (OSError, ValueError, zipfile.BadZipFile) as exc:
            # Keep the numeric facts already indexed when the archive cannot be read.
            print(f"Warning: {doc_id}: reparsing failed ({exc}); keeping its existing numeric facts")
            facts = _source_numeric_facts(source, doc_id)
        if facts:
            destination.executemany(fact_insert, facts)
            issues = assess_facts(doc_id, facts)
            if issues:
                destination.executemany(quality_insert, issues)
            destination.commit()
            total += len(facts)
        if number % 100 == 0 or number == 1:
            print(f"  re-parsed {number:,} of {len(doc_ids):,} filings ({total:,} facts)")
    destination.execute(
        "UPDATE parse_runs SET fact_count = (SELECT COUNT(*) FROM xbrl_facts WHERE xbrl_facts.doc_id = parse_runs.doc_id), "
        "section_count = 0"
    )
    destination.commit()
    return total


def _finalize(path: Path, *, vacuum: bool) -> None:
    conn = connect_write(path)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        if vacuum:
            conn.execute("VACUUM")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        result = conn.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn.close()
    if result != "ok":
        raise RuntimeError(f"{path.name} failed its quick check: {result}")


def _output_path(source: Path, shard: str, output_folder: str | None) -> Path:
    folder = Path(output_folder).expanduser().resolve() if output_folder else source.parent
    return folder / f"filings_{shard}{_REBUILT_SUFFIX}"


def rebuild_shard(source_path: Path, output_path: Path, shard: str, *, batch_size: int = 250, vacuum: bool = True) -> dict[str, int]:
    """Build ``output_path`` from the yearly file ``source_path``; a partial file is left as ``.partial``."""
    partial = output_path.with_name(output_path.name + ".partial")
    for path in (output_path, partial):
        if path.exists():
            raise SystemExit(f"Refusing to overwrite {path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    source = connect_read(source_path)
    try:
        info = shard_info(source)
        if info.get("shard") != shard:
            raise SystemExit(f"{source_path} holds year {info.get('shard') or 'unknown'}, not {shard}")
        destination = connect_write(partial)
        try:
            initialize_managed_database(destination)
            initialize_shard(destination, shard)
            destination.commit()
            counts = _copy_catalog_rows(source, destination, batch_size=batch_size)
            counts["xbrl_facts"] = _rebuild_facts(source, destination)
        finally:
            destination.close()
    finally:
        source.close()
    _finalize(partial, vacuum=vacuum)
    partial.replace(output_path)
    return counts


def main() -> int:
    args = _parser().parse_args()
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be positive")
    if args.switch and not args.apply:
        raise SystemExit("--switch needs --apply")
    storage = FilingStorage(args.index)
    if storage.layout() != LAYOUT_SPLIT:
        raise SystemExit("Filing storage needs to be split first (main.py filings split, or Admin → Filing storage)")
    registered = {row["shard"]: row for row in storage.shards()}
    wanted = [validate_shard_id(shard) for shard in args.shard] if args.shard else sorted(registered)
    unknown = [shard for shard in wanted if shard not in registered]
    if unknown:
        raise SystemExit(f"No yearly file for {', '.join(unknown)}")

    plan = []
    for shard in wanted:
        row = registered[shard]
        source = Path(row["file"])
        if not row["exists"]:
            raise SystemExit(f"{source.name} is missing; restore it or run `main.py filings check --repair` first")
        output = _output_path(source, shard, args.output_folder)
        plan.append((shard, source, output))
        print(f"{shard}: {row['filing_count']:,} filings, {row['size_bytes'] or 0:,} bytes -> {output}")
    if not args.apply:
        print("Dry run; pass --apply to build the new files.")
        return 0
    needed = max((source.stat().st_size for _, source, _ in plan), default=0)
    for _, _, output in plan:
        output.parent.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(output.parent).free < needed:
            raise SystemExit(f"Not enough free space in {output.parent} for a rebuilt file of up to {needed:,} bytes")

    from src.filings import maintenance

    for shard, source, output in plan:
        print(f"Rebuilding {shard} ...")
        counts = rebuild_shard(source, output, shard, batch_size=args.batch_size, vacuum=not args.no_vacuum)
        print(f"  {shard}: " + ", ".join(f"{key}={value:,}" for key, value in counts.items()))
        if args.switch:
            try:
                maintenance.move_shard(storage, shard, str(output))
                maintenance.check(storage, repair=True, shard=shard)
            except FilingStorageError as exc:
                raise SystemExit(f"Could not switch {shard} over: {exc}") from exc
            print(f"  the index now uses {output}; the old file {source} can be deleted once you are satisfied")
        else:
            print(f"  built {output}; switch with --switch or `main.py filings move-shard {shard} {output}`")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
