"""Split the old single ``filings.db`` into the filing index and one file per submission year.

The operator starts it (Admin -> Filing storage, or ``main.py filings split``);
it never starts by itself. The old file is only ever read: it is attached
read-only while the new files are written beside it.

1. ``filings.next.db`` (the new index) is created beside ``filings.db``, and the
   yearly files in ``filings/`` (or in a folder chosen for this run).
2. Year by year, in chunks of filings ordered by ``doc_id``, each filing's
   metadata and archive are copied, the archive's SHA-256 is computed from the
   bytes just read and compared with the recorded hash, and its parsed rows
   follow. Each chunk is one commit; a resumed run skips filings already copied.
3. After each year, row counts are compared with the old file and
   ``PRAGMA quick_check`` runs; the result is recorded in ``filing_shards``.
4. The tables that span filings are copied into the new index, and only then is
   it marked ``layout = split-v1, state = ready``.
5. On the next server start (``switch_over``, before anything opens the file)
   ``filings.db`` becomes ``filings.db.migrated`` and ``filings.next.db``
   becomes ``filings.db``. Windows cannot rename a file a process has open, so
   this cannot happen while the server runs. The retired file stays until the
   operator deletes it; renaming it back rolls the switch back.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import (
    attach,
    connect_read,
    connect_write,
    initialize_managed_database,
    quote_identifier,
)

from .storage import (
    CROSS_FILING_TABLES,
    INDEX_DDL,
    INDEX_SCHEMA_VERSION,
    LAYOUT_EMPTY,
    LAYOUT_LEGACY,
    LAYOUT_SPLIT,
    METADATA_COLUMNS,
    SHARD_CONTENT_TABLES,
    FilingStorage,
    FilingStorageError,
    MissingSubmissionDateError,
    filing_shards_dir,
    index_rows,
    initialize_shard,
    next_index_path,
    now,
    retired_index_path,
    shard_file_name,
    shard_for,
    upsert_index_rows,
)

logger = logging.getLogger(__name__)

CHUNK_SIZE = 200
# Free space wanted on the target disk: the old file's size plus this margin.
SPACE_MARGIN = 1.05
_PUBLISH_CHUNK = 2000

Progress = Callable[[int, int, str], None]
Checkpoint = Callable[[], None]


class SplitError(FilingStorageError):
    """The split cannot go on; the message names what to fix."""


def _no_progress(done: int, total: int, message: str) -> None:
    return None


def _no_checkpoint() -> None:
    return None


def _sidecars(path: Path) -> list[Path]:
    """A SQLite file with its ``-wal`` and ``-shm`` files."""
    return [path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")]


def file_bytes(path: Path) -> int:
    """A database's size on disk, its write-ahead log included."""
    return sum(part.stat().st_size for part in _sidecars(path)[:2] if part.exists())


def free_bytes(folder: Path) -> int:
    """Free space on the disk holding ``folder`` (or its nearest existing parent)."""
    probe = Path(folder).expanduser()
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    return int(shutil.disk_usage(probe).free)


def _values(path: Path) -> dict[str, str]:
    """``storage_layout`` of an index file ({} when missing or not an index)."""
    if not path.exists():
        return {}
    conn = connect_read(path)
    try:
        return {str(key): str(value) for key, value in conn.execute("SELECT key, value FROM storage_layout")}
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def _years(conn: sqlite3.Connection, schema: str = "main") -> tuple[dict[str, list[str]], list[str]]:
    """``{year: [doc_id, ...]}`` of the old file (ordered by doc_id), and the filings without a usable date.

    Reads ``doc_id`` and ``submitted_at`` only: both sit before the archive column.
    """
    years: dict[str, list[str]] = {}
    undated: list[str] = []
    for doc_id, submitted_at in conn.execute(f"SELECT doc_id, submitted_at FROM {schema}.filings ORDER BY doc_id"):
        try:
            years.setdefault(shard_for(submitted_at), []).append(str(doc_id))
        except MissingSubmissionDateError:
            undated.append(str(doc_id))
    return years, undated


def _target_folder(index: Path, target_folder: str | Path | None, recorded: str | None) -> Path:
    if target_folder:
        target = Path(target_folder).expanduser()
        if not target.is_absolute():
            raise SplitError("The target folder must be an absolute path")
        if recorded and Path(recorded) != target:
            raise SplitError(
                f"A split into {recorded} is under way; finish it there, or delete {next_index_path(index).name} to start again"
            )
        return target
    return Path(recorded) if recorded else filing_shards_dir(index)


def preflight(index_path: str | Path, target_folder: str | Path | None = None) -> dict[str, Any]:
    """What a split would create and whether the target disk has room; reads no archive."""
    index = Path(index_path).expanduser()
    storage = FilingStorage(index)
    layout = storage.layout()
    result: dict[str, Any] = {"layout": layout, "index": str(index), "needed": layout == LAYOUT_LEGACY}
    if layout != LAYOUT_LEGACY:
        return result
    upcoming = next_index_path(index)
    state = _values(upcoming)
    target = _target_folder(index, target_folder, state.get("target_folder"))
    conn = connect_read(index)
    try:
        years, undated = _years(conn)
    finally:
        conn.close()
    source_bytes = file_bytes(index)
    total = sum(len(ids) for ids in years.values())
    per_filing = source_bytes / total if total else 0.0
    written = sum(file_bytes(target / shard_file_name(year)) for year in years) + file_bytes(upcoming)
    needed = max(0, int(source_bytes * SPACE_MARGIN) - written)
    free = free_bytes(target)
    result.update(
        source_bytes=source_bytes,
        filings=total,
        target_folder=str(target),
        default_folder=target == filing_shards_dir(index),
        free_bytes=free,
        written_bytes=written,
        needed_bytes=needed,
        enough_space=free >= needed,
        undated=undated[:20],
        undated_count=len(undated),
        state=state.get("state"),
        years=[
            {
                "shard": year,
                "filings": len(ids),
                "estimated_bytes": int(len(ids) * per_filing),
                "path": str(target / shard_file_name(year)),
            }
            for year, ids in sorted(years.items())
        ],
        files=[str(upcoming)] + [str(target / shard_file_name(year)) for year in sorted(years)],
    )
    return result


def _create_next_index(upcoming: Path, source: Path, target: Path) -> None:
    conn = connect_write(upcoming)
    try:
        initialize_managed_database(conn)
        conn.executescript(INDEX_DDL)
        stamp = now()
        # No ``layout`` row yet: the file only becomes an index once the copy is complete.
        conn.executemany(
            "INSERT OR IGNORE INTO storage_layout(key, value) VALUES (?, ?)",
            (
                ("state", "copying"),
                ("schema_version", str(INDEX_SCHEMA_VERSION)),
                ("migrated_from", str(source)),
                ("target_folder", str(target)),
                ("started_at", stamp),
                ("created_at", stamp),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _columns(conn: sqlite3.Connection, schema: str, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA {schema}.table_info({quote_identifier(table)})")]


def _common_columns(conn: sqlite3.Connection, table: str, source: str = "old") -> str:
    present = set(_columns(conn, source, table))
    return ", ".join(quote_identifier(column) for column in _columns(conn, "main", table) if column in present)


def _copy_year(
    source: Path,
    path: Path,
    shard: str,
    doc_ids: list[str],
    *,
    chunk_size: int,
    on_chunk: Callable[[int], None],
    checkpoint: Checkpoint,
) -> int:
    """Copy one year's filings into its file; returns how many were copied by this run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_write(path, uri=True)
    try:
        initialize_managed_database(conn)
        initialize_shard(conn, shard)
        conn.commit()
        attach(conn, source, "old", read_only=True)
        present = {str(row[0]) for row in conn.execute("SELECT doc_id FROM main.filings")}
        remaining = [doc_id for doc_id in doc_ids if doc_id not in present]
        on_chunk(len(doc_ids) - len(remaining))
        old_tables = {str(row[0]) for row in conn.execute("SELECT name FROM old.sqlite_master WHERE type = 'table'")}
        per_doc = {
            table: _common_columns(conn, table)
            for table in SHARD_CONTENT_TABLES
            if table != "filing_archives" and table in old_tables
        }
        metadata = ", ".join(METADATA_COLUMNS)
        insert_filing = (
            f"INSERT INTO main.filings ({metadata}) VALUES ({', '.join(':' + column for column in METADATA_COLUMNS)})"
        )
        copied = 0
        for start in range(0, len(remaining), chunk_size):
            checkpoint()
            chunk = remaining[start:start + chunk_size]
            marks = ",".join("?" * len(chunk))
            mismatched: list[str] = []
            conn.execute("BEGIN")
            try:
                rows = conn.execute(
                    f"SELECT {metadata}, archive_content FROM old.filings WHERE doc_id IN ({marks}) ORDER BY doc_id",
                    chunk,
                )
                for row in rows:
                    conn.execute(insert_filing, {column: row[column] for column in METADATA_COLUMNS})
                    if row["archive_content"] is None:
                        continue
                    content = bytes(row["archive_content"])
                    digest = hashlib.sha256(content).hexdigest()
                    if row["archive_sha256"] and str(row["archive_sha256"]).lower() != digest:
                        mismatched.append(str(row["doc_id"]))
                        continue
                    conn.execute(
                        "INSERT INTO main.filing_archives (doc_id, sha256, size, content) VALUES (?, ?, ?, ?)",
                        (row["doc_id"], digest, len(content), content),
                    )
                if mismatched:
                    raise SplitError(
                        f"{len(mismatched)} archive(s) in {shard} do not match their recorded SHA-256 "
                        f"({', '.join(mismatched[:10])}); the old file is unchanged"
                    )
                for table, columns in per_doc.items():
                    conn.execute(
                        f"INSERT INTO main.{quote_identifier(table)} ({columns}) "
                        f"SELECT {columns} FROM old.{quote_identifier(table)} WHERE doc_id IN ({marks})",
                        chunk,
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            copied += len(chunk)
            on_chunk(len(chunk))
        return copied
    finally:
        conn.close()


def _verify_year(path: Path, source: Path, shard: str, expected: int, *, full: bool = False) -> str:
    """Compare the year's row counts with the old file and check the file; returns ``ok`` or raises."""
    conn = connect_write(path, uri=True)
    try:
        attach(conn, source, "old", read_only=True)
        old_tables = {str(row[0]) for row in conn.execute("SELECT name FROM old.sqlite_master WHERE type = 'table'")}
        problems: list[str] = []
        filings = int(conn.execute("SELECT COUNT(*) FROM main.filings").fetchone()[0])
        if filings != expected:
            problems.append(f"filings {filings} of {expected}")
        mine = "SELECT doc_id FROM main.filings"
        archives = int(conn.execute("SELECT COUNT(*) FROM main.filing_archives").fetchone()[0])
        source_archives = int(
            conn.execute(
                f"SELECT COUNT(*) FROM old.filings WHERE doc_id IN ({mine}) AND archive_content IS NOT NULL"
            ).fetchone()[0]
        )
        if archives != source_archives:
            problems.append(f"archives {archives} of {source_archives}")
        for table in SHARD_CONTENT_TABLES:
            if table == "filing_archives" or table not in old_tables:
                continue
            copied = int(conn.execute(f"SELECT COUNT(*) FROM main.{quote_identifier(table)}").fetchone()[0])
            original = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM old.{quote_identifier(table)} WHERE doc_id IN ({mine})"
                ).fetchone()[0]
            )
            if copied != original:
                problems.append(f"{table} {copied} of {original}")
        if problems:
            raise SplitError(f"The copy of {shard} does not match the old file ({'; '.join(problems)})")
        conn.execute("DETACH DATABASE old")
        return integrity(conn, full=full)
    finally:
        conn.close()


def integrity(conn: sqlite3.Connection, *, full: bool = False) -> str:
    """``ok``, or the first problems ``PRAGMA quick_check`` (``integrity_check`` when full) reports."""
    pragma = "integrity_check" if full else "quick_check"
    messages = [str(row[0]) for row in conn.execute(f"PRAGMA main.{pragma}(20)")]
    return "ok" if messages == ["ok"] else "; ".join(messages)[:1000]


def _publish_year(upcoming: FilingStorage, shard: str, path: Path, check_result: str) -> None:
    """Write the year's index rows and counters into the new index."""
    conn = connect_read(path)
    try:
        doc_ids = [str(row[0]) for row in conn.execute("SELECT doc_id FROM filings ORDER BY doc_id")]
        rows: list[dict[str, Any]] = []
        for start in range(0, len(doc_ids), _PUBLISH_CHUNK):
            rows.extend(index_rows(conn, doc_ids[start:start + _PUBLISH_CHUNK], shard))
    finally:
        conn.close()
    index = connect_write(upcoming.index_path)
    try:
        index.execute("BEGIN IMMEDIATE")
        upsert_index_rows(index, rows)
        upcoming.refresh_counters(index, shard)
        stamp = now()
        index.execute(
            "UPDATE filing_shards SET file_bytes = ?, checked_at = ?, check_result = ?, updated_at = ? WHERE shard = ?",
            (file_bytes(path), stamp, check_result, stamp, shard),
        )
        index.commit()
    except BaseException:
        index.rollback()
        raise
    finally:
        index.close()


def _copy_cross_filing_tables(upcoming: Path, source: Path) -> dict[str, int]:
    """Copy translations, watermarks, metrics, observations and the bond/ownership logs into the new index."""
    conn = connect_write(upcoming, uri=True)
    copied: dict[str, int] = {}
    try:
        attach(conn, source, "old", read_only=True)
        old = {
            str(row["name"]): row
            for row in conn.execute("SELECT name, sql FROM old.sqlite_master WHERE type = 'table'")
        }
        conn.execute("BEGIN")
        for table in CROSS_FILING_TABLES:
            if table not in old:
                continue
            if not _columns(conn, "main", table):
                conn.execute(str(old[table]["sql"]))
                for (sql,) in conn.execute(
                    "SELECT sql FROM old.sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL",
                    (table,),
                ).fetchall():
                    conn.execute(re.sub(r"^CREATE (UNIQUE )?INDEX ", r"CREATE \1INDEX IF NOT EXISTS ", str(sql), count=1))
            columns = _common_columns(conn, table)
            conn.execute(f"DELETE FROM main.{quote_identifier(table)}")
            copied[table] = conn.execute(
                f"INSERT INTO main.{quote_identifier(table)} ({columns}) SELECT {columns} FROM old.{quote_identifier(table)}"
            ).rowcount
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return copied


def _finish(upcoming: Path) -> None:
    conn = connect_write(upcoming)
    try:
        stamp = now()
        conn.executemany(
            "INSERT OR REPLACE INTO storage_layout(key, value) VALUES (?, ?)",
            (("layout", LAYOUT_SPLIT), ("state", "ready"), ("migrated_at", stamp)),
        )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()


def _checkpoint_file(path: Path) -> None:
    conn = connect_write(path)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()


def _drop_year(upcoming: FilingStorage, shard: str, path: Path) -> None:
    """Forget a year of an unfinished split: delete its file and its row in the new index."""
    for sidecar in _sidecars(path):
        sidecar.unlink(missing_ok=True)
    conn = connect_write(upcoming.index_path)
    try:
        conn.execute("DELETE FROM filings WHERE shard = ?", (shard,))
        conn.execute("DELETE FROM filing_shards WHERE shard = ?", (shard,))
        conn.commit()
    finally:
        conn.close()


def _registered_path(storage: FilingStorage, shard: str) -> Path | None:
    conn = storage.connect_index()
    try:
        row = conn.execute("SELECT path FROM filing_shards WHERE shard = ?", (shard,)).fetchone()
    finally:
        conn.close()
    return storage.resolve_shard_path(str(row[0])) if row else None


def split(
    index_path: str | Path,
    *,
    target_folder: str | Path | None = None,
    progress: Progress = _no_progress,
    checkpoint: Checkpoint = _no_checkpoint,
    chunk_size: int = CHUNK_SIZE,
    switch: bool = False,
) -> dict[str, Any]:
    """Copy the old single file into ``filings.next.db`` and the yearly files; resumable.

    ``progress(done, total, message)`` follows filings; ``checkpoint()`` is called at
    each chunk boundary and may raise to stop (a soft cancel). ``switch`` renames the
    files at the end too, which is only safe with the server stopped.
    """
    source = Path(index_path).expanduser()
    storage = FilingStorage(source)
    if storage.layout() != LAYOUT_LEGACY:
        raise SplitError("filings.db is not in the old single-file layout, so there is nothing to split")
    upcoming_path = next_index_path(source)
    state = _values(upcoming_path)
    if upcoming_path.exists() and not state and FilingStorage(upcoming_path).layout() != LAYOUT_EMPTY:
        raise SplitError(f"{upcoming_path.name} exists but is not a filing index being built; move it away first")
    target = _target_folder(source, target_folder, state.get("target_folder"))
    started = time.monotonic()
    result: dict[str, Any] = {"index": str(source), "next": str(upcoming_path), "target_folder": str(target), "years": []}
    if state.get("state") != "ready":
        conn = connect_read(source)
        try:
            years, undated = _years(conn)
        finally:
            conn.close()
        if undated:
            raise SplitError(
                f"{len(undated)} filing(s) have no submission date, so their year is unknown: {', '.join(undated[:10])}"
            )
        free, needed = free_bytes(target), int(file_bytes(source) * SPACE_MARGIN)
        written = sum(file_bytes(target / shard_file_name(year)) for year in years)
        if free + written < needed:
            raise SplitError(
                f"Not enough free space in {target}: {free / 1e9:.1f} GB free, about {(needed - written) / 1e9:.1f} GB needed"
            )
        _create_next_index(upcoming_path, source, target)
        upcoming = FilingStorage(upcoming_path)
        total = sum(len(ids) for ids in years.values())
        done = 0
        for year in sorted(years):
            doc_ids = years[year]
            year_started = time.monotonic()
            # A resumed run keeps writing where the first run started the year.
            registered = _registered_path(upcoming, year)
            path = registered or target / shard_file_name(year)
            if registered is None and path.exists():
                raise SplitError(f"{path} already exists and is not part of this split; move it away first")
            upcoming.create_shard_file(year, path)
            upcoming.register_shard(year, path)
            position = done

            def on_chunk(count: int, year: str = year, start: int = done, size: int = len(doc_ids)) -> None:
                nonlocal position
                position += count
                progress(position, total, f"Copying {year}: {position - start} of {size} filings")

            copied = _copy_year(
                source, path, year, doc_ids, chunk_size=chunk_size, on_chunk=on_chunk, checkpoint=checkpoint
            )
            progress(done + len(doc_ids), total, f"Checking {year}")
            try:
                check_result = _verify_year(path, source, year, len(doc_ids))
                if check_result != "ok":
                    raise SplitError(f"{path.name} failed its check: {check_result}")
            except SplitError as exc:
                # A resumed run only adds the filings a year lacks, so a year that does
                # not match the old file is copied anew: its file is this split's own.
                _drop_year(upcoming, year, path)
                raise SplitError(f"{exc}. Its file was removed; run the split again to copy {year} anew") from exc
            _publish_year(upcoming, year, path, check_result)
            _checkpoint_file(path)
            done += len(doc_ids)
            result["years"].append(
                {
                    "shard": year,
                    "filings": len(doc_ids),
                    "copied": copied,
                    "file_bytes": file_bytes(path),
                    "seconds": round(time.monotonic() - year_started, 1),
                }
            )
            logger.info("Filing storage split: %s copied (%d filings, %s)", year, len(doc_ids), path)
        checkpoint()
        progress(total, total, "Copying translations, metrics and step logs")
        result["tables"] = _copy_cross_filing_tables(upcoming_path, source)
        _finish(upcoming_path)
    result["seconds"] = round(time.monotonic() - started, 1)
    result["state"] = "ready"
    if switch:
        result["switch"] = switch_over(source)
        result["state"] = "split" if result["switch"]["status"] == "switched" else "ready"
    return result


def switch_over(index_path: str | Path) -> dict[str, Any]:
    """Put a finished split in place: ``filings.db`` -> ``filings.db.migrated``, ``filings.next.db`` -> ``filings.db``.

    Only when the new index is ready and ``filings.db`` still has the old layout. The
    translations and step logs are copied again first, so what was written to the old file
    between the split and this restart is kept. A failed rename puts back what was moved
    and leaves the split ready for the next start.
    """
    index = Path(index_path).expanduser()
    upcoming = next_index_path(index)
    if not upcoming.exists():
        return {"status": "none"}
    values = _values(upcoming)
    if values.get("layout") != LAYOUT_SPLIT or values.get("state") != "ready":
        return {"status": "copying"}
    if FilingStorage(index).layout() != LAYOUT_LEGACY:
        return {"status": "stale", "error": f"{index.name} is not in the old layout; {upcoming.name} was left alone"}
    retired = retired_index_path(index)
    if retired.exists():
        return {"status": "failed", "error": f"{retired.name} already exists; move it away first"}
    try:
        _copy_cross_filing_tables(upcoming, index)
    except (sqlite3.Error, OSError) as exc:
        logger.warning("Filing storage switch-over failed, restart to finish: %s", exc)
        return {"status": "failed", "error": str(exc)}
    # The old file and its write-ahead files move together, so nothing committed is left behind.
    moves = list(zip(_sidecars(index) + _sidecars(upcoming), _sidecars(retired) + _sidecars(index), strict=True))
    done: list[tuple[Path, Path]] = []
    try:
        for old, new in moves:
            if not old.exists():
                continue
            if new.exists():
                raise OSError(f"{new.name} is in the way")
            os.rename(old, new)
            done.append((old, new))
    except OSError as exc:
        for old, new in reversed(done):
            try:
                os.rename(new, old)
            except OSError:
                logger.exception("Could not move %s back to %s", new, old)
        logger.warning("Filing storage switch-over failed, restart to finish: %s", exc)
        return {"status": "failed", "error": str(exc)}
    logger.info("Filing storage switched to the split layout; the old file is %s", retired)
    return {"status": "switched", "retired": str(retired)}


def retired_files(index_path: str | Path) -> list[Path]:
    """The old single file kept after the switch-over, with its write-ahead files."""
    return [path for path in _sidecars(retired_index_path(index_path)) if path.exists()]


def delete_retired(index_path: str | Path) -> int:
    """Delete ``filings.db.migrated``; returns the bytes freed."""
    index = Path(index_path).expanduser()
    if FilingStorage(index).layout() != LAYOUT_SPLIT:
        raise SplitError("filings.db is not split yet, so the old file is still in use")
    freed = 0
    for path in retired_files(index):
        size = path.stat().st_size
        path.unlink()
        freed += size
    return freed


def status(index_path: str | Path) -> dict[str, Any]:
    """Where a split stands: ``legacy``, ``splitting``, ``ready`` (restart to finish) or ``split``; opens no yearly file."""
    index = Path(index_path).expanduser()
    layout = FilingStorage(index).layout()
    upcoming = next_index_path(index)
    values = _values(upcoming) if layout == LAYOUT_LEGACY else {}
    if layout == LAYOUT_LEGACY:
        if values.get("state") == "ready" and values.get("layout") == LAYOUT_SPLIT:
            state = "ready"
        elif values:
            state = "splitting"
        else:
            state = "legacy"
    else:
        state = "split"
    result: dict[str, Any] = {"layout": layout, "state": state}
    if values:
        conn = connect_read(upcoming)
        try:
            result["next"] = {
                "path": str(upcoming),
                "values": values,
                "years": [dict(row) for row in conn.execute("SELECT * FROM filing_shards ORDER BY shard")],
            }
        finally:
            conn.close()
    retired = retired_files(index)
    if retired:
        result["retired"] = {"path": str(retired[0]), "bytes": sum(path.stat().st_size for path in retired)}
    return result


def notice(index_path: str | Path) -> dict[str, Any] | None:
    """What the overview and Admin pages say while a split is due; None once filing storage is split.

    Cheap enough for every overview request: a layout check and ``os.stat`` calls.
    """
    index = Path(index_path).expanduser()
    if FilingStorage(index).layout() != LAYOUT_LEGACY:
        return None
    state = status(index)["state"]
    return {"state": state, "needed_bytes": int(file_bytes(index) * SPACE_MARGIN)}
