"""Looking after the yearly filing files: check, repair, reindex, compact, move, close.

All of these run in a worker (or from ``main.py filings``), never in the server.
The index is a cache of what the yearly files hold (see ``storage``), so a check
compares the two and a repair brings the index back in line; nothing here deletes
a filing.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import connect_read, connect_write

from .split import file_bytes, free_bytes, integrity
from .storage import (
    LAYOUT_SPLIT,
    FilingStorage,
    FilingStorageError,
    LegacyLayoutError,
    index_rows,
    now,
    shard_info,
    upsert_index_rows,
    validate_shard_id,
)

logger = logging.getLogger(__name__)

Progress = Callable[[int, int, str], None]

# Columns whose difference between a yearly file and the index means the index is stale.
_COMPARED = ("status", "parse_error", "archive_sha256", "archive_size", "submitted_at", "form_code", "edinet_code", "updated_at")
_EXAMPLES = 20


def _no_progress(done: int, total: int, message: str) -> None:
    return None


def _require_split(storage: FilingStorage) -> None:
    if storage.layout() != LAYOUT_SPLIT:
        raise LegacyLayoutError()


def _unregistered_files(storage: FilingStorage, registered: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Yearly files in the filings folder the index does not list, with the shard each says it holds."""
    known = {Path(row["file"]).resolve(strict=False) for row in registered.values()}
    found: list[dict[str, Any]] = []
    if not storage.shards_dir.is_dir():
        return found
    for path in sorted(storage.shards_dir.glob("filings_*.db")):
        if path.resolve(strict=False) in known:
            continue
        try:
            conn = connect_read(path)
        except (OSError, sqlite3.Error):
            continue
        try:
            shard = shard_info(conn).get("shard")
        finally:
            conn.close()
        found.append({"file": str(path), "shard": shard})
    return found


def check(
    storage: FilingStorage,
    *,
    repair: bool = False,
    full: bool = False,
    integrity_check: bool = True,
    shard: str | None = None,
    progress: Progress = _no_progress,
) -> dict[str, Any]:
    """Compare the index with every yearly file (or one), and with ``repair`` put the index right.

    Reports missing files, filings the index lists that their file lacks, filings a
    file holds that the index does not list, stale index rows and counter drift. A
    repair adds and refreshes index rows from the files, marks filings missing from
    their file ``status = 'missing'`` (so a backfill fetches them again), registers
    yearly files the index forgot, and recounts. ``PRAGMA quick_check`` runs on each
    file (``integrity_check`` with ``full``); the result is recorded in ``filing_shards``.
    """
    _require_split(storage)
    registered = {row["shard"]: row for row in storage.shards()}
    unregistered = _unregistered_files(storage, registered) if shard is None else []
    if repair:
        for item in unregistered:
            if item["shard"] and item["shard"] not in registered:
                storage.register_shard(validate_shard_id(item["shard"]), Path(item["file"]))
                item["registered"] = True
        registered = {row["shard"]: row for row in storage.shards()}
    names = [shard] if shard else sorted(registered)
    results: list[dict[str, Any]] = []
    for position, name in enumerate(names, start=1):
        if name not in registered:
            raise FilingStorageError(f"No yearly file {name} is registered")
        progress(position - 1, len(names), f"Checking {name}")
        results.append(_check_shard(storage, registered[name], repair=repair, full=full, integrity_check=integrity_check))
    progress(len(names), len(names), "Checked")
    for item in unregistered:
        # A copy of a year the index already has elsewhere (a rebuilt or moved file) is
        # only worth mentioning; a year nobody lists is a problem.
        item["copy_of_registered"] = bool(item["shard"]) and item["shard"] in registered and not item.get("registered")
    ok = all(item["copy_of_registered"] for item in unregistered) and all(item["ok"] for item in results)
    return {"ok": ok, "repaired": repair, "shards": results, "unregistered": unregistered}


def _check_shard(
    storage: FilingStorage,
    row: dict[str, Any],
    *,
    repair: bool,
    full: bool,
    integrity_check: bool,
) -> dict[str, Any]:
    name = str(row["shard"])
    path = Path(row["file"])
    index = storage.connect_index()
    try:
        listed = {
            str(item["doc_id"]): dict(item)
            for item in index.execute(
                f"SELECT doc_id, {', '.join(_COMPARED)}, fact_count, issue_count FROM filings WHERE shard = ?",
                (name,),
            )
        }
    finally:
        index.close()
    result: dict[str, Any] = {"shard": name, "file": str(path), "indexed": len(listed)}
    if not path.is_file():
        result.update(ok=False, state="missing", problems=[f"{path.name} is missing"])
        if repair and row["state"] != "missing":
            _update_shard_row(storage, name, state="missing")
        return result
    conn = connect_read(path)
    try:
        info = shard_info(conn)
        held = {
            str(item["doc_id"]): dict(item)
            for item in conn.execute(f"SELECT doc_id, {', '.join(_COMPARED)} FROM filings")
        }
        facts = dict(
            conn.execute(
                "SELECT doc_id, COUNT(*) FROM xbrl_facts WHERE numeric_value IS NOT NULL AND is_nil = 0 GROUP BY doc_id"
            ).fetchall()
        )
        issues = dict(conn.execute("SELECT doc_id, COUNT(*) FROM quality_issues GROUP BY doc_id").fetchall())
        check_result = integrity(conn, full=full) if integrity_check else None
        only_in_file = sorted(set(held) - set(listed))
        missing = sorted(doc_id for doc_id in set(listed) - set(held) if listed[doc_id]["status"] != "missing")
        stale = sorted(
            doc_id
            for doc_id in set(held) & set(listed)
            if any(held[doc_id][column] != listed[doc_id][column] for column in _COMPARED)
            or int(facts.get(doc_id, 0)) != int(listed[doc_id]["fact_count"] or 0)
            or int(issues.get(doc_id, 0)) != int(listed[doc_id]["issue_count"] or 0)
        )
        archive_bytes = sum(int(item["archive_size"] or 0) for item in listed.values())
        problems: list[str] = []
        if info.get("shard") != name:
            problems.append(f"{path.name} says it holds {info.get('shard') or 'nothing'}, not {name}")
        if missing:
            problems.append(f"{len(missing)} filing(s) listed in the index are missing from {path.name}")
        if only_in_file:
            problems.append(f"{len(only_in_file)} filing(s) in {path.name} are not in the index")
        if stale:
            problems.append(f"{len(stale)} index row(s) are out of date")
        if int(row["filing_count"] or 0) != len(listed) or int(row["archive_bytes"] or 0) != archive_bytes:
            problems.append("the year's counters have drifted")
        if check_result not in (None, "ok"):
            problems.append(f"integrity: {check_result}")
        rows = index_rows(conn, only_in_file + stale, name) if repair and (only_in_file or stale) else []
    finally:
        conn.close()
    result.update(
        ok=not problems,
        state=row["state"],
        filings=len(held),
        missing=missing[:_EXAMPLES],
        missing_count=len(missing),
        unlisted=only_in_file[:_EXAMPLES],
        unlisted_count=len(only_in_file),
        stale_count=len(stale),
        integrity=check_result,
        problems=problems,
    )
    if repair:
        _repair(storage, name, rows, missing)
        result["state"] = "active" if row["state"] == "missing" else row["state"]
    stamp = now()
    summary = "ok" if not problems else "; ".join(problems)[:1000]
    if repair and problems:
        summary = f"repaired: {summary}"[:1000]
    _update_shard_row(
        storage,
        name,
        state=result["state"],
        file_bytes=file_bytes(path),
        checked_at=stamp if integrity_check else None,
        check_result=summary if integrity_check else None,
    )
    return result


def _repair(storage: FilingStorage, shard: str, rows: list[dict[str, Any]], missing: list[str]) -> None:
    conn = storage.connect_index(write=True)
    try:
        conn.execute("BEGIN IMMEDIATE")
        upsert_index_rows(conn, rows)
        stamp = now()
        for start in range(0, len(missing), 500):
            chunk = missing[start:start + 500]
            conn.execute(
                f"UPDATE filings SET status = 'missing', updated_at = ? WHERE doc_id IN ({','.join('?' * len(chunk))})",
                (stamp, *chunk),
            )
        storage.refresh_counters(conn, shard)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def _update_shard_row(storage: FilingStorage, shard: str, **values: Any) -> None:
    values = {key: value for key, value in values.items() if value is not None}
    if not values:
        return
    conn = storage.connect_index(write=True)
    try:
        assignments = ", ".join(f"{key} = ?" for key in values)
        conn.execute(
            f"UPDATE filing_shards SET {assignments}, updated_at = ? WHERE shard = ?",
            (*values.values(), now(), shard),
        )
        conn.commit()
    finally:
        conn.close()


def reindex(storage: FilingStorage, *, progress: Progress = _no_progress) -> dict[str, Any]:
    """Rebuild the index's ``filings`` and ``filing_shards`` rows from the yearly files (reads no archive)."""
    return check(storage, repair=True, integrity_check=False, progress=progress)


def compact(storage: FilingStorage, shard: str) -> dict[str, Any]:
    """``VACUUM`` one yearly file; needs free space about the size of that file."""
    _require_split(storage)
    path = storage.shard_path(validate_shard_id(shard))
    if not path.is_file():
        raise FilingStorageError(f"{path.name} is missing")
    before = file_bytes(path)
    free = free_bytes(path.parent)
    if free < before:
        raise FilingStorageError(
            f"Compacting {path.name} needs about {before / 1e9:.1f} GB free; {free / 1e9:.1f} GB is free"
        )
    conn = connect_write(path)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("VACUUM")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    after = file_bytes(path)
    _update_shard_row(storage, shard, file_bytes=after)
    return {"shard": shard, "before_bytes": before, "after_bytes": after}


def move_shard(storage: FilingStorage, shard: str, new_path: str | Path) -> dict[str, Any]:
    """Point the index at a yearly file the operator moved (server stopped); checks the file says it is ``shard``."""
    _require_split(storage)
    validate_shard_id(shard)
    path = Path(new_path).expanduser()
    if not path.is_file():
        raise FilingStorageError(f"{path} does not exist")
    conn = connect_read(path)
    try:
        held = shard_info(conn).get("shard")
    finally:
        conn.close()
    if held != shard:
        raise FilingStorageError(f"{path.name} holds {held or 'no year'}, not {shard}")
    index = storage.connect_index()
    try:
        known = index.execute("SELECT 1 FROM filing_shards WHERE shard = ?", (shard,)).fetchone()
    finally:
        index.close()
    if known is None:
        storage.register_shard(shard, path)
    stored = storage.stored_shard_path(path)
    conn = storage.connect_index(write=True)
    try:
        conn.execute(
            "UPDATE filing_shards SET path = ?, state = CASE WHEN state = 'missing' THEN 'active' ELSE state END, "
            "file_bytes = ?, updated_at = ? WHERE shard = ?",
            (stored, file_bytes(path), now(), shard),
        )
        conn.commit()
    finally:
        conn.close()
    return {"shard": shard, "path": stored}


def close_past_years(storage: FilingStorage, *, today: date | None = None, progress: Progress = _no_progress) -> dict[str, Any]:
    """Mark past years ``closed`` (from February of the following year), compacting each once.

    A closed year still takes late writes (a re-parse or a backfill), which make it
    ``active`` again. A backup then needs each closed year only once.
    """
    _require_split(storage)
    today = today or date.today()
    last_closable = today.year - 1 if today.month >= 2 else today.year - 2
    candidates = [
        row["shard"]
        for row in storage.shards()
        if row["state"] == "active" and row["exists"] and str(row["shard"]).isdigit() and int(row["shard"]) <= last_closable
    ]
    closed: list[dict[str, Any]] = []
    for position, shard in enumerate(candidates, start=1):
        progress(position - 1, len(candidates), f"Closing {shard}")
        detail = compact(storage, shard)
        _update_shard_row(storage, shard, state="closed")
        closed.append(detail)
    progress(len(candidates), len(candidates), "Closed")
    return {"closed": closed}


def checkpoint_all(storage: FilingStorage, mode: str = "PASSIVE") -> list[tuple[str, Any]]:
    """Checkpoint the index and every yearly file that exists; returns ``(file, result)`` pairs."""
    mode = mode.upper()
    if mode not in ("PASSIVE", "FULL", "RESTART", "TRUNCATE"):
        raise ValueError(f"Unknown checkpoint mode {mode}")
    paths = [storage.index_path] if storage.index_path.exists() else []
    if storage.layout() == LAYOUT_SPLIT:
        paths.extend(Path(row["file"]) for row in storage.shards() if row["exists"])
    results: list[tuple[str, Any]] = []
    for path in paths:
        conn = connect_write(path)
        try:
            results.append((str(path), tuple(conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone())))
        finally:
            conn.close()
    return results
