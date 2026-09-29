"""Move irreplaceable databases from ``data/databases`` to the state directory.

Run with the server and pipeline stopped::

    python -m src.orchestrator.common.migrate_state_databases [--dry-run]

Each database's write-ahead log is first folded into the main file, which also
confirms nothing else has it open, and then the file is moved. Databases that
``config/database_paths.json`` names explicitly are left where they are, and a
database is never moved over an existing file.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
from dataclasses import dataclass

from src.orchestrator.common import db_config

_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")


@dataclass(frozen=True)
class PendingMove:
    key: str
    source: str
    target: str


def pending_moves() -> list[PendingMove]:
    """State databases still at their previous location and not yet moved."""
    moves = []
    for spec in db_config.DATABASES:
        legacy = db_config.legacy_database_path(spec)
        if legacy is None or db_config.configured_database_path(spec.key):
            continue
        target = db_config.default_database_path(spec)
        if os.path.exists(legacy) and not os.path.exists(target):
            moves.append(PendingMove(spec.key, legacy, target))
    return moves


def _checkpoint(path: str) -> None:
    """Fold the write-ahead log into the database file; fails if the file is in use.

    Leaving WAL mode is only possible when no other connection has the
    database open, and an exclusive lock covers databases not in WAL mode.
    The application re-enables WAL when it next opens the file.
    """
    conn = sqlite3.connect(path, timeout=0, isolation_level=None)
    try:
        (mode,) = conn.execute("PRAGMA journal_mode = DELETE").fetchone()
        if str(mode).lower() != "delete":
            raise sqlite3.OperationalError("another connection has it open")
        conn.execute("BEGIN EXCLUSIVE")
        conn.execute("ROLLBACK")
    finally:
        conn.close()


def move_database(move: PendingMove) -> None:
    try:
        _checkpoint(move.source)
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            f"{move.source} is in use ({exc}); stop the server and pipeline, then try again."
        ) from exc
    os.makedirs(os.path.dirname(move.target), exist_ok=True)
    shutil.move(move.source, move.target)
    for suffix in _SIDECAR_SUFFIXES:
        if os.path.exists(move.source + suffix):
            shutil.move(move.source + suffix, move.target + suffix)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="List the moves without making them.")
    args = parser.parse_args(argv)

    moves = pending_moves()
    if not moves:
        print("Nothing to move: every state database is already in the state directory.")
        return 0
    for move in moves:
        print(f"{'Would move' if args.dry_run else 'Moving'} {move.key}: {move.source} -> {move.target}")
        if not args.dry_run:
            try:
                move_database(move)
            except RuntimeError as exc:
                print(f"Stopped: {exc}", file=sys.stderr)
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
