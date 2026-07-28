#!/usr/bin/env python3
"""Inspect or checkpoint the Filings.db WAL file."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.orchestrator.common.db_config import get_filings_db  # noqa: E402
from src.orchestrator.common.sqlite import connect_write  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Checkpoint and truncate the SQLite WAL for Filings.db."
    )
    parser.add_argument(
        "--database",
        default=os.getenv("EDINET_FILINGS_DB") or get_filings_db(),
        help="Filings.db path (default: configured filings database)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Run PRAGMA wal_checkpoint(TRUNCATE); otherwise inspect only.",
    )
    return parser


def _sizes(path: Path) -> tuple[int, int, int]:
    return tuple(
        path.with_name(path.name + suffix).stat().st_size
        if path.with_name(path.name + suffix).exists()
        else 0
        for suffix in ("", "-wal", "-shm")
    )


def main() -> int:
    args = _parser().parse_args()
    path = Path(args.database).expanduser().resolve(strict=True)
    main_size, wal_size, shm_size = _sizes(path)
    print(f"Database: {main_size:,} bytes")
    print(f"WAL: {wal_size:,} bytes")
    print(f"SHM: {shm_size:,} bytes")
    if not args.apply:
        print("Dry run; stop application readers and pass --apply to checkpoint the WAL.")
        return 0

    conn = connect_write(path)
    try:
        result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        conn.close()
    print(f"Checkpoint result: busy={result[0]}, log_pages={result[1]}, checkpointed_pages={result[2]}")
    _, wal_after, _ = _sizes(path)
    print(f"WAL after checkpoint: {wal_after:,} bytes")
    if result[0]:
        print("Checkpoint was blocked by an active reader; no truncation was completed.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
