#!/usr/bin/env python3
"""Inspect or checkpoint the write-ahead logs of filings.db and the yearly filing files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.filings.maintenance import checkpoint_all  # noqa: E402
from src.filings.storage import LAYOUT_SPLIT, FilingStorage  # noqa: E402
from src.orchestrator.common.db_config import get_filings_db  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Checkpoint and truncate the SQLite WAL of filings.db and of every yearly filing file."
    )
    parser.add_argument(
        "--database",
        default=get_filings_db(),
        help="filings.db path (default: the configured filing index)",
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
    index = Path(args.database).expanduser().resolve(strict=True)
    storage = FilingStorage(index)
    paths = [index]
    if storage.layout() == LAYOUT_SPLIT:
        paths.extend(Path(row["file"]) for row in storage.shards() if row["exists"])
    for path in paths:
        main_size, wal_size, shm_size = _sizes(path)
        print(f"{path.name}: database {main_size:,} bytes, WAL {wal_size:,} bytes, SHM {shm_size:,} bytes")
    if not args.apply:
        print("Dry run; stop application readers and pass --apply to checkpoint the WAL files.")
        return 0

    blocked = 0
    for name, result in checkpoint_all(storage, "TRUNCATE"):
        path = Path(name)
        print(
            f"{path.name}: busy={result[0]}, log_pages={result[1]}, checkpointed_pages={result[2]}, "
            f"WAL after checkpoint {_sizes(path)[1]:,} bytes"
        )
        blocked += bool(result[0])
    if blocked:
        print("A checkpoint was blocked by an active reader; not every WAL was truncated.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
