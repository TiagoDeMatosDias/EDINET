#!/usr/bin/env python3
"""Drop extracted filing members after confirming archives remain available."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.filings.catalog import FilingCatalog  # noqa: E402
from src.orchestrator.common.db_config import get_filings_db  # noqa: E402
from src.orchestrator.common.sqlite import connect_read, connect_write  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Remove extracted artifact BLOBs while retaining compressed ZIP "
            "archives in Filings.db."
        )
    )
    parser.add_argument(
        "--database",
        default=os.getenv("EDINET_FILINGS_DB") or get_filings_db(),
        help="Filings.db path (default: configured filings database)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the cleanup; without this flag the command is a dry run.",
    )
    parser.add_argument(
        "--no-vacuum",
        action="store_true",
        help="Clear BLOBs without compacting the SQLite file afterward.",
    )
    return parser


def _print_summary(summary: dict[str, int]) -> None:
    print(f"Artifacts: {summary['artifact_count']:,}")
    print(f"Extracted BLOBs: {summary['content_count']:,}")
    print(f"Extracted bytes: {summary['content_bytes']:,}")
    print(f"Safe to clear: {summary['safely_clearable_count']:,}")


def _read_summary(path: Path) -> dict[str, int]:
    conn = connect_read(path)
    try:
        row = conn.execute(
            """SELECT COUNT(*) AS artifact_count,
                      SUM(CASE WHEN content IS NOT NULL THEN 1 ELSE 0 END)
                          AS content_count,
                      COALESCE(SUM(length(content)), 0) AS content_bytes,
                      SUM(CASE WHEN content IS NOT NULL
                                    AND EXISTS (
                                        SELECT 1 FROM filings f
                                         WHERE f.doc_id = artifacts.doc_id
                                           AND f.archive_content IS NOT NULL
                                    ) THEN 1 ELSE 0 END) AS safely_clearable_count
                 FROM artifacts"""
        ).fetchone()
    finally:
        conn.close()
    return {
        key: int(row[key] or 0)
        for key in (
            "artifact_count",
            "content_count",
            "content_bytes",
            "safely_clearable_count",
        )
    }


def _vacuum(path: Path) -> None:
    source_size = path.stat().st_size
    free_bytes = shutil.disk_usage(path.parent).free
    if free_bytes < source_size:
        raise RuntimeError(
            f"VACUUM needs roughly {source_size:,} free bytes; "
            f"only {free_bytes:,} are available"
        )
    conn = connect_write(path)
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()


def main() -> int:
    args = _parser().parse_args()
    path = Path(args.database).expanduser().resolve(strict=True)
    summary = _read_summary(path)
    _print_summary(summary)
    if not args.apply:
        print("Dry run; pass --apply to remove extracted BLOBs.")
        return 0
    if summary["content_count"] != summary["safely_clearable_count"]:
        print("Refusing cleanup because some extracted BLOBs lack a retained archive.")
        return 2
    catalog = FilingCatalog(path)
    cleared = catalog.clear_artifact_content()
    print(f"Cleared {cleared:,} extracted artifact BLOBs.")
    if args.no_vacuum:
        print("SQLite file was not compacted; run without --no-vacuum when space allows.")
        return 0
    _vacuum(path)
    print("SQLite VACUUM completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
