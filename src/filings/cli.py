"""``main.py filings ...``: split, check, reindex, compact, move or close the yearly filing files.

Run these with the server stopped when they change files (split, move-shard),
or let the Admin page queue the same work as worker jobs.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .storage import FilingStorage, FilingStorageError


def add_parser(commands: Any) -> None:
    filings = commands.add_parser("filings", help="Split, check or repair filing storage; see `filings --help`.")
    actions = filings.add_subparsers(dest="filings_command")
    split = actions.add_parser(
        "split",
        help="Split the old single filings.db into the index and one file per year (server stopped), then switch over.",
    )
    split.add_argument("--dry-run", action="store_true", help="Show what would be created and the space needed.")
    split.add_argument("--target-folder", help="An absolute folder for the yearly files, for this run only.")
    split.add_argument("--no-switch", action="store_true", help="Copy only; the next server start switches over.")
    check = actions.add_parser("check", help="Compare the index with every yearly file.")
    check.add_argument("--repair", action="store_true", help="Put the index right where it differs.")
    check.add_argument("--full", action="store_true", help="Run PRAGMA integrity_check instead of quick_check.")
    check.add_argument("--shard", help="Check one year only.")
    actions.add_parser("reindex", help="Rebuild the index's filing rows from the yearly files.")
    compact = actions.add_parser("compact", help="VACUUM one yearly file.")
    compact.add_argument("shard")
    move = actions.add_parser("move-shard", help="Point the index at a yearly file you moved (server stopped).")
    move.add_argument("shard")
    move.add_argument("path")
    actions.add_parser("close-years", help="Mark past years closed, compacting each once.")
    actions.add_parser("status", help="Show the layout and the yearly files.")


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False, default=str))


def _gb(value: Any) -> str:
    return f"{int(value or 0) / 1e9:.1f} GB"


def _progress(done: int, total: int, message: str) -> None:
    print(f"\r{done:>7}/{total:<7} {message[:60]:<60}", end="", file=sys.stderr, flush=True)


def _split(index: str, args: argparse.Namespace) -> int:
    from . import split as splitting

    if args.dry_run:
        plan = splitting.preflight(index, args.target_folder)
        if not plan["needed"]:
            print(f"Nothing to split: {index} uses the {plan['layout']} layout.")
            return 0
        print(f"Old file: {plan['index']} ({_gb(plan['source_bytes'])}, {plan['filings']} filings)")
        print(f"Yearly files go to: {plan['target_folder']}")
        print(
            f"Free space: {_gb(plan['free_bytes'])}; needed: about {_gb(plan['needed_bytes'])}"
            + ("" if plan["enough_space"] else "  -- NOT ENOUGH")
        )
        if plan["undated_count"]:
            print(f"Filings without a submission date (the split refuses them): {', '.join(plan['undated'])}")
        for year in plan["years"]:
            print(f"  {year['shard']}: {year['filings']:>7} filings, about {_gb(year['estimated_bytes'])} -> {year['path']}")
        print(f"Files created: {len(plan['files'])}")
        return 0
    result = splitting.split(index, target_folder=args.target_folder, progress=_progress, switch=not args.no_switch)
    print(file=sys.stderr)
    for year in result["years"]:
        print(f"  {year['shard']}: {year['filings']} filings, {_gb(year['file_bytes'])}, {year['seconds']} s")
    print(f"Split finished in {result['seconds']} s.")
    switch = result.get("switch")
    if switch is None:
        print("Start the server to switch over to the new files.")
    elif switch["status"] == "switched":
        print(f"Switched over; the old file is kept as {switch['retired']} until you delete it.")
    else:
        print(f"Not switched over ({switch.get('error') or switch['status']}); the next server start tries again.")
    return 0


def run(args: argparse.Namespace) -> int:
    from src.orchestrator.common.db_config import get_filings_db

    from . import maintenance

    index = get_filings_db()
    storage = FilingStorage(index)
    command = args.filings_command or "status"
    try:
        if command == "split":
            return _split(index, args)
        if command == "status":
            from .storage_api import storage_overview

            overview = storage_overview(index)
            overview.pop("preflight", None)
            _print(overview)
            return 0
        if command == "check":
            result = maintenance.check(storage, repair=args.repair, full=args.full, shard=args.shard, progress=_progress)
            print(file=sys.stderr)
            _print(result)
            return 0 if result["ok"] or args.repair else 1
        if command == "reindex":
            result = maintenance.reindex(storage, progress=_progress)
            print(file=sys.stderr)
            _print(result)
            return 0
        if command == "compact":
            _print(maintenance.compact(storage, args.shard))
            return 0
        if command == "move-shard":
            _print(maintenance.move_shard(storage, args.shard, args.path))
            return 0
        if command == "close-years":
            _print(maintenance.close_past_years(storage, progress=_progress))
            return 0
    except FilingStorageError as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        return 1
    print(f"Unknown filings command {command}", file=sys.stderr)
    return 2
