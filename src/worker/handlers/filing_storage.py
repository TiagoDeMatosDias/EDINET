"""Filing storage jobs: split the old single file, check, compact a year, delete the retired file."""

from __future__ import annotations

import logging
from typing import Any

from src.worker.context import JobContext, JobError

logger = logging.getLogger(__name__)


def _storage() -> Any:
    from src.filings.storage import FilingStorage
    from src.orchestrator.common.db_config import get_filings_db

    return FilingStorage(get_filings_db())


def _progress(context: JobContext) -> Any:
    def report(done: int, total: int, message: str) -> None:
        context.report_progress(100.0 * done / total if total else None, message, {"done": done, "total": total})

    return report


def split(context: JobContext) -> dict:
    """Copy filings.db into the index and yearly files; the switch-over happens at the next server start."""
    from src.filings.split import SplitError
    from src.filings.split import split as run_split

    storage = _storage()
    try:
        result = run_split(
            storage.index_path,
            target_folder=context.payload.get("target_folder") or None,
            progress=_progress(context),
            checkpoint=context.checkpoint,
        )
    except SplitError as exc:
        raise JobError(str(exc)) from exc
    result["message"] = "Filing storage is split. Restart the server to finish."
    return result


def check(context: JobContext) -> dict:
    """Compare the index with every yearly file; with ``repair`` put the index right."""
    from src.filings.maintenance import check as run_check
    from src.filings.storage import FilingStorageError

    try:
        return run_check(
            _storage(),
            repair=bool(context.payload.get("repair")),
            full=bool(context.payload.get("full")),
            shard=context.payload.get("shard") or None,
            progress=_progress(context),
        )
    except FilingStorageError as exc:
        raise JobError(str(exc)) from exc


def compact(context: JobContext) -> dict:
    """``VACUUM`` one yearly file."""
    from src.filings.maintenance import compact as run_compact
    from src.filings.storage import FilingStorageError

    shard = str(context.payload.get("shard") or "")
    context.report_progress(None, f"Compacting {shard}")
    try:
        return run_compact(_storage(), shard)
    except FilingStorageError as exc:
        raise JobError(str(exc)) from exc


def cleanup(context: JobContext) -> dict:
    """Delete ``filings.db.migrated``, the old single file kept after the switch-over."""
    from src.filings.split import SplitError, delete_retired

    try:
        freed = delete_retired(_storage().index_path)
    except (SplitError, OSError) as exc:
        raise JobError(str(exc)) from exc
    return {"freed_bytes": freed}
