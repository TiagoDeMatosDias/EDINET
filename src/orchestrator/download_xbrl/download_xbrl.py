"""Pipeline step for retaining and indexing EDINET type-1 XBRL packages.

Supports three modes:
- **explicit**: download a comma-separated list of document IDs.
- **backfill**: query Base.db for all documents with ``xbrlFlag = \"1\"``
  that do not yet have a type-1 archive, and download them in bounded batches.
- **all**: query Base.db for every eligible XBRL document that has not yet
  been marked as downloaded, and process the complete queue.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any

from src.orchestrator.common import StepDefinition, StepFieldDefinition

logger = logging.getLogger(__name__)
_MAX_DOCUMENTS_PER_RUN = 100
_MAX_DOWNLOAD_WORKERS = 10
_STATUS_BATCH_SIZE = 100


def _document_ids(step_cfg: dict) -> list[str]:
    raw = step_cfg.get("document_ids", step_cfg.get("docIDs", ""))
    if isinstance(raw, str):
        values = raw.split(",")
    elif isinstance(raw, list):
        values = raw
    else:
        values = []
    return [str(v).strip() for v in values if str(v).strip()][: _MAX_DOCUMENTS_PER_RUN]


def _backfill_ids(step_cfg: dict) -> list[str]:
    """Query Base.db for documents eligible for XBRL download that are
    not yet archived.  Respects the *max_documents* step config field.
    """
    from src.filings.runtime import catalog

    max_docs = int(step_cfg.get("max_documents", _MAX_DOCUMENTS_PER_RUN))
    doc_type_code = str(step_cfg.get("doc_type_code", "120")).strip()
    try:
        from src.orchestrator.common.db_config import get_db1
        from src.orchestrator.common.sqlite import connect_read
    except Exception:
        return []

    db1_path = get_db1()
    if not os.path.exists(db1_path):
        return []

    conn = connect_read(db1_path)
    try:
        where = (
            "WHERE xbrlFlag = '1' "
            "  AND Downloaded IN ('True', 'Checked_Unavailable') "
            "  AND legalStatus IN ('1', '2')"
        )
        params: list = []
        if doc_type_code:
            where += " AND docTypeCode = ?"
            params.append(doc_type_code)
        params.append(max_docs)
        rows = conn.execute(
            "SELECT docID, edinetCode, submitDateTime, periodStart, periodEnd, "
            "formCode, docDescription, filerName "
            "FROM DocumentList "
            + where +
            " ORDER BY submitDateTime DESC "
            "LIMIT ?",
            params,
        ).fetchall()
    finally:
        conn.close()

    # Filter out documents already in the filing catalog
    ids = []
    for row in rows:
        doc_id = str(row["docID"]).strip()
        if not doc_id:
            continue
        existing = catalog.get_filing(doc_id)
        if existing is not None and existing["status"] in ("parsed", "archived", "error"):
            continue
        ids.append(doc_id)

    return ids


def _create_document_list_indexes(
    connection,
    table_name: str,
    *,
    include_xbrl_queue: bool = False,
) -> None:
    """Create the lookup indexes used by XBRL document selection."""
    from src.orchestrator.common.sqlite import quote_identifier

    quoted_table = quote_identifier(table_name)
    columns = {
        str(row[1]).casefold(): str(row[1])
        for row in connection.execute(
            f"PRAGMA table_info({quoted_table})"
        ).fetchall()
    }
    doc_id = columns.get("docid")
    if doc_id:
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS "
            f"{quote_identifier('idx_document_list_docid')} "
            f"ON {quoted_table} ({quote_identifier(doc_id)})"
        )

    queue_columns = (
        "xbrlflag",
        "legalstatus",
        "doctypecode",
        "xbrldownloaded",
        "submitdatetime",
        "docid",
    )
    if include_xbrl_queue and all(column in columns for column in queue_columns):
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS "
            f"{quote_identifier('idx_document_list_xbrl_queue')} "
            f"ON {quoted_table} ("
            f"{quote_identifier(columns['xbrlflag'])}, "
            f"{quote_identifier(columns['legalstatus'])}, "
            f"{quote_identifier(columns['doctypecode'])}, "
            f"{quote_identifier(columns['submitdatetime'])} DESC, "
            f"{quote_identifier(columns['docid'])}) "
            f"WHERE COALESCE({quote_identifier(columns['xbrldownloaded'])}, 'False') <> 'True'"
        )


def _ensure_document_list_indexes() -> None:
    """Ensure the document lookup index exists for non-all XBRL modes."""
    try:
        from src.orchestrator.common.db_config import get_db1
        from src.orchestrator.common.sqlite import connect_write
    except Exception:
        return

    db1_path = get_db1()
    if not os.path.exists(db1_path):
        return

    conn = connect_write(db1_path)
    try:
        table_row = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND lower(name) = lower(?) LIMIT 1",
            ("DocumentList",),
        ).fetchone()
        if table_row is None:
            return
        _create_document_list_indexes(conn, str(table_row[0]))
        conn.commit()
    finally:
        conn.close()


def _ensure_xbrl_status_column():
    """Return a writable Base.db connection with the XBRL status column ready.

    ``DocumentList.Downloaded`` is reserved for the legacy type-5 CSV
    downloader, so XBRL acquisition uses its own marker.  The column is added
    lazily to keep existing databases backwards compatible.
    """
    try:
        from src.orchestrator.common.db_config import get_db1
        from src.orchestrator.common.sqlite import connect_write, quote_identifier
    except Exception:
        return None

    db1_path = get_db1()
    if not os.path.exists(db1_path):
        return None

    conn = connect_write(db1_path)
    try:
        table_row = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND lower(name) = lower(?) LIMIT 1",
            ("DocumentList",),
        ).fetchone()
        if table_row is None:
            conn.close()
            return None

        table_name = str(table_row[0])
        quoted_table = quote_identifier(table_name)
        columns = {
            str(row[1]).casefold()
            for row in conn.execute(f"PRAGMA table_info({quoted_table})").fetchall()
        }
        if "xbrldownloaded" not in columns:
            conn.execute(
                f"ALTER TABLE {quoted_table} "
                'ADD COLUMN "XbrlDownloaded" TEXT NOT NULL DEFAULT \'False\''
            )

        _create_document_list_indexes(
            conn,
            table_name,
            include_xbrl_queue=True,
        )
        conn.commit()

        return conn
    except Exception:
        conn.close()
        raise


def _all_ids(step_cfg: dict | None = None) -> list[str]:
    """Return eligible, unmarked Base.db documents for the configured type."""
    step_cfg = step_cfg or {}
    doc_type_code = str(step_cfg.get("doc_type_code", "120")).strip()
    conn = _ensure_xbrl_status_column()
    if conn is None:
        return []

    try:
        where = (
            'WHERE "xbrlFlag" = \'1\' '
            '  AND "legalStatus" IN (\'1\', \'2\') '
            '  AND COALESCE("XbrlDownloaded", \'False\') <> \'True\''
        )
        params: list[str] = []
        if doc_type_code:
            where += ' AND "docTypeCode" = ?'
            params.append(doc_type_code)
        rows = conn.execute(
            'SELECT "docID" FROM "DocumentList" '
            + where
            + ' ORDER BY "submitDateTime" DESC, "docID"',
            params,
        ).fetchall()
    finally:
        conn.close()

    return [str(row[0]).strip() for row in rows if str(row[0]).strip()]


def _flush_xbrl_statuses(connection, pending: list[tuple[str, str]]) -> None:
    """Persist a batch of all-mode XBRL outcomes in Base.db."""
    if connection is None:
        return
    if not pending:
        return
    connection.executemany(
        'UPDATE "DocumentList" SET "XbrlDownloaded" = ? WHERE "docID" = ?',
        [(status, document_id) for document_id, status in pending],
    )
    connection.commit()
    pending.clear()


def _queue_xbrl_status(
    connection,
    pending: list[tuple[str, str]],
    document_id: str,
    status: str,
) -> None:
    """Queue an all-mode status and commit only at the batch boundary."""
    if connection is None:
        return
    pending.append((document_id, status))
    if len(pending) >= _STATUS_BATCH_SIZE:
        _flush_xbrl_statuses(connection, pending)


def _set_xbrl_status(connection, document_id: str, status: str) -> None:
    """Persist one all-mode XBRL outcome in Base.db."""
    _flush_xbrl_statuses(connection, [(document_id, status)])


def _iter_download_results(
    client,
    document_ids: list[str],
    metadata: dict[str, dict],
) -> Iterator[tuple[str, dict[str, Any], bytes | None, Exception | None]]:
    """Download with at most five in-flight requests and yield results as ready.

    The caller remains responsible for catalog ingestion, so SQLite writes stay
    on the main thread while the worker threads only perform HTTP requests.
    """
    if not document_ids:
        return

    document_iterator = iter(document_ids)
    with ThreadPoolExecutor(
        max_workers=_MAX_DOWNLOAD_WORKERS,
        thread_name_prefix="xbrl-download",
    ) as executor:
        pending = {}

        def submit_next() -> bool:
            try:
                document_id = next(document_iterator)
            except StopIteration:
                return False
            future = executor.submit(client.download_type1, document_id)
            pending[future] = (
                document_id,
                metadata.get(document_id, {"xbrl_flag": "1"}),
            )
            return True

        for _ in range(_MAX_DOWNLOAD_WORKERS):
            if not submit_next():
                break

        while pending:
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                document_id, meta = pending.pop(future)
                try:
                    yield document_id, meta, future.result(), None
                except Exception as exc:
                    yield document_id, meta, None, exc
                submit_next()


def _load_base_metadata(document_ids: list[str]) -> dict[str, dict]:
    """Load Base.db metadata in bounded IN queries to avoid SQLite limits."""
    if not document_ids:
        return {}

    try:
        from src.orchestrator.common.db_config import get_db1
        from src.orchestrator.common.sqlite import connect_read
    except Exception:
        return {}

    db1_path = get_db1()
    if not os.path.exists(db1_path):
        return {}

    metadata: dict[str, dict] = {}
    conn = None
    try:
        conn = connect_read(db1_path)
        for start in range(0, len(document_ids), 500):
            batch = document_ids[start : start + 500]
            placeholders = ",".join("?" for _ in batch)
            rows = conn.execute(
                "SELECT docID, edinetCode, filerName, submitDateTime, periodStart, periodEnd, formCode "
                f"FROM DocumentList WHERE docID IN ({placeholders})",
                batch,
            ).fetchall()
            for row in rows:
                metadata[str(row["docID"]).strip()] = {
                    "edinet_code": row["edinetCode"] or "",
                    "submitter_name": row["filerName"] or "",
                    "submitted_at": row["submitDateTime"] or "",
                    "period_start": row["periodStart"] or "",
                    "period_end": row["periodEnd"] or "",
                    "form_code": row["formCode"] or "",
                    "xbrl_flag": "1",
                }
    except Exception:
        logger.warning(
            "Could not look up metadata from Base.db for %d documents",
            len(document_ids),
            exc_info=True,
        )
    finally:
        if conn is not None:
            conn.close()

    return metadata


def _prepare_download_ids(
    document_ids: list[str],
    catalog,
    *,
    overwrite: bool,
    mode: str,
    status_connection,
    pending_statuses: list[tuple[str, str]],
    context,
) -> tuple[list[str], int, int]:
    """Filter catalogued filings before starting concurrent HTTP work."""
    download_ids: list[str] = []
    skipped = 0
    processed = 0
    total = len(document_ids)
    for doc_id in document_ids:
        if not overwrite:
            existing = catalog.get_filing(doc_id)
            if existing is not None and existing["status"] in ("parsed", "archived", "error"):
                if mode == "all" and existing["status"] in ("parsed", "archived"):
                    _queue_xbrl_status(status_connection, pending_statuses, doc_id, "True")
                skipped += 1
                processed += 1
                if context is not None:
                    context.report_progress(
                        processed,
                        total,
                        f"Skipping existing XBRL filing {doc_id}",
                    )
                logger.debug(
                    "XBRL for %s already in catalog (status=%s), skipping",
                    doc_id,
                    existing["status"],
                )
                continue
        download_ids.append(doc_id)
    return download_ids, skipped, processed


def _process_download_results(
    client,
    download_ids: list[str],
    doc_metadata: dict[str, dict],
    catalog,
    *,
    mode: str,
    status_connection,
    pending_statuses: list[tuple[str, str]],
    context,
    processed: int,
    total: int,
) -> tuple[int, int, int]:
    """Ingest completed downloads on the main thread and report outcomes."""
    from src.filings.acquisition import EdinetAcquisitionError

    downloaded = 0
    failed = 0
    for doc_id, meta, content, download_error in _iter_download_results(
        client,
        download_ids,
        doc_metadata,
    ):
        if download_error is not None:
            failed += 1
            if mode == "all":
                status = (
                    "Checked_Unavailable"
                    if isinstance(download_error, EdinetAcquisitionError)
                    else "Checked_Error"
                )
                _queue_xbrl_status(status_connection, pending_statuses, doc_id, status)
            logger.warning("XBRL download failed for %s: %s", doc_id, download_error)
        else:
            assert content is not None
            try:
                client.ingest_type1(doc_id, content, catalog, meta)
                downloaded += 1
                if mode == "all":
                    _queue_xbrl_status(status_connection, pending_statuses, doc_id, "True")
            except Exception as exc:
                failed += 1
                if mode == "all":
                    status = (
                        "Checked_Unavailable"
                        if isinstance(exc, EdinetAcquisitionError)
                        else "Checked_Error"
                    )
                    _queue_xbrl_status(status_connection, pending_statuses, doc_id, status)
                logger.warning("XBRL download failed for %s: %s", doc_id, exc)

        processed += 1
        if context is not None:
            context.report_progress(
                processed,
                total,
                f"Processed XBRL filing {doc_id}",
            )
    return downloaded, failed, processed


def run_download_xbrl(config, overwrite=False, context=None):
    """Download configured document IDs with bounded progress and storage."""
    from src.filings.acquisition import EdinetDownloadClient
    from src.filings.runtime import catalog
    step_cfg = config.get("download_xbrl_config", {})
    mode = str(step_cfg.get("mode", "explicit")).strip().lower()

    if mode != "all":
        _ensure_document_list_indexes()

    if mode == "backfill":
        document_ids = _backfill_ids(step_cfg)
        if not document_ids:
            logger.info("XBRL backfill: no eligible documents found")
            return {"mode": "backfill", "document_count": 0}
    elif mode == "all":
        document_ids = _all_ids(step_cfg)
        if not document_ids:
            logger.info("XBRL all: no eligible documents found")
            return {"mode": "all", "document_count": 0}
    elif mode == "explicit":
        document_ids = _document_ids(step_cfg)
        if not document_ids:
            raise ValueError(
                "download_xbrl requires one or more document_ids "
                "(or set mode=backfill/all)"
            )
    else:
        raise ValueError(
            f"download_xbrl mode must be explicit, backfill, or all; got {mode!r}"
        )

    provider_token = str(step_cfg.get("provider_token", "")).strip()
    if not provider_token:
        provider_token = str(config.get("API_KEY", ""))
    if not provider_token:
        provider_token = os.getenv("EDINET_API_TOKEN", "")
    if not provider_token:
        logger.warning(
            "download_xbrl: no provider token available. "
            "Set API_KEY in pipeline config, EDINET_API_TOKEN env var, or provider_token in step config."
        )
        return {
            "mode": mode,
            "candidates": len(document_ids),
            "downloaded": 0,
            "skipped": 0,
            "failed": 0,
            "error": "No EDINET provider token configured",
        }
    client = EdinetDownloadClient(provider_token)

    downloaded = 0
    skipped = 0
    failed = 0

    doc_metadata = _load_base_metadata(document_ids)
    status_connection = None
    pending_statuses: list[tuple[str, str]] = []
    try:
        if mode == "all":
            status_connection = _ensure_xbrl_status_column()

        download_ids, skipped, processed = _prepare_download_ids(
            document_ids,
            catalog,
            overwrite=overwrite,
            mode=mode,
            status_connection=status_connection,
            pending_statuses=pending_statuses,
            context=context,
        )
        downloaded, failed, _ = _process_download_results(
            client,
            download_ids,
            doc_metadata,
            catalog,
            mode=mode,
            status_connection=status_connection,
            pending_statuses=pending_statuses,
            context=context,
            processed=processed,
            total=len(document_ids),
        )
    finally:
        _flush_xbrl_statuses(status_connection, pending_statuses)
        if status_connection is not None:
            status_connection.close()
        close_client = getattr(client, "close", None)
        if callable(close_client):
            close_client()

    if context is not None:
        context.report_progress(
            len(document_ids),
            len(document_ids),
            f"XBRL complete: {downloaded} downloaded, {skipped} skipped, {failed} failed",
        )

    logger.info(
        "XBRL acquisition complete: %d downloaded, %d skipped, %d failed of %d candidates",
        downloaded, skipped, failed, len(document_ids),
    )
    return {
        "mode": mode,
        "candidates": len(document_ids),
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
    }


STEP_DEFINITION = StepDefinition(
    name="download_xbrl",
    handler=run_download_xbrl,
    display_name="Download XBRL filings",
    input_fields=(
        StepFieldDefinition(
            "mode",
            "str",
            default="explicit",
            label="Download mode",
            description=(
                "Explicit: provide document IDs. Backfill: bounded auto-discovery from Base.db. "
                "All: process every eligible XBRL document not marked as downloaded."
            ),
            choices=("explicit", "backfill", "all"),
        ),
        StepFieldDefinition(
            "document_ids",
            "str",
            default="",
            label="Document IDs",
            description="Comma-separated EDINET document IDs. Only used in explicit mode.",
        ),
        StepFieldDefinition(
            "max_documents",
            "int",
            default="100",
            label="Max documents per run",
            description="Maximum number of documents in one backfill run; ignored by all mode.",
        ),
        StepFieldDefinition(
            "doc_type_code",
            "str",
            default="120",
            label="Document type filter",
            description="Backfill/all filter: 120=annual, 130=semi-annual, 140=quarterly; leave empty for all types.",
        ),
        StepFieldDefinition(
            "provider_token",
            "str",
            default="",
            label="Provider token (optional)",
            description="Override the API key. Uses pipeline API_KEY or EDINET_API_TOKEN env var when empty.",
        ),
    ),
)
