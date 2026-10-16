"""Filing jobs: download one filing, or backfill XBRL packages for many."""

from __future__ import annotations

import logging
from typing import Any

from src.worker.context import JobContext, JobError

logger = logging.getLogger(__name__)


def acquire(context: JobContext) -> dict:
    """Download and index one filing; returns what ``POST /api/filings/acquire`` used to."""
    from src.filings.acquisition import EdinetAcquisitionError, EdinetDownloadClient
    from src.filings.catalog import FilingCatalog
    from src.orchestrator.common.db_config import get_filings_db

    metadata: dict[str, Any] = dict(context.payload)
    doc_id = str(metadata.get("doc_id") or "")
    if not doc_id:
        raise JobError("A filing document id is required.")
    context.report_progress(None, f"Downloading {doc_id}")
    client = None
    try:
        client = EdinetDownloadClient.from_settings()
        fact_count = client.acquire_type1(doc_id, FilingCatalog(get_filings_db()), metadata)
    except (ValueError, EdinetAcquisitionError) as exc:
        raise JobError(str(exc)) from exc
    finally:
        if client is not None:
            client.close()
    return {"doc_id": doc_id, "status": "parsed", "fact_count": fact_count}


def xbrl_backfill(context: JobContext) -> dict:
    """Download XBRL packages for eligible documents; returns the downloaded/skipped/failed/total summary."""
    from src.filings.acquisition import EdinetDownloadClient
    from src.filings.catalog import FilingCatalog
    from src.orchestrator.common.db_config import get_filings_db, get_market_db
    from src.orchestrator.common.sqlite import connect_read

    limit = min(max(int(context.payload.get("limit") or 50), 1), 200)
    doc_type_code = str(context.payload.get("doc_type_code") or "").strip()

    where = (
        "WHERE xbrlFlag = '1' "
        "  AND Downloaded IN ('True', 'Checked_Unavailable') "
        "  AND legalStatus IN ('1', '2')"
    )
    params: list[Any] = []
    if doc_type_code:
        where += " AND docTypeCode = ?"
        params.append(doc_type_code)
    params.append(limit)
    try:
        conn = connect_read(get_market_db())
    except OSError:
        return {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}
    try:
        rows = conn.execute(
            "SELECT docID, edinetCode, submitDateTime, periodStart, periodEnd, "
            "formCode, docDescription, filerName "
            "FROM DocumentList " + where + " ORDER BY submitDateTime DESC LIMIT ?",
            params,
        ).fetchall()
    finally:
        conn.close()

    catalog = FilingCatalog(get_filings_db())
    try:
        client = EdinetDownloadClient.from_settings()
    except ValueError as exc:
        raise JobError(str(exc)) from exc
    downloaded = skipped = failed = 0
    try:
        for index, row in enumerate(rows, start=1):
            context.checkpoint()
            doc_id = str(row["docID"]).strip()
            if doc_id:
                existing = catalog.get_filing(doc_id)
                if existing is not None and existing["status"] in ("parsed", "archived", "error"):
                    skipped += 1
                else:
                    try:
                        client.acquire_type1(
                            doc_id,
                            catalog,
                            {
                                "edinet_code": row["edinetCode"] or "",
                                "submitted_at": row["submitDateTime"] or "",
                                "period_start": row["periodStart"] or "",
                                "period_end": row["periodEnd"] or "",
                                "doc_type_code": row["formCode"] or "",
                            },
                        )
                        downloaded += 1
                    except Exception as exc:  # noqa: BLE001  # continue the independent backfill batch
                        logger.warning("XBRL backfill failed for %s: %s", doc_id, exc)
                        failed += 1
            context.report_progress(100.0 * index / len(rows), f"Processed {index} of {len(rows)} filings")
    finally:
        client.close()
    return {"downloaded": downloaded, "skipped": skipped, "failed": failed, "total": len(rows)}
