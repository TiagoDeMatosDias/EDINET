"""Pipeline step for retaining and indexing EDINET type-1 XBRL packages.

Supports two modes:
- **explicit**: download a comma-separated list of document IDs.
- **backfill**: query Base.db for all documents with ``xbrlFlag = \"1\"``
  that do not yet have a type-1 archive, and download them in bounded batches.
"""

from __future__ import annotations

import logging
import os

from src.orchestrator.common import StepDefinition, StepFieldDefinition

logger = logging.getLogger(__name__)
_MAX_DOCUMENTS_PER_RUN = 100


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
        rows = conn.execute(
            "SELECT docID, edinetCode, submitDateTime, periodStart, periodEnd, "
            "formCode, docDescription, filerName "
            "FROM DocumentList "
            "WHERE xbrlFlag = '1' "
            "  AND Downloaded IN ('True', 'Checked_Unavailable') "
            "  AND legalStatus IN ('1', '2') "
            "ORDER BY submitDateTime DESC "
            "LIMIT ?",
            (max_docs,),
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
        if existing is not None and existing["status"] in ("parsed", "archived"):
            continue
        ids.append(doc_id)

    return ids


def run_download_xbrl(config, overwrite=False, context=None):
    """Download configured document IDs with bounded progress and storage."""
    from src.filings.acquisition import EdinetDownloadClient
    from src.filings.runtime import catalog
    step_cfg = config.get("download_xbrl_config", {})
    mode = str(step_cfg.get("mode", "explicit")).strip().lower()

    if mode == "backfill":
        document_ids = _backfill_ids(step_cfg)
        if not document_ids:
            logger.info("XBRL backfill: no eligible documents found")
            return {"mode": "backfill", "document_count": 0}
    else:
        document_ids = _document_ids(step_cfg)
        if not document_ids:
            raise ValueError("download_xbrl requires one or more document_ids (or set mode=backfill)")

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

    # Build a lookup of metadata from Base.db for these document IDs
    doc_metadata: dict[str, dict] = {}
    try:
        from src.orchestrator.common.db_config import get_db1
        from src.orchestrator.common.sqlite import connect_read
        db1 = get_db1()
        if os.path.exists(db1):
            conn = connect_read(db1)
            placeholders = ",".join("?" for _ in document_ids)
            rows = conn.execute(
                f"SELECT docID, edinetCode, filerName, submitDateTime, periodStart, periodEnd, formCode "
                f"FROM DocumentList WHERE docID IN ({placeholders})",
                document_ids,
            ).fetchall()
            for row in rows:
                doc_metadata[str(row["docID"]).strip()] = {
                    "edinet_code": row["edinetCode"] or "",
                    "submitter_name": row["filerName"] or "",
                    "submitted_at": row["submitDateTime"] or "",
                    "period_start": row["periodStart"] or "",
                    "period_end": row["periodEnd"] or "",
                    "form_code": row["formCode"] or "",
                    "xbrl_flag": "1",
                }
            conn.close()
    except Exception:
        logger.warning("Could not look up metadata from Base.db for %d documents", len(document_ids), exc_info=True)

    for index, doc_id in enumerate(document_ids):
        if context is not None:
            context.report_progress(index, len(document_ids), f"Downloading XBRL filing {doc_id}")

        if not overwrite:
            existing = catalog.get_filing(doc_id)
            if existing is not None and existing["status"] in ("parsed", "archived"):
                skipped += 1
                logger.debug("XBRL for %s already archived, skipping", doc_id)
                continue

        meta = doc_metadata.get(doc_id, {"xbrl_flag": "1"})
        try:
            client.acquire_type1(doc_id, catalog, meta)
            downloaded += 1
        except Exception as exc:
            failed += 1
            logger.warning("XBRL download failed for %s: %s", doc_id, exc)

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
            description="Explicit: provide document IDs. Backfill: auto-discover eligible documents from Base.db.",
            choices=("explicit", "backfill"),
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
            description="Maximum number of documents to process in one backfill run.",
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
