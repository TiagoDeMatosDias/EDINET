"""Refresh the ownership tables: read new annual reports, then decide who each row is.

Annual reports come from the filing catalog, one archive at a time: the
selecting query reads the filing index (``filings.db``) and each archive comes
from its year's file. A report whose year's file is missing is skipped with a
warning and read on a later run. Every stored year is read, so
a holder's stake and a board's membership can be followed over time.

The log of reports read is in ``filings.db`` and what they say is in
``market.db``, so a report counts as read only while its rows are still
there: a rebuilt market.db has its reports read again.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from collections.abc import Callable
from typing import Any

from src.filings.storage import ShardReader, ShardUnavailableError
from src.orchestrator.common.sqlite import connect_read

from . import build, store
from .names import person_key
from .parsing import PARSER_VERSION, parse_annual_report

logger = logging.getLogger(__name__)

ANNUAL_FORM_CODE = "030000"
_COMMIT_EVERY = 50

Progress = Callable[[int, int, str], None]


def _no_progress(done: int, total: int, message: str) -> None:
    return None


def pending_reports(conn: sqlite3.Connection, filings_db_path: str, *, reparse: bool = False, latest_only: bool = False) -> list[dict[str, Any]]:
    """Annual reports in the filing catalog not yet read by this parser version, newest first within each company."""
    if not filings_db_path or not os.path.exists(filings_db_path):
        return []
    done: set[str] = set()
    if not reparse:
        done = {
            row[0] for row in conn.execute(
                "SELECT doc_id FROM Ownership_Documents d WHERE parser_version = ? AND (status IN ('empty', 'unavailable') "
                "OR status = 'parsed' AND (EXISTS (SELECT 1 FROM Ownership_Shareholders s WHERE s.doc_id = d.doc_id) "
                "OR EXISTS (SELECT 1 FROM Ownership_Officers o WHERE o.doc_id = d.doc_id)))",
                (PARSER_VERSION,),
            )
        }
    source = connect_read(filings_db_path)
    try:
        rows = source.execute(
            "SELECT doc_id, edinet_code, submitter_name, period_end, submitted_at FROM filings WHERE form_code = ? "
            "ORDER BY edinet_code, period_end DESC, submitted_at DESC",
            (ANNUAL_FORM_CODE,),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        source.close()
    documents = [
        {"doc_id": row[0], "edinet_code": row[1], "filer_name": row[2] or "", "period_end": (row[3] or "")[:10], "submitted_at": row[4] or ""}
        for row in rows if row[1]
    ]
    if latest_only:
        latest: dict[str, dict[str, Any]] = {}
        for document in documents:
            latest.setdefault(document["edinet_code"], document)
        documents = list(latest.values())
    return [document for document in documents if document["doc_id"] not in done]


def _keep_newest_careers(report: Any, order: tuple[str, str], newest: dict[tuple[str, str], tuple[str, str]]) -> None:
    """Drop the career summaries a newer report of the company already carries, and one of a pair within a report.

    ``newest`` maps each person to the report (period end, submission time) that holds their career.
    """
    kept: set[tuple[str, str]] = set()
    for officer in report.officers:
        key = person_key(officer.name, officer.birth_date)
        if newest.get(key, ("", "")) > order or key in kept:
            officer.career = []
        elif officer.career:
            kept.add(key)
            newest[key] = order


def read_reports(conn: sqlite3.Connection, documents: list[dict[str, Any]], filings_db_path: str, progress: Progress = _no_progress) -> dict[str, int]:
    """Read ``documents`` (grouped by company, newest first) and store what each says."""
    counts = {"documents": 0, "shareholders": 0, "officers": 0, "empty": 0, "errors": 0, "skipped": 0}
    if not documents:
        return counts
    newest: dict[tuple[str, str], tuple[str, str]] = {}
    company = None
    with ShardReader(filings_db_path) as source:
        for index, document in enumerate(documents, start=1):
            doc_id, code = document["doc_id"], document["edinet_code"]
            if code != company:
                company, newest = code, store.career_holders(conn, code, person_key)
            try:
                archive = source.archive(doc_id)
            except ShardUnavailableError as exc:
                # The year's file is missing: not recorded, so the report is read once it is back.
                counts["skipped"] += 1
                logger.warning("Shareholders and officers in %s skipped: %s", doc_id, exc)
            else:
                if archive is None:
                    store.record_document(conn, doc_id, document, status="unavailable", parser_version=PARSER_VERSION, error="The catalog holds no archive for this filing")
                else:
                    try:
                        report = parse_annual_report(archive)
                    except Exception as exc:  # noqa: BLE001 - one malformed report must not stop the rest
                        counts["errors"] += 1
                        store.record_document(conn, doc_id, document, status="error", parser_version=PARSER_VERSION, error=f"{type(exc).__name__}: {exc}"[:500])
                        logger.warning("Shareholders and officers in %s could not be read: %s", doc_id, exc)
                    else:
                        found = bool(report.shareholders or report.officers)
                        _keep_newest_careers(report, (document["period_end"], document["submitted_at"]), newest)
                        store.replace_report(conn, doc_id, document, report)
                        store.record_document(conn, doc_id, document, status="parsed" if found else "empty", report=report, parser_version=PARSER_VERSION)
                        counts["documents"] += 1
                        counts["empty"] += not found
                        counts["shareholders"] += len(report.shareholders)
                        counts["officers"] += len(report.officers)
            following = documents[index]["edinet_code"] if index < len(documents) else None
            if following != code:
                # A report newer than those stored before takes over the careers it repeats.
                store.prune_careers(conn, code, person_key)
            if index % _COMMIT_EVERY == 0:
                conn.commit()
            progress(index, len(documents), f"Read shareholders and officers in {doc_id}")
    conn.commit()
    return counts


def rebuild(conn: sqlite3.Connection) -> dict[str, Any]:
    detail = build.build_entities(conn)
    store.mark_update(conn, "entities", detail)
    conn.commit()
    return detail


def update_ownership(
    *,
    market_db: str,
    filings_db_path: str,
    reparse: bool = False,
    latest_only: bool = False,
    max_documents: int = 0,
    progress: Progress = _no_progress,
) -> dict[str, Any]:
    store.ensure_ownership_tables(market_db, filings_db_path)
    conn = store.connect(market_db, filings_db_path)
    result: dict[str, Any] = {}
    try:
        documents = pending_reports(conn, filings_db_path, reparse=reparse, latest_only=latest_only)
        if max_documents > 0:
            documents = documents[:max_documents]
        result["reports"] = read_reports(conn, documents, filings_db_path, progress)
        store.mark_update(conn, "reports", result["reports"])
        progress(1, 1, "Identifying shareholders and officers")
        result["entities"] = rebuild(conn)
    finally:
        conn.close()
    return result
