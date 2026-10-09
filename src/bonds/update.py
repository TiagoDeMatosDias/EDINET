"""Refresh ``Bonds.db``: read new filings, update the yield curve, rebuild the bond list.

Issuance supplements are listed in Base.db's ``DocumentList`` (document type
100) and downloaded from EDINET, since the XBRL step does not fetch them; the
ZIP is kept so later parser versions can re-read it. Bond schedules come
from annual reports already in the filing catalog (Filings.db), read one
archive at a time; the selecting query only touches columns stored before
the archive column, which keeps it fast on a large catalog.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, timedelta
from typing import Any

from src.orchestrator.common.sqlite import connect_read, connect_write

from . import build, jsda, market, store
from .parsing import PARSER_VERSION, parse_bond_schedule, parse_issuance

logger = logging.getLogger(__name__)

ISSUANCE_DOC_TYPE = "100"
ANNUAL_FORM_CODE = "030000"
_DOWNLOAD_WORKERS = 4
_COMMIT_EVERY = 25

Progress = Callable[[int, int, str], None]


def _no_progress(done: int, total: int, message: str) -> None:
    return None


def _processed(conn: sqlite3.Connection, kind: str, reparse: bool) -> set[str]:
    if reparse:
        return set()
    rows = conn.execute(
        "SELECT doc_id FROM Bond_Documents WHERE kind = ? AND parser_version = ? AND status IN ('parsed', 'empty', 'unavailable')",
        (kind, PARSER_VERSION),
    )
    return {row[0] for row in rows}


def pending_issuances(conn: sqlite3.Connection, *, reparse: bool = False) -> list[dict[str, Any]]:
    """Bond supplements in ``DocumentList`` that have not been read by this parser version."""
    done = _processed(conn, "issuance", reparse)
    try:
        rows = conn.execute(
            "SELECT docID, edinetCode, filerName, submitDateTime, periodEnd, formCode, docDescription FROM DocumentList "
            "WHERE docTypeCode = ? AND xbrlFlag = '1' AND COALESCE(withdrawalStatus, '0') = '0' "
            "ORDER BY submitDateTime DESC",
            (ISSUANCE_DOC_TYPE,),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [
        {
            "doc_id": str(row[0]).strip(), "edinet_code": row[1] or "", "filer_name": row[2] or "",
            "submitted_at": row[3] or "", "period_end": row[4] or "", "form_code": row[5] or "", "description": row[6] or "",
        }
        for row in rows
        if str(row[0] or "").strip() and str(row[0]).strip() not in done
    ]


def pending_annual_reports(conn: sqlite3.Connection, filings_db_path: str, *, reparse: bool = False) -> list[dict[str, Any]]:
    """Each company's latest annual report in the filing catalog, when not yet read."""
    if not filings_db_path or not os.path.exists(filings_db_path):
        return []
    done = _processed(conn, "annual", reparse)
    source = connect_read(filings_db_path)
    try:
        rows = source.execute(
            "SELECT doc_id, edinet_code, submitter_name, period_end, submitted_at FROM filings WHERE form_code = ? "
            "ORDER BY edinet_code, period_end, submitted_at",
            (ANNUAL_FORM_CODE,),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        source.close()
    latest: dict[str, Any] = {}
    for row in rows:
        if row[1]:
            latest[row[1]] = row
    return [
        {"doc_id": row[0], "edinet_code": row[1], "filer_name": row[2] or "", "period_end": (row[3] or "")[:10], "submitted_at": row[4] or "", "form_code": ANNUAL_FORM_CODE, "description": "有価証券報告書"}
        for row in latest.values()
        if row[0] not in done
    ]


def _stored_archive(conn: sqlite3.Connection, doc_id: str) -> bytes | None:
    row = conn.execute("SELECT archive FROM Bond_Documents WHERE doc_id = ?", (doc_id,)).fetchone()
    return bytes(row[0]) if row and row[0] else None


def _downloads(client: Any, conn: sqlite3.Connection, documents: list[dict[str, Any]]) -> Iterator[tuple[dict[str, Any], bytes | None, Exception | None]]:
    """Archives for ``documents``: stored copies first, then EDINET with a few requests in flight."""
    missing: list[dict[str, Any]] = []
    for document in documents:
        archive = _stored_archive(conn, document["doc_id"])
        if archive:
            yield document, archive, None
        else:
            missing.append(document)
    if not missing:
        return
    if client is None:
        for document in missing:
            yield document, None, RuntimeError("No EDINET API key: set edinet.api_key on the Admin page")
        return
    queue = iter(missing)
    with ThreadPoolExecutor(max_workers=_DOWNLOAD_WORKERS, thread_name_prefix="bond-download") as executor:
        running: dict[Any, dict[str, Any]] = {}

        def submit() -> None:
            document = next(queue, None)
            if document is not None:
                running[executor.submit(client.download_type1, document["doc_id"])] = document

        for _ in range(_DOWNLOAD_WORKERS):
            submit()
        while running:
            finished, _ = wait(running, return_when=FIRST_COMPLETED)
            for future in finished:
                document = running.pop(future)
                try:
                    yield document, future.result(), None
                except Exception as exc:  # noqa: BLE001 - one unavailable filing must not stop the rest
                    yield document, None, exc
                submit()


def read_issuances(conn: sqlite3.Connection, documents: list[dict[str, Any]], client: Any, progress: Progress = _no_progress) -> dict[str, int]:
    from src.filings.acquisition import EdinetAcquisitionError

    counts = {"documents": 0, "bonds": 0, "unavailable": 0, "errors": 0}
    for index, (document, archive, error) in enumerate(_downloads(client, conn, documents), start=1):
        doc_id = document["doc_id"]
        if error is not None or archive is None:
            unavailable = isinstance(error, EdinetAcquisitionError)
            counts["unavailable" if unavailable else "errors"] += 1
            store.record_document(conn, doc_id, "issuance", document, status="unavailable" if unavailable else "error", parser_version=PARSER_VERSION, error=str(error)[:500])
            logger.warning("Bond supplement %s could not be downloaded: %s", doc_id, error)
        else:
            try:
                bonds = parse_issuance(archive)
            except Exception as exc:  # noqa: BLE001 - keep the archive and record why it failed
                counts["errors"] += 1
                store.record_document(conn, doc_id, "issuance", document, status="error", parser_version=PARSER_VERSION, error=f"{type(exc).__name__}: {exc}"[:500], archive=archive)
                logger.warning("Bond supplement %s could not be read: %s", doc_id, exc)
            else:
                store.replace_issuances(conn, doc_id, document["edinet_code"], document["submitted_at"], [bond.as_dict() for bond in bonds])
                store.record_document(conn, doc_id, "issuance", document, status="parsed" if bonds else "empty", bond_count=len(bonds), parser_version=PARSER_VERSION, archive=archive)
                counts["documents"] += 1
                counts["bonds"] += len(bonds)
        if index % _COMMIT_EVERY == 0:
            conn.commit()
        progress(index, len(documents), f"Read bond supplement {doc_id}")
    conn.commit()
    return counts


def read_annual_reports(conn: sqlite3.Connection, documents: list[dict[str, Any]], filings_db_path: str, progress: Progress = _no_progress) -> dict[str, int]:
    counts = {"documents": 0, "with_bonds": 0, "rows": 0, "errors": 0}
    if not documents:
        return counts
    source = connect_read(filings_db_path)
    try:
        for index, document in enumerate(documents, start=1):
            doc_id = document["doc_id"]
            row = source.execute("SELECT archive_content FROM filings WHERE doc_id = ?", (doc_id,)).fetchone()
            archive = bytes(row[0]) if row and row[0] else None
            if archive is None:
                store.record_document(conn, doc_id, "annual", document, status="unavailable", parser_version=PARSER_VERSION, error="The catalog holds no archive for this filing")
            else:
                try:
                    rows = parse_bond_schedule(archive)
                except Exception as exc:  # noqa: BLE001 - one malformed report must not stop the rest
                    counts["errors"] += 1
                    store.record_document(conn, doc_id, "annual", document, status="error", parser_version=PARSER_VERSION, error=f"{type(exc).__name__}: {exc}"[:500])
                    logger.warning("Bond schedule in %s could not be read: %s", doc_id, exc)
                else:
                    store.replace_schedule(conn, doc_id, document["edinet_code"], document["period_end"], [item.as_dict() for item in rows])
                    store.record_document(conn, doc_id, "annual", document, status="parsed" if rows else "empty", bond_count=len(rows), parser_version=PARSER_VERSION)
                    counts["documents"] += 1
                    counts["with_bonds"] += bool(rows)
                    counts["rows"] += len(rows)
            if index % _COMMIT_EVERY == 0:
                conn.commit()
            progress(index, len(documents), f"Read the bond schedule in {doc_id}")
    finally:
        source.close()
    conn.commit()
    return counts


def update_curve(conn: sqlite3.Connection, fetch: Callable[..., list[tuple[str, float, float]]] = market.fetch_jgb_curve) -> dict[str, Any]:
    """Fetch the month's JGB curve, and the full history when the stored curve is missing or stale."""
    latest = conn.execute("SELECT MAX(curve_date) FROM JGB_Yields").fetchone()[0]
    first_of_month = date.today().replace(day=1)
    history = latest is None or latest < (first_of_month - timedelta(days=1)).isoformat()
    points = fetch(history=history)
    days = market.store_jgb_curve(conn, points)
    conn.commit()
    latest = conn.execute("SELECT MAX(curve_date) FROM JGB_Yields").fetchone()[0]
    return {"days": days, "history": history, "latest": latest}


def update_market_prices(conn: sqlite3.Connection, days: int, fetch: Callable[..., tuple[list[dict[str, Any]], dict[str, Any]]] = jsda.fetch_reference_prices) -> dict[str, Any]:
    """Read the newest JSDA reference-price files not yet stored."""
    stored = {row[0] for row in conn.execute("SELECT DISTINCT price_date FROM Bond_Market_Prices")}
    rows, detail = fetch(days=days, skip=stored)
    jsda.store_reference_prices(conn, rows)
    conn.commit()
    latest = conn.execute("SELECT MAX(price_date) FROM Bond_Market_Prices").fetchone()[0]
    return {**detail, "rows": len(rows), "latest": latest}


def rebuild(conn: sqlite3.Connection, *, today: str | None = None) -> dict[str, Any]:
    bonds = build.build_bonds(conn, today=today)
    store.replace_bonds(conn, bonds)
    detail = build.summary(bonds)
    store.mark_update(conn, "bonds", detail)
    conn.commit()
    return detail


def update_bonds(
    *,
    market_db: str,
    filings_db_path: str | None,
    client: Any = None,
    issuances: bool = True,
    annual_reports: bool = True,
    curve: bool = True,
    market_prices: bool = True,
    market_days: int = 5,
    reparse: bool = False,
    max_documents: int = 0,
    progress: Progress = _no_progress,
    today: str | None = None,
) -> dict[str, Any]:
    store.ensure_bond_tables(market_db)
    conn = connect_write(market_db)
    result: dict[str, Any] = {}
    try:
        if issuances:
            documents = pending_issuances(conn, reparse=reparse)
            if max_documents > 0:
                documents = documents[:max_documents]
            result["issuances"] = read_issuances(conn, documents, client, progress)
            store.mark_update(conn, "issuances", result["issuances"])
        if annual_reports:
            documents = pending_annual_reports(conn, filings_db_path or "", reparse=reparse)
            if max_documents > 0:
                documents = documents[:max_documents]
            result["annual_reports"] = read_annual_reports(conn, documents, filings_db_path or "", progress)
            store.mark_update(conn, "annual_reports", result["annual_reports"])
        if curve:
            try:
                result["curve"] = update_curve(conn)
                store.mark_update(conn, "curve", result["curve"])
            except Exception as exc:  # noqa: BLE001 - the bond list is still worth rebuilding without a new curve
                logger.warning("The JGB curve could not be updated: %s", exc)
                result["curve"] = {"error": str(exc)}
        if market_prices and market_days > 0:
            try:
                result["market_prices"] = update_market_prices(conn, market_days)
                store.mark_update(conn, "market_prices", result["market_prices"])
            except Exception as exc:  # noqa: BLE001 - reference prices are optional; terms and spreads at issue still work
                logger.warning("JSDA reference prices could not be read: %s", exc)
                result["market_prices"] = {"error": str(exc)}
        progress(1, 1, "Merging bonds")
        result["bonds"] = rebuild(conn, today=today)
    finally:
        conn.close()
    return result
