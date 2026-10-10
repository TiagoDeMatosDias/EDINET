"""English names for the companies EDINET's code list names only in Japanese.

``CompanyInfo.Company_Name`` comes from the code list's alphabetic name, which
EDINET leaves blank for most filers, large listed companies among them. Every
annual report states the filer's name in English in its cover data
(``jpdei_cor:FilerNameInEnglishDEI``). This reads that name from each such
company's latest stored annual report and fills the blank, so Company Analysis
and everything else that names a company from ``CompanyInfo`` shows it in
English. A name the code list supplies is never replaced.

``Company_English_Names`` keeps what was read and from which filing: importing
the code list replaces ``CompanyInfo``, and the names are put back from here
without opening the filings again.
"""

from __future__ import annotations

import io
import logging
import os
import re
import sqlite3
import unicodedata
import zipfile
from collections.abc import Callable
from datetime import datetime, timezone
from html import unescape
from typing import Any

from src.orchestrator.common.sqlite import connect_read, connect_write

logger = logging.getLogger(__name__)

ANNUAL_FORM_CODE = "030000"
DDL = """
CREATE TABLE IF NOT EXISTS Company_English_Names (
    edinet_code TEXT PRIMARY KEY,
    name_en TEXT NOT NULL,
    doc_id TEXT,
    updated_at TEXT NOT NULL
)
"""
_ENGLISH_FILER_NAME = re.compile(r'<ix:nonNumeric\b[^>]*\bname="jpdei_cor:FilerNameInEnglishDEI"[^>]*>(.*?)</ix:nonNumeric>', re.IGNORECASE | re.DOTALL)
_DASHES = set("-‐‑‒–—―ー−")

Progress = Callable[[int, int, str], None]


def filer_english_name(archive: bytes) -> str:
    """The filer's name in English as the filing itself states it; "" when it states none."""
    with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
        pages = [name for name in bundle.namelist() if name.lower().endswith((".htm", ".html")) and "publicdoc" in name.lower()]
        # The cover data is in the header page, which sorts first; the other pages are read only if it is missing.
        for name in sorted(pages, key=lambda name: ("header" not in name.lower(), name)):
            match = _ENGLISH_FILER_NAME.search(bundle.read(name).decode("utf-8", "replace"))
            if match:
                text = unicodedata.normalize("NFKC", unescape(re.sub(r"<[^>]+>", " ", match.group(1))))
                value = re.sub(r"\s+", " ", text).strip()
                return "" if value and all(char in _DASHES for char in value) else value
    return ""


def _latest_annual_reports(filings_db_path: str) -> dict[str, str]:
    """Each company's latest annual report in the filing catalog; only columns stored before the archive are read."""
    source = connect_read(filings_db_path)
    try:
        rows = source.execute(
            "SELECT doc_id, edinet_code FROM filings WHERE form_code = ? ORDER BY edinet_code, period_end, submitted_at",
            (ANNUAL_FORM_CODE,),
        ).fetchall()
    finally:
        source.close()
    return {str(row[1]): str(row[0]) for row in rows if row[1]}


def read_names(conn: sqlite3.Connection, filings_db_path: str, progress: Progress | None = None) -> dict[str, int]:
    """Read the English name from the latest annual report of each company the code list leaves unnamed.

    A report is opened once: again only when the company has filed a newer one.
    """
    counts = {"read": 0, "named": 0}
    conn.execute(DDL)
    if not filings_db_path or not os.path.exists(filings_db_path):
        return counts
    try:
        listed = {str(row[0]): str(row[1] or "").strip() for row in conn.execute("SELECT Company_Code, Company_Name FROM CompanyInfo") if row[0]}
        latest = _latest_annual_reports(filings_db_path)
    except sqlite3.Error as exc:
        logger.warning("English company names could not be read: %s", exc)
        return counts
    stored = {str(row[0]): (str(row[1]), row[2]) for row in conn.execute("SELECT edinet_code, name_en, doc_id FROM Company_English_Names")}
    # Unnamed by the code list: blank now, or still carrying the name an earlier report gave.
    pending = sorted(
        code for code, name in listed.items()
        if code in latest and latest[code] != stored.get(code, ("", None))[1] and (not name or name == stored.get(code, ("", None))[0])
    )
    if not pending:
        return counts
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    source = connect_read(filings_db_path)
    try:
        for index, code in enumerate(pending, start=1):
            row = source.execute("SELECT archive_content FROM filings WHERE doc_id = ?", (latest[code],)).fetchone()
            if row and row[0]:
                try:
                    name = filer_english_name(bytes(row[0]))
                except Exception as exc:  # noqa: BLE001 - one unreadable archive must not stop the rest
                    logger.debug("No English name read from %s: %s", latest[code], exc)
                    name = ""
                previous = stored.get(code, ("", None))[0]
                if previous and previous != name:
                    # The company renamed itself: the name filled from its earlier report follows.
                    conn.execute("UPDATE CompanyInfo SET Company_Name = ? WHERE Company_Code = ? AND Company_Name = ?", (name, code, previous))
                conn.execute(
                    "INSERT INTO Company_English_Names (edinet_code, name_en, doc_id, updated_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(edinet_code) DO UPDATE SET name_en = excluded.name_en, doc_id = excluded.doc_id, updated_at = excluded.updated_at",
                    (code, name, latest[code], stamp),
                )
                counts["read"] += 1
                counts["named"] += bool(name)
            if progress is not None:
                progress(index, len(pending), f"Read the English name of {code}")
    finally:
        source.close()
    return counts


def apply_names(conn: sqlite3.Connection) -> int:
    """Fill ``CompanyInfo.Company_Name`` where the code list left it blank; returns how many companies were named."""
    conn.execute(DDL)
    try:
        cursor = conn.execute(
            "UPDATE CompanyInfo SET Company_Name = (SELECT name_en FROM Company_English_Names WHERE edinet_code = CompanyInfo.Company_Code) "
            "WHERE TRIM(COALESCE(Company_Name, '')) = '' "
            "AND Company_Code IN (SELECT edinet_code FROM Company_English_Names WHERE name_en != '')"
        )
    except sqlite3.Error as exc:
        logger.warning("English company names could not be filled in: %s", exc)
        return 0
    return int(cursor.rowcount or 0)


def fill(conn: sqlite3.Connection, filings_db_path: str | None, progress: Progress | None = None) -> dict[str, Any]:
    """Read the names not read yet and fill them into ``CompanyInfo``, on an open write connection."""
    counts: dict[str, Any] = read_names(conn, filings_db_path or "", progress)
    counts["filled"] = apply_names(conn)
    conn.commit()
    return counts


def fill_english_names(market_db: str, filings_db_path: str | None, progress: Progress | None = None) -> dict[str, Any]:
    conn = connect_write(market_db)
    try:
        return fill(conn, filings_db_path, progress)
    finally:
        conn.close()
