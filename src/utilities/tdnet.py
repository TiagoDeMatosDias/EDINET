"""TDnet (適時開示情報閲覧サービス) split-disclosure event capture.

Japanese stock splits are not EDINET-reportable events; the regulator-mandated
venue is the Tokyo Stock Exchange timely-disclosure service (TDnet), where a
split decision must be announced the same day the board resolves it.  This
module queries TDnet's search endpoint for split/consolidation announcement
titles and records every match as an event row in ``Tdnet_Disclosures``
(Standardized.db), so a scheduled pipeline step never loses a disclosure even
though TDnet only serves a rolling ~30 day window.

TDnet titles do not carry the split ratio or effective date, and pure split
notices have no XBRL attachment, so this module deliberately captures the
event (ticker, company, announcement time, links) and leaves ratio/date
confirmation to the existing provider/price-heuristic flows in
``src/portfolio/split_detection.py``.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

TDNET_BASE_URL = "https://www.release.tdnet.info"
TDNET_SEARCH_URL = f"{TDNET_BASE_URL}/onsf/TDJFSearch"
TDNET_MAX_LOOKBACK_DAYS = 30
_TDNET_REQUEST_TIMEOUT = (10.0, 60.0)
_TDNET_MAX_ATTEMPTS = 3
_TDNET_RETRY_BASE_SECONDS = 2.0

DEFAULT_SPLIT_KEYWORDS = ("株式分割", "株式併合")

_TDNET_DDL = """
CREATE TABLE IF NOT EXISTS Tdnet_Disclosures (
    disclosure_id   TEXT PRIMARY KEY,
    announced_at    TEXT NOT NULL,
    ticker          TEXT,
    company_name    TEXT,
    title           TEXT,
    keyword         TEXT,
    pdf_url         TEXT,
    xbrl_url        TEXT,
    exchange        TEXT,
    is_split_event  INTEGER NOT NULL DEFAULT 1,
    processed_at    TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_tdnet_disclosures_ticker
    ON Tdnet_Disclosures(ticker, announced_at);
CREATE INDEX IF NOT EXISTS idx_tdnet_disclosures_announced
    ON Tdnet_Disclosures(announced_at);
"""


class TdnetFetchError(RuntimeError):
    """Raised when TDnet cannot provide the disclosure listing."""


def ensure_tdnet_tables(db2_path: str | None = None, conn: sqlite3.Connection | None = None) -> None:
    """Create the ``Tdnet_Disclosures`` table and indexes if missing."""
    if conn is not None:
        conn.executescript(_TDNET_DDL)
        return
    if not db2_path:
        raise ValueError("ensure_tdnet_tables requires db2_path or conn")
    own_conn = sqlite3.connect(db2_path)
    try:
        own_conn.executescript(_TDNET_DDL)
        own_conn.commit()
    finally:
        own_conn.close()


def extract_disclosure_id(pdf_url: str | None) -> str | None:
    """Derive a stable disclosure id from the PDF path (``/inbs/1401....pdf``)."""
    if not pdf_url:
        return None
    stem = str(pdf_url).rsplit("/", 1)[-1]
    if stem.endswith(".pdf"):
        stem = stem[: -len(".pdf")]
    return stem or None


def parse_disclosure_rows(html: str) -> list[dict]:
    """Parse one TDnet search-results page into disclosure event dicts."""
    soup = BeautifulSoup(html or "", "html.parser")
    rows: list[dict] = []
    for tr in soup.select("#maintable tr"):
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue
        title_link = cells[3].find("a")
        title = title_link.get_text(" ", strip=True) if title_link else cells[3].get_text(" ", strip=True)
        pdf_href = title_link.get("href") if title_link else None
        xbrl_link = cells[4].find("a") if len(cells) > 4 else None
        rows.append(
            {
                "announced_at": cells[0].get_text(strip=True).replace("/", "-"),
                "ticker": cells[1].get_text(strip=True) or None,
                "company_name": cells[2].get_text(" ", strip=True) or None,
                "title": title,
                "pdf_url": f"{TDNET_BASE_URL}{pdf_href}" if pdf_href else None,
                "xbrl_url": f"{TDNET_BASE_URL}{xbrl_link.get('href')}" if xbrl_link else None,
                "exchange": cells[5].get_text(" ", strip=True) or None,
            }
        )
    return rows


def _fetch_search_page(
    session: requests.Session,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    keyword: str,
) -> str:
    """POST one TDnet keyword search with bounded retries."""
    payload = {"t0": start_yyyymmdd, "t1": end_yyyymmdd, "q": keyword, "m": "0"}
    last_error: Exception | None = None
    for attempt in range(1, _TDNET_MAX_ATTEMPTS + 1):
        try:
            response = session.post(
                TDNET_SEARCH_URL,
                data=payload,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=_TDNET_REQUEST_TIMEOUT,
            )
            if response.status_code != 200:
                raise TdnetFetchError(f"TDnet returned HTTP {response.status_code}")
            return response.text
        except (requests.RequestException, TdnetFetchError) as exc:
            last_error = exc
            if attempt >= _TDNET_MAX_ATTEMPTS:
                break
            delay = _TDNET_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            logger.info(
                "TDnet search failed on attempt %s/%s (%s); retrying in %.1fs",
                attempt, _TDNET_MAX_ATTEMPTS, exc, delay,
            )
            time.sleep(delay)
    raise TdnetFetchError(f"TDnet search failed for {keyword!r}: {last_error}")


def search_split_disclosures(
    start_date: str,
    end_date: str,
    keywords: tuple[str, ...] = DEFAULT_SPLIT_KEYWORDS,
    session: requests.Session | None = None,
) -> list[dict]:
    """Fetch TDnet split-related disclosures between YYYYMMDD dates.

    Returns one dict per unique disclosure (deduplicated across keywords),
    each carrying the ``keyword`` that matched it.
    """
    own_session = session or requests.Session()
    try:
        merged: dict[str, dict] = {}
        for keyword in keywords:
            html = _fetch_search_page(own_session, start_date, end_date, keyword)
            for row in parse_disclosure_rows(html):
                row["keyword"] = keyword
                disclosure_id = extract_disclosure_id(row["pdf_url"])
                if not disclosure_id:
                    logger.warning("TDnet row without PDF link skipped: %s", row["title"])
                    continue
                row["disclosure_id"] = disclosure_id
                merged.setdefault(disclosure_id, row)
        return list(merged.values())
    finally:
        if own_session is not session:
            own_session.close()


def record_disclosures(conn: sqlite3.Connection, rows: list[dict]) -> dict:
    """Upsert disclosure events; existing rows are never downgraded."""
    new_count = 0
    for row in rows:
        existing = conn.execute(
            "SELECT 1 FROM Tdnet_Disclosures WHERE disclosure_id = ?",
            (row["disclosure_id"],),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE Tdnet_Disclosures SET updated_at = datetime('now') "
                "WHERE disclosure_id = ?",
                (row["disclosure_id"],),
            )
            continue
        conn.execute(
            """INSERT INTO Tdnet_Disclosures
               (disclosure_id, announced_at, ticker, company_name, title,
                keyword, pdf_url, xbrl_url, exchange, is_split_event)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
            (
                row["disclosure_id"], row["announced_at"], row["ticker"],
                row["company_name"], row["title"], row.get("keyword"),
                row["pdf_url"], row["xbrl_url"], row["exchange"],
            ),
        )
        new_count += 1
    conn.commit()
    return {"events_seen": len(rows), "events_new": new_count}


def run_tdnet_split_check(
    db2_path: str,
    lookback_days: int = 7,
    keywords: tuple[str, ...] = DEFAULT_SPLIT_KEYWORDS,
) -> dict:
    """Fetch recent TDnet split disclosures and record them as events."""
    lookback_days = max(1, min(int(lookback_days), TDNET_MAX_LOOKBACK_DAYS))
    end_day = date.today()
    start_day = end_day - timedelta(days=lookback_days)
    rows = search_split_disclosures(
        start_day.strftime("%Y%m%d"),
        end_day.strftime("%Y%m%d"),
        keywords=keywords,
    )
    own_conn = sqlite3.connect(db2_path, timeout=60)
    try:
        ensure_tdnet_tables(conn=own_conn)
        counts = record_disclosures(own_conn, rows)
    finally:
        own_conn.close()
    return {
        "window_start": start_day.isoformat(),
        "window_end": end_day.isoformat(),
        "keywords": list(keywords),
        **counts,
    }
