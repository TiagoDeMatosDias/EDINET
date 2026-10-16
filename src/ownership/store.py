"""Ownership tables: the rebuildable record of who owns and runs each company.

``Ownership_Documents`` logs every annual report the ownership step has read,
with the share statistics it states. It lives in ``filings.db`` beside the
filings it describes; every other table is in ``market.db``, and connections
that need both attach ``filings.db`` as ``filings`` (see ``connect``).
``Ownership_Shareholders`` and ``Ownership_Officers`` hold what each report
says, one row per listed shareholder or officer, as filed. The build then
decides who each row is: it fills ``entity_id`` (and, for a shareholder, the
``account`` its name carries) and marks the rows of each company's latest
report, and writes ``Ownership_Entities``, one row per person or organisation.
Reading the reports is the slow part, so the build can be repeated from these
tables without opening a filing again.

A career summary is kept only with the latest report that names the person
at a company: it changes by a line a year, and one copy is enough.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import attach, connect_write, move_table

FILINGS_SCHEMA = "filings"

# In filings.db.
DOCUMENTS_DDL = """
CREATE TABLE IF NOT EXISTS Ownership_Documents (
    doc_id TEXT PRIMARY KEY,
    edinet_code TEXT NOT NULL,
    filer_name TEXT,
    period_end TEXT,
    submitted_at TEXT,
    status TEXT NOT NULL,
    shareholders_as_of TEXT,
    officers_as_of TEXT,
    shareholder_count INTEGER NOT NULL DEFAULT 0,
    officer_count INTEGER NOT NULL DEFAULT 0,
    officer_source TEXT,
    proposed INTEGER NOT NULL DEFAULT 0,
    top_shares REAL,
    top_ratio REAL,
    men INTEGER,
    women INTEGER,
    statistics TEXT,
    parser_version INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    processed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ownership_documents_company ON Ownership_Documents(edinet_code, period_end);
"""

# In market.db.
DDL = """
CREATE TABLE IF NOT EXISTS Ownership_Shareholders (
    doc_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    edinet_code TEXT NOT NULL,
    period_end TEXT,
    as_of TEXT,
    basis TEXT NOT NULL,
    rank INTEGER NOT NULL,
    name TEXT NOT NULL,
    address TEXT,
    shares REAL,
    ratio REAL,
    entity_id TEXT,
    account TEXT,
    latest INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (doc_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_ownership_shareholders_company ON Ownership_Shareholders(edinet_code, period_end);
CREATE INDEX IF NOT EXISTS idx_ownership_shareholders_entity ON Ownership_Shareholders(entity_id, edinet_code, period_end);

CREATE TABLE IF NOT EXISTS Ownership_Officers (
    doc_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    edinet_code TEXT NOT NULL,
    period_end TEXT,
    as_of TEXT,
    proposed INTEGER NOT NULL DEFAULT 0,
    name TEXT NOT NULL,
    name_en TEXT,
    member TEXT,
    title TEXT,
    birth_date TEXT,
    term TEXT,
    term_start TEXT,
    term_end TEXT,
    shares REAL,
    other_shares REAL,
    outside INTEGER NOT NULL DEFAULT 0,
    source TEXT,
    career TEXT,
    entity_id TEXT,
    latest INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (doc_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_ownership_officers_company ON Ownership_Officers(edinet_code, period_end);
CREATE INDEX IF NOT EXISTS idx_ownership_officers_entity ON Ownership_Officers(entity_id, edinet_code, period_end);

CREATE TABLE IF NOT EXISTS Ownership_Entities (
    entity_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    name_ja TEXT,
    edinet_code TEXT,
    birth_date TEXT,
    address TEXT,
    headline TEXT,
    name_key TEXT,
    holdings INTEGER NOT NULL DEFAULT 0,
    past_holdings INTEGER NOT NULL DEFAULT 0,
    positions INTEGER NOT NULL DEFAULT 0,
    past_positions INTEGER NOT NULL DEFAULT 0,
    search TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_ownership_entities_rank ON Ownership_Entities(kind, holdings DESC, positions DESC);
CREATE INDEX IF NOT EXISTS idx_ownership_entities_filer ON Ownership_Entities(edinet_code);
CREATE INDEX IF NOT EXISTS idx_ownership_entities_name ON Ownership_Entities(name_key);

CREATE TABLE IF NOT EXISTS Ownership_Updates (
    source TEXT PRIMARY KEY,
    updated_at TEXT NOT NULL,
    detail TEXT
);
"""

SHAREHOLDER_COLUMNS = ("basis", "rank", "name", "address", "shares", "ratio")
OFFICER_COLUMNS = (
    "proposed", "name", "name_en", "member", "title", "birth_date", "term", "term_start", "term_end", "shares", "other_shares",
    "outside", "source", "career",
)
ENTITY_COLUMNS = (
    "entity_id", "kind", "name", "name_ja", "edinet_code", "birth_date", "address", "headline", "name_key", "holdings",
    "past_holdings", "positions", "past_positions", "search",
)


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_documents_table(filings_db: str | Path) -> None:
    Path(filings_db).parent.mkdir(parents=True, exist_ok=True)
    conn = connect_write(filings_db)
    try:
        conn.executescript(DOCUMENTS_DDL)
        conn.commit()
    finally:
        conn.close()


def ensure_ownership_tables(market_db: str | Path, filings_db: str | Path) -> None:
    """Create the tables in both databases, moving a document log that earlier versions kept in market.db."""
    ensure_documents_table(filings_db)
    Path(market_db).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(market_db, filings_db)
    try:
        conn.executescript(DDL)
        move_table(conn, "Ownership_Documents", FILINGS_SCHEMA)
        conn.commit()
    finally:
        conn.close()


def connect(market_db: str | Path, filings_db: str | Path) -> sqlite3.Connection:
    """A writable connection to market.db with filings.db attached, so every ownership table can be named."""
    conn = connect_write(market_db)
    try:
        attach(conn, filings_db, FILINGS_SCHEMA)
    except BaseException:
        conn.close()
        raise
    return conn


def career_text(entries: Iterable[tuple[str, str]]) -> str | None:
    """Career entries as stored: one ``date<TAB>text`` line each."""
    return "\n".join(f"{date}\t{' '.join(str(text).split())}" for date, text in entries) or None


def career_entries(text: str | None) -> list[dict[str, str]]:
    entries = []
    for line in (text or "").split("\n"):
        if line:
            date, _, body = line.partition("\t")
            entries.append({"date": date, "text": body})
    return entries


def _statistics(report: Any) -> str | None:
    """The report's share statistics as stored: one JSON object, absent when it states none."""
    values = {
        "shareholders": getattr(report, "shareholder_count", None),
        "issued_shares": getattr(report, "issued_shares", None),
        "treasury_shares": getattr(report, "treasury_shares", None),
        "treasury_ratio": getattr(report, "treasury_ratio", None),
        "distribution": getattr(report, "distribution", None) or None,
    }
    values = {key: value for key, value in values.items() if value is not None}
    return json.dumps(values, ensure_ascii=False) if values else None


def statistics(text: str | None) -> dict[str, Any]:
    try:
        values = json.loads(text or "{}")
    except ValueError:
        return {}
    return values if isinstance(values, dict) else {}


def _value(value: Any) -> Any:
    return int(value) if isinstance(value, bool) else value


def record_document(conn: sqlite3.Connection, doc_id: str, meta: dict[str, Any], *, status: str, report: Any = None,
                    parser_version: int = 0, error: str | None = None) -> None:
    officers = list(getattr(report, "officers", []) or [])
    conn.execute(
        """
        INSERT INTO Ownership_Documents (doc_id, edinet_code, filer_name, period_end, submitted_at, status,
            shareholders_as_of, officers_as_of, shareholder_count, officer_count, officer_source, proposed,
            top_shares, top_ratio, men, women, statistics, parser_version, error, processed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(doc_id) DO UPDATE SET
            edinet_code = excluded.edinet_code, filer_name = excluded.filer_name, period_end = excluded.period_end,
            submitted_at = excluded.submitted_at, status = excluded.status,
            shareholders_as_of = excluded.shareholders_as_of, officers_as_of = excluded.officers_as_of,
            shareholder_count = excluded.shareholder_count, officer_count = excluded.officer_count,
            officer_source = excluded.officer_source, proposed = excluded.proposed, top_shares = excluded.top_shares,
            top_ratio = excluded.top_ratio, men = excluded.men, women = excluded.women, statistics = excluded.statistics,
            parser_version = excluded.parser_version, error = excluded.error, processed_at = excluded.processed_at
        """,
        (
            doc_id, meta.get("edinet_code") or "", meta.get("filer_name"), meta.get("period_end"), meta.get("submitted_at"), status,
            getattr(report, "shareholders_as_of", None), getattr(report, "officers_as_of", None),
            len(getattr(report, "shareholders", []) or []), len(officers),
            officers[0].source if officers else None, int(any(officer.proposed for officer in officers)),
            getattr(report, "top_shares", None), getattr(report, "top_ratio", None),
            getattr(report, "men", None), getattr(report, "women", None), _statistics(report), parser_version, error, now(),
        ),
    )


def replace_report(conn: sqlite3.Connection, doc_id: str, meta: dict[str, Any], report: Any) -> None:
    """Store what one report says, replacing anything read from it before."""
    code, period_end = meta.get("edinet_code") or "", meta.get("period_end")
    conn.execute("DELETE FROM Ownership_Shareholders WHERE doc_id = ?", (doc_id,))
    conn.execute("DELETE FROM Ownership_Officers WHERE doc_id = ?", (doc_id,))
    columns = ("doc_id", "seq", "edinet_code", "period_end", "as_of", *SHAREHOLDER_COLUMNS)
    conn.executemany(
        f"INSERT INTO Ownership_Shareholders ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
        [
            (doc_id, row.seq, code, period_end, report.shareholders_as_of or period_end, *(_value(getattr(row, column)) for column in SHAREHOLDER_COLUMNS))
            for row in report.shareholders
        ],
    )
    columns = ("doc_id", "seq", "edinet_code", "period_end", "as_of", *OFFICER_COLUMNS)
    as_of = report.officers_as_of or str(meta.get("submitted_at") or "")[:10] or None
    conn.executemany(
        f"INSERT INTO Ownership_Officers ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
        [
            (doc_id, row.seq, code, period_end, as_of, *(career_text(row.career) if column == "career" else _value(getattr(row, column)) for column in OFFICER_COLUMNS))
            for row in report.officers
        ],
    )


def career_holders(conn: sqlite3.Connection, edinet_code: str, person_key: Any) -> dict[Any, tuple[str, str]]:
    """For each person at a company, the report (period end, submission time) that holds their career summary."""
    rows = conn.execute(
        "SELECT o.name, o.birth_date, o.period_end, d.submitted_at FROM Ownership_Officers o "
        "LEFT JOIN Ownership_Documents d ON d.doc_id = o.doc_id WHERE o.edinet_code = ? AND o.career IS NOT NULL",
        (edinet_code,),
    ).fetchall()
    holders: dict[Any, tuple[str, str]] = {}
    for name, birth_date, period_end, submitted_at in rows:
        key = person_key(name, birth_date)
        order = (period_end or "", submitted_at or "")
        if order > holders.get(key, ("", "")):
            holders[key] = order
    return holders


def prune_careers(conn: sqlite3.Connection, edinet_code: str, person_key: Any) -> int:
    """Keep each person's career summary at a company only with the latest report that carries it.

    ``person_key`` maps a filed name and date of birth to who that is, so the
    same person is recognised however a year's report spaces the name.
    """
    rows = conn.execute(
        "SELECT o.doc_id, o.seq, o.name, o.birth_date, o.period_end, d.submitted_at, o.proposed FROM Ownership_Officers o "
        "LEFT JOIN Ownership_Documents d ON d.doc_id = o.doc_id WHERE o.edinet_code = ? AND o.career IS NOT NULL",
        (edinet_code,),
    ).fetchall()
    newest: dict[Any, tuple[Any, ...]] = {}
    for row in rows:
        key = person_key(row[2], row[3])
        order = (row[4] or "", row[5] or "", row[6], row[0], row[1])
        if key not in newest or order > newest[key]:
            newest[key] = order
    stale = [(row[0], row[1]) for row in rows if (row[4] or "", row[5] or "", row[6], row[0], row[1]) != newest[person_key(row[2], row[3])]]
    conn.executemany("UPDATE Ownership_Officers SET career = NULL WHERE doc_id = ? AND seq = ?", stale)
    return len(stale)


def mark_update(conn: sqlite3.Connection, source: str, detail: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO Ownership_Updates (source, updated_at, detail) VALUES (?, ?, ?) "
        "ON CONFLICT(source) DO UPDATE SET updated_at = excluded.updated_at, detail = excluded.detail",
        (source, now(), json.dumps(detail, ensure_ascii=False, default=str)),
    )
