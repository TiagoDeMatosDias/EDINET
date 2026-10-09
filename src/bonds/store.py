"""Tables in ``Bonds.db``, the rebuildable store of bond terms and the government yield curve.

``Bond_Documents`` logs every filing the bond step has read (issuance
supplements keep their ZIP so terms can be re-read without downloading);
``Bond_Issuances`` and ``Bond_Schedule_Rows`` hold what each filing says;
``Bonds`` is the merged view the application reads, one row per bond;
``JGB_Yields`` is the Ministry of Finance par-yield curve by day.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import connect_write

DDL = """
CREATE TABLE IF NOT EXISTS Bond_Documents (
    doc_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    edinet_code TEXT,
    filer_name TEXT,
    submitted_at TEXT,
    period_end TEXT,
    form_code TEXT,
    description TEXT,
    status TEXT NOT NULL,
    bond_count INTEGER NOT NULL DEFAULT 0,
    parser_version INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    processed_at TEXT NOT NULL,
    archive BLOB
);
CREATE INDEX IF NOT EXISTS idx_bond_documents_company ON Bond_Documents(edinet_code, kind, period_end);

CREATE TABLE IF NOT EXISTS Bond_Issuances (
    doc_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    edinet_code TEXT,
    submitted_at TEXT,
    name TEXT,
    series INTEGER,
    currency TEXT,
    amount REAL,
    denomination REAL,
    issue_price REAL,
    coupon REAL,
    coupon_text TEXT,
    coupon_kind TEXT,
    frequency INTEGER,
    interest_dates TEXT,
    issue_date TEXT,
    maturity TEXT,
    maturity_text TEXT,
    perpetual INTEGER,
    call_date TEXT,
    offering TEXT,
    collateral TEXT,
    negative_pledge INTEGER,
    covenants TEXT,
    features TEXT,
    seniority TEXT,
    ratings TEXT,
    notes TEXT,
    PRIMARY KEY (doc_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_bond_issuances_company ON Bond_Issuances(edinet_code);

CREATE TABLE IF NOT EXISTS Bond_Schedule_Rows (
    doc_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    edinet_code TEXT,
    period_end TEXT,
    issuer TEXT,
    name TEXT,
    series INTEGER,
    issue_date TEXT,
    issue_date_text TEXT,
    opening REAL,
    closing REAL,
    current_portion REAL,
    coupon REAL,
    coupon_text TEXT,
    collateral TEXT,
    maturity TEXT,
    maturity_text TEXT,
    note TEXT,
    currency_note TEXT,
    currency TEXT,
    features TEXT,
    seniority TEXT,
    consolidated INTEGER,
    PRIMARY KEY (doc_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_bond_schedule_company ON Bond_Schedule_Rows(edinet_code, period_end);

CREATE TABLE IF NOT EXISTS Bonds (
    bond_id TEXT PRIMARY KEY,
    edinet_code TEXT NOT NULL,
    company_name TEXT,
    company_name_en TEXT,
    ticker TEXT,
    industry TEXT,
    listed INTEGER,
    issuer TEXT,
    is_parent INTEGER NOT NULL,
    name TEXT,
    series INTEGER,
    currency TEXT,
    seniority TEXT,
    features TEXT,
    coupon REAL,
    coupon_kind TEXT,
    frequency INTEGER,
    issue_date TEXT,
    maturity TEXT,
    maturity_text TEXT,
    perpetual INTEGER,
    call_date TEXT,
    amount_issued REAL,
    issue_price REAL,
    outstanding REAL,
    outstanding_as_of TEXT,
    current_portion REAL,
    status TEXT,
    collateral TEXT,
    offering TEXT,
    private INTEGER,
    ratings TEXT,
    rating TEXT,
    rating_agency TEXT,
    rating_notch REAL,
    rating_inferred INTEGER,
    issue_yield REAL,
    issue_tenor REAL,
    jgb_at_issue REAL,
    issue_spread REAL,
    issuance_doc_id TEXT,
    issuance_submitted_at TEXT,
    schedule_doc_id TEXT,
    schedule_period_end TEXT,
    updated_at TEXT,
    jsda_code TEXT,
    jsda_name TEXT,
    market_date TEXT,
    market_price REAL,
    market_change REAL,
    market_yield REAL,
    market_spread REAL,
    market_reporters INTEGER
);
CREATE INDEX IF NOT EXISTS idx_bonds_company ON Bonds(edinet_code);
CREATE INDEX IF NOT EXISTS idx_bonds_status ON Bonds(status, maturity);

CREATE TABLE IF NOT EXISTS Bond_Market_Prices (
    price_date TEXT NOT NULL,
    code TEXT NOT NULL,
    kind TEXT,
    name TEXT,
    maturity TEXT,
    coupon REAL,
    yield REAL,
    price REAL,
    change REAL,
    simple_yield REAL,
    reporters INTEGER,
    PRIMARY KEY (price_date, code)
);
CREATE INDEX IF NOT EXISTS idx_bond_market_prices_terms ON Bond_Market_Prices(maturity, coupon);

CREATE TABLE IF NOT EXISTS JGB_Yields (
    curve_date TEXT NOT NULL,
    tenor REAL NOT NULL,
    yield REAL NOT NULL,
    PRIMARY KEY (curve_date, tenor)
);

CREATE TABLE IF NOT EXISTS Bond_Updates (
    source TEXT PRIMARY KEY,
    updated_at TEXT NOT NULL,
    detail TEXT
);
"""

ISSUANCE_COLUMNS = (
    "name", "series", "currency", "amount", "denomination", "issue_price", "coupon", "coupon_text", "coupon_kind",
    "frequency", "interest_dates", "issue_date", "maturity", "maturity_text", "perpetual", "call_date", "offering",
    "collateral", "negative_pledge", "covenants", "features", "seniority", "ratings", "notes",
)
SCHEDULE_COLUMNS = (
    "issuer", "name", "series", "issue_date", "issue_date_text", "opening", "closing", "current_portion", "coupon",
    "coupon_text", "collateral", "maturity", "maturity_text", "note", "currency_note", "currency", "features", "seniority",
    "consolidated",
)
BOND_COLUMNS = (
    "bond_id", "edinet_code", "company_name", "company_name_en", "ticker", "industry", "listed", "issuer", "is_parent",
    "name", "series", "currency", "seniority", "features", "coupon", "coupon_kind", "frequency", "issue_date",
    "maturity", "maturity_text", "perpetual", "call_date", "amount_issued", "issue_price", "outstanding",
    "outstanding_as_of", "current_portion", "status", "collateral", "offering", "private", "ratings", "rating",
    "rating_agency", "rating_notch", "rating_inferred", "issue_yield", "issue_tenor", "jgb_at_issue", "issue_spread",
    "issuance_doc_id", "issuance_submitted_at", "schedule_doc_id", "schedule_period_end", "updated_at",
    "jsda_code", "jsda_name", "market_date", "market_price", "market_change", "market_yield", "market_spread", "market_reporters",
)
_COLUMN_TYPES = {"market_reporters": "INTEGER", "market_price": "REAL", "market_change": "REAL", "market_yield": "REAL", "market_spread": "REAL"}
JSON_COLUMNS = {"features", "ratings"}


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_bond_tables(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect_write(path)
    try:
        conn.executescript(DDL)
        # Columns added after a database was created: Bonds is rebuilt on every run, so empty columns are enough.
        existing = {row[1] for row in conn.execute("PRAGMA table_info(Bonds)")}
        for column in BOND_COLUMNS:
            if column not in existing:
                conn.execute(f'ALTER TABLE Bonds ADD COLUMN "{column}" {_COLUMN_TYPES.get(column, "TEXT")}')
        conn.commit()
    finally:
        conn.close()


def _value(column: str, value: Any) -> Any:
    if column in JSON_COLUMNS:
        return json.dumps(value or [], ensure_ascii=False)
    if isinstance(value, bool):
        return int(value)
    return value


def record_document(conn, doc_id: str, kind: str, meta: dict[str, Any], *, status: str, bond_count: int = 0,
                    parser_version: int = 0, error: str | None = None, archive: bytes | None = None) -> None:
    conn.execute(
        """
        INSERT INTO Bond_Documents (doc_id, kind, edinet_code, filer_name, submitted_at, period_end, form_code,
            description, status, bond_count, parser_version, error, processed_at, archive)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(doc_id) DO UPDATE SET
            kind = excluded.kind, edinet_code = excluded.edinet_code, filer_name = excluded.filer_name,
            submitted_at = excluded.submitted_at, period_end = excluded.period_end, form_code = excluded.form_code,
            description = excluded.description, status = excluded.status, bond_count = excluded.bond_count,
            parser_version = excluded.parser_version, error = excluded.error, processed_at = excluded.processed_at,
            archive = COALESCE(excluded.archive, Bond_Documents.archive)
        """,
        (
            doc_id, kind, meta.get("edinet_code"), meta.get("filer_name"), meta.get("submitted_at"),
            meta.get("period_end"), meta.get("form_code"), meta.get("description"), status, bond_count,
            parser_version, error, now(), archive,
        ),
    )


def replace_issuances(conn, doc_id: str, edinet_code: str, submitted_at: str, bonds: Iterable[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM Bond_Issuances WHERE doc_id = ?", (doc_id,))
    columns = ("doc_id", "seq", "edinet_code", "submitted_at", *ISSUANCE_COLUMNS)
    conn.executemany(
        f"INSERT INTO Bond_Issuances ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
        [(doc_id, bond["seq"], edinet_code, submitted_at, *(_value(column, bond.get(column)) for column in ISSUANCE_COLUMNS)) for bond in bonds],
    )


def replace_schedule(conn, doc_id: str, edinet_code: str, period_end: str, rows: Iterable[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM Bond_Schedule_Rows WHERE doc_id = ?", (doc_id,))
    columns = ("doc_id", "seq", "edinet_code", "period_end", *SCHEDULE_COLUMNS)
    conn.executemany(
        f"INSERT INTO Bond_Schedule_Rows ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
        [(doc_id, row["seq"], edinet_code, period_end, *(_value(column, row.get(column)) for column in SCHEDULE_COLUMNS)) for row in rows],
    )


def replace_bonds(conn, bonds: Iterable[dict[str, Any]]) -> int:
    conn.execute("DELETE FROM Bonds")
    rows = [tuple(_value(column, bond.get(column)) for column in BOND_COLUMNS) for bond in bonds]
    conn.executemany(
        f"INSERT INTO Bonds ({', '.join(BOND_COLUMNS)}) VALUES ({', '.join('?' for _ in BOND_COLUMNS)})",
        rows,
    )
    return len(rows)


def mark_update(conn, source: str, detail: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO Bond_Updates (source, updated_at, detail) VALUES (?, ?, ?) "
        "ON CONFLICT(source) DO UPDATE SET updated_at = excluded.updated_at, detail = excluded.detail",
        (source, now(), json.dumps(detail, ensure_ascii=False, default=str)),
    )


def decode(row: Any) -> dict[str, Any]:
    """A row as a dict with its JSON columns parsed."""
    item = dict(row)
    for column in JSON_COLUMNS & item.keys():
        try:
            item[column] = json.loads(item[column] or "[]")
        except (TypeError, ValueError):
            item[column] = []
    return item
