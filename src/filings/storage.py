"""Where filings are stored: a small index and one database per submission year.

Layout (``filings.db`` is wherever ``storage.filings_db_path`` puts it)::

    filings.db               the index: one row per filing naming the yearly file
                             that holds it, plus the tables that span filings
                             (translations, watermarks, metrics, observations, and
                             the logs of the bond and ownership steps)
    filings/filings_2025.db  a shard: the archives of the filings submitted in
                             2025 and everything parsed from them

Lists, searches and counts read only the index, which holds no archive. A shard
is opened when one filing is opened or a pipeline step reads archives, and
never all at once: SQLite attaches at most ten databases, so code that spans
years opens one shard at a time.

The index names each filing's shard (the year of ``submitted_at``, as text) and
``filing_shards`` names each shard's file, relative to the index's folder unless
an absolute path was recorded (a year moved to another disk). Shards are the
source of truth: every shard keeps a copy of its filings' metadata, so the
index's ``filings`` and ``filing_shards`` rows can be rebuilt from them.

Writes go to the shard first and then to the index (in WAL mode a transaction
is atomic per file, not across attached files). After a crash in between, a
shard may hold a filing the index does not list yet, or lists with an older
status; re-ingesting the filing or ``filings check --repair`` fixes that. The
index never points at content that was not committed.

An installation from before the split keeps everything in one ``filings.db``
(the ``legacy`` layout). It is still read as before, but filing writes are
refused until it is split (Admin -> Filing storage, or ``main.py filings split``).
"""

from __future__ import annotations

import logging
import re
import sqlite3
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import (
    connect_read,
    connect_write,
    initialize_managed_database,
    is_protected_database,
    transaction,
)
from src.paths import filing_shards_dir

logger = logging.getLogger(__name__)

LAYOUT_SPLIT = "split-v1"
LAYOUT_LEGACY = "legacy"
LAYOUT_EMPTY = "empty"

INDEX_SCHEMA_VERSION = 1
SHARD_SCHEMA_VERSION = 1

# A split leaves the new index here until the switch-over renames it.
NEXT_INDEX_SUFFIX = ".next.db"
# The old single file, kept after the switch-over until the operator deletes it.
RETIRED_SUFFIX = ".migrated"

DEFAULT_BUSY_TIMEOUT_MS = 30_000
_LOOKUP_CHUNK = 500
_SHARD_ID = re.compile(r"^[0-9A-Za-z][0-9A-Za-z_-]{0,31}$")

# Known before download, from EDINET's document list.
FILING_COLUMNS = (
    "doc_id", "edinet_code", "submitter_name", "period_start", "period_end",
    "submitted_at", "form_code", "doc_type_code", "xbrl_flag", "csv_flag",
)
# The metadata every filing row has, in the index and in its shard.
METADATA_COLUMNS = FILING_COLUMNS + (
    "archive_sha256", "archive_size", "status", "parse_error", "created_at", "updated_at",
)
# The index row: metadata plus where the filing is and what it holds.
INDEX_COLUMNS = METADATA_COLUMNS + ("shard", "fact_count", "issue_count")

# Tables of a shard that hold rows per filing, keyed by ``doc_id``.
SHARD_CONTENT_TABLES = (
    "filing_archives", "artifacts", "xbrl_contexts", "xbrl_units", "xbrl_facts",
    "quality_issues", "parse_runs",
)
# Tables of the index that span filings, carried over unchanged from the old file.
CROSS_FILING_TABLES = (
    "filing_translations", "data_watermarks", "metric_catalog", "observations",
    "observation_sources", "observation_dependencies", "Bond_Documents", "Ownership_Documents",
)

INDEX_DDL = """
CREATE TABLE IF NOT EXISTS storage_layout (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS filing_shards (
    shard TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    state TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    filing_count INTEGER NOT NULL DEFAULT 0,
    archive_bytes INTEGER NOT NULL DEFAULT 0,
    file_bytes INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    checked_at TEXT,
    check_result TEXT
);
CREATE TABLE IF NOT EXISTS filings (
    doc_id TEXT PRIMARY KEY,
    edinet_code TEXT,
    submitter_name TEXT,
    period_start TEXT,
    period_end TEXT,
    submitted_at TEXT,
    form_code TEXT,
    doc_type_code TEXT,
    xbrl_flag TEXT,
    csv_flag TEXT,
    shard TEXT NOT NULL REFERENCES filing_shards(shard),
    archive_sha256 TEXT,
    archive_size INTEGER,
    status TEXT NOT NULL,
    parse_error TEXT,
    fact_count INTEGER NOT NULL DEFAULT 0,
    issue_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_filings_company_date ON filings(edinet_code, submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_filings_submitted ON filings(submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_filings_form ON filings(form_code, edinet_code, period_end);
CREATE INDEX IF NOT EXISTS idx_filings_shard ON filings(shard);
CREATE TABLE IF NOT EXISTS data_watermarks (
    source_name TEXT PRIMARY KEY,
    source_version TEXT,
    max_available_at TEXT,
    row_count INTEGER,
    refreshed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS filing_translations (
    source_hash TEXT PRIMARY KEY,
    source_text TEXT NOT NULL,
    translated_text TEXT NOT NULL,
    translator_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS metric_catalog (
    metric_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    value_kind TEXT NOT NULL,
    unit_family TEXT,
    source_table TEXT,
    source_column TEXT,
    concept_qname TEXT,
    formula_json TEXT,
    formula_version TEXT,
    valid_from TEXT,
    valid_to TEXT
);
CREATE TABLE IF NOT EXISTS observations (
    observation_id TEXT PRIMARY KEY,
    company_code TEXT NOT NULL,
    metric_id TEXT NOT NULL,
    value_numeric REAL,
    value_text TEXT,
    unit TEXT,
    currency TEXT,
    period_start TEXT,
    period_end TEXT,
    available_at TEXT NOT NULL,
    doc_id TEXT,
    source_kind TEXT NOT NULL,
    calculation_version TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_obs_company_metric ON observations(company_code, metric_id, period_end);
CREATE TABLE IF NOT EXISTS observation_sources (
    observation_id TEXT NOT NULL,
    fact_id TEXT,
    source_fact_hash TEXT,
    extraction_rule TEXT,
    selection_reason TEXT,
    confidence REAL,
    PRIMARY KEY(observation_id, fact_id)
);
CREATE TABLE IF NOT EXISTS observation_dependencies (
    observation_id TEXT NOT NULL,
    input_observation_id TEXT NOT NULL,
    role TEXT,
    transform_json TEXT,
    PRIMARY KEY(observation_id, input_observation_id)
);
"""

SHARD_DDL = """
CREATE TABLE IF NOT EXISTS shard_info (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS filings (
    doc_id TEXT PRIMARY KEY,
    edinet_code TEXT,
    submitter_name TEXT,
    period_start TEXT,
    period_end TEXT,
    submitted_at TEXT,
    form_code TEXT,
    doc_type_code TEXT,
    xbrl_flag TEXT,
    csv_flag TEXT,
    archive_sha256 TEXT,
    archive_size INTEGER,
    status TEXT NOT NULL,
    parse_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS filing_archives (
    doc_id TEXT PRIMARY KEY REFERENCES filings(doc_id) ON DELETE CASCADE,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    content BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    member_path TEXT NOT NULL,
    kind TEXT NOT NULL,
    media_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    UNIQUE(doc_id, member_path)
);
CREATE INDEX IF NOT EXISTS idx_artifacts_doc ON artifacts(doc_id);
CREATE TABLE IF NOT EXISTS xbrl_contexts (
    context_id TEXT NOT NULL,
    doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    entity_identifier TEXT,
    period_start TEXT,
    period_end TEXT,
    instant TEXT,
    PRIMARY KEY(doc_id, context_id)
);
CREATE TABLE IF NOT EXISTS xbrl_units (
    unit_id TEXT NOT NULL,
    doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    measure TEXT,
    PRIMARY KEY(doc_id, unit_id)
);
CREATE TABLE IF NOT EXISTS xbrl_facts (
    fact_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    artifact_id TEXT,
    concept TEXT NOT NULL,
    namespace_uri TEXT,
    context_id TEXT,
    unit_id TEXT,
    value_text TEXT,
    numeric_value REAL,
    decimals TEXT,
    is_nil INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_facts_doc_concept ON xbrl_facts(doc_id, concept);
CREATE TABLE IF NOT EXISTS quality_issues (
    issue_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    severity TEXT NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    fact_id TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quality_doc ON quality_issues(doc_id, severity);
CREATE TABLE IF NOT EXISTS parse_runs (
    parse_run_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'started',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    fact_count INTEGER,
    section_count INTEGER,
    warning_count INTEGER DEFAULT 0,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_parse_runs_doc ON parse_runs(doc_id, started_at DESC);
"""

# Materialized narrative sections; optional, since sections are read from the archive on demand.
NARRATIVE_DDL = """
CREATE TABLE IF NOT EXISTS sections (
    section_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
    artifact_id TEXT,
    ordinal INTEGER NOT NULL,
    title TEXT,
    text TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sections_doc ON sections(doc_id, ordinal);
CREATE VIRTUAL TABLE IF NOT EXISTS section_search USING fts5(
    section_id UNINDEXED, doc_id UNINDEXED, title, text
);
"""


class FilingStorageError(RuntimeError):
    """Filing storage cannot do what was asked; the message may be shown to users."""


class LegacyLayoutError(FilingStorageError):
    """A filing write on the single-file layout, which is read-only until it is split."""

    def __init__(self) -> None:
        super().__init__("Filing storage needs to be split first (Admin → Filing storage)")


class ShardUnavailableError(FilingStorageError):
    """The yearly file holding a filing is missing."""

    def __init__(self, shard: str, path: Path) -> None:
        self.shard = shard
        self.path = path
        super().__init__(f"This filing's archive is unavailable: {path.name} is missing")


class MissingSubmissionDateError(FilingStorageError):
    """A filing without a submission date: its year, and so its file, is unknown."""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def shard_for(submitted_at: str | None) -> str:
    """The shard of a filing: the calendar year it was submitted (Japan time, as EDINET gives it).

    Takes the first four characters of ``submitted_at`` (``2026-07-27 17:07`` or ISO). The
    year is never guessed: a filing without one is refused.
    """
    text = str(submitted_at or "").strip()
    year = text[:4]
    if len(year) == 4 and year.isdigit() and (len(text) == 4 or not text[4].isdigit()) and 1900 < int(year) < 3000:
        return year
    raise MissingSubmissionDateError(f"The filing has no submission date ({text or 'empty'}), so its year is unknown")


def validate_shard_id(shard: str) -> str:
    if not _SHARD_ID.match(str(shard or "")):
        raise FilingStorageError(f"Invalid shard id {shard!r}")
    return str(shard)


def shard_file_name(shard: str) -> str:
    return f"filings_{validate_shard_id(shard)}.db"


def next_index_path(index_path: str | Path) -> Path:
    path = Path(index_path).expanduser()
    return path.with_name(path.stem + NEXT_INDEX_SUFFIX)


def retired_index_path(index_path: str | Path) -> Path:
    path = Path(index_path).expanduser()
    return path.with_name(path.name + RETIRED_SUFFIX)


def detect_layout(conn: sqlite3.Connection, schema: str = "main") -> str:
    """``split-v1`` (an index), ``legacy`` (one file holding everything) or ``empty``."""
    tables = {
        str(row[0])
        for row in conn.execute(f"SELECT name FROM {schema}.sqlite_master WHERE type = 'table'")
    }
    if "storage_layout" in tables:
        row = conn.execute(f"SELECT value FROM {schema}.storage_layout WHERE key = 'layout'").fetchone()
        if row is not None:
            return str(row[0])
    if "filings" in tables:
        return LAYOUT_LEGACY
    return LAYOUT_EMPTY


def initialize_shard(conn: sqlite3.Connection, shard: str, *, narrative: bool = False) -> None:
    """Create a shard's schema on an open write connection (idempotent)."""
    conn.executescript(SHARD_DDL)
    if narrative:
        conn.executescript(NARRATIVE_DDL)
    stamp = now()
    conn.executemany(
        "INSERT OR IGNORE INTO shard_info(key, value) VALUES (?, ?)",
        (("shard", shard), ("schema_version", str(SHARD_SCHEMA_VERSION)), ("created_at", stamp)),
    )


def shard_info(conn: sqlite3.Connection, schema: str = "main") -> dict[str, str]:
    try:
        return {str(key): str(value) for key, value in conn.execute(f"SELECT key, value FROM {schema}.shard_info")}
    except sqlite3.Error:
        return {}


def _chunks(items: list[str], size: int = _LOOKUP_CHUNK) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


class FilingStorage:
    """The index file and the yearly files it points to."""

    def __init__(self, index_path: str | Path, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS) -> None:
        self.index_path = Path(index_path).expanduser()
        self.busy_timeout_ms = busy_timeout_ms
        self._layout_cache: tuple[tuple[int, int], str] | None = None

    @property
    def shards_dir(self) -> Path:
        return filing_shards_dir(self.index_path)

    # -- layout ---------------------------------------------------------------

    def layout(self) -> str:
        """The index file's layout; re-read whenever the file is replaced (the switch-over)."""
        try:
            stat = self.index_path.stat()
        except FileNotFoundError:
            return LAYOUT_EMPTY
        key = (stat.st_dev, stat.st_ino)
        if self._layout_cache is not None and self._layout_cache[0] == key:
            return self._layout_cache[1]
        if stat.st_size == 0:
            return LAYOUT_EMPTY
        conn = connect_read(self.index_path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            layout = detect_layout(conn)
        finally:
            conn.close()
        if layout != LAYOUT_EMPTY:
            self._layout_cache = (key, layout)
        return layout

    def is_split(self) -> bool:
        return self.layout() == LAYOUT_SPLIT

    def initialize_index(self) -> str:
        """Create the index on a new install, or bring an index's schema up to date.

        A file in the old single-file layout is left exactly as it is.
        """
        layout = self.layout()
        if layout == LAYOUT_LEGACY:
            return layout
        if layout not in (LAYOUT_EMPTY, LAYOUT_SPLIT):
            raise FilingStorageError(f"filings.db uses the layout {layout!r}, which this version cannot read")
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect_write(self.index_path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            if layout == LAYOUT_EMPTY:
                initialize_managed_database(conn)
            conn.executescript(INDEX_DDL)
            if layout == LAYOUT_EMPTY:
                stamp = now()
                conn.executemany(
                    "INSERT OR IGNORE INTO storage_layout(key, value) VALUES (?, ?)",
                    (
                        ("layout", LAYOUT_SPLIT),
                        ("schema_version", str(INDEX_SCHEMA_VERSION)),
                        ("state", "ready"),
                        ("created_at", stamp),
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        self._layout_cache = None
        return LAYOUT_SPLIT

    def storage_values(self) -> dict[str, str]:
        """The ``storage_layout`` rows of a split index ({} otherwise)."""
        if not self.index_path.exists():
            return {}
        conn = connect_read(self.index_path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            return {str(key): str(value) for key, value in conn.execute("SELECT key, value FROM storage_layout")}
        except sqlite3.Error:
            return {}
        finally:
            conn.close()

    # -- connections -----------------------------------------------------------

    def connect_index(self, write: bool = False) -> sqlite3.Connection:
        if write:
            return connect_write(self.index_path, busy_timeout_ms=self.busy_timeout_ms)
        return connect_read(self.index_path, busy_timeout_ms=self.busy_timeout_ms)

    def resolve_shard_path(self, stored: str) -> Path:
        """A ``filing_shards.path`` value as a file path; relative paths start at the index's folder."""
        path = Path(stored).expanduser()
        return path if path.is_absolute() else self.index_path.parent / path

    def stored_shard_path(self, path: Path) -> str:
        """How a shard file is recorded: relative to the index's folder when inside it, else absolute."""
        path = Path(path).expanduser()
        try:
            return path.resolve(strict=False).relative_to(self.index_path.parent.resolve(strict=False)).as_posix()
        except ValueError:
            return str(path.resolve(strict=False))

    def default_shard_path(self, shard: str) -> Path:
        return self.shards_dir / shard_file_name(shard)

    def shard_path(self, shard: str, conn: sqlite3.Connection | None = None) -> Path:
        """Where a shard's file is: its ``filing_shards`` row, or the default place for a new one."""
        validate_shard_id(shard)
        own = conn is None
        if conn is None:
            if not self.index_path.exists():
                return self.default_shard_path(shard)
            conn = self.connect_index()
        try:
            row = conn.execute("SELECT path FROM filing_shards WHERE shard = ?", (shard,)).fetchone()
        except sqlite3.Error:
            row = None
        finally:
            if own:
                conn.close()
        return self.resolve_shard_path(str(row[0])) if row else self.default_shard_path(shard)

    def open_shard(self, shard: str, write: bool = False) -> sqlite3.Connection:
        """One connection to a shard; raises ``ShardUnavailableError`` when its file is missing."""
        path = self.shard_path(shard)
        if not path.is_file():
            raise ShardUnavailableError(shard, path)
        if write:
            return connect_write(path, busy_timeout_ms=self.busy_timeout_ms)
        return connect_read(path, busy_timeout_ms=self.busy_timeout_ms)

    def ensure_shard(self, shard: str, *, narrative: bool = False) -> Path:
        """The file of ``shard``, created with its schema and registered in the index when new.

        Only workers do this: the server's read-only guard covers the shards folder.
        A registered shard whose file is missing is not recreated (that would hide
        the missing year); a closed shard becomes active again.
        """
        validate_shard_id(shard)
        if self.layout() != LAYOUT_SPLIT:
            raise LegacyLayoutError()
        conn = self.connect_index()
        try:
            row = conn.execute("SELECT path, state FROM filing_shards WHERE shard = ?", (shard,)).fetchone()
        finally:
            conn.close()
        if row is not None:
            path = self.resolve_shard_path(str(row["path"]))
            if not path.is_file():
                raise ShardUnavailableError(shard, path)
            if row["state"] == "closed":
                with transaction(self.index_path, busy_timeout_ms=self.busy_timeout_ms) as index:
                    index.execute(
                        "UPDATE filing_shards SET state = 'active', updated_at = ? WHERE shard = ?",
                        (now(), shard),
                    )
            return path
        path = self.default_shard_path(shard)
        if is_protected_database(path):
            raise FilingStorageError("The server never creates filing storage; a worker does")
        self.create_shard_file(shard, path, narrative=narrative)
        self.register_shard(shard, path)
        return path

    def create_shard_file(self, shard: str, path: Path, *, narrative: bool = False) -> None:
        """Create (or complete) a shard file's schema; idempotent."""
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect_write(path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            initialize_managed_database(conn)
            initialize_shard(conn, shard, narrative=narrative)
            conn.commit()
        finally:
            conn.close()

    def register_shard(self, shard: str, path: Path, *, state: str = "active") -> None:
        stamp = now()
        with transaction(self.index_path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO filing_shards (shard, path, state, schema_version, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (shard, self.stored_shard_path(path), state, SHARD_SCHEMA_VERSION, stamp, stamp),
            )

    # -- lookups ---------------------------------------------------------------

    def shards(self) -> list[dict[str, Any]]:
        """Every registered shard with its resolved path, whether the file exists, and its size."""
        if self.layout() != LAYOUT_SPLIT:
            return []
        conn = self.connect_index()
        try:
            rows = [dict(row) for row in conn.execute("SELECT * FROM filing_shards ORDER BY shard")]
        finally:
            conn.close()
        for row in rows:
            path = self.resolve_shard_path(str(row["path"]))
            row["file"] = str(path)
            row["exists"] = path.is_file()
            row["size_bytes"] = path.stat().st_size if row["exists"] else None
        return rows

    def shard_of(self, doc_id: str, conn: sqlite3.Connection | None = None) -> str | None:
        return self.shards_of([doc_id], conn).get(doc_id)

    def shards_of(self, doc_ids: Iterable[str], conn: sqlite3.Connection | None = None) -> dict[str, str]:
        """``{doc_id: shard}`` for the listed filings the index knows."""
        wanted = list(dict.fromkeys(str(doc_id) for doc_id in doc_ids if doc_id))
        if not wanted:
            return {}
        own = conn is None
        if conn is None:
            conn = self.connect_index()
        try:
            found: dict[str, str] = {}
            for chunk in _chunks(wanted):
                marks = ",".join("?" * len(chunk))
                found.update(
                    (str(row[0]), str(row[1]))
                    for row in conn.execute(f"SELECT doc_id, shard FROM filings WHERE doc_id IN ({marks})", chunk)
                )
            return found
        finally:
            if own:
                conn.close()

    # -- index rows, derived from the shards -------------------------------------

    def publish(self, shard: str, doc_ids: Iterable[str]) -> None:
        """Copy the listed filings' rows from ``shard`` into the index, after a write to the shard.

        The index row is the shard's metadata plus the shard id, the numeric fact count
        and the quality-issue count; the shard's counters move with it. Filings the
        shard does not hold are left alone.
        """
        wanted = list(dict.fromkeys(str(doc_id) for doc_id in doc_ids))
        if not wanted:
            return
        shard_conn = self.open_shard(shard)
        try:
            rows = index_rows(shard_conn, wanted, shard)
        finally:
            shard_conn.close()
        if not rows:
            return
        with transaction(self.index_path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            added = 0
            delta = 0
            for row in rows:
                old = conn.execute("SELECT archive_size FROM filings WHERE doc_id = ?", (row["doc_id"],)).fetchone()
                size = int(row["archive_size"] or 0)
                if old is None:
                    added += 1
                    delta += size
                else:
                    delta += size - int(old[0] or 0)
            upsert_index_rows(conn, rows)
            conn.execute(
                "UPDATE filing_shards SET filing_count = filing_count + ?, archive_bytes = archive_bytes + ?, "
                "updated_at = ? WHERE shard = ?",
                (added, delta, now(), shard),
            )

    def refresh_counters(self, conn: sqlite3.Connection, shard: str | None = None) -> None:
        """Recount ``filing_shards.filing_count`` and ``archive_bytes`` from the index rows."""
        where = "WHERE s.shard = ?" if shard else ""
        conn.execute(
            f"""UPDATE filing_shards AS s SET
                    filing_count = (SELECT COUNT(*) FROM filings f WHERE f.shard = s.shard),
                    archive_bytes = (SELECT COALESCE(SUM(archive_size), 0) FROM filings f WHERE f.shard = s.shard),
                    updated_at = ?
                {where}""",
            (now(), shard) if shard else (now(),),
        )


def index_rows(conn: sqlite3.Connection, doc_ids: list[str], shard: str, schema: str = "main") -> list[dict[str, Any]]:
    """The index rows of ``doc_ids`` as a shard holds them (filings it lacks are left out)."""
    columns = ", ".join(f"f.{column}" for column in METADATA_COLUMNS)
    rows: list[dict[str, Any]] = []
    for chunk in _chunks(doc_ids):
        marks = ",".join("?" * len(chunk))
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT {columns}, ? AS shard,
                       (SELECT COUNT(*) FROM {schema}.xbrl_facts x
                         WHERE x.doc_id = f.doc_id AND x.numeric_value IS NOT NULL AND x.is_nil = 0) AS fact_count,
                       (SELECT COUNT(*) FROM {schema}.quality_issues q WHERE q.doc_id = f.doc_id) AS issue_count
                  FROM {schema}.filings f
                 WHERE f.doc_id IN ({marks})""",
                (shard, *chunk),
            )
        )
    return rows


_INDEX_UPSERT = (
    f"INSERT INTO filings ({', '.join(INDEX_COLUMNS)}) "
    f"VALUES ({', '.join(':' + column for column in INDEX_COLUMNS)}) "
    "ON CONFLICT(doc_id) DO UPDATE SET "
    # A filing never moves: the shard of an existing row is kept.
    + ", ".join(f"{column}=excluded.{column}" for column in INDEX_COLUMNS if column not in ("doc_id", "shard", "created_at"))
)


def upsert_index_rows(conn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> None:
    """Insert or refresh index rows (from ``index_rows``) on an index write connection."""
    conn.executemany(_INDEX_UPSERT, [dict(row) for row in rows])


class ShardReader:
    """Reads archives across years, keeping at most one read connection per shard.

    Connections open on first use and close at exit; nothing is attached, so a
    reader that walks companies (and so jumps between years) holds at most one
    open file per year. On the old single-file layout every read goes to that file.
    """

    def __init__(self, storage: FilingStorage | str | Path) -> None:
        self.storage = storage if isinstance(storage, FilingStorage) else FilingStorage(storage)
        self._index: sqlite3.Connection | None = None
        self._connections: dict[str, sqlite3.Connection] = {}
        self._layout: str | None = None

    def __enter__(self) -> "ShardReader":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()
        if self._index is not None:
            self._index.close()
            self._index = None

    @property
    def layout(self) -> str:
        if self._layout is None:
            self._layout = self.storage.layout()
        return self._layout

    def index(self) -> sqlite3.Connection:
        if self._index is None:
            self._index = self.storage.connect_index()
        return self._index

    def connection(self, shard: str) -> sqlite3.Connection:
        """The read connection holding ``shard``'s content (the old file on the single-file layout)."""
        if self.layout == LAYOUT_LEGACY:
            return self.index()
        conn = self._connections.get(shard)
        if conn is None:
            conn = self.storage.open_shard(shard)
            self._connections[shard] = conn
        return conn

    def content(self, doc_id: str) -> sqlite3.Connection | None:
        """The connection holding ``doc_id``'s archive and parsed rows; None when the index lacks it."""
        if self.layout == LAYOUT_LEGACY:
            return self.index()
        if self.layout != LAYOUT_SPLIT:
            return None
        shard = self.storage.shard_of(doc_id, self.index())
        return None if shard is None else self.connection(shard)

    def archive(self, doc_id: str) -> bytes | None:
        """The filing's ZIP, or None when the catalog holds none; ``ShardUnavailableError`` when its year is missing."""
        conn = self.content(doc_id)
        if conn is None:
            return None
        return read_archive(conn, doc_id, legacy=self.layout == LAYOUT_LEGACY)

    def archives(self, doc_ids: Iterable[str]) -> Iterator[tuple[str, bytes | None]]:
        """``(doc_id, archive or None)`` for each filing, in the order given."""
        wanted = [str(doc_id) for doc_id in doc_ids]
        legacy = self.layout == LAYOUT_LEGACY
        if not legacy and self.layout != LAYOUT_SPLIT:
            for doc_id in wanted:
                yield doc_id, None
            return
        for chunk in _chunks(wanted):
            shards = {} if legacy else self.storage.shards_of(chunk, self.index())
            for doc_id in chunk:
                if legacy:
                    yield doc_id, read_archive(self.index(), doc_id, legacy=True)
                elif doc_id not in shards:
                    yield doc_id, None
                else:
                    yield doc_id, read_archive(self.connection(shards[doc_id]), doc_id, legacy=False)


def read_archive(conn: sqlite3.Connection, doc_id: str, *, legacy: bool) -> bytes | None:
    """One filing's archive from a shard (``filing_archives``) or the old single file (``filings``)."""
    sql = (
        "SELECT archive_content FROM filings WHERE doc_id = ?"
        if legacy
        else "SELECT content FROM filing_archives WHERE doc_id = ?"
    )
    row = conn.execute(sql, (doc_id,)).fetchone()
    return bytes(row[0]) if row is not None and row[0] is not None else None
