"""The filing catalog: filings, their archives, and the XBRL index built from them.

Every read and write of filing content goes through ``FilingCatalog``; see
``storage`` for the layout (an index plus one file per submission year). Lists,
searches and counts read the index only. Reading one filing's archive, facts,
sections or quality issues opens its yearly file; writes go to that file first
and then refresh the filing's index row.

On the old single-file layout the catalog reads everything from that file as
before and refuses filing writes until it is split.
"""

from __future__ import annotations

import hashlib
import sqlite3
import zipfile
from collections import Counter
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import connect_read, table_exists, transaction

from .archive import DEFAULT_ARCHIVE_POLICY, extract_zip_member
from .storage import (
    LAYOUT_LEGACY,
    LAYOUT_SPLIT,
    METADATA_COLUMNS,
    FilingStorage,
    FilingStorageError,
    LegacyLayoutError,
    ShardReader,
    ShardUnavailableError,
    now,
    read_archive,
    shard_for,
)
from .xbrl import parse_narrative

_FILING_METADATA_COLUMNS = ", ".join(METADATA_COLUMNS)
_ARTIFACT_COLUMNS = "artifact_id, doc_id, member_path, kind, media_type, size_bytes, sha256"
_SHARD_FILING_UPSERT = (
    f"INSERT INTO filings ({_FILING_METADATA_COLUMNS}) "
    f"VALUES ({', '.join(':' + column for column in METADATA_COLUMNS)}) "
    "ON CONFLICT(doc_id) DO UPDATE SET "
    + ", ".join(f"{column}=excluded.{column}" for column in METADATA_COLUMNS if column not in ("doc_id", "created_at"))
)


class FilingCatalog:
    """Filing metadata, retained archives, and the rebuildable indexes parsed from them."""

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = 30_000,
        include_narrative_index: bool = False,
    ) -> None:
        self.path = Path(path).expanduser()
        self.busy_timeout_ms = busy_timeout_ms
        self.include_narrative_index = include_narrative_index
        self.storage = FilingStorage(self.path, busy_timeout_ms=busy_timeout_ms)
        self.initialize()

    def initialize(self) -> None:
        """Create the index on a new install; an old single file is left untouched."""
        self.storage.initialize_index()

    @property
    def layout(self) -> str:
        return self.storage.layout()

    def _is_legacy(self) -> bool:
        return self.storage.layout() == LAYOUT_LEGACY

    def require_writable(self) -> None:
        """Raise ``LegacyLayoutError`` while the old single file waits to be split."""
        if self._is_legacy():
            raise LegacyLayoutError()

    # -- writes: the filing's shard first, then its index row --------------------

    def _write_target(self, doc_id: str, submitted_at: str | None = None) -> tuple[str, Path]:
        """The shard a write of ``doc_id`` goes to: where the index has it, else its submission year."""
        layout = self.storage.layout()
        if layout == LAYOUT_LEGACY:
            raise LegacyLayoutError()
        if layout != LAYOUT_SPLIT:
            self.initialize()
        shard = self.storage.shard_of(doc_id)
        if shard is None:
            if submitted_at is None:
                raise FilingStorageError(f"{doc_id} is not in the filing catalog")
            shard = shard_for(submitted_at)
        return shard, self.storage.ensure_shard(shard, narrative=self.include_narrative_index)

    def upsert_filing(self, values: dict[str, Any]) -> None:
        """Store one filing's metadata and archive (``archive_content``; None removes it)."""
        doc_id = str(values["doc_id"])
        shard, path = self._write_target(doc_id, str(values.get("submitted_at") or ""))
        row = {column: values.get(column) for column in METADATA_COLUMNS}
        archive = values.get("archive_content")
        with transaction(path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(_SHARD_FILING_UPSERT, row)
            if archive is None:
                conn.execute("DELETE FROM filing_archives WHERE doc_id = ?", (doc_id,))
            else:
                content = bytes(archive)
                conn.execute(
                    "INSERT OR REPLACE INTO filing_archives (doc_id, sha256, size, content) VALUES (?, ?, ?, ?)",
                    (doc_id, hashlib.sha256(content).hexdigest(), len(content), content),
                )
        self.storage.publish(shard, [doc_id])

    def replace_parsed_content(
        self,
        doc_id: str,
        artifacts: Iterable[dict[str, Any]],
        contexts: Iterable[dict[str, Any]],
        units: Iterable[dict[str, Any]],
        facts: Iterable[dict[str, Any]],
        _sections: Iterable[dict[str, Any]],
    ) -> None:
        shard, path = self._write_target(doc_id)
        with transaction(path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            numeric_facts = [
                fact
                for fact in facts
                if fact.get("numeric_value") is not None and not fact.get("is_nil")
            ]
            if table_exists(conn, "section_search"):
                conn.execute("DELETE FROM section_search WHERE doc_id = ?", (doc_id,))
            if table_exists(conn, "sections"):
                conn.execute("DELETE FROM sections WHERE doc_id = ?", (doc_id,))
            for table in ("xbrl_facts", "xbrl_units", "xbrl_contexts", "artifacts"):
                conn.execute(f"DELETE FROM {table} WHERE doc_id = ?", (doc_id,))
            conn.executemany(
                f"""INSERT INTO artifacts({_ARTIFACT_COLUMNS})
                    VALUES (:artifact_id, :doc_id, :member_path, :kind, :media_type, :size_bytes, :sha256)""",
                artifacts,
            )
            conn.executemany(
                "INSERT OR IGNORE INTO xbrl_contexts(context_id, doc_id, entity_identifier, period_start, period_end, instant) VALUES (:context_id, :doc_id, :entity_identifier, :period_start, :period_end, :instant)",
                contexts,
            )
            conn.executemany(
                "INSERT OR IGNORE INTO xbrl_units(unit_id, doc_id, measure) VALUES (:unit_id, :doc_id, :measure)",
                units,
            )
            conn.executemany(
                """INSERT OR IGNORE INTO xbrl_facts
                (fact_id, doc_id, artifact_id, concept, namespace_uri, context_id,
                 unit_id, value_text, numeric_value, decimals, is_nil)
                VALUES (:fact_id, :doc_id, :artifact_id, :concept, :namespace_uri,
                        :context_id, :unit_id, :value_text, :numeric_value,
                        :decimals, :is_nil)""",
                numeric_facts,
            )
            # Narrative sections are intentionally not persisted.  The
            # immutable ZIP is the canonical source and catalog.list_sections
            # reconstructs sections on demand when the viewer requests them.
        self.storage.publish(shard, [doc_id])

    def replace_quality_issues(self, doc_id: str, issues: list[dict[str, Any]]) -> None:
        # An issue is identified by filing, code, and fact; a filing that repeats
        # a fact id raises the same issue twice, which is one issue, not a
        # reason to fail the whole filing.
        shard, path = self._write_target(doc_id)
        with transaction(path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute("DELETE FROM quality_issues WHERE doc_id = ?", (doc_id,))
            conn.executemany(
                """INSERT OR IGNORE INTO quality_issues
                   (issue_id, doc_id, severity, code, message, fact_id, created_at)
                   VALUES (:issue_id, :doc_id, :severity, :code, :message, :fact_id, :created_at)""",
                issues,
            )
        self.storage.publish(shard, [doc_id])

    def set_status(self, doc_id: str, status: str, parse_error: str | None, updated_at: str) -> None:
        shard, path = self._write_target(doc_id)
        with transaction(path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                "UPDATE filings SET status = ?, parse_error = ?, updated_at = ? WHERE doc_id = ?",
                (status, parse_error, updated_at, doc_id),
            )
        self.storage.publish(shard, [doc_id])

    def record_parse_run(
        self,
        doc_id: str,
        parser_version: str,
        status: str,
        fact_count: int = 0,
        section_count: int = 0,
        warning_count: int = 0,
        error_message: str | None = None,
        parse_run_id: str | None = None,
    ) -> str:
        from uuid import uuid4

        run_id = parse_run_id or str(uuid4())
        stamp = now()
        completed = stamp if status in ("completed", "failed") else None
        shard, path = self._write_target(doc_id)
        with transaction(path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO parse_runs
                   (parse_run_id, doc_id, parser_version, status, started_at, completed_at,
                    fact_count, section_count, warning_count, error_message)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, doc_id, parser_version, status, stamp, completed, fact_count, section_count, warning_count, error_message),
            )
            if status == "completed":
                conn.execute("UPDATE filings SET updated_at = ? WHERE doc_id = ?", (stamp, doc_id))
        self.storage.publish(shard, [doc_id])
        return run_id

    # -- reads of one filing's content (its shard, or the old single file) ----

    @contextmanager
    def _content(self, doc_id: str) -> Iterator[sqlite3.Connection | None]:
        """A read connection holding ``doc_id``'s content, or None when the catalog lacks the filing.

        Raises ``ShardUnavailableError`` when the filing's yearly file is missing.
        """
        layout = self.storage.layout()
        if layout == LAYOUT_LEGACY:
            conn = connect_read(self.path, busy_timeout_ms=self.busy_timeout_ms)
        elif layout == LAYOUT_SPLIT:
            shard = self.storage.shard_of(doc_id)
            if shard is None:
                yield None
                return
            conn = self.storage.open_shard(shard)
        else:
            yield None
            return
        try:
            yield conn
        finally:
            conn.close()

    def _content_all(self, doc_id: str, query: str, params: tuple[Any, ...]) -> list[sqlite3.Row]:
        with self._content(doc_id) as conn:
            return [] if conn is None else list(conn.execute(query, params).fetchall())

    def _content_one(self, doc_id: str, query: str, params: tuple[Any, ...]) -> sqlite3.Row | None:
        with self._content(doc_id) as conn:
            return None if conn is None else conn.execute(query, params).fetchone()

    def get_archive_content(self, doc_id: str) -> bytes | None:
        """Return the retained compressed ZIP for one filing, if available."""
        legacy = self._is_legacy()
        with self._content(doc_id) as conn:
            return None if conn is None else read_archive(conn, doc_id, legacy=legacy)

    def archives(self, doc_ids: Iterable[str]) -> Iterator[tuple[str, bytes | None]]:
        """``(doc_id, archive or None)`` for each filing, keeping one connection per year open while iterating."""
        with ShardReader(self.storage) as reader:
            yield from reader.archives(doc_ids)

    def list_artifacts(self, doc_id: str) -> list[sqlite3.Row]:
        return self._content_all(
            doc_id,
            f"SELECT {_ARTIFACT_COLUMNS} FROM artifacts WHERE doc_id = ? ORDER BY member_path",
            (doc_id,),
        )

    def get_artifact_content(self, artifact_id: str, doc_id: str | None = None) -> dict[str, Any] | None:
        """Return artifact metadata plus the member extracted from the filing's archive.

        ``doc_id`` names the filing; without it every year is searched for the artifact.
        """
        if self._is_legacy():
            row = self._one(
                """SELECT a.*, f.archive_content AS _archive_content
                   FROM artifacts a
                   JOIN filings f ON f.doc_id = a.doc_id
                  WHERE a.artifact_id = ?""",
                (artifact_id,),
            )
            if row is None:
                return None
            result = dict(row)
            archive = result.pop("_archive_content", None)
            if result.get("content") is None and archive is not None:
                result["content"] = extract_zip_member(bytes(archive), str(result["member_path"]), DEFAULT_ARCHIVE_POLICY)
            return result
        if doc_id is None:
            doc_id = self._artifact_doc_id(artifact_id)
            if doc_id is None:
                return None
        with self._content(doc_id) as conn:
            if conn is None:
                return None
            row = conn.execute(
                f"SELECT {_ARTIFACT_COLUMNS} FROM artifacts WHERE artifact_id = ? AND doc_id = ?",
                (artifact_id, doc_id),
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            archive = read_archive(conn, doc_id, legacy=False)
        result["content"] = (
            extract_zip_member(archive, str(result["member_path"]), DEFAULT_ARCHIVE_POLICY)
            if archive is not None
            else None
        )
        return result

    def _artifact_doc_id(self, artifact_id: str) -> str | None:
        for shard in self.storage.shards():
            if not shard["exists"]:
                continue
            conn = self.storage.open_shard(str(shard["shard"]))
            try:
                row = conn.execute("SELECT doc_id FROM artifacts WHERE artifact_id = ?", (artifact_id,)).fetchone()
            finally:
                conn.close()
            if row is not None:
                return str(row[0])
        return None

    _FACT_WITH_PERIOD = (
        "SELECT f.*, c.period_start, c.period_end, c.instant "
        "FROM xbrl_facts f "
        "LEFT JOIN xbrl_contexts c ON c.doc_id = f.doc_id AND c.context_id = f.context_id "
        "WHERE f.doc_id = ? AND f.numeric_value IS NOT NULL AND f.is_nil = 0 "
    )

    def list_facts(self, doc_id: str, concept: str | None = None, limit: int = 500) -> list[sqlite3.Row]:
        """Return numeric facts with their context's reporting period."""
        concept_filter = "AND f.concept LIKE ? " if concept else ""
        params: tuple[Any, ...] = (doc_id, concept) if concept else (doc_id,)
        return self._content_all(
            doc_id,
            f"{self._FACT_WITH_PERIOD}{concept_filter}ORDER BY f.concept LIMIT ?",
            (*params, max(1, min(limit, 5000))),
        )

    def statement_facts(self, doc_id: str) -> list[sqlite3.Row]:
        """Every numeric fact of one filing with its period, for statement tables."""
        return self._content_all(doc_id, f"{self._FACT_WITH_PERIOD}ORDER BY f.rowid", (doc_id,))

    def _on_demand_sections(
        self,
        doc_id: str,
        query: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Parse narrative members from the retained ZIP only when requested."""
        archive_content = self.get_archive_content(doc_id)
        if archive_content is None:
            return []
        try:
            artifacts = self._content_all(
                doc_id,
                """SELECT artifact_id, member_path
                     FROM artifacts
                    WHERE doc_id = ? AND kind = 'narrative'
                    ORDER BY member_path""",
                (doc_id,),
            )
        except sqlite3.OperationalError:
            artifacts = []

        cap = max(1, min(limit, 10_000))
        needle = query.strip().casefold() if query and query.strip() else ""
        sections: list[dict[str, Any]] = []
        for artifact in artifacts:
            try:
                content = extract_zip_member(
                    archive_content,
                    str(artifact["member_path"]),
                    DEFAULT_ARCHIVE_POLICY,
                )
            except (OSError, ValueError, LookupError, zipfile.BadZipFile):
                continue
            for section in parse_narrative(
                content,
                doc_id,
                str(artifact["artifact_id"]),
            ):
                if needle and needle not in (
                    f"{section.get('title', '')}\n{section.get('text', '')}".casefold()
                ):
                    continue
                sections.append(section)
                if len(sections) >= cap:
                    return sections
        return sections

    def list_sections(self, doc_id: str, query: str | None = None, limit: int = 200) -> list[Any]:
        cap = max(1, min(limit, 1000))
        normalized_query = query.strip() if query else ""
        try:
            if normalized_query:
                fts_phrase = '"' + normalized_query.replace('"', '""') + '"'
                stored = self._content_all(
                    doc_id,
                    """SELECT s.* FROM section_search f JOIN sections s ON s.section_id = f.section_id
                       WHERE f.doc_id = ? AND section_search MATCH ? ORDER BY s.ordinal LIMIT ?""",
                    (doc_id, fts_phrase, cap),
                )
            else:
                stored = self._content_all(
                    doc_id,
                    "SELECT * FROM sections WHERE doc_id = ? ORDER BY ordinal LIMIT ?",
                    (doc_id, cap),
                )
        except sqlite3.OperationalError:
            stored = []
        return stored or self._on_demand_sections(doc_id, normalized_query, cap)

    def get_section(self, doc_id: str, section_id: str) -> Any | None:
        try:
            stored = self._content_one(
                doc_id,
                "SELECT * FROM sections WHERE doc_id = ? AND section_id = ?",
                (doc_id, section_id),
            )
        except sqlite3.OperationalError:
            stored = None
        if stored is not None:
            return stored
        return next(
            (
                section
                for section in self._on_demand_sections(doc_id, limit=10_000)
                if section["section_id"] == section_id
            ),
            None,
        )

    def list_taxonomy(self, doc_id: str) -> list[sqlite3.Row]:
        return self._content_all(
            doc_id,
            "SELECT DISTINCT namespace_uri, concept FROM xbrl_facts WHERE doc_id = ? AND namespace_uri IS NOT NULL ORDER BY namespace_uri, concept",
            (doc_id,),
        )

    def list_quality_issues(self, doc_id: str) -> list[sqlite3.Row]:
        return self._content_all(
            doc_id,
            "SELECT * FROM quality_issues WHERE doc_id = ? ORDER BY severity DESC, issue_id",
            (doc_id,),
        )

    def latest_parse_run(self, doc_id: str) -> sqlite3.Row | None:
        return self._content_one(
            doc_id,
            "SELECT * FROM parse_runs WHERE doc_id = ? ORDER BY started_at DESC LIMIT 1",
            (doc_id,),
        )

    # -- reads of the index (never an archive) ---------------------------------

    def get_filing(self, doc_id: str) -> sqlite3.Row | None:
        return self._one(
            f"SELECT {_FILING_METADATA_COLUMNS} FROM filings WHERE doc_id = ?",
            (doc_id,),
        )

    def list_company(self, edinet_code: str, limit: int = 100, offset: int = 0) -> list[sqlite3.Row]:
        return self._all(
            f"SELECT {_FILING_METADATA_COLUMNS} FROM filings "
            "WHERE edinet_code = ? ORDER BY submitted_at DESC LIMIT ? OFFSET ?",
            (edinet_code, max(1, min(limit, 500)), max(0, offset)),
        )

    def list_recent(
        self,
        company_code: str | None = None,
        limit: int = 100,
        offset: int = 0,
        form_codes: Iterable[str] | None = None,
    ) -> list[sqlite3.Row]:
        """Newest submissions first, optionally for one company or some EDINET form codes."""
        cap = max(1, min(limit, 500))
        clauses: list[str] = []
        params: list[Any] = []
        if company_code:
            clauses.append("edinet_code = ?")
            params.append(company_code)
        forms = [code for code in (form_codes or ()) if code]
        if forms:
            clauses.append(f"form_code IN ({','.join('?' * len(forms))})")
            params.extend(forms)
        where = f"WHERE {' AND '.join(clauses)} " if clauses else ""
        return self._all(
            f"SELECT {_FILING_METADATA_COLUMNS} FROM filings {where}"
            "ORDER BY submitted_at DESC LIMIT ? OFFSET ?",
            (*params, cap, max(0, offset)),
        )

    def form_coverage(self) -> list[sqlite3.Row]:
        """Filings and companies per EDINET form code, largest first."""
        return self._all(
            "SELECT form_code, COUNT(*) AS filings, "
            "COUNT(DISTINCT NULLIF(TRIM(COALESCE(edinet_code, '')), '')) AS companies "
            "FROM filings GROUP BY form_code ORDER BY filings DESC",
            (),
        )

    def coverage_summary(self) -> dict[str, Any]:
        """Return compact filing and company counts for the Filing Explorer landing page.

        Reads the index only. On the old single-file layout every column read here
        sits before ``archive_content`` or comes from an index, so the summary never
        pages through the retained archives there either.
        """
        with_issues = (
            "(SELECT COUNT(DISTINCT doc_id) FROM quality_issues)"
            if self._is_legacy()
            else "SUM(CASE WHEN issue_count > 0 THEN 1 ELSE 0 END)"
        )
        row = self._one(
            f"""
            SELECT
                COUNT(*) AS unique_filings,
                COUNT(DISTINCT NULLIF(TRIM(COALESCE(edinet_code, '')), '')) AS unique_companies,
                MIN(NULLIF(submitted_at, '')) AS first_submitted,
                MAX(NULLIF(submitted_at, '')) AS last_submitted,
                {with_issues} AS filings_with_issues
            FROM filings
            """,
            (),
        )
        return {
            "unique_filings": int(row["unique_filings"] or 0) if row else 0,
            "unique_companies": int(row["unique_companies"] or 0) if row else 0,
            "filings_with_issues": int(row["filings_with_issues"] or 0) if row else 0,
            "first_submitted": row["first_submitted"] if row else None,
            "last_submitted": row["last_submitted"] if row else None,
        }

    def quality_coverage(self) -> dict[str, Any]:
        if self._is_legacy():
            total = self._one("SELECT COUNT(*) AS cnt FROM filings", ())
            parsed = self._one("SELECT COUNT(*) AS cnt FROM filings WHERE status = 'parsed'", ())
            issues = self._one("SELECT COUNT(DISTINCT doc_id) AS cnt FROM quality_issues", ())
            counts = (total, parsed, issues)
            return {
                key: int(row["cnt"] or 0) if row else 0
                for key, row in zip(("total_filings", "parsed_filings", "filings_with_issues"), counts, strict=True)
            }
        row = self._one(
            "SELECT COUNT(*) AS total_filings, "
            "COALESCE(SUM(CASE WHEN status = 'parsed' THEN 1 ELSE 0 END), 0) AS parsed_filings, "
            "COALESCE(SUM(CASE WHEN issue_count > 0 THEN 1 ELSE 0 END), 0) AS filings_with_issues "
            "FROM filings",
            (),
        )
        return {
            key: int(row[key] or 0) if row else 0
            for key in ("total_filings", "parsed_filings", "filings_with_issues")
        }

    def quality_summary(self) -> list[dict[str, Any]]:
        """Quality issues by severity and code across every year (missing years are left out)."""
        query = "SELECT severity, code, COUNT(*) AS count FROM quality_issues GROUP BY severity, code"
        if self._is_legacy():
            rows = [dict(row) for row in self._all(query, ())]
        else:
            totals: Counter[tuple[str, str]] = Counter()
            for shard in self.storage.shards():
                if not shard["exists"]:
                    continue
                conn = self.storage.open_shard(str(shard["shard"]))
                try:
                    for severity, code, count in conn.execute(query):
                        totals[(severity, code)] += int(count)
                finally:
                    conn.close()
            rows = [{"severity": severity, "code": code, "count": count} for (severity, code), count in totals.items()]
        return sorted(rows, key=lambda row: (str(row["severity"]), -int(row["count"])))

    def shards(self) -> list[dict[str, Any]]:
        """The yearly files: id, path, state, counters, and whether each file exists."""
        return self.storage.shards()

    def check(self, *, repair: bool = False, full: bool = False) -> dict[str, Any]:
        """Compare the index with the yearly files; ``repair`` fixes what the index got wrong."""
        from .maintenance import check

        return check(self.storage, repair=repair, full=full)

    # -- tables that span filings (in the index) --------------------------------

    def upsert_watermark(self, source_name: str, **kwargs: Any) -> None:
        stamp = now()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            columns = ", ".join(kwargs.keys())
            placeholders = ", ".join(f":{k}" for k in kwargs)
            conn.execute(
                f"""INSERT INTO data_watermarks (source_name, refreshed_at, {columns})
                    VALUES (:source_name, :refreshed_at, {placeholders})
                    ON CONFLICT(source_name) DO UPDATE SET
                    refreshed_at=excluded.refreshed_at, {", ".join(f"{k}=excluded.{k}" for k in kwargs)}""",
                {"source_name": source_name, "refreshed_at": stamp, **kwargs},
            )

    def upsert_metric(self, metric_id: str, **values: Any) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            columns = ", ".join(values.keys())
            placeholders = ", ".join(f":{k}" for k in values)
            set_clause = ", ".join(f"{k}=excluded.{k}" for k in values)
            conn.execute(
                f"INSERT INTO metric_catalog (metric_id, {columns}) VALUES (:metric_id, {placeholders}) "
                f"ON CONFLICT(metric_id) DO UPDATE SET {set_clause}",
                {"metric_id": metric_id, **values},
            )

    def get_metric(self, metric_id: str) -> sqlite3.Row | None:
        return self._one("SELECT * FROM metric_catalog WHERE metric_id = ?", (metric_id,))

    def list_metrics(self) -> list[sqlite3.Row]:
        return self._all("SELECT * FROM metric_catalog ORDER BY display_name", ())

    def insert_observation(self, observation_id: str, **values: Any) -> None:
        columns = ", ".join(values.keys())
        placeholders = ", ".join(f":{k}" for k in values)
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO observations (observation_id, {columns}) VALUES (:observation_id, {placeholders})",
                {"observation_id": observation_id, **values},
            )

    def get_observation(self, observation_id: str) -> sqlite3.Row | None:
        return self._one("SELECT * FROM observations WHERE observation_id = ?", (observation_id,))

    def find_observations(
        self,
        company_code: str,
        metric_id: str,
        period_end: str | None = None,
        limit: int = 100,
    ) -> list[sqlite3.Row]:
        query = """SELECT * FROM observations
                   WHERE company_code = ? AND metric_id = ?
                   AND (? IS NULL OR period_end = ?)
                   ORDER BY available_at DESC LIMIT ?"""
        return self._all(query, (company_code, metric_id, period_end, period_end, limit))

    def insert_observation_source(self, observation_id: str, fact_id: str, **values: Any) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            columns = ", ".join(values.keys())
            placeholders = ", ".join(f":{k}" for k in values)
            extra_cols = f", {columns}" if columns else ""
            extra_vals = f", {placeholders}" if placeholders else ""
            conn.execute(
                f"INSERT OR REPLACE INTO observation_sources (observation_id, fact_id{extra_cols}) "
                f"VALUES (:observation_id, :fact_id{extra_vals})",
                {"observation_id": observation_id, "fact_id": fact_id, **values},
            )

    def get_observation_sources(self, observation_id: str) -> list[sqlite3.Row]:
        return self._all(
            "SELECT * FROM observation_sources WHERE observation_id = ?",
            (observation_id,),
        )

    def lookup_translations(self, texts: list[str], *, version: int = 3) -> dict[str, str]:
        """Cached translations of ``texts`` as ``{source: english}`` (current translator version only).

        Read-only, so the web server can consult the cache without ever writing filings.db.
        """
        if not texts:
            return {}
        by_hash = {hashlib.sha256(t.encode("utf-8")).hexdigest(): t for t in texts}
        found: dict[str, str] = {}
        hashes = list(by_hash)
        for start in range(0, len(hashes), 500):
            chunk = hashes[start:start + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows = self._all(
                f"SELECT source_hash, translated_text FROM filing_translations WHERE source_hash IN ({placeholders}) AND translator_version = ?",
                (*chunk, version),
            )
            found.update({by_hash[r["source_hash"]]: r["translated_text"] for r in rows})
        return found

    def store_translations(self, translations: dict[str, str], *, version: int = 3) -> None:
        """Persist translated text keyed by source text."""
        if not translations:
            return
        stamp = now()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            for source, translated in translations.items():
                h = hashlib.sha256(source.encode("utf-8")).hexdigest()
                conn.execute(
                    "INSERT OR REPLACE INTO filing_translations (source_hash, source_text, translated_text, translator_version, created_at) VALUES (?, ?, ?, ?, ?)",
                    (h, source, translated, version, stamp),
                )

    def _one(self, query: str, params: tuple[Any, ...]) -> sqlite3.Row | None:
        conn = connect_read(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            return conn.execute(query, params).fetchone()
        finally:
            conn.close()

    def _all(self, query: str, params: tuple[Any, ...]) -> list[sqlite3.Row]:
        conn = connect_read(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            return list(conn.execute(query, params).fetchall())
        finally:
            conn.close()


__all__ = ["FilingCatalog", "FilingStorageError", "LegacyLayoutError", "ShardUnavailableError"]
