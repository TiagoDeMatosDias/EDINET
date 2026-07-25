"""Rebuildable filing catalog and normalized XBRL index."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

from src.orchestrator.common.sqlite import connect_write, transaction


class FilingCatalog:
    """Persist filing metadata separately from the immutable archive."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 30_000) -> None:
        self.path = Path(path).expanduser()
        self.busy_timeout_ms = busy_timeout_ms
        self.initialize()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(
                """
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
                    archive_path TEXT,
                    archive_content BLOB,
                    archive_sha256 TEXT,
                    archive_size INTEGER,
                    status TEXT NOT NULL,
                    parse_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_filings_company_date
                    ON filings(edinet_code, submitted_at DESC);
                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
                    member_path TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    content BLOB,
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
                    is_nil INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(doc_id, artifact_id, concept, context_id, unit_id, value_text)
                );
                CREATE INDEX IF NOT EXISTS idx_facts_doc_concept
                    ON xbrl_facts(doc_id, concept);
                CREATE TABLE IF NOT EXISTS sections (
                    section_id TEXT PRIMARY KEY,
                    doc_id TEXT NOT NULL REFERENCES filings(doc_id) ON DELETE CASCADE,
                    artifact_id TEXT,
                    ordinal INTEGER NOT NULL,
                    title TEXT,
                    text TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_sections_doc ON sections(doc_id, ordinal);
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
                CREATE VIRTUAL TABLE IF NOT EXISTS section_search USING fts5(
                    section_id UNINDEXED, doc_id UNINDEXED, title, text
                );
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
                CREATE INDEX IF NOT EXISTS idx_obs_company_metric
                    ON observations(company_code, metric_id, period_end);
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
            )
            trans_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(filing_translations)").fetchall()
            }
            if "translator_version" not in trans_columns:
                conn.execute("ALTER TABLE filing_translations ADD COLUMN translator_version INTEGER NOT NULL DEFAULT 1")
            artifact_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(artifacts)").fetchall()
            }
            if "content" not in artifact_columns:
                conn.execute("ALTER TABLE artifacts ADD COLUMN content BLOB")
            filing_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(filings)").fetchall()
            }
            if "archive_content" not in filing_columns:
                conn.execute("ALTER TABLE filings ADD COLUMN archive_content BLOB")
            conn.commit()
        finally:
            conn.close()

    def upsert_filing(self, values: dict[str, Any]) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO filings
                (doc_id, edinet_code, submitter_name, period_start, period_end,
                 submitted_at, form_code, doc_type_code, xbrl_flag, csv_flag,
                 archive_path, archive_content, archive_sha256, archive_size, status, parse_error,
                 created_at, updated_at)
                VALUES (:doc_id, :edinet_code, :submitter_name, :period_start,
                        :period_end, :submitted_at, :form_code, :doc_type_code,
                        :xbrl_flag, :csv_flag, :archive_path, :archive_content, :archive_sha256,
                        :archive_size, :status, :parse_error, :created_at, :updated_at)
                ON CONFLICT(doc_id) DO UPDATE SET
                  edinet_code=excluded.edinet_code, submitter_name=excluded.submitter_name,
                  period_start=excluded.period_start, period_end=excluded.period_end,
                  submitted_at=excluded.submitted_at, form_code=excluded.form_code,
                  doc_type_code=excluded.doc_type_code, xbrl_flag=excluded.xbrl_flag,
                  csv_flag=excluded.csv_flag, archive_path=excluded.archive_path,
                  archive_content=excluded.archive_content,
                  archive_sha256=excluded.archive_sha256, archive_size=excluded.archive_size,
                  status=excluded.status, parse_error=excluded.parse_error,
                  updated_at=excluded.updated_at""",
                values,
            )

    def replace_parsed_content(
        self,
        doc_id: str,
        artifacts: Iterable[dict[str, Any]],
        contexts: Iterable[dict[str, Any]],
        units: Iterable[dict[str, Any]],
        facts: Iterable[dict[str, Any]],
        sections: Iterable[dict[str, Any]],
    ) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute("DELETE FROM section_search WHERE doc_id = ?", (doc_id,))
            for table in ("sections", "xbrl_facts", "xbrl_units", "xbrl_contexts", "artifacts"):
                conn.execute(f"DELETE FROM {table} WHERE doc_id = ?", (doc_id,))
            conn.executemany(
                "INSERT INTO artifacts(artifact_id, doc_id, member_path, kind, media_type, size_bytes, sha256, content) VALUES (:artifact_id, :doc_id, :member_path, :kind, :media_type, :size_bytes, :sha256, :content)",
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
                facts,
            )
            conn.executemany(
                "INSERT INTO sections(section_id, doc_id, artifact_id, ordinal, title, text) VALUES (:section_id, :doc_id, :artifact_id, :ordinal, :title, :text)",
                sections,
            )
            conn.executemany(
                "INSERT INTO section_search(section_id, doc_id, title, text) VALUES (:section_id, :doc_id, :title, :text)",
                sections,
            )

    def replace_quality_issues(self, doc_id: str, issues: list[dict[str, Any]]) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute("DELETE FROM quality_issues WHERE doc_id = ?", (doc_id,))
            conn.executemany(
                """INSERT INTO quality_issues
                   (issue_id, doc_id, severity, code, message, fact_id, created_at)
                   VALUES (:issue_id, :doc_id, :severity, :code, :message, :fact_id, :created_at)""",
                issues,
            )

    def list_quality_issues(self, doc_id: str) -> list[sqlite3.Row]:
        return self._all(
            "SELECT * FROM quality_issues WHERE doc_id = ? ORDER BY severity DESC, issue_id",
            (doc_id,),
        )

    def set_status(self, doc_id: str, status: str, parse_error: str | None, updated_at: str) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                "UPDATE filings SET status = ?, parse_error = ?, updated_at = ? WHERE doc_id = ?",
                (status, parse_error, updated_at, doc_id),
            )

    def get_filing(self, doc_id: str) -> sqlite3.Row | None:
        return self._one("SELECT * FROM filings WHERE doc_id = ?", (doc_id,))

    def list_company(self, edinet_code: str, limit: int = 100, offset: int = 0) -> list[sqlite3.Row]:
        return self._all(
            "SELECT * FROM filings WHERE edinet_code = ? ORDER BY submitted_at DESC LIMIT ? OFFSET ?",
            (edinet_code, max(1, min(limit, 500)), max(0, offset)),
        )

    def list_recent(self, company_code: str | None = None, limit: int = 100, offset: int = 0) -> list[sqlite3.Row]:
        cap = max(1, min(limit, 500))
        if company_code:
            return self._all(
                "SELECT * FROM filings WHERE edinet_code = ? ORDER BY submitted_at DESC LIMIT ? OFFSET ?",
                (company_code, cap, max(0, offset)),
            )
        return self._all(
            "SELECT * FROM filings ORDER BY submitted_at DESC LIMIT ? OFFSET ?",
            (cap, max(0, offset)),
        )

    def coverage(self) -> list[sqlite3.Row]:
        return self._all(
            "SELECT status, COUNT(*) AS filing_count, COUNT(DISTINCT edinet_code) AS company_count FROM filings GROUP BY status ORDER BY status",
            (),
        )

    def list_artifacts(self, doc_id: str) -> list[sqlite3.Row]:
        return self._all("SELECT artifact_id, doc_id, member_path, kind, media_type, size_bytes, sha256 FROM artifacts WHERE doc_id = ? ORDER BY member_path", (doc_id,))

    def get_artifact_content(self, artifact_id: str) -> sqlite3.Row | None:
        """Return artifact metadata plus content BLOB."""
        return self._one("SELECT * FROM artifacts WHERE artifact_id = ?", (artifact_id,))

    def list_facts(self, doc_id: str, concept: str | None = None, limit: int = 500) -> list[sqlite3.Row]:
        if concept:
            return self._all(
                "SELECT * FROM xbrl_facts WHERE doc_id = ? AND concept LIKE ? ORDER BY concept LIMIT ?",
                (doc_id, concept, max(1, min(limit, 5000))),
            )
        return self._all(
            "SELECT * FROM xbrl_facts WHERE doc_id = ? ORDER BY concept LIMIT ?",
            (doc_id, max(1, min(limit, 5000))),
        )

    def list_sections(self, doc_id: str, query: str | None = None, limit: int = 200) -> list[sqlite3.Row]:
        cap = max(1, min(limit, 1000))
        normalized_query = query.strip() if query else ""
        if normalized_query:
            fts_phrase = '"' + normalized_query.replace('"', '""') + '"'
            return self._all(
                """SELECT s.* FROM section_search f JOIN sections s ON s.section_id = f.section_id
                   WHERE f.doc_id = ? AND section_search MATCH ? ORDER BY s.ordinal LIMIT ?""",
                (doc_id, fts_phrase, cap),
            )
        return self._all(
            "SELECT * FROM sections WHERE doc_id = ? ORDER BY ordinal LIMIT ?",
            (doc_id, cap),
        )

    def get_section(self, doc_id: str, section_id: str) -> sqlite3.Row | None:
        return self._one(
            "SELECT * FROM sections WHERE doc_id = ? AND section_id = ?",
            (doc_id, section_id),
        )

    def list_taxonomy(self, doc_id: str) -> list[sqlite3.Row]:
        facts = self._all(
            "SELECT DISTINCT namespace_uri, concept FROM xbrl_facts WHERE doc_id = ? AND namespace_uri IS NOT NULL ORDER BY namespace_uri, concept",
            (doc_id,),
        )
        return facts

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
        from datetime import datetime, timezone

        run_id = parse_run_id or str(uuid4())
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        completed = now if status in ("completed", "failed") else None
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO parse_runs
                   (parse_run_id, doc_id, parser_version, status, started_at, completed_at,
                    fact_count, section_count, warning_count, error_message)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, doc_id, parser_version, status, now, completed, fact_count, section_count, warning_count, error_message),
            )
            if status == "completed":
                conn.execute(
                    "UPDATE filings SET updated_at = ? WHERE doc_id = ?",
                    (now, doc_id),
                )
        return run_id

    def latest_parse_run(self, doc_id: str) -> sqlite3.Row | None:
        return self._one(
            "SELECT * FROM parse_runs WHERE doc_id = ? ORDER BY started_at DESC LIMIT 1",
            (doc_id,),
        )

    def upsert_watermark(self, source_name: str, **kwargs: Any) -> None:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            columns = ", ".join(kwargs.keys())
            placeholders = ", ".join(f":{k}" for k in kwargs)
            conn.execute(
                f"""INSERT INTO data_watermarks (source_name, refreshed_at, {columns})
                    VALUES (:source_name, :refreshed_at, {placeholders})
                    ON CONFLICT(source_name) DO UPDATE SET
                    refreshed_at=excluded.refreshed_at, {", ".join(f"{k}=excluded.{k}" for k in kwargs)}""",
                {"source_name": source_name, "refreshed_at": now, **kwargs},
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

    def quality_summary(self) -> list[sqlite3.Row]:
        return self._all(
            "SELECT severity, code, COUNT(*) AS count FROM quality_issues GROUP BY severity, code ORDER BY severity, count DESC",
            (),
        )

    def quality_coverage(self) -> dict[str, Any]:
        total = self._all("SELECT COUNT(*) AS cnt FROM filings", ())
        parsed = self._all("SELECT COUNT(*) AS cnt FROM filings WHERE status = 'parsed'", ())
        with_issues = self._all("SELECT COUNT(DISTINCT doc_id) AS cnt FROM quality_issues", ())
        return {
            "total_filings": int(total[0]["cnt"]) if total else 0,
            "parsed_filings": int(parsed[0]["cnt"]) if parsed else 0,
            "filings_with_issues": int(with_issues[0]["cnt"]) if with_issues else 0,
        }

    def lookup_translations(self, texts: list[str], *, version: int = 2) -> dict[str, str]:
        """Return cached translations for a batch of source texts (only current translator version)."""
        import hashlib

        if not texts:
            return {}
        hashes = [hashlib.sha256(t.encode("utf-8")).hexdigest() for t in texts]
        placeholders = ",".join("?" for _ in hashes)
        rows = self._all(
            f"SELECT source_hash, translated_text FROM filing_translations WHERE source_hash IN ({placeholders}) AND translator_version = ?",
            [*hashes, version],
        )
        return {r["source_hash"]: r["translated_text"] for r in rows}

    def store_translations(self, translations: dict[str, str], *, version: int = 2) -> None:
        """Persist translated text keyed by source text."""
        import hashlib
        from datetime import datetime, timezone

        if not translations:
            return
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            for source, translated in translations.items():
                h = hashlib.sha256(source.encode("utf-8")).hexdigest()
                conn.execute(
                    "INSERT OR REPLACE INTO filing_translations (source_hash, source_text, translated_text, translator_version, created_at) VALUES (?, ?, ?, ?, ?)",
                    (h, source, translated, version, now),
                )

    def _one(self, query: str, params: tuple[Any, ...]) -> sqlite3.Row | None:
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            return conn.execute(query, params).fetchone()
        finally:
            conn.close()

    def _all(self, query: str, params: tuple[Any, ...]) -> list[sqlite3.Row]:
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            return list(conn.execute(query, params).fetchall())
        finally:
            conn.close()
