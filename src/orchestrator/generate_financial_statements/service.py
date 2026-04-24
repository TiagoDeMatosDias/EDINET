import logging
import os
import re
import sqlite3
import uuid

import pandas as pd

from src.orchestrator.common.sqlite import OrchestratorProcessorBase

logger = logging.getLogger("src.data_processing")

_DB_HELPER = OrchestratorProcessorBase()


_SOURCE_TABLE_NAME = "financialData_full"
_FINANCIAL_STATEMENTS_COLUMNS = [
    ("docID", "TEXT PRIMARY KEY"),
    ("edinetCode", "TEXT"),
    ("docTypeCode", "TEXT"),
    ("submitDateTime", "TEXT"),
    ("periodStart", "TEXT"),
    ("periodEnd", "TEXT"),
    ("release_id", "TEXT"),
]
_STATEMENT_TABLES = ("IncomeStatement", "BalanceSheet", "CashflowStatement")
_ALLOWED_CONTEXT_IDS = (
    "CurrentYearDuration",
    "CurrentYearInstant",
    "FilingDateInstant",
)
_DOCUMENT_BATCH_SIZE = 1000
_PROGRESS_LOG_INTERVAL = 100


def _clean_label(value):
    text = str(value or "").strip()
    return re.sub(r"\s+", " ", text)


def _extract_date_token(value):
    match = re.search(r"\d{4}-\d{2}-\d{2}", str(value or ""))
    return match.group(0) if match else None


def _ensure_case_insensitive_columns(helper, conn, table_name, columns):
    info = conn.execute(f"PRAGMA table_info({helper._sql_ident(table_name)})").fetchall()
    existing_by_lower = {str(row[1]).lower(): str(row[1]) for row in info}
    resolved = {}

    for column_name, column_type in columns:
        normalized_name = _clean_label(column_name)
        if not normalized_name:
            continue

        actual_name = existing_by_lower.get(normalized_name.lower())
        if actual_name:
            resolved[normalized_name.lower()] = actual_name
            continue

        conn.execute(
            f"ALTER TABLE {helper._sql_ident(table_name)} "
            f"ADD COLUMN {helper._sql_ident(normalized_name)} {column_type}"
        )
        existing_by_lower[normalized_name.lower()] = normalized_name
        resolved[normalized_name.lower()] = normalized_name

    return resolved


def _ensure_financial_statement_tables(helper, conn, overwrite=False):
    if overwrite:
        conn.executescript(
            """
            DROP TABLE IF EXISTS FinancialStatements;
            DROP TABLE IF EXISTS IncomeStatement;
            DROP TABLE IF EXISTS BalanceSheet;
            DROP TABLE IF EXISTS CashflowStatement;
            """
        )

    fs_cols_sql = ",\n          ".join(
        f'"{name}" {col_type}' for name, col_type in _FINANCIAL_STATEMENTS_COLUMNS
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS FinancialStatements (\n"
        f"          {fs_cols_sql}\n"
        "        )"
    )
    helper._ensure_typed_table_columns(
        conn,
        "FinancialStatements",
        [(name, column_type) for name, column_type in _FINANCIAL_STATEMENTS_COLUMNS if name != "docID"],
    )

    for table_name in _STATEMENT_TABLES:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS \"{table_name}\" (\"docID\" TEXT PRIMARY KEY)"
        )


def _resolve_source_table(helper, conn, source_schema, source_table):
    source_actual = helper._resolve_table_name_in_schema(conn, source_schema, source_table)
    if not source_actual:
        raise ValueError(
            f"source_table {source_table!r} was not found in schema {source_schema!r}."
        )
    return source_actual


def _source_table_ref(helper, source_schema, source_table):
    return f"{helper._sql_ident(source_schema)}.{helper._sql_ident(source_table)}"


def _iter_doc_batches(doc_ids, batch_size):
    for start in range(0, len(doc_ids), batch_size):
        yield doc_ids[start : start + batch_size]


def _load_release_catalog(conn, taxonomy_table_name):
    rows = conn.execute(
        f"""
        SELECT DISTINCT CAST(release_id AS TEXT) AS release_id
        FROM {taxonomy_table_name}
        WHERE release_id IS NOT NULL
        ORDER BY CAST(release_id AS TEXT)
        """
    ).fetchall()
    if not rows:
        raise ValueError("Target_Database must contain Taxonomy rows before generating financial statements.")

    return [
        {
            "release_id": str(row[0]),
            "date_token": _extract_date_token(row[0]),
        }
        for row in rows
    ]


def _resolve_release_id(submit_datetime, release_catalog):
    if not release_catalog:
        return None

    submit_token = _extract_date_token(submit_datetime)
    if not submit_token:
        return release_catalog[-1]["release_id"]

    eligible = [
        entry
        for entry in release_catalog
        if entry.get("date_token") and entry["date_token"] <= submit_token
    ]
    if eligible:
        return eligible[-1]["release_id"]

    dated = [entry for entry in release_catalog if entry.get("date_token")]
    if dated:
        logger.warning(
            "No taxonomy release on or before submitDateTime=%s; falling back to earliest available release %s.",
            submit_datetime,
            dated[0]["release_id"],
        )
        return dated[0]["release_id"]

    return release_catalog[0]["release_id"]


def _fetch_pending_doc_ids(helper, conn, source_schema, source_table):
    source_ref = _source_table_ref(helper, source_schema, source_table)
    col_names = helper._resolve_source_col_names(conn, source_schema, source_table)
    doc_col = col_names.get("docID") or "docID"
    submit_col = col_names.get("submitDateTime")
    submit_expr = helper._source_column_expr("s", submit_col)

    rows = conn.execute(
        f"""
        SELECT CAST(s.{helper._sql_ident(doc_col)} AS TEXT) AS docID
        FROM {source_ref} s
        LEFT JOIN {helper._sql_ident('FinancialStatements')} fs
            ON CAST(s.{helper._sql_ident(doc_col)} AS TEXT) = fs.{helper._sql_ident('docID')}
        WHERE s.{helper._sql_ident(doc_col)} IS NOT NULL
          AND fs.{helper._sql_ident('docID')} IS NULL
        GROUP BY CAST(s.{helper._sql_ident(doc_col)} AS TEXT)
        ORDER BY
            COALESCE(MIN(CAST({submit_expr} AS TEXT)), ''),
            CAST(s.{helper._sql_ident(doc_col)} AS TEXT)
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def _load_metadata_batch(helper, conn, source_schema, source_table, doc_ids):
    if not doc_ids:
        return pd.DataFrame(
            columns=[
                "docID",
                "edinetCode",
                "docTypeCode",
                "submitDateTime",
                "periodStart",
                "periodEnd",
            ]
        )

    source_ref = _source_table_ref(helper, source_schema, source_table)
    col_names = helper._resolve_source_col_names(conn, source_schema, source_table)
    doc_col = col_names.get("docID") or "docID"
    placeholders = ", ".join("?" for _ in doc_ids)

    return pd.read_sql_query(
        f"""
        SELECT
            CAST(s.{helper._sql_ident(doc_col)} AS TEXT) AS docID,
            MAX(CAST({helper._source_column_expr('s', col_names.get('edinetCode'))} AS TEXT)) AS edinetCode,
            MAX(CAST({helper._source_column_expr('s', col_names.get('docTypeCode'))} AS TEXT)) AS docTypeCode,
            MAX(CAST({helper._source_column_expr('s', col_names.get('submitDateTime'))} AS TEXT)) AS submitDateTime,
            MIN(CAST({helper._source_column_expr('s', col_names.get('periodStart'))} AS TEXT)) AS periodStart,
            MAX(CAST({helper._source_column_expr('s', col_names.get('periodEnd'))} AS TEXT)) AS periodEnd
        FROM {source_ref} s
        WHERE s.{helper._sql_ident(doc_col)} IN ({placeholders})
        GROUP BY CAST(s.{helper._sql_ident(doc_col)} AS TEXT)
        ORDER BY CAST(s.{helper._sql_ident(doc_col)} AS TEXT)
        """,
        conn,
        params=list(doc_ids),
    )


def _load_taxonomy_bundle(conn, helper, taxonomy_table_name, release_id, granularity_level):
    rows = conn.execute(
        f"""
        SELECT
            CAST(statement_family AS TEXT) AS statement_family,
            CAST(concept_qname AS TEXT) AS concept_qname,
            CAST(primary_label_en AS TEXT) AS primary_label_en
        FROM {taxonomy_table_name}
        WHERE release_id = ?
          AND statement_family IN ('IncomeStatement', 'BalanceSheet', 'CashflowStatement')
          AND value_type = 'number'
          AND level <= ?
        ORDER BY statement_family, level, primary_label_en, concept_qname
        """,
        (str(release_id), int(granularity_level)),
    ).fetchall()
    if not rows:
        raise ValueError(
            f"No Taxonomy rows were found for release_id={release_id!r} at granularity_level={granularity_level}."
        )

    groups_by_family = {family: {} for family in _STATEMENT_TABLES}

    for row in rows:
        statement_family = str(row["statement_family"] or "")
        concept_qname = helper._normalise_taxonomy_term(row["concept_qname"]) or str(row["concept_qname"] or "")
        display_label = _clean_label(row["primary_label_en"])

        if statement_family not in _STATEMENT_TABLES or not concept_qname or not display_label:
            continue

        label_key = display_label.lower()
        family_groups = groups_by_family[statement_family]
        group = family_groups.get(label_key)
        if group is None:
            group = {
                "label_key": label_key,
                "display_label": display_label,
                "column_name": display_label,
                "concepts": [],
            }
            family_groups[label_key] = group

        if concept_qname not in group["concepts"]:
            group["concepts"].append(concept_qname)

    concept_set = {
        concept_qname
        for family_groups in groups_by_family.values()
        for group in family_groups.values()
        for concept_qname in group["concepts"]
    }
    return {
        "groups_by_family": groups_by_family,
        "concept_set": concept_set,
    }


def _build_taxonomy_mapping_frame(taxonomy_bundle):
    rows = []
    for statement_family, family_groups in taxonomy_bundle["groups_by_family"].items():
        for group in family_groups.values():
            for concept_qname in group["concepts"]:
                rows.append(
                    {
                        "statement_family": statement_family,
                        "concept_qname": concept_qname,
                        "column_name": group["column_name"],
                    }
                )

    return pd.DataFrame(rows, columns=["statement_family", "concept_qname", "column_name"])


def _synchronize_statement_tables(helper, conn, taxonomy_bundle):
    for statement_family in _STATEMENT_TABLES:
        group_columns = [
            (group["display_label"], "REAL")
            for group in taxonomy_bundle["groups_by_family"][statement_family].values()
        ]
        resolved_columns = _ensure_case_insensitive_columns(
            helper,
            conn,
            statement_family,
            group_columns,
        )
        for group in taxonomy_bundle["groups_by_family"][statement_family].values():
            group["column_name"] = resolved_columns.get(
                group["label_key"],
                group["display_label"],
            )


def _load_fact_batch(helper, conn, source_schema, source_table, doc_ids):
    if not doc_ids:
        return pd.DataFrame(columns=["docID", "concept_qname", "value"])

    source_ref = _source_table_ref(helper, source_schema, source_table)
    col_names = helper._resolve_source_col_names(conn, source_schema, source_table)
    doc_col = col_names.get("docID") or "docID"
    term_col = col_names.get("AccountingTerm") or "AccountingTerm"
    period_col = col_names.get("Period") or "Period"
    amount_col = col_names.get("Amount") or "Amount"
    doc_placeholders = ", ".join("?" for _ in doc_ids)
    context_placeholders = ", ".join("?" for _ in _ALLOWED_CONTEXT_IDS)

    df = pd.read_sql_query(
        f"""
        SELECT
            CAST(s.{helper._sql_ident(doc_col)} AS TEXT) AS docID,
            CAST(s.{helper._sql_ident(term_col)} AS TEXT) AS concept_qname,
            CAST(s.{helper._sql_ident(amount_col)} AS TEXT) AS raw_amount
        FROM {source_ref} s
        WHERE s.{helper._sql_ident(doc_col)} IN ({doc_placeholders})
          AND s.{helper._sql_ident(period_col)} IN ({context_placeholders})
          AND s.{helper._sql_ident(term_col)} IS NOT NULL
        """,
        conn,
        params=[*doc_ids, *_ALLOWED_CONTEXT_IDS],
    )
    if df.empty:
        return pd.DataFrame(columns=["docID", "concept_qname", "value"])

    concept_map = {
        value: helper._normalise_taxonomy_term(value) or str(value or "")
        for value in df["concept_qname"].dropna().unique()
    }
    df["concept_qname"] = df["concept_qname"].map(concept_map)

    amount_map = {
        value: helper._try_real(value)
        for value in df["raw_amount"].dropna().unique()
    }
    df["value"] = df["raw_amount"].map(amount_map)
    return df.loc[df["value"].notna(), ["docID", "concept_qname", "value"]]


def _build_statement_batch_frames(metadata_batch_df, facts_batch_df, taxonomy_bundle):
    doc_id_frame = metadata_batch_df[["docID"]].drop_duplicates().reset_index(drop=True)
    mapping_df = taxonomy_bundle["mapping_df"]

    aggregated = pd.DataFrame(columns=["statement_family", "docID", "column_name", "value"])
    if not facts_batch_df.empty and not mapping_df.empty:
        filtered = facts_batch_df.loc[
            facts_batch_df["concept_qname"].isin(taxonomy_bundle["concept_set"])
        ]
        if not filtered.empty:
            merged = filtered.merge(mapping_df, on="concept_qname", how="inner")
            if not merged.empty:
                aggregated = (
                    merged.groupby(["statement_family", "docID", "column_name"], as_index=False)["value"]
                    .sum()
                )

    statement_frames = {}
    for statement_family in _STATEMENT_TABLES:
        family_frame = doc_id_frame.copy()
        if not aggregated.empty:
            family_agg = aggregated.loc[aggregated["statement_family"] == statement_family]
        else:
            family_agg = pd.DataFrame(columns=["docID", "column_name", "value"])

        if not family_agg.empty:
            wide = family_agg.pivot(index="docID", columns="column_name", values="value").reset_index()
            wide.columns.name = None
            family_frame = family_frame.merge(wide, on="docID", how="left")

        statement_frames[statement_family] = family_frame

    return statement_frames


def _bulk_replace_rows(conn, helper, table_name, dataframe):
    if dataframe is None or dataframe.empty:
        return

    frame = dataframe.copy()
    if "docID" in frame.columns:
        frame = frame.drop_duplicates(subset=["docID"], keep="last")

    temp_name = f"_tmp_{table_name}_{uuid.uuid4().hex[:8]}"
    columns = list(frame.columns)
    columns_sql = ", ".join(helper._sql_ident(column_name) for column_name in columns)

    frame.to_sql(temp_name, conn, if_exists="replace", index=False)
    try:
        conn.execute(
            f"INSERT OR REPLACE INTO {helper._sql_ident(table_name)} ({columns_sql}) "
            f"SELECT {columns_sql} FROM {helper._sql_ident(temp_name)}"
        )
    finally:
        conn.execute(f"DROP TABLE IF EXISTS {helper._sql_ident(temp_name)}")


def generate_financial_statements(
    source_database,
    target_database,
    granularity_level,
    overwrite=False,
    helper=None,
):
    """Generate taxonomy-backed financial statement tables from raw EDINET rows."""
    helper = helper or _DB_HELPER
    source_db = source_database
    target_db = target_database
    if not source_db:
        raise ValueError("source_database is required for generate_financial_statements.")
    if not target_db:
        raise ValueError("target_database is required for generate_financial_statements.")
    try:
        granularity = max(int(granularity_level), 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("granularity_level must be an integer for generate_financial_statements.") from exc

    same_db = os.path.abspath(source_db) == os.path.abspath(target_db)
    conn = sqlite3.connect(target_db)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        source_schema = "main"
        if not same_db:
            conn.execute("ATTACH DATABASE ? AS src", (source_db,))
            source_schema = "src"

        source_actual = _resolve_source_table(helper, conn, source_schema, _SOURCE_TABLE_NAME)
        taxonomy_actual = helper._resolve_table_name_in_schema(conn, "main", "Taxonomy")
        if not taxonomy_actual:
            raise ValueError("Target_Database must contain a Taxonomy table before generating financial statements.")

        _ensure_financial_statement_tables(helper, conn, overwrite=overwrite)
        release_catalog = _load_release_catalog(conn, helper._sql_ident(taxonomy_actual))
        pending_doc_ids = _fetch_pending_doc_ids(helper, conn, source_schema, source_actual)
        total_documents = len(pending_doc_ids)
        taxonomy_cache = {}
        processed_documents = 0

        logger.info(
            "Generating financial statements for %d pending docID(s) at granularity_level=%d.",
            total_documents,
            granularity,
        )

        for doc_batch in _iter_doc_batches(pending_doc_ids, _DOCUMENT_BATCH_SIZE):
            metadata_batch_df = _load_metadata_batch(
                helper,
                conn,
                source_schema,
                source_actual,
                doc_batch,
            )
            if metadata_batch_df.empty:
                continue

            metadata_batch_df["release_id"] = metadata_batch_df["submitDateTime"].map(
                lambda value: _resolve_release_id(value, release_catalog)
            )
            if metadata_batch_df["release_id"].isna().any():
                unresolved = metadata_batch_df.loc[metadata_batch_df["release_id"].isna(), "docID"].tolist()
                raise ValueError(f"No taxonomy release could be resolved for docID(s)={unresolved!r}.")

            facts_batch_df = _load_fact_batch(
                helper,
                conn,
                source_schema,
                source_actual,
                metadata_batch_df["docID"].tolist(),
            )

            financial_statement_frames = []
            statement_frames_by_family = {statement_family: [] for statement_family in _STATEMENT_TABLES}

            for release_id, release_metadata_df in metadata_batch_df.groupby("release_id", sort=False):
                cache_key = (str(release_id), granularity)
                taxonomy_bundle = taxonomy_cache.get(cache_key)
                if taxonomy_bundle is None:
                    taxonomy_bundle = _load_taxonomy_bundle(
                        conn,
                        helper,
                        helper._sql_ident(taxonomy_actual),
                        release_id,
                        granularity,
                    )
                    _synchronize_statement_tables(helper, conn, taxonomy_bundle)
                    taxonomy_bundle["mapping_df"] = _build_taxonomy_mapping_frame(taxonomy_bundle)
                    taxonomy_cache[cache_key] = taxonomy_bundle

                release_doc_ids = release_metadata_df["docID"].tolist()
                if facts_batch_df.empty:
                    release_facts_df = pd.DataFrame(columns=["docID", "concept_qname", "value"])
                else:
                    release_facts_df = facts_batch_df.loc[
                        facts_batch_df["docID"].isin(release_doc_ids)
                    ].copy()

                financial_statement_frames.append(
                    release_metadata_df[
                        [
                            "docID",
                            "edinetCode",
                            "docTypeCode",
                            "submitDateTime",
                            "periodStart",
                            "periodEnd",
                            "release_id",
                        ]
                    ].copy()
                )

                release_statement_frames = _build_statement_batch_frames(
                    release_metadata_df,
                    release_facts_df,
                    taxonomy_bundle,
                )
                for statement_family, frame in release_statement_frames.items():
                    statement_frames_by_family[statement_family].append(frame)

            _bulk_replace_rows(
                conn,
                helper,
                "FinancialStatements",
                pd.concat(financial_statement_frames, ignore_index=True),
            )
            for statement_family, frames in statement_frames_by_family.items():
                _bulk_replace_rows(
                    conn,
                    helper,
                    statement_family,
                    pd.concat(frames, ignore_index=True),
                )

            processed_documents += len(metadata_batch_df)
            conn.commit()

            if processed_documents % _PROGRESS_LOG_INTERVAL == 0 or processed_documents == total_documents:
                logger.info(
                    "Processed %d/%d docID(s) for generate_financial_statements (%d remaining).",
                    processed_documents,
                    total_documents,
                    total_documents - processed_documents,
                )

        conn.commit()
        logger.info(
            "Completed generate_financial_statements: %d document(s) processed.",
            processed_documents,
        )
        return {
            "status": "completed",
            "documents_processed": processed_documents,
            "granularity_level": granularity,
        }
    finally:
        conn.close()
