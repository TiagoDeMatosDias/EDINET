import logging
import sqlite3
import time

from src.orchestrator.common.sqlite import OrchestratorProcessorBase

logger = logging.getLogger("src.data_processing")

_DB_HELPER = OrchestratorProcessorBase()


def populate_business_descriptions_en(
    target_database,
    providers_config,
    table_name="FinancialStatements",
    docid_column="docID",
    source_column="DescriptionOfBusiness",
    target_column="DescriptionOfBusiness_EN",
    source_language="ja",
    target_language="en",
    overwrite=False,
    batch_size=25,
    helper=None,
):
    """Populate English business descriptions from configured translation APIs."""
    helper = helper or _DB_HELPER
    if not target_database:
        raise ValueError("target_database is required for populate_business_descriptions_en.")
    if not providers_config:
        raise ValueError("providers_config is required for populate_business_descriptions_en.")

    from src.orchestrator.populate_business_descriptions_en.description_translation import (
        TranslationError,
        load_translation_providers,
        translate_text_with_providers,
    )

    batch_size = max(int(batch_size or 25), 1)
    providers, provider_settings = load_translation_providers(providers_config)
    chunk_char_limit = provider_settings.get("chunk_char_limit", 700)
    row_delay_seconds = float(provider_settings.get("row_delay_seconds", 0.0) or 0.0)
    slow_provider_warning_seconds = float(
        provider_settings.get("slow_provider_warning_seconds", 10.0) or 10.0
    )
    provider_names = [getattr(provider, "name", type(provider).__name__) for provider in providers]

    conn = sqlite3.connect(target_database)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        actual_table = helper._resolve_table_name_in_schema(conn, "main", table_name)
        if not actual_table:
            raise RuntimeError(f"Table '{table_name}' not found in target database.")

        actual_docid = helper._resolve_column_name(conn, actual_table, docid_column)
        actual_source = helper._resolve_column_name(conn, actual_table, source_column)
        if not actual_docid:
            raise RuntimeError(f"Column '{docid_column}' not found in table '{actual_table}'.")
        if not actual_source:
            raise RuntimeError(f"Column '{source_column}' not found in table '{actual_table}'.")

        actual_target = helper._resolve_column_name(conn, actual_table, target_column)
        if not actual_target:
            helper._ensure_typed_table_columns(conn, actual_table, [(target_column, "TEXT")])
            actual_target = target_column
            conn.commit()

        actual_company = helper._resolve_column_name(conn, actual_table, "edinetCode")
        actual_period_end = helper._resolve_column_name(conn, actual_table, "periodEnd")

        logger.info(
            "Populate Business Descriptions EN starting: table=%s source=%s target=%s overwrite=%s batch_size=%d providers=%s chunk_char_limit=%s row_delay_seconds=%.2f",
            actual_table,
            actual_source,
            actual_target,
            overwrite,
            batch_size,
            provider_names,
            chunk_char_limit,
            row_delay_seconds,
        )

        existing_translation_count = 0
        if not overwrite:
            existing_translation_count = conn.execute(
                f"SELECT COUNT(*) FROM {helper._sql_ident(actual_table)} "
                f"WHERE {helper._sql_ident(actual_target)} IS NOT NULL "
                f"AND TRIM(CAST({helper._sql_ident(actual_target)} AS TEXT)) <> ''"
            ).fetchone()[0]

        translated_rows = 0
        failed_rows = 0
        processed_rows = 0
        provider_usage = {}
        stopped_early = False
        stop_reason = ""

        attempted_docids = helper._sql_ident("_tmp_desc_translation_attempted")
        conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS {attempted_docids} (docID TEXT PRIMARY KEY)")
        conn.execute(f"DELETE FROM {attempted_docids}")

        def _qualified_column(column_name, alias=None):
            qualified = helper._sql_ident(column_name)
            if alias:
                return f"{alias}.{qualified}"
            return qualified

        def _nonblank_target_expr(alias=None):
            qualified_target = _qualified_column(actual_target, alias)
            return (
                f"{qualified_target} IS NOT NULL "
                f"AND TRIM(CAST({qualified_target} AS TEXT)) <> ''"
            )

        def _company_key_expr(alias=None):
            qualified_docid = f"CAST({_qualified_column(actual_docid, alias)} AS TEXT)"
            if not actual_company:
                return qualified_docid
            qualified_company = _qualified_column(actual_company, alias)
            return (
                f"COALESCE(NULLIF(TRIM(CAST({qualified_company} AS TEXT)), ''), "
                f"{qualified_docid})"
            )

        company_key_base_expr = _company_key_expr()
        eligible_where_clauses = [
            f"{helper._sql_ident(actual_docid)} IS NOT NULL",
            f"{helper._sql_ident(actual_source)} IS NOT NULL",
            f"TRIM(CAST({helper._sql_ident(actual_source)} AS TEXT)) <> ''",
        ]
        if not overwrite:
            eligible_where_clauses.append(
                f"({helper._sql_ident(actual_target)} IS NULL "
                f"OR TRIM(CAST({helper._sql_ident(actual_target)} AS TEXT)) = '')"
            )
        eligible_row_count = conn.execute(
            f"SELECT COUNT(*) FROM {helper._sql_ident(actual_table)} "
            f"WHERE {' AND '.join(eligible_where_clauses)}"
        ).fetchone()[0]
        eligible_company_count = conn.execute(
            f"SELECT COUNT(*) FROM ("
            f"SELECT {company_key_base_expr} AS company_key "
            f"FROM {helper._sql_ident(actual_table)} "
            f"WHERE {' AND '.join(eligible_where_clauses)} "
            f"GROUP BY company_key"
            f")"
        ).fetchone()[0]
        logger.info(
            "Populate Business Descriptions EN found %d eligible row(s) across %d company(s); existing_translation_rows=%d",
            eligible_row_count,
            eligible_company_count,
            existing_translation_count,
        )
        if eligible_row_count == 0:
            logger.info("Populate Business Descriptions EN has no rows to translate.")

        batch_index = 0
        attempted_company_keys: set[str] = set()
        updated_company_keys: set[str] = set()
        while True:
            where_clauses = [
                f"t.{helper._sql_ident(actual_docid)} IS NOT NULL",
                f"t.{helper._sql_ident(actual_source)} IS NOT NULL",
                f"TRIM(CAST(t.{helper._sql_ident(actual_source)} AS TEXT)) <> ''",
                f"attempted.docID IS NULL",
            ]
            params = []
            if not overwrite:
                where_clauses.append(
                    f"(t.{helper._sql_ident(actual_target)} IS NULL "
                    f"OR TRIM(CAST(t.{helper._sql_ident(actual_target)} AS TEXT)) = '')"
                )

            base_from = (
                f"FROM {helper._sql_ident(actual_table)} t "
                f"LEFT JOIN {attempted_docids} attempted "
                f"ON attempted.docID = t.{helper._sql_ident(actual_docid)}"
            )

            if actual_company and actual_period_end:
                company_key_expr = _company_key_expr("t")
                period_null_sort_expr = (
                    f"CASE WHEN t.{helper._sql_ident(actual_period_end)} IS NULL "
                    f"OR TRIM(CAST(t.{helper._sql_ident(actual_period_end)} AS TEXT)) = '' THEN 1 ELSE 0 END"
                )
                period_sort_expr = f"CAST(t.{helper._sql_ident(actual_period_end)} AS TEXT)"
                docid_sort_expr = f"CAST(t.{helper._sql_ident(actual_docid)} AS TEXT)"
                sql = f"""
                WITH company_status AS (
                    SELECT
                        {_company_key_expr("base")} AS company_key,
                        MAX(CASE WHEN {_nonblank_target_expr("base")} THEN 1 ELSE 0 END) AS company_has_translation
                    FROM {helper._sql_ident(actual_table)} base
                    GROUP BY company_key
                ),
                eligible AS (
                    SELECT
                        t.{helper._sql_ident(actual_docid)} AS doc_id,
                        t.{helper._sql_ident(actual_source)} AS source_text,
                        t.{helper._sql_ident(actual_target)} AS current_target,
                        {company_key_expr} AS company_key,
                        COALESCE(status.company_has_translation, 0) AS company_has_translation,
                        ROW_NUMBER() OVER (
                            PARTITION BY {company_key_expr}
                            ORDER BY {period_null_sort_expr}, {period_sort_expr} DESC, {docid_sort_expr} DESC
                        ) AS company_report_rank,
                        {period_null_sort_expr} AS period_null_sort,
                        {period_sort_expr} AS period_sort_value
                        {base_from}
                        LEFT JOIN company_status status
                        ON status.company_key = {company_key_expr}
                    WHERE {' AND '.join(where_clauses)}
                )
                SELECT doc_id, source_text, current_target, company_key
                FROM eligible
                ORDER BY
                    CASE
                        WHEN company_report_rank = 1 AND company_has_translation = 0 THEN 0
                        WHEN company_report_rank = 1 AND company_has_translation = 1 THEN 1
                        WHEN company_has_translation = 0 THEN 2
                        ELSE 3
                    END,
                    period_null_sort,
                    period_sort_value DESC,
                    CAST(doc_id AS TEXT) DESC
                LIMIT ?
                """
            else:
                sql = (
                    f"SELECT t.{helper._sql_ident(actual_docid)}, "
                    f"t.{helper._sql_ident(actual_source)}, "
                    f"t.{helper._sql_ident(actual_target)}, "
                    f"CAST(t.{helper._sql_ident(actual_docid)} AS TEXT) "
                    f"{base_from} "
                    f"WHERE {' AND '.join(where_clauses)} "
                    f"ORDER BY CAST(t.{helper._sql_ident(actual_docid)} AS TEXT) DESC "
                    f"LIMIT ?"
                )
            params.append(batch_size)
            next_batch_number = batch_index + 1
            if next_batch_number == 1:
                logger.info("Populate Business Descriptions EN selecting initial batch.")
            selection_started_at = time.perf_counter()
            rows = conn.execute(sql, params).fetchall()
            selection_elapsed = time.perf_counter() - selection_started_at
            if next_batch_number == 1 or selection_elapsed >= 5.0:
                logger.info(
                    "Populate Business Descriptions EN selected %d row(s) for batch %d in %.2fs.",
                    len(rows),
                    next_batch_number,
                    selection_elapsed,
                )
            if not rows:
                break

            batch_index += 1
            batch_processed = 0
            batch_translated = 0
            batch_failed = 0
            batch_company_updates = 0

            stop_requested = False
            for doc_id, source_text, current_target, company_key in rows:
                if not overwrite and str(current_target or "").strip():
                    conn.execute(
                        f"INSERT OR IGNORE INTO {attempted_docids}(docID) VALUES (?)",
                        (doc_id,),
                    )
                    continue
                normalized_company_key = str(company_key or doc_id).strip() or str(doc_id)
                if normalized_company_key not in attempted_company_keys:
                    attempted_company_keys.add(normalized_company_key)
                    attempted_company_count = len(attempted_company_keys)
                    if attempted_company_count == 1 or attempted_company_count % 10 == 0:
                        logger.info(
                            "Populate Business Descriptions EN started company %d/%d (docID=%s).",
                            attempted_company_count,
                            eligible_company_count,
                            doc_id,
                        )
                provider_log_context = None
                provider_log_activity = False
                if attempted_company_count == 1 or attempted_company_count % 10 == 0:
                    provider_log_context = (
                        f"company {attempted_company_count}/{eligible_company_count} "
                        f"(docID={doc_id})"
                    )
                    provider_log_activity = True
                processed_rows += 1
                batch_processed += 1
                try:
                    translated_text, provider_name = translate_text_with_providers(
                        source_text,
                        providers,
                        source_language=source_language,
                        target_language=target_language,
                        chunk_char_limit=chunk_char_limit,
                        retire_failed_providers=True,
                        log_context=provider_log_context,
                        log_provider_activity=provider_log_activity,
                        slow_request_warning_seconds=slow_provider_warning_seconds,
                    )
                except TranslationError as exc:
                    failed_rows += 1
                    if not providers:
                        stopped_early = True
                        stop_reason = str(exc)
                        logger.warning(
                            "Stopping Populate Business Descriptions EN early after %d attempted row(s): %s",
                            processed_rows,
                            stop_reason,
                        )
                        stop_requested = True
                        break
                    conn.execute(
                        f"INSERT OR IGNORE INTO {attempted_docids}(docID) VALUES (?)",
                        (doc_id,),
                    )
                    batch_failed += 1
                    logger.warning(
                        "Could not translate %s.%s for %s=%s: %s",
                        actual_table,
                        actual_target,
                        actual_docid,
                        doc_id,
                        exc,
                    )
                    continue

                clean_translation = str(translated_text or "").strip()
                if not clean_translation:
                    failed_rows += 1
                    batch_failed += 1
                    continue

                conn.execute(
                    f"UPDATE {helper._sql_ident(actual_table)} "
                    f"SET {helper._sql_ident(actual_target)} = ? "
                    f"WHERE {helper._sql_ident(actual_docid)} = ?",
                    (clean_translation, doc_id),
                )
                translated_rows += 1
                batch_translated += 1
                provider_usage[provider_name] = provider_usage.get(provider_name, 0) + 1
                conn.execute(
                    f"INSERT OR IGNORE INTO {attempted_docids}(docID) VALUES (?)",
                    (doc_id,),
                )

                if normalized_company_key not in updated_company_keys:
                    updated_company_keys.add(normalized_company_key)
                    batch_company_updates += 1
                    updated_company_count = len(updated_company_keys)
                    if updated_company_count == 1 or updated_company_count % 10 == 0:
                        logger.info(
                            "Populate Business Descriptions EN updated %d/%d company(s).",
                            updated_company_count,
                            eligible_company_count,
                        )

                if row_delay_seconds > 0:
                    time.sleep(row_delay_seconds)

            conn.commit()
            logger.info(
                "Populate Business Descriptions EN progress: batch=%d batch_processed=%d batch_translated=%d batch_failed=%d batch_company_updates=%d total_processed=%d total_translated=%d total_failed=%d updated_companies=%d/%d active_providers=%s",
                batch_index,
                batch_processed,
                batch_translated,
                batch_failed,
                batch_company_updates,
                processed_rows,
                translated_rows,
                failed_rows,
                len(updated_company_keys),
                eligible_company_count,
                [getattr(provider, "name", type(provider).__name__) for provider in providers],
            )
            if stop_requested:
                break

        logger.info(
            "Populate Business Descriptions EN completed. Processed=%d translated=%d failed=%d existing=%d stopped_early=%s providers=%s",
            processed_rows,
            translated_rows,
            failed_rows,
            existing_translation_count,
            stopped_early,
            provider_usage,
        )
        return {
            "processed_rows": processed_rows,
            "translated_rows": translated_rows,
            "failed_rows": failed_rows,
            "existing_translation_rows": existing_translation_count,
            "provider_usage": provider_usage,
            "stopped_early": stopped_early,
            "stop_reason": stop_reason,
        }
    finally:
        conn.close()