import logging
import os
import random
import sqlite3

import numpy as np
import pandas as pd

from src.orchestrator.common.sqlite import OrchestratorProcessorBase

logger = logging.getLogger("src.data_processing")

_DB_HELPER = OrchestratorProcessorBase()


_PLACEHOLDER_RATIO_TABLES = ("PerShare", "Valuation", "Quality")


def _ensure_placeholder_ratio_tables(conn, overwrite=False):
    if overwrite:
        conn.executescript(
            """
            DROP TABLE IF EXISTS PerShare;
            DROP TABLE IF EXISTS Valuation;
            DROP TABLE IF EXISTS Quality;
            """
        )

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS PerShare (
          docID TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS Valuation (
          docID TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS Quality (
          docID TEXT PRIMARY KEY
        );
        """
    )


def _seed_placeholder_ratio_docids(helper, conn, source_schema):
    fs_actual = helper._resolve_table_name_in_schema(conn, source_schema, "FinancialStatements")
    if not fs_actual:
        return 0

    fs_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(fs_actual)}"
    for table_name in _PLACEHOLDER_RATIO_TABLES:
        conn.execute(
            f"INSERT OR IGNORE INTO {helper._sql_ident(table_name)}({helper._sql_ident('docID')}) "
            f"SELECT DISTINCT {helper._sql_ident('docID')} FROM {fs_ref} "
            f"WHERE {helper._sql_ident('docID')} IS NOT NULL"
        )
    return conn.execute(f"SELECT COUNT(*) FROM {fs_ref}").fetchone()[0]


def generate_ratios(
    source_database,
    target_database,
    formulas_config,
    overwrite=False,
    batch_size=5000,
    helper=None,
):
    """Scaffold placeholder ratio tables pending a full rework."""
    helper = helper or _DB_HELPER
    source_db = source_database
    target_db = target_database
    if not source_db:
        raise ValueError("source_database is required for generate_ratios.")
    if not target_db:
        raise ValueError("target_database is required for generate_ratios.")
    formulas_path = formulas_config
    if not formulas_path:
        raise ValueError("Formulas_Config is required for generate_ratios.")

    same_db = os.path.abspath(source_db) == os.path.abspath(target_db)

    conn = sqlite3.connect(target_db)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        source_schema = "main"
        if not same_db:
            conn.execute("ATTACH DATABASE ? AS src", (source_db,))
            source_schema = "src"

        _ensure_placeholder_ratio_tables(conn, overwrite=overwrite)
        seeded_documents = _seed_placeholder_ratio_docids(helper, conn, source_schema)
        conn.commit()
        logger.warning(
            "generate_ratios is currently a placeholder scaffold. Seeded %d docID row(s) into empty ratio tables only.",
            seeded_documents,
        )
        return {
            "status": "placeholder",
            "documents_seeded": seeded_documents,
            "tables": list(_PLACEHOLDER_RATIO_TABLES),
            "formulas_config": formulas_path,
            "batch_size": max(int(batch_size or 5000), 1),
        }
    finally:
        conn.close()


def collect_historical_output_columns(metric_columns):
    """Build ordered output-column list for historical-ratio tables."""
    windows = [1, 2, 3, 5, 10]
    output_cols = []
    for col in metric_columns:
        output_cols.append(col)
        for w in windows:
            output_cols.append(f"{col}_{w}Year_Average")
            output_cols.append(f"{col}_{w}Year_Growth")
        output_cols.append(f"{col}_StdDev")
        output_cols.append(f"{col}_ZScore_IntraCompany")
        output_cols.append(f"{col}_ZScore_AllCompanies")
    return output_cols


def compute_historical_metrics(df, metric_columns, all_companies_stats=None):
    """Compute rolling averages, standard deviation, and z-scores for *metric_columns*."""
    windows = [1, 2, 3, 5, 10]

    if df.empty:
        return df

    df = df.copy()
    df["periodEnd"] = pd.to_datetime(df["periodEnd"], errors="coerce")
    df.sort_values(["edinetCode", "periodEnd", "docID"], inplace=True)

    for col in metric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce", downcast="float")
        grouped = df.groupby("edinetCode")[col]

        for w in windows:
            df[f"{col}_{w}Year_Average"] = grouped.transform(
                lambda s, window=w: s.rolling(window=window, min_periods=1).mean()
            )
            prev = grouped.transform(lambda s, shift=w: s.shift(shift))
            df[f"{col}_{w}Year_Growth"] = np.where(
                (prev > 0) & (df[col] >= 0),
                np.power(df[col] / prev, 1.0 / w) - 1.0,
                np.nan,
            )

        std_series = grouped.transform(
            lambda s: s.expanding(min_periods=1).std(ddof=0)
        )
        mean_series = grouped.transform(
            lambda s: s.expanding(min_periods=1).mean()
        )

        z_intra = np.where(
            std_series > 0,
            (df[col] - mean_series) / std_series,
            np.where(std_series == 0, 0, np.nan),
        )
        df[f"{col}_StdDev"] = std_series
        df[f"{col}_ZScore_IntraCompany"] = pd.Series(z_intra, index=df.index)

        if all_companies_stats and col in all_companies_stats:
            stats = all_companies_stats[col]
            period_mean = df["periodEnd"].map(stats["mean"])
            period_std = df["periodEnd"].map(stats["std"])
        else:
            period_mean = df.groupby("periodEnd")[col].transform("mean")
            period_std = df.groupby("periodEnd")[col].transform("std")
        z_all = np.where(
            period_std > 0,
            (df[col] - period_mean) / period_std,
            np.where(period_std == 0, 0, np.nan),
        )
        df[f"{col}_ZScore_AllCompanies"] = pd.Series(z_all, index=df.index)

    return df


def build_cross_sectional_stats(
    conn,
    source_ref,
    fs_ref,
    metric_cols,
    chunk_size=200000,
    metric_exprs=None,
    helper=None,
):
    """Build period-level mean/std for each metric without loading full dataset into memory."""
    helper = helper or _DB_HELPER
    stats_acc: dict[str, dict[pd.Timestamp, dict[str, float]]] = {
        col: {} for col in metric_cols
    }

    metric_exprs = metric_exprs or {}
    metric_select = ", ".join(
        f"{metric_exprs.get(col, f's.{helper._sql_ident(col)}')} AS {helper._sql_ident(col)}"
        for col in metric_cols
    )
    sql = f"""
    SELECT fs.periodEnd AS periodEnd, {metric_select}
    FROM {source_ref} s
    INNER JOIN {fs_ref} fs ON fs.docID = s.docID
    WHERE s.docID IS NOT NULL
      AND fs.periodEnd IS NOT NULL
    """

    for chunk in pd.read_sql_query(sql, conn, chunksize=chunk_size):
        if chunk.empty:
            continue
        chunk["periodEnd"] = pd.to_datetime(chunk["periodEnd"], errors="coerce")
        chunk = chunk[chunk["periodEnd"].notna()]
        if chunk.empty:
            continue

        for col in metric_cols:
            vals = pd.to_numeric(chunk[col], errors="coerce")
            local = pd.DataFrame({"periodEnd": chunk["periodEnd"], "v": vals})
            local = local[local["v"].notna()]
            if local.empty:
                continue
            agg = local.groupby("periodEnd")["v"].agg(["count", "sum"])
            agg["sumsq"] = local.assign(v2=local["v"] * local["v"]).groupby("periodEnd")["v2"].sum()

            acc = stats_acc[col]
            for period, row in agg.iterrows():
                bucket = acc.setdefault(period, {"count": 0.0, "sum": 0.0, "sumsq": 0.0})
                bucket["count"] += float(row["count"])
                bucket["sum"] += float(row["sum"])
                bucket["sumsq"] += float(row["sumsq"])

    result = {}
    for col in metric_cols:
        periods = []
        means = []
        stds = []
        for period, row in stats_acc[col].items():
            n = row["count"]
            s = row["sum"]
            ss = row["sumsq"]
            mean = (s / n) if n > 0 else np.nan
            if n > 1:
                var = (ss - (s * s) / n) / (n - 1)
                var = max(var, 0.0)
                std = float(np.sqrt(var))
            else:
                std = np.nan
            periods.append(period)
            means.append(mean)
            stds.append(std)

        if periods:
            s_mean = pd.Series(means, index=pd.to_datetime(periods))
            s_std = pd.Series(stds, index=pd.to_datetime(periods))
        else:
            s_mean = pd.Series(dtype=float)
            s_std = pd.Series(dtype=float)
        result[col] = {"mean": s_mean, "std": s_std}

    return result


def ensure_historical_table_schema(conn, table_name, output_columns, helper=None):
    """Create historical table and add any missing columns."""
    helper = helper or _DB_HELPER
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {helper._sql_ident(table_name)} ("
        f"{helper._sql_ident('docID')} TEXT PRIMARY KEY"
        f")"
    )

    info = conn.execute(f"PRAGMA table_info({helper._sql_ident(table_name)})").fetchall()
    existing_cols = {row[1] for row in info}

    for col in output_columns:
        if col in existing_cols:
            continue
        col_type = "TEXT" if col in ("edinetCode", "periodEnd") else "REAL"
        conn.execute(
            f"ALTER TABLE {helper._sql_ident(table_name)} "
            f"ADD COLUMN {helper._sql_ident(col)} {col_type}"
        )


def generate_historical_ratios(
    source_database,
    target_database,
    overwrite=False,
    company_batch_size=200,
    helper=None,
):
    """Generate historical-ratio tables from PerShare/Quality/Valuation."""
    helper = helper or _DB_HELPER
    source_db = source_database
    target_db = target_database
    if not source_db:
        raise ValueError("source_database is required for generate_historical_ratios.")
    if not target_db:
        raise ValueError("target_database is required for generate_historical_ratios.")
    same_db = os.path.abspath(source_db) == os.path.abspath(target_db)

    source_to_target = {
        "PerShare": "Pershare_Historical",
        "Quality": "Quality_Historical",
        "Valuation": "Valuation_Historical",
    }

    conn = sqlite3.connect(target_db)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        source_schema = "main"
        if not same_db:
            conn.execute("ATTACH DATABASE ? AS src", (source_db,))
            source_schema = "src"

        fs_actual = helper._resolve_table_name_in_schema(conn, source_schema, "FinancialStatements")
        if not fs_actual:
            raise RuntimeError("Source table 'FinancialStatements' not found; required for Generate Historical Ratios.")
        fs_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(fs_actual)}"

        helper._create_index_if_not_exists(conn, source_schema, fs_actual, ["docID"])
        helper._create_index_if_not_exists(conn, source_schema, fs_actual, ["edinetCode", "periodEnd"])

        if overwrite:
            conn.executescript(
                """
                DROP TABLE IF EXISTS Pershare_Historical;
                DROP TABLE IF EXISTS Quality_Historical;
                DROP TABLE IF EXISTS Valuation_Historical;
                """
            )

        for source_table, target_table in source_to_target.items():
            source_actual = helper._resolve_table_name_in_schema(conn, source_schema, source_table)
            if not source_actual:
                logger.warning("Generate Historical Ratios: source table '%s' not found, skipping.", source_table)
                continue

            source_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(source_actual)}"
            helper._create_index_if_not_exists(conn, source_schema, source_actual, ["docID"])
            source_cols_info = conn.execute(
                f"PRAGMA {helper._sql_ident(source_schema)}.table_info({helper._sql_ident(source_actual)})"
            ).fetchall()
            source_cols = [row[1] for row in source_cols_info]
            metric_cols = [c for c in source_cols if c != "docID"]

            metric_exprs: dict[str, str] = {}
            if source_table == "PerShare" and "SharePrice" not in metric_cols:
                fs_cols_info = conn.execute(
                    f"PRAGMA {helper._sql_ident(source_schema)}.table_info({helper._sql_ident(fs_actual)})"
                ).fetchall()
                fs_cols = {row[1] for row in fs_cols_info}
                if "SharePrice" in fs_cols:
                    metric_cols.append("SharePrice")
                    metric_exprs["SharePrice"] = f"fs.{helper._sql_ident('SharePrice')}"

            if not metric_cols:
                logger.info(
                    "Generate Historical Ratios: source table '%s' has no metric columns, skipping.",
                    source_table,
                )
                continue

            cross_stats = build_cross_sectional_stats(
                conn,
                source_ref,
                fs_ref,
                metric_cols,
                metric_exprs=metric_exprs,
                helper=helper,
            )

            company_sql = f"""
            SELECT DISTINCT fs.edinetCode
            FROM {source_ref} s
            INNER JOIN {fs_ref} fs ON fs.docID = s.docID
            WHERE fs.edinetCode IS NOT NULL
            ORDER BY fs.edinetCode
            """
            companies = [r[0] for r in conn.execute(company_sql).fetchall()]
            if not companies:
                logger.info("Generate Historical Ratios: no companies found for '%s'.", source_table)
                continue

            output_cols = ["docID", "edinetCode", "periodEnd"] + collect_historical_output_columns(metric_cols)
            ensure_historical_table_schema(conn, target_table, [c for c in output_cols if c != "docID"], helper=helper)
            helper._create_index_if_not_exists(conn, "main", target_table, ["edinetCode", "periodEnd"])

            select_metric_cols = ", ".join(
                f"{metric_exprs.get(col, f's.{helper._sql_ident(col)}')} AS {helper._sql_ident(col)}"
                for col in metric_cols
            )
            cols_sql = ", ".join(helper._sql_ident(c) for c in output_cols)

            pending_batches = []
            batch_size = max(int(company_batch_size or 200), 1)
            for i in range(0, len(companies), batch_size):
                company_batch = companies[i:i + batch_size]
                if not company_batch:
                    continue

                placeholders = ", ".join(["?"] * len(company_batch))
                sql = f"""
                SELECT
                    s.docID AS docID,
                    fs.edinetCode AS edinetCode,
                    fs.periodEnd AS periodEnd,
                    {select_metric_cols}
                FROM {source_ref} s
                INNER JOIN {fs_ref} fs ON fs.docID = s.docID
                WHERE s.docID IS NOT NULL
                  AND fs.edinetCode IN ({placeholders})
                ORDER BY fs.edinetCode, fs.periodEnd, s.docID
                """
                df = pd.read_sql_query(sql, conn, params=company_batch)
                if df.empty:
                    continue

                df = compute_historical_metrics(df, metric_cols, all_companies_stats=cross_stats)
                pending_batches.append(df[output_cols].copy())

                if len(pending_batches) >= 5:
                    merged = pd.concat(pending_batches, ignore_index=True)
                    temp_name = f"_tmp_{target_table}_{random.randint(1000, 9999)}"
                    merged.to_sql(temp_name, conn, if_exists="replace", index=False)
                    conn.execute(
                        f"INSERT OR REPLACE INTO {helper._sql_ident(target_table)} ({cols_sql}) "
                        f"SELECT {cols_sql} FROM {helper._sql_ident(temp_name)}"
                    )
                    conn.execute(f"DROP TABLE IF EXISTS {helper._sql_ident(temp_name)}")
                    conn.commit()
                    pending_batches.clear()

            if pending_batches:
                merged = pd.concat(pending_batches, ignore_index=True)
                temp_name = f"_tmp_{target_table}_{random.randint(1000, 9999)}"
                merged.to_sql(temp_name, conn, if_exists="replace", index=False)
                conn.execute(
                    f"INSERT OR REPLACE INTO {helper._sql_ident(target_table)} ({cols_sql}) "
                    f"SELECT {cols_sql} FROM {helper._sql_ident(temp_name)}"
                )
                conn.execute(f"DROP TABLE IF EXISTS {helper._sql_ident(temp_name)}")
                conn.commit()

        logger.info("Generate Historical Ratios completed.")
    finally:
        conn.close()