import ast
import logging
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
import random
import numpy as np
import json
import os
import re
import time

logger = logging.getLogger(__name__)

class data:
    def __init__(self):
        pass

    def _table_exists(self, conn, table_name):
        """Return True if *table_name* exists in the SQLite database."""
        cur = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cur.fetchone() is not None

    def _resolve_table_name_in_schema(self, conn, schema_name, table_name):
        """Return actual table name in schema using case-insensitive match, else None."""
        if schema_name == "main":
            sql = (
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND lower(name)=lower(?) LIMIT 1"
            )
        else:
            sql = (
                f"SELECT name FROM {self._sql_ident(schema_name)}.sqlite_master "
                "WHERE type='table' AND lower(name)=lower(?) LIMIT 1"
            )
        row = conn.execute(sql, (table_name,)).fetchone()
        return row[0] if row else None

    def _sql_ident(self, name):
        """Safely quote an SQLite identifier (table/column/schema name)."""
        return '"' + str(name).replace('"', '""') + '"'

    def _sql_literal(self, value):
        """Safely quote a SQL literal value."""
        return "'" + str(value).replace("'", "''") + "'"

    def _create_index_if_not_exists(self, conn, schema_name, table_name, columns):
        """Create an index on *table_name(columns)* when possible."""
        if not columns:
            return

        safe_table = re.sub(r"\W+", "_", str(table_name))
        safe_cols = "_".join(re.sub(r"\W+", "_", str(c)) for c in columns)
        idx_name = f"ix_{safe_table}_{safe_cols}"

        if schema_name == "main":
            idx_ref = self._sql_ident(idx_name)
        else:
            idx_ref = f"{self._sql_ident(schema_name)}.{self._sql_ident(idx_name)}"

        table_ref = f"{self._sql_ident(schema_name)}.{self._sql_ident(table_name)}"
        cols_sql = ", ".join(self._sql_ident(c) for c in columns)

        try:
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {idx_ref} ON {table_ref} ({cols_sql})"
            )
        except Exception as exc:
            logger.debug(
                "Skipping index creation for %s(%s): %s",
                table_ref,
                ", ".join(columns),
                exc,
            )

    def _load_financial_statement_mappings(self, mappings_config_path):
        """Load mappings config and normalize into {table: {column: mapping_dict}}."""
        with open(mappings_config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        mappings = raw.get("Mappings", [])
        normalized = {
            "FinancialStatements": {},
            "IncomeStatement": {},
            "BalanceSheet": {},
            "CashflowStatement": {},
        }

        for entry in mappings:
            if not isinstance(entry, dict):
                continue
            table = entry.get("Table")
            name = entry.get("Name")
            if table not in normalized or not name:
                continue
            normalized[table][name] = {
                "Terms": entry.get("Terms", []) or [],
                "periods": entry.get("periods", []) or [],
            }

        return normalized

    def _collect_financial_statement_filters(self, mappings):
        """Return the union of relevant terms/periods across statement mappings."""
        relevant_terms = set()
        relevant_periods = set()
        has_unrestricted_periods = False

        for table_mappings in mappings.values():
            for mapping in table_mappings.values():
                if not isinstance(mapping, dict):
                    continue

                relevant_terms.update(t for t in (mapping.get("Terms", []) or []) if t)

                periods = [p for p in (mapping.get("periods", []) or []) if p]
                if periods:
                    relevant_periods.update(periods)
                else:
                    has_unrestricted_periods = True

        return {
            "terms": sorted(relevant_terms),
            "periods": sorted(relevant_periods),
            "has_unrestricted_periods": has_unrestricted_periods,
        }

    def _build_source_relevance_predicate(self, source_alias, filters, col_names=None):
        """Build SQL predicate limiting rows to mapped accounting terms/periods."""
        col_names = col_names or {}
        col_at = col_names.get("AccountingTerm", "AccountingTerm")
        col_period = col_names.get("Period", "Period")

        terms = filters.get("terms", []) if filters else []
        periods = filters.get("periods", []) if filters else []
        has_unrestricted_periods = bool(filters.get("has_unrestricted_periods")) if filters else False

        if not terms:
            return "1=1"

        term_list = ", ".join(self._sql_literal(term) for term in terms)
        conditions = [f"{source_alias}.{self._sql_ident(col_at)} IN ({term_list})"]

        if periods and not has_unrestricted_periods:
            period_list = ", ".join(self._sql_literal(period) for period in periods)
            conditions.append(f"{source_alias}.{self._sql_ident(col_period)} IN ({period_list})")

        return " AND ".join(conditions)

    def _resolve_source_col_names(self, conn, schema_name, table_name):
        """Detect the actual column names for AccountingTerm, Period, Amount in the source table.

        When the source table uses the raw EDINET column names (e.g. financialdata_full uses
        Japanese names like '要素ID' instead of 'AccountingTerm'), this method returns the
        actual names so SQL expressions can reference them correctly.

        Returns a dict mapping standard names to the actual names used in the table.
        Falls back to the standard names if the table cannot be introspected.
        """
        try:
            if schema_name == "main":
                rows = conn.execute(
                    f"PRAGMA table_info({self._sql_ident(table_name)})"
                ).fetchall()
            else:
                rows = conn.execute(
                    f"PRAGMA {self._sql_ident(schema_name)}.table_info({self._sql_ident(table_name)})"
                ).fetchall()
        except Exception:
            rows = []

        table_cols = {row[1] for row in rows}

        # Fixed mapping: alternative (e.g. raw EDINET Japanese) name → standard name.
        # Keys are the column names used in tables like financialdata_full;
        # values are the project-standard names used everywhere else.
        ALTERNATIVE_TO_STANDARD = {
            "要素ID":      "AccountingTerm",
            "コンテキストID": "Period",
            "ユニットID":   "Currency",
            "値":          "Amount",
        }

        # Build reverse: standard name → alternative name
        reverse_map = {v: k for k, v in ALTERNATIVE_TO_STANDARD.items()}

        result = {}
        for standard_name in ("AccountingTerm", "Period", "Amount"):
            if standard_name in table_cols:
                result[standard_name] = standard_name
            elif standard_name in reverse_map and reverse_map[standard_name] in table_cols:
                result[standard_name] = reverse_map[standard_name]
            else:
                result[standard_name] = standard_name  # fallback to standard

        return result

    def _build_amount_case_expr(self, mapping, source_alias="s",
                               col_accounting_term="AccountingTerm",
                               col_period="Period", col_amount="Amount",
                               value_type="REAL"):
        """Build MAX(CASE WHEN ... THEN CAST(Amount AS REAL) END) SQL expression.

        Args:
            mapping: Mapping dict with 'Terms' and 'periods' keys.
            source_alias: SQL alias for the source table.
            col_accounting_term: Actual column name for AccountingTerm in the source table.
            col_period: Actual column name for Period in the source table.
            col_amount: Actual column name for Amount in the source table.
        """
        if not mapping:
            return "NULL"

        terms = mapping.get("Terms", []) or []
        periods = mapping.get("periods", []) or []
        if not terms:
            return "NULL"

        term_list = ", ".join(self._sql_literal(t) for t in terms)
        conditions = [f"{source_alias}.{self._sql_ident(col_accounting_term)} IN ({term_list})"]

        if periods:
            period_list = ", ".join(self._sql_literal(p) for p in periods)
            conditions.append(f"{source_alias}.{self._sql_ident(col_period)} IN ({period_list})")

        storage_type = str(value_type or "REAL").upper()
        if storage_type == "TEXT":
            value_expr = f"CAST({source_alias}.{self._sql_ident(col_amount)} AS TEXT)"
        else:
            value_expr = f"CAST({source_alias}.{self._sql_ident(col_amount)} AS REAL)"

        condition_sql = " AND ".join(conditions)
        return (
            f"MAX(CASE WHEN {condition_sql} "
            f"THEN {value_expr} END)"
        )

    def _is_safe_identifier(self, name):
        """Return True when *name* is a simple SQL identifier-like token."""
        return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", str(name or "")))

    def _mapping_storage_type(self, mapping):
        """Return SQLite storage type for a mapped financial-statement column."""
        terms = [str(term or "") for term in (mapping or {}).get("Terms", []) or []]
        if any("TextBlock" in term for term in terms):
            return "TEXT"
        return "REAL"

    def _build_financial_statement_table_specs(self, mappings):
        """Build ordered table specs for normalized financial-statement output."""
        table_specs = {
            "FinancialStatements": [
                ("edinetCode", "TEXT"),
                ("docID", "TEXT"),
                ("docTypeCode", "TEXT"),
                ("periodStart", "TEXT"),
                ("periodEnd", "TIMESTAMP"),
                ("DescriptionOfBusiness_EN", "TEXT"),
            ],
            "IncomeStatement": [("docID", "TEXT")],
            "BalanceSheet": [("docID", "TEXT")],
            "CashflowStatement": [("docID", "TEXT")],
        }

        reserved = {
            table_name: {col_name for col_name, _ in column_specs}
            for table_name, column_specs in table_specs.items()
        }
        reserved["FinancialStatements"].add("SharePrice")

        for table_name, table_mappings in (mappings or {}).items():
            if table_name not in table_specs:
                continue
            for col_name, mapping in table_mappings.items():
                if not self._is_safe_identifier(col_name):
                    logger.warning(
                        "Skipping mapped column %r for table %s because the name is not a safe SQL identifier.",
                        col_name,
                        table_name,
                    )
                    continue
                if col_name in reserved[table_name]:
                    logger.warning(
                        "Skipping mapped column %r for table %s because that column is reserved.",
                        col_name,
                        table_name,
                    )
                    continue

                table_specs[table_name].append(
                    (col_name, self._mapping_storage_type(mapping))
                )
                reserved[table_name].add(col_name)

        table_specs["FinancialStatements"].append(("SharePrice", "REAL"))
        return table_specs

    def _ensure_typed_table_columns(self, conn, table_name, columns):
        """Ensure *columns* exist in *table_name* and return newly added names."""
        info = conn.execute(f"PRAGMA table_info({self._sql_ident(table_name)})").fetchall()
        existing_cols = {row[1] for row in info}
        added_cols = []

        for col_name, col_type in columns:
            if col_name in existing_cols:
                continue
            conn.execute(
                f"ALTER TABLE {self._sql_ident(table_name)} "
                f"ADD COLUMN {self._sql_ident(col_name)} {col_type}"
            )
            existing_cols.add(col_name)
            added_cols.append(col_name)

        return added_cols

    def _resolve_column_name(self, conn, table_name, column_name):
        """Return actual column name in *table_name* using case-insensitive lookup."""
        info = conn.execute(f"PRAGMA table_info({self._sql_ident(table_name)})").fetchall()
        by_lower = {str(row[1]).lower(): str(row[1]) for row in info}
        return by_lower.get(str(column_name or "").lower())

    def _load_generate_ratios_definitions(self, formulas_config_path):
        """Load and normalize Generate Ratios formulas config."""
        with open(formulas_config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        normalized = {
            "PerShare": [],
            "Valuation": [],
            "Quality": [],
        }

        if not isinstance(raw, dict):
            return normalized

        for table_name in normalized:
            items = raw.get(table_name, [])
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                col = (item.get("Column") or "").strip()
                formula = (item.get("Formula") or "").strip()
                if not col or not formula:
                    continue
                if not self._is_safe_identifier(col):
                    logger.warning("Generate Ratios: skipping invalid column name '%s' in table '%s'", col, table_name)
                    continue
                normalized[table_name].append({"Column": col, "Formula": formula})

        return normalized

    def _formula_to_sql_expr_and_refs(self, formula, alias_map):
        """Compile a formula string to SQL and return (sql_expr, refs)."""
        expr_ast = ast.parse(formula, mode="eval")
        refs = set()

        def _walk(node):
            if isinstance(node, ast.Expression):
                return _walk(node.body)

            if isinstance(node, ast.BinOp):
                op_map = {
                    ast.Add: "+",
                    ast.Sub: "-",
                    ast.Mult: "*",
                    ast.Div: "/",
                }
                op = op_map.get(type(node.op))
                if not op:
                    raise ValueError("Unsupported operator in formula")
                return f"({_walk(node.left)} {op} {_walk(node.right)})"

            if isinstance(node, ast.UnaryOp):
                if isinstance(node.op, ast.USub):
                    return f"(-{_walk(node.operand)})"
                if isinstance(node.op, ast.UAdd):
                    return f"(+{_walk(node.operand)})"
                raise ValueError("Unsupported unary operator in formula")

            if isinstance(node, ast.Constant):
                if isinstance(node.value, (int, float)):
                    return str(node.value)
                raise ValueError("Only numeric constants are allowed in formulas")

            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                table_name = node.value.id
                column_name = node.attr
                if table_name not in alias_map:
                    raise ValueError(f"Unknown table reference '{table_name}'")
                if not self._is_safe_identifier(column_name):
                    raise ValueError(f"Invalid column reference '{column_name}'")
                refs.add((table_name, column_name))
                return f"{alias_map[table_name]}.{self._sql_ident(column_name)}"

            raise ValueError("Unsupported expression in formula")

        sql_expr = _walk(expr_ast)
        return sql_expr, refs

    def _build_generate_ratios_execution_plan(self, definitions):
        """Build an execution order for formula columns with dependency resolution."""
        alias_map = {
            "FinancialStatements": "fs",
            "IncomeStatement": "is1",
            "BalanceSheet": "bs",
            "CashflowStatement": "cs",
            "PerShare": "ps",
            "Valuation": "va",
            "Quality": "qu",
        }

        formula_nodes = {}
        refs_by_node = {}
        unresolved_messages = []

        for table_name in ("PerShare", "Valuation", "Quality"):
            for item in definitions.get(table_name, []):
                col = item["Column"]
                formula = item["Formula"]
                node = (table_name, col)
                try:
                    sql_expr, refs = self._formula_to_sql_expr_and_refs(formula, alias_map)
                except Exception as exc:
                    unresolved_messages.append(
                        f"{table_name}.{col}: invalid formula '{formula}' ({exc})"
                    )
                    continue

                formula_nodes[node] = {
                    "table": table_name,
                    "column": col,
                    "formula": formula,
                    "sql_expr": sql_expr,
                }
                refs_by_node[node] = refs

        dependencies = {}
        for node, refs in refs_by_node.items():
            deps = set()
            for ref in refs:
                if ref in formula_nodes:
                    deps.add(ref)
            dependencies[node] = deps

        remaining = set(formula_nodes.keys())
        ready = sorted([n for n, deps in dependencies.items() if not deps])
        execution_order = []

        while ready:
            current = ready.pop(0)
            if current not in remaining:
                continue
            execution_order.append(formula_nodes[current])
            remaining.remove(current)

            newly_ready = []
            for node in remaining:
                if current in dependencies[node]:
                    dependencies[node].remove(current)
                    if not dependencies[node]:
                        newly_ready.append(node)
            ready.extend(sorted(newly_ready))

        if remaining:
            for node in sorted(remaining):
                deps = ", ".join(f"{t}.{c}" for t, c in sorted(dependencies[node]))
                unresolved_messages.append(
                    f"{node[0]}.{node[1]}: cyclic/unresolved dependencies ({deps})"
                )

        return execution_order, unresolved_messages

    def _ensure_generate_ratios_tables(self, conn):
        """Create Generate Ratios output tables if they do not exist."""
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

    def _ensure_table_columns(self, conn, table_name, columns):
        """Ensure all *columns* exist in *table_name* (REAL type for new columns)."""
        info = conn.execute(f"PRAGMA table_info({self._sql_ident(table_name)})").fetchall()
        existing_cols = {row[1] for row in info}
        for col in columns:
            if col not in existing_cols:
                conn.execute(
                    f"ALTER TABLE {self._sql_ident(table_name)} ADD COLUMN {self._sql_ident(col)} REAL"
                )

    def generate_ratios(
        self,
        source_database,
        target_database,
        formulas_config,
        overwrite=False,
        batch_size=5000,
    ):
        """Generate PerShare / Valuation / Quality tables from financial statements.

        Notes:
        - One row per docID in each generated table.
        - Columns are driven by *formulas_config*.
        - Formula dependencies are resolved dynamically (best effort).
        - Unresolvable cyclic formulas are logged and skipped.
        """
        source_db = source_database
        target_db = target_database
        if not source_db:
            raise ValueError("source_database is required for generate_ratios.")
        if not target_db:
            raise ValueError("target_database is required for generate_ratios.")
        formulas_path = formulas_config
        if not formulas_path:
            raise ValueError("Formulas_Config is required for generate_ratios.")

        definitions = self._load_generate_ratios_definitions(formulas_path)
        execution_order, unresolved = self._build_generate_ratios_execution_plan(definitions)

        same_db = os.path.abspath(source_db) == os.path.abspath(target_db)
        batch_size = max(int(batch_size or 5000), 1)

        conn = sqlite3.connect(target_db)
        try:
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")

            source_schema = "main"
            if not same_db:
                conn.execute("ATTACH DATABASE ? AS src", (source_db,))
                source_schema = "src"

            base_required = ["FinancialStatements", "IncomeStatement", "BalanceSheet", "CashflowStatement"]
            base_refs = {}
            for table_name in base_required:
                actual = self._resolve_table_name_in_schema(conn, source_schema, table_name)
                if not actual:
                    raise RuntimeError(
                        f"Source table '{table_name}' not found in source database; required for Generate Ratios."
                    )
                base_refs[table_name] = f"{self._sql_ident(source_schema)}.{self._sql_ident(actual)}"

            if overwrite:
                logger.info("Overwrite enabled - resetting PerShare / Valuation / Quality tables.")
                conn.executescript(
                    """
                    DROP TABLE IF EXISTS PerShare;
                    DROP TABLE IF EXISTS Valuation;
                    DROP TABLE IF EXISTS Quality;
                    """
                )

            self._ensure_generate_ratios_tables(conn)

            # Ensure all configured columns are present before execution
            for table_name in ("PerShare", "Valuation", "Quality"):
                cols = [item["Column"] for item in definitions.get(table_name, [])]
                self._ensure_table_columns(conn, table_name, cols)

            fs_ref = base_refs["FinancialStatements"]

            # Add missing docIDs from FinancialStatements into all 3 output tables
            for table_name in ("PerShare", "Valuation", "Quality"):
                conn.execute(
                    f"INSERT OR IGNORE INTO {self._sql_ident(table_name)}(docID) "
                    f"SELECT DISTINCT docID FROM {fs_ref} WHERE docID IS NOT NULL"
                )

            tmp_docids = self._sql_ident("_tmp_ratio_docids")
            conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS {tmp_docids} (docID TEXT PRIMARY KEY)")

            doc_cursor = conn.execute(f"SELECT docID FROM {fs_ref} WHERE docID IS NOT NULL ORDER BY docID")
            total_docs = 0

            join_sql = (
                f"FROM {base_refs['FinancialStatements']} fs "
                f"LEFT JOIN {base_refs['IncomeStatement']} is1 ON is1.docID = fs.docID "
                f"LEFT JOIN {base_refs['BalanceSheet']} bs ON bs.docID = fs.docID "
                f"LEFT JOIN {base_refs['CashflowStatement']} cs ON cs.docID = fs.docID "
                f"LEFT JOIN {self._sql_ident('PerShare')} ps ON ps.docID = fs.docID "
                f"LEFT JOIN {self._sql_ident('Valuation')} va ON va.docID = fs.docID "
                f"LEFT JOIN {self._sql_ident('Quality')} qu ON qu.docID = fs.docID"
            )

            while True:
                batch = [row[0] for row in doc_cursor.fetchmany(batch_size)]
                if not batch:
                    break

                with conn:
                    conn.execute(f"DELETE FROM {tmp_docids}")
                    conn.executemany(
                        f"INSERT OR IGNORE INTO {tmp_docids}(docID) VALUES (?)",
                        [(d,) for d in batch],
                    )

                    for item in execution_order:
                        table_name = item["table"]
                        col_name = item["column"]
                        expr_sql = item["sql_expr"]

                        update_sql = f"""
                        UPDATE {self._sql_ident(table_name)} AS tgt
                        SET {self._sql_ident(col_name)} = (
                            SELECT {expr_sql}
                            {join_sql}
                            WHERE fs.docID = tgt.docID
                        )
                        WHERE tgt.docID IN (SELECT docID FROM {tmp_docids})
                        """
                        conn.execute(update_sql)

                total_docs += len(batch)
                if total_docs % (batch_size * 10) == 0:
                    logger.info("Generate Ratios progress: %d docs processed", total_docs)

            for msg in unresolved:
                logger.warning("Generate Ratios: %s", msg)

            logger.info(
                "Generate Ratios completed. Processed %d doc(s); executed %d formula(s); skipped %d unresolved.",
                total_docs,
                len(execution_order),
                len(unresolved),
            )
        finally:
            conn.close()

    def _collect_historical_output_columns(self, metric_columns):
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

    def _compute_historical_metrics(self, df, metric_columns, all_companies_stats=None):
        """Compute rolling averages, standard deviation, and z-scores for *metric_columns*.

        Notes:
        - Intra-company z-score is computed within each `edinetCode` time series.
        - All-companies z-score is computed cross-sectionally per `periodEnd`
          (i.e., compare companies in the same reporting period).
        """
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

            # Expanding (cumulative) stats up to the current row per company.
            # This makes each docID's standard deviation/mean depend on prior rows
            # plus the current row, consistent with rolling-average behavior.
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

            # Cross-sectional z-score: compare each company against peers in same period.
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

    def _build_cross_sectional_stats(
        self,
        conn,
        source_ref,
        fs_ref,
        metric_cols,
        chunk_size=200000,
        metric_exprs=None,
    ):
        """Build period-level mean/std for each metric without loading full dataset into memory."""
        stats_acc: dict[str, dict[pd.Timestamp, dict[str, float]]] = {
            col: {} for col in metric_cols
        }

        metric_exprs = metric_exprs or {}
        metric_select = ", ".join(
            f"{metric_exprs.get(col, f's.{self._sql_ident(col)}')} AS {self._sql_ident(col)}"
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
                    var = (ss - (s * s) / n) / (n - 1)  # sample variance (ddof=1)
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

    def _ensure_historical_table_schema(self, conn, table_name, output_columns):
        """Create historical table and add any missing columns."""
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self._sql_ident(table_name)} ("
            f"{self._sql_ident('docID')} TEXT PRIMARY KEY"
            f")"
        )

        info = conn.execute(f"PRAGMA table_info({self._sql_ident(table_name)})").fetchall()
        existing_cols = {row[1] for row in info}

        for col in output_columns:
            if col in existing_cols:
                continue
            col_type = "TEXT" if col in ("edinetCode", "periodEnd") else "REAL"
            conn.execute(
                f"ALTER TABLE {self._sql_ident(table_name)} "
                f"ADD COLUMN {self._sql_ident(col)} {col_type}"
            )

    def generate_historical_ratios(
        self,
        source_database,
        target_database,
        overwrite=False,
        company_batch_size=200,
    ):
        """Generate historical-ratio tables from PerShare/Quality/Valuation.

        Output tables:
        - Pershare_Historical
        - Quality_Historical
        - Valuation_Historical
        """
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

            fs_actual = self._resolve_table_name_in_schema(conn, source_schema, "FinancialStatements")
            if not fs_actual:
                raise RuntimeError("Source table 'FinancialStatements' not found; required for Generate Historical Ratios.")
            fs_ref = f"{self._sql_ident(source_schema)}.{self._sql_ident(fs_actual)}"

            # Indexes for heavy joins/grouping on very large datasets.
            self._create_index_if_not_exists(conn, source_schema, fs_actual, ["docID"])
            self._create_index_if_not_exists(conn, source_schema, fs_actual, ["edinetCode", "periodEnd"])

            if overwrite:
                conn.executescript(
                    """
                    DROP TABLE IF EXISTS Pershare_Historical;
                    DROP TABLE IF EXISTS Quality_Historical;
                    DROP TABLE IF EXISTS Valuation_Historical;
                    """
                )

            for source_table, target_table in source_to_target.items():
                source_actual = self._resolve_table_name_in_schema(conn, source_schema, source_table)
                if not source_actual:
                    logger.warning("Generate Historical Ratios: source table '%s' not found, skipping.", source_table)
                    continue

                source_ref = f"{self._sql_ident(source_schema)}.{self._sql_ident(source_actual)}"
                self._create_index_if_not_exists(conn, source_schema, source_actual, ["docID"])
                source_cols_info = conn.execute(
                    f"PRAGMA {self._sql_ident(source_schema)}.table_info({self._sql_ident(source_actual)})"
                ).fetchall()
                source_cols = [row[1] for row in source_cols_info]
                metric_cols = [c for c in source_cols if c != "docID"]

                # Ensure PerShare historical table also includes SharePrice-based
                # historical metrics even when SharePrice is not persisted in
                # the PerShare source table.
                metric_exprs: dict[str, str] = {}
                if source_table == "PerShare" and "SharePrice" not in metric_cols:
                    fs_cols_info = conn.execute(
                        f"PRAGMA {self._sql_ident(source_schema)}.table_info({self._sql_ident(fs_actual)})"
                    ).fetchall()
                    fs_cols = {row[1] for row in fs_cols_info}
                    if "SharePrice" in fs_cols:
                        metric_cols.append("SharePrice")
                        metric_exprs["SharePrice"] = f"fs.{self._sql_ident('SharePrice')}"

                if not metric_cols:
                    logger.info(
                        "Generate Historical Ratios: source table '%s' has no metric columns, skipping.",
                        source_table,
                    )
                    continue

                # Cross-sectional (all-companies) period stats, computed once per table.
                cross_stats = self._build_cross_sectional_stats(
                    conn,
                    source_ref,
                    fs_ref,
                    metric_cols,
                    metric_exprs=metric_exprs,
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

                output_cols = ["docID", "edinetCode", "periodEnd"] + self._collect_historical_output_columns(metric_cols)
                self._ensure_historical_table_schema(conn, target_table, [c for c in output_cols if c != "docID"])
                self._create_index_if_not_exists(conn, "main", target_table, ["edinetCode", "periodEnd"])

                select_metric_cols = ", ".join(
                    f"{metric_exprs.get(col, f's.{self._sql_ident(col)}')} AS {self._sql_ident(col)}"
                    for col in metric_cols
                )
                cols_sql = ", ".join(self._sql_ident(c) for c in output_cols)

                pending_batches = []
                for i in range(0, len(companies), max(int(company_batch_size or 200), 1)):
                    company_batch = companies[i:i + max(int(company_batch_size or 200), 1)]
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

                    df = self._compute_historical_metrics(df, metric_cols, all_companies_stats=cross_stats)
                    pending_batches.append(df[output_cols].copy())

                    if len(pending_batches) >= 5:
                        merged = pd.concat(pending_batches, ignore_index=True)
                        temp_name = f"_tmp_{target_table}_{random.randint(1000, 9999)}"
                        merged.to_sql(temp_name, conn, if_exists="replace", index=False)
                        conn.execute(
                            f"INSERT OR REPLACE INTO {self._sql_ident(target_table)} ({cols_sql}) "
                            f"SELECT {cols_sql} FROM {self._sql_ident(temp_name)}"
                        )
                        conn.execute(f"DROP TABLE IF EXISTS {self._sql_ident(temp_name)}")
                        conn.commit()
                        pending_batches.clear()

                if pending_batches:
                    merged = pd.concat(pending_batches, ignore_index=True)
                    temp_name = f"_tmp_{target_table}_{random.randint(1000, 9999)}"
                    merged.to_sql(temp_name, conn, if_exists="replace", index=False)
                    conn.execute(
                        f"INSERT OR REPLACE INTO {self._sql_ident(target_table)} ({cols_sql}) "
                        f"SELECT {cols_sql} FROM {self._sql_ident(temp_name)}"
                    )
                    conn.execute(f"DROP TABLE IF EXISTS {self._sql_ident(temp_name)}")
                    conn.commit()

            logger.info("Generate Historical Ratios completed.")
        finally:
            conn.close()

    def _create_financial_statement_tables(self, conn, table_specs):
        """Create or expand financial statement tables and docID uniqueness indexes."""
        index_names = {
            "FinancialStatements": "ux_fs_docid",
            "IncomeStatement": "ux_is_docid",
            "BalanceSheet": "ux_bs_docid",
            "CashflowStatement": "ux_cf_docid",
        }
        added_columns = {}

        for table_name, column_specs in table_specs.items():
            cols_sql = ",\n              ".join(
                f"{self._sql_ident(col_name)} {col_type}"
                for col_name, col_type in column_specs
            )
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self._sql_ident(table_name)} (\n"
                f"              {cols_sql}\n"
                f"            )"
            )

            added = self._ensure_typed_table_columns(conn, table_name, column_specs)
            if added:
                logger.info(
                    "Expanded %s with %d mapped column(s): %s",
                    table_name,
                    len(added),
                    ", ".join(added),
                )
            added_columns[table_name] = added

            conn.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {self._sql_ident(index_names[table_name])} "
                f"ON {self._sql_ident(table_name)}({self._sql_ident('docID')})"
            )

        return added_columns

    def _insert_base_financial_statements(
        self,
        conn,
        source_ref,
        temp_docids,
        mappings,
        fs_column_specs,
        company_ref,
        prices_ref,
        col_names=None,
        relevance_predicate="1=1",
    ):
        """Insert/update FinancialStatements base rows for the current docID batch."""
        col_names = col_names or {}
        col_at = col_names.get("AccountingTerm", "AccountingTerm")
        col_period = col_names.get("Period", "Period")
        col_amount = col_names.get("Amount", "Amount")
        fs_map = mappings.get("FinancialStatements", {})

        base_select_exprs = [
            "MAX(s.edinetCode) AS edinetCode",
            "s.docID AS docID",
            "MAX(s.docTypeCode) AS docTypeCode",
            "MIN(s.periodStart) AS periodStart",
            "MAX(s.periodEnd) AS periodEnd",
        ]
        insert_columns = ["edinetCode", "docID", "docTypeCode", "periodStart", "periodEnd"]
        select_columns = [
            "b.edinetCode",
            "b.docID",
            "b.docTypeCode",
            "b.periodStart",
            "b.periodEnd",
        ]

        for col_name, col_type in fs_column_specs:
            expr = self._build_amount_case_expr(
                fs_map.get(col_name),
                col_accounting_term=col_at,
                col_period=col_period,
                col_amount=col_amount,
                value_type=col_type,
            )
            base_select_exprs.append(f"{expr} AS {self._sql_ident(col_name)}")
            insert_columns.append(col_name)
            select_columns.append(f"b.{self._sql_ident(col_name)}")

        insert_columns.append("SharePrice")
        select_columns.append(
            "("
            "SELECT sp.Price "
            f"FROM {prices_ref} sp "
            f"JOIN {company_ref} c ON c.Company_Ticker = sp.Ticker "
            "WHERE c.EdinetCode = b.edinetCode "
            "  AND sp.Date <= b.periodEnd "
            "ORDER BY sp.Date DESC "
            "LIMIT 1"
            ") AS SharePrice"
        )

        base_sql = ",\n                            ".join(base_select_exprs)
        insert_sql = ", ".join(self._sql_ident(col_name) for col_name in insert_columns)
        select_sql = ",\n                    ".join(select_columns)

        sql = f"""
                WITH base AS (
                        SELECT
                            {base_sql}
                        FROM {source_ref} s
                        INNER JOIN {temp_docids} t ON t.docID = s.docID
                        WHERE {relevance_predicate}
                        GROUP BY s.docID
                )
                INSERT OR REPLACE INTO {self._sql_ident('FinancialStatements')}
                    ({insert_sql})
                SELECT
                    {select_sql}
                FROM base b
                """
        conn.execute(sql)

    def _insert_statement_table_rows(self, conn, source_ref, temp_docids, table_name, column_specs, mappings, col_names=None, relevance_predicate="1=1"):
        """Insert/update one statement table from config mappings for current docID batch."""
        col_names = col_names or {}
        col_at = col_names.get("AccountingTerm", "AccountingTerm")
        col_period = col_names.get("Period", "Period")
        col_amount = col_names.get("Amount", "Amount")
        table_mappings = mappings.get(table_name, {})

        select_exprs = ["s.docID AS docID"]
        for col, col_type in column_specs:
            expr = self._build_amount_case_expr(
                table_mappings.get(col),
                col_accounting_term=col_at,
                col_period=col_period,
                col_amount=col_amount,
                value_type=col_type,
            )
            select_exprs.append(f"{expr} AS {self._sql_ident(col)}")

        col_list = ", ".join([self._sql_ident("docID")] + [self._sql_ident(c) for c, _ in column_specs])
        sql = f"""
        INSERT OR REPLACE INTO {self._sql_ident(table_name)} ({col_list})
        SELECT
          {", ".join(select_exprs)}
        FROM {source_ref} s
        INNER JOIN {temp_docids} t ON t.docID = s.docID
                WHERE {relevance_predicate}
        GROUP BY s.docID
        """
        conn.execute(sql)

    def generate_financial_statements(
        self,
        source_database,
        source_table,
        target_database,
        mappings_config,
        company_table=None,
        prices_table=None,
        overwrite=False,
        batch_size=2500,
    ):
        """Generate normalized financial-statement tables from raw EDINET records.

        Supports source and target DB separation by attaching the source
        database when needed. Processing is resumable for missing rows, and
        reruns backfill all relevant documents when new mapped columns are added.
        """
        source_db = source_database
        target_db = target_database
        if not source_db:
            raise ValueError("source_database is required for generate_financial_statements.")
        if not target_db:
            raise ValueError("target_database is required for generate_financial_statements.")
        source_tbl = source_table or "financialData_full"
        company_tbl = company_table or "companyInfo"
        prices_tbl = prices_table or "stock_prices"
        mappings_path = mappings_config
        if not mappings_path:
            raise ValueError("Mappings_Config is required for generate_financial_statements.")

        batch_size = max(int(batch_size or 2500), 1)
        mappings = self._load_financial_statement_mappings(mappings_path)
        filter_config = self._collect_financial_statement_filters(mappings)
        table_specs = self._build_financial_statement_table_specs(mappings)
        fs_column_specs = [
            (col_name, col_type)
            for col_name, col_type in table_specs["FinancialStatements"]
            if col_name not in {
                "edinetCode",
                "docID",
                "docTypeCode",
                "periodStart",
                "periodEnd",
                "DescriptionOfBusiness_EN",
                "SharePrice",
            }
        ]
        statement_column_specs = {
            table_name: [
                (col_name, col_type)
                for col_name, col_type in table_specs[table_name]
                if col_name != "docID"
            ]
            for table_name in ("IncomeStatement", "BalanceSheet", "CashflowStatement")
        }

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

            source_ref = f"{self._sql_ident(source_schema)}.{self._sql_ident(source_tbl)}"

            # Detect actual column names (handles financialdata_full Japanese names)
            col_names = self._resolve_source_col_names(conn, source_schema, source_tbl)
            if any(v != k for k, v in col_names.items()):
                logger.info(
                    "generate_financial_statements: source table uses non-standard column names %s",
                    col_names,
                )

            relevance_predicate = self._build_source_relevance_predicate(
                "s",
                filter_config,
                col_names=col_names,
            )

            if overwrite:
                logger.info("Overwrite enabled - resetting financial statement tables.")
                conn.executescript(
                    """
                    DROP TABLE IF EXISTS FinancialStatements;
                    DROP TABLE IF EXISTS IncomeStatement;
                    DROP TABLE IF EXISTS BalanceSheet;
                    DROP TABLE IF EXISTS CashflowStatement;
                    """
                )

            added_columns = self._create_financial_statement_tables(conn, table_specs)
            schema_expanded = any(cols for cols in added_columns.values())
            if schema_expanded and not overwrite:
                logger.info(
                    "Detected new financial statement columns; reprocessing all relevant documents to backfill them."
                )

            # Resolve company/prices tables with fallback to source DB (if attached)
            company_in_main = self._resolve_table_name_in_schema(conn, "main", company_tbl)
            prices_in_main = self._resolve_table_name_in_schema(conn, "main", prices_tbl)

            company_schema = "main"
            prices_schema = "main"
            company_actual = company_in_main
            prices_actual = prices_in_main

            if not company_actual and source_schema != "main":
                company_actual = self._resolve_table_name_in_schema(conn, source_schema, company_tbl)
                if company_actual:
                    company_schema = source_schema

            if not prices_actual and source_schema != "main":
                prices_actual = self._resolve_table_name_in_schema(conn, source_schema, prices_tbl)
                if prices_actual:
                    prices_schema = source_schema

            if not company_actual:
                raise RuntimeError(
                    f"Company table '{company_tbl}' not found in target or source database; "
                    "required for SharePrice lookup."
                )
            if not prices_actual:
                raise RuntimeError(
                    f"Stock prices table '{prices_tbl}' not found in target or source database; "
                    "required for SharePrice lookup."
                )

            company_ref = f"{self._sql_ident(company_schema)}.{self._sql_ident(company_actual)}"
            prices_ref = f"{self._sql_ident(prices_schema)}.{self._sql_ident(prices_actual)}"

            temp_docids = self._sql_ident("_tmp_fs_docids")
            conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS {temp_docids} (docID TEXT PRIMARY KEY)")

            if schema_expanded and not overwrite:
                pending_sql = f"""
                SELECT DISTINCT s.docID
                FROM {source_ref} s
                WHERE s.docID IS NOT NULL
                  AND {relevance_predicate}
                ORDER BY s.docID
                """
            else:
                pending_sql = f"""
                SELECT DISTINCT s.docID
                FROM {source_ref} s
                LEFT JOIN FinancialStatements fs ON fs.docID = s.docID
                LEFT JOIN IncomeStatement is1 ON is1.docID = s.docID
                LEFT JOIN BalanceSheet bs ON bs.docID = s.docID
                LEFT JOIN CashflowStatement cs ON cs.docID = s.docID
                WHERE s.docID IS NOT NULL
                  AND {relevance_predicate}
                  AND (fs.docID IS NULL OR is1.docID IS NULL OR bs.docID IS NULL OR cs.docID IS NULL)
                ORDER BY s.docID
                """

            doc_cursor = conn.execute(pending_sql)
            total_docs = 0
            while True:
                batch = [row[0] for row in doc_cursor.fetchmany(batch_size)]
                if not batch:
                    break

                with conn:
                    conn.execute(f"DELETE FROM {temp_docids}")
                    conn.executemany(
                        f"INSERT OR IGNORE INTO {temp_docids}(docID) VALUES (?)",
                        [(d,) for d in batch],
                    )

                    self._insert_base_financial_statements(
                        conn,
                        source_ref,
                        temp_docids,
                        mappings,
                        fs_column_specs,
                        company_ref,
                        prices_ref,
                        col_names=col_names,
                        relevance_predicate=relevance_predicate,
                    )

                    for table_name in ("IncomeStatement", "BalanceSheet", "CashflowStatement"):
                        self._insert_statement_table_rows(
                            conn,
                            source_ref,
                            temp_docids,
                            table_name,
                            statement_column_specs[table_name],
                            mappings,
                            col_names=col_names,
                            relevance_predicate=relevance_predicate,
                        )

                total_docs += len(batch)
                if total_docs % (batch_size * 10) == 0:
                    logger.info("Generate Financial Statements progress: %d docs processed", total_docs)

            logger.info("Generate Financial Statements completed. Processed %d document(s).", total_docs)
        finally:
            conn.close()

    def populate_business_descriptions_en(
        self,
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
    ):
        """Populate English business descriptions from configured translation APIs."""
        if not target_database:
            raise ValueError("target_database is required for populate_business_descriptions_en.")
        if not providers_config:
            raise ValueError("providers_config is required for populate_business_descriptions_en.")

        from src.description_translation import (
            TranslationError,
            load_translation_providers,
            translate_text_with_providers,
        )

        batch_size = max(int(batch_size or 25), 1)
        providers, provider_settings = load_translation_providers(providers_config)
        chunk_char_limit = provider_settings.get("chunk_char_limit", 700)
        row_delay_seconds = float(provider_settings.get("row_delay_seconds", 0.0) or 0.0)

        conn = sqlite3.connect(target_database)
        try:
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")

            actual_table = self._resolve_table_name_in_schema(conn, "main", table_name)
            if not actual_table:
                raise RuntimeError(f"Table '{table_name}' not found in target database.")

            actual_docid = self._resolve_column_name(conn, actual_table, docid_column)
            actual_source = self._resolve_column_name(conn, actual_table, source_column)
            if not actual_docid:
                raise RuntimeError(f"Column '{docid_column}' not found in table '{actual_table}'.")
            if not actual_source:
                raise RuntimeError(f"Column '{source_column}' not found in table '{actual_table}'.")

            actual_target = self._resolve_column_name(conn, actual_table, target_column)
            if not actual_target:
                self._ensure_typed_table_columns(conn, actual_table, [(target_column, "TEXT")])
                actual_target = target_column
                conn.commit()

            existing_translation_count = 0
            if not overwrite:
                existing_translation_count = conn.execute(
                    f"SELECT COUNT(*) FROM {self._sql_ident(actual_table)} "
                    f"WHERE {self._sql_ident(actual_target)} IS NOT NULL "
                    f"AND TRIM(CAST({self._sql_ident(actual_target)} AS TEXT)) <> ''"
                ).fetchone()[0]

            translated_rows = 0
            failed_rows = 0
            processed_rows = 0
            provider_usage = {}
            last_docid = None

            while True:
                where_clauses = [
                    f"{self._sql_ident(actual_docid)} IS NOT NULL",
                    f"{self._sql_ident(actual_source)} IS NOT NULL",
                    f"TRIM(CAST({self._sql_ident(actual_source)} AS TEXT)) <> ''",
                ]
                params = []
                if not overwrite:
                    where_clauses.append(
                        f"({self._sql_ident(actual_target)} IS NULL "
                        f"OR TRIM(CAST({self._sql_ident(actual_target)} AS TEXT)) = '')"
                    )
                if last_docid is not None:
                    where_clauses.append(f"{self._sql_ident(actual_docid)} > ?")
                    params.append(last_docid)

                sql = (
                    f"SELECT {self._sql_ident(actual_docid)}, {self._sql_ident(actual_source)} "
                    f"FROM {self._sql_ident(actual_table)} "
                    f"WHERE {' AND '.join(where_clauses)} "
                    f"ORDER BY {self._sql_ident(actual_docid)} "
                    f"LIMIT ?"
                )
                params.append(batch_size)
                rows = conn.execute(sql, params).fetchall()
                if not rows:
                    break

                for doc_id, source_text in rows:
                    last_docid = doc_id
                    processed_rows += 1
                    try:
                        translated_text, provider_name = translate_text_with_providers(
                            source_text,
                            providers,
                            source_language=source_language,
                            target_language=target_language,
                            chunk_char_limit=chunk_char_limit,
                        )
                    except TranslationError as exc:
                        failed_rows += 1
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
                        continue

                    conn.execute(
                        f"UPDATE {self._sql_ident(actual_table)} "
                        f"SET {self._sql_ident(actual_target)} = ? "
                        f"WHERE {self._sql_ident(actual_docid)} = ?",
                        (clean_translation, doc_id),
                    )
                    translated_rows += 1
                    provider_usage[provider_name] = provider_usage.get(provider_name, 0) + 1

                    if row_delay_seconds > 0:
                        time.sleep(row_delay_seconds)

                conn.commit()

            logger.info(
                "Populate Business Descriptions EN completed. Processed=%d translated=%d failed=%d existing=%d providers=%s",
                processed_rows,
                translated_rows,
                failed_rows,
                existing_translation_count,
                provider_usage,
            )
            return {
                "processed_rows": processed_rows,
                "translated_rows": translated_rows,
                "failed_rows": failed_rows,
                "existing_translation_rows": existing_translation_count,
                "provider_usage": provider_usage,
            }
        finally:
            conn.close()


    def add_missing_columns(self, conn, table_name, df):
        """Add columns to the SQLite table that are present in the DataFrame but not in the table.

        Args:
            conn (sqlite3.Connection): SQLite connection object.
            table_name (str): Name of the table to modify.
            df (pd.DataFrame): DataFrame whose columns are compared against the table.

        Returns:
            None
        """
        cursor = conn.cursor()
        
        # Get existing columns in the table
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = [info[1] for info in cursor.fetchall()]
        
        # Add missing columns
        for column in df.columns:
            if column not in existing_columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{column}' TEXT")
        
        conn.commit()


    def delete_table(self, table_name, connection=None):
        """Delete a table from the SQLite database if it exists.

        Args:
            table_name (str): Name of the table to delete.
            connection (sqlite3.Connection, optional): Existing database
                connection. A new connection is opened and closed automatically
                when omitted.

        Returns:
            None
        """
        try:
            if connection is None:
                db_path = getattr(self, "DB_PATH", None)
                if not db_path:
                    raise ValueError(
                        "delete_table requires a connection or self.DB_PATH to be set."
                    )
                conn = sqlite3.connect(db_path)
            else:
                conn = connection
            cursor = conn.cursor()
            
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
        except Exception as e:
            print(f"An error occurred while deleting table {table_name}: {e}")
        finally:
            if connection is None:
                conn.close()

    def copy_table(self, conn, source_table, target_table):
        """Copy all rows and columns from one SQLite table to a new table.

        Args:
            conn (sqlite3.Connection): SQLite connection object.
            source_table (str): Name of the source table.
            target_table (str): Name of the new target table to create.

        Returns:
            None
        """
        cursor = conn.cursor()        
        # Create the target table if it doesn't exist
        cursor.execute(f"CREATE TABLE {target_table} AS SELECT * FROM {source_table}")        
        
        conn.commit()

    def parse_edinet_taxonomy(self, xsd_file, table_name, connection=None, db_path=None):
        """
        Parses an EDINET Taxonomy XSD file and stores relevant elements in an SQLite database.

        Args:
            xsd_file: Path to the EDINET XSD file.
            table_name: Name of the SQLite table to write elements into.
            connection: Optional existing SQLite connection.  A new one is
                opened (and closed) automatically when omitted.
            db_path: Path to the SQLite database file. Required when
                *connection* is not provided.
        """
        tree = ET.parse(xsd_file)
        root = tree.getroot()
        namespace = "{http://www.w3.org/2001/XMLSchema}"

        elements = []
        for elem in root.findall(f"{namespace}element"):
            name = elem.get("name")
            elem_id = elem.get("id")
            abstract = elem.get("abstract", "false")
            balance = elem.get("{http://www.xbrl.org/2003/instance}balance")
            period_type = elem.get("{http://www.xbrl.org/2003/instance}periodType")

            elem_id_adjusted = self._adjust_string(elem_id, "jppfs_cor_", "jppfs_cor:")

            if period_type == "instant" and abstract == "false":
                statement = "Balance Sheet"
            elif period_type == "duration" and abstract == "false" and balance is not None:
                statement = "Income Statement"
            elif period_type == "duration" and abstract == "false" and balance is None:
                statement = "Cashflow Statement"
            else:
                statement = "Other Statement"

            if statement == "Balance Sheet" and balance == "credit":
                elem_type = "Liability"
            elif statement == "Balance Sheet" and balance == "debit":
                elem_type = "Asset"
            elif statement == "Income Statement" and balance == "debit":
                elem_type = "Expense"
            elif statement == "Income Statement" and balance == "credit":
                elem_type = "Income"
            else:
                elem_type = "Other"

            if elem_id and name:
                elements.append((elem_id_adjusted, name, statement, elem_type))

        if connection is None:
            if not db_path:
                raise ValueError("Either connection or db_path is required for parse_edinet_taxonomy.")
            conn = sqlite3.connect(db_path)
        else:
            conn = connection

        try:
            self._create_table(conn, table_name, ["Id", "Name", "Statement", "Type"])
            self._insert_data(conn, table_name, elements)
            conn.commit()
        finally:
            if connection is None:
                conn.close()

    # ------------------------------------------------------------------
    # Private helpers for parse_edinet_taxonomy
    # ------------------------------------------------------------------

    def _create_table(self, conn, table_name, columns):
        """Create *table_name* with TEXT columns if it does not already exist."""
        column_definitions = ", ".join([f"{col} TEXT" for col in columns])
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})")

    def _insert_data(self, conn, table_name, rows):
        """Bulk-insert *rows* (list of tuples) into *table_name*."""
        if not rows:
            return
        placeholders = ", ".join(["?" for _ in rows[0]])
        conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)

    def _adjust_string(self, input_string, check_substring, replace_substring):
        """Replace the leading *check_substring* with *replace_substring* once."""
        if input_string and input_string.startswith(check_substring):
            return input_string.replace(check_substring, replace_substring, 1)
        return input_string
