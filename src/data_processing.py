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

import src.taxonomy_processing as taxonomy_processing

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
        """Load canonical-metric config into {table: {column: mapping_dict}}.

        Supports both the legacy ``Mappings`` format and the newer ``Metrics``
        format used by the taxonomy-backed compatibility layer.
        """
        with open(mappings_config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        normalized = {
            "FinancialStatements": {},
            "IncomeStatement": {},
            "BalanceSheet": {},
            "CashflowStatement": {},
        }

        if isinstance(raw, dict) and isinstance(raw.get("Metrics"), list):
            items = raw.get("Metrics", [])
            for entry in items:
                if not isinstance(entry, dict):
                    continue
                table = entry.get("OutputTable") or entry.get("Table")
                name = entry.get("Key") or entry.get("Name")
                if table not in normalized or not name:
                    continue

                selectors = entry.get("Selectors", []) or []
                terms = []
                periods = []
                statement_family = None
                for selector in selectors:
                    if not isinstance(selector, dict):
                        continue
                    selector_terms = selector.get("concepts") or selector.get("Terms") or []
                    selector_periods = selector.get("periods") or []
                    if selector.get("statement_family") and not statement_family:
                        statement_family = selector.get("statement_family")
                    terms.extend(term for term in selector_terms if term)
                    periods.extend(period for period in selector_periods if period)

                if not terms:
                    terms = entry.get("Terms", []) or []
                if not periods:
                    periods = entry.get("periods", []) or []

                normalized[table][name] = {
                    "Terms": list(dict.fromkeys(terms)),
                    "periods": list(dict.fromkeys(periods)),
                    "statement_family": statement_family,
                    "ValueType": entry.get("ValueType"),
                }
            return normalized

        mappings = raw.get("Mappings", []) if isinstance(raw, dict) else []
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
                "statement_family": entry.get("statement_family"),
                "ValueType": entry.get("ValueType"),
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

    def _build_statement_family_fallbacks(self, mappings):
        """Map configured concept terms to statement families for non-taxonomy fallback."""
        fallbacks = {}
        for table_name in ("IncomeStatement", "BalanceSheet", "CashflowStatement"):
            for mapping in (mappings.get(table_name, {}) or {}).values():
                if not isinstance(mapping, dict):
                    continue
                for term in mapping.get("Terms", []) or []:
                    normalized = self._normalise_taxonomy_term(term) or term
                    if normalized and normalized not in fallbacks:
                        fallbacks[normalized] = table_name
        return fallbacks

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
        """Detect the actual source column names in the raw financial-data table.

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
            "項目名":       "ItemName",
            "コンテキストID": "Period",
            "相対年度":     "RelativeYear",
            "連結・個別":    "Consolidation",
            "期間・時点":    "PeriodType",
            "ユニットID":   "Currency",
            "単位":         "UnitName",
            "値":          "Amount",
        }

        # Build reverse: standard name → alternative name
        reverse_map = {v: k for k, v in ALTERNATIVE_TO_STANDARD.items()}

        result = {}
        required_columns = {
            "AccountingTerm",
            "Period",
            "Amount",
            "docID",
            "edinetCode",
            "docTypeCode",
            "periodStart",
            "periodEnd",
        }

        for standard_name in (
            "AccountingTerm",
            "ItemName",
            "Period",
            "RelativeYear",
            "Consolidation",
            "PeriodType",
            "Currency",
            "UnitName",
            "Amount",
            "docID",
            "edinetCode",
            "docTypeCode",
            "submitDateTime",
            "periodStart",
            "periodEnd",
        ):
            if standard_name in table_cols:
                result[standard_name] = standard_name
            elif standard_name in reverse_map and reverse_map[standard_name] in table_cols:
                result[standard_name] = reverse_map[standard_name]
            else:
                result[standard_name] = standard_name if standard_name in required_columns else None

        return result

    def _source_column_expr(self, alias, column_name):
        """Return a qualified source-column expression or NULL when missing."""
        if not column_name:
            return "NULL"
        return f"{alias}.{self._sql_ident(column_name)}"

    def _normalise_taxonomy_term(self, value):
        """Normalize taxonomy identifiers such as jppfs_cor_X -> jppfs_cor:X."""
        text = str(value or "").strip()
        if not text:
            return None
        if ":" in text:
            return text
        match = re.match(r"^([A-Za-z0-9\-]+_[A-Za-z0-9\-]+)_(.+)$", text)
        if match:
            return f"{match.group(1)}:{match.group(2)}"
        return text

    def _taxonomy_prefix(self, value):
        normalized = self._normalise_taxonomy_term(value)
        if not normalized or ":" not in normalized:
            return None
        return normalized.split(":", 1)[0]

    def _taxonomy_local_name(self, value):
        normalized = self._normalise_taxonomy_term(value)
        if not normalized:
            return None
        if ":" in normalized:
            return normalized.split(":", 1)[1]
        return normalized

    def _try_real(self, value):
        """Best-effort numeric conversion used by normalized fact loading."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)

        text = str(value).strip()
        if not text:
            return None

        normalized = (
            text.replace(",", "")
            .replace("△", "-")
            .replace("▲", "-")
            .replace("−", "-")
        )
        if normalized.startswith("(") and normalized.endswith(")"):
            normalized = f"-{normalized[1:-1]}"
        try:
            return float(normalized)
        except ValueError:
            return None

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
        explicit_type = str((mapping or {}).get("ValueType") or "").strip().upper()
        if explicit_type in {"TEXT", "REAL", "INTEGER"}:
            return explicit_type
        terms = [str(term or "") for term in (mapping or {}).get("Terms", []) or []]
        if any("TextBlock" in term for term in terms):
            return "TEXT"
        return "REAL"

    def _build_fact_value_case_expr(self, mapping, facts_alias="f", value_column=None, text_column=None):
        """Build MAX(CASE WHEN ...) against the normalized statement_facts table."""
        if not mapping:
            return "NULL"

        terms = mapping.get("Terms", []) or []
        periods = mapping.get("periods", []) or []
        statement_family = mapping.get("statement_family")
        if not terms:
            return "NULL"

        value_col = text_column if self._mapping_storage_type(mapping) == "TEXT" else value_column
        if not value_col:
            return "NULL"

        term_list = ", ".join(self._sql_literal(self._normalise_taxonomy_term(term) or term) for term in terms)
        conditions = [f"{facts_alias}.{self._sql_ident('concept_qname')} IN ({term_list})"]
        if periods:
            period_list = ", ".join(self._sql_literal(period) for period in periods)
            conditions.append(f"{facts_alias}.{self._sql_ident('source_period')} IN ({period_list})")
        if statement_family:
            conditions.append(
                f"{facts_alias}.{self._sql_ident('statement_family')} = {self._sql_literal(statement_family)}"
            )

        return (
            f"MAX(CASE WHEN {' AND '.join(conditions)} "
            f"THEN {facts_alias}.{self._sql_ident(value_col)} END)"
        )

    def _build_financial_statement_table_specs(self, mappings):
        """Build ordered table specs for the FinancialStatements output table."""
        table_specs = {
            "FinancialStatements": [
                ("edinetCode", "TEXT"),
                ("docID", "TEXT"),
                ("docTypeCode", "TEXT"),
                ("periodStart", "TEXT"),
                ("periodEnd", "TIMESTAMP"),
                ("taxonomy_release_id", "INTEGER"),
                ("release_resolution_method", "TEXT"),
                ("release_resolution_note", "TEXT"),
                ("DescriptionOfBusiness_EN", "TEXT"),
            ],
        }

        reserved = {
            table_name: {col_name for col_name, _ in column_specs}
            for table_name, column_specs in table_specs.items()
        }
        reserved["FinancialStatements"].add("SharePrice")

        for col_name, mapping in self._collect_financial_statement_metric_map(mappings).items():
            if not self._is_safe_identifier(col_name):
                logger.warning(
                    "Skipping mapped column %r for table FinancialStatements because the name is not a safe SQL identifier.",
                    col_name,
                )
                continue
            if col_name in reserved["FinancialStatements"]:
                logger.warning(
                    "Skipping mapped column %r for table FinancialStatements because that column is reserved.",
                    col_name,
                )
                continue

            table_specs["FinancialStatements"].append(
                (col_name, self._mapping_storage_type(mapping))
            )
            reserved["FinancialStatements"].add(col_name)

        table_specs["FinancialStatements"].append(("SharePrice", "REAL"))
        return table_specs

    def _collect_financial_statement_metric_map(self, mappings):
        """Return only doc-level FinancialStatements mappings.

        Taxonomy-backed statement tables are now driven by taxonomy hierarchy rather
        than promoting statement-family metrics into FinancialStatements.
        """
        return {
            col_name: mapping
            for col_name, mapping in (mappings.get("FinancialStatements", {}) or {}).items()
            if col_name
        }

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

    def _get_table_columns_in_schema(self, conn, schema_name, table_name):
        """Return table columns for a table that may live in an attached schema."""
        if schema_name == "main":
            rows = conn.execute(
                f"PRAGMA table_info({self._sql_ident(table_name)})"
            ).fetchall()
        else:
            rows = conn.execute(
                f"PRAGMA {self._sql_ident(schema_name)}.table_info({self._sql_ident(table_name)})"
            ).fetchall()
        return [str(row[1]) for row in rows]

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

    def _collect_generate_ratios_base_columns(self, definitions):
        """Return the set of base-table columns referenced by ratio formulas."""
        alias_map = {
            "FinancialStatements": "fs",
            "IncomeStatement": "is1",
            "BalanceSheet": "bs",
            "CashflowStatement": "cs",
            "PerShare": "ps",
            "Valuation": "va",
            "Quality": "qu",
        }
        base_columns = {
            "FinancialStatements": set(),
            "IncomeStatement": set(),
            "BalanceSheet": set(),
            "CashflowStatement": set(),
        }

        for table_name in ("PerShare", "Valuation", "Quality"):
            for item in definitions.get(table_name, []):
                formula = item.get("Formula") or ""
                try:
                    _sql_expr, refs = self._formula_to_sql_expr_and_refs(formula, alias_map)
                except Exception:
                    continue
                for ref_table, ref_col in refs:
                    if ref_table in base_columns:
                        base_columns[ref_table].add(ref_col)

        return {
            table_name: sorted(columns)
            for table_name, columns in base_columns.items()
            if columns
        }

    def _canonical_metrics_config_path(self):
        """Return the bundled canonical metrics config path."""
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(repo_root, "config", "reference", "canonical_metrics_config.json")

    def _create_ratio_statement_metrics_temp_table(
        self,
        conn,
        source_schema,
        logical_table,
        temp_table_name,
        required_columns,
        canonical_mappings,
    ):
        """Build a temporary wide metrics table for ratio generation."""
        fs_actual = self._resolve_table_name_in_schema(conn, source_schema, "FinancialStatements")
        if fs_actual:
            fs_columns = {
                col.lower(): col
                for col in self._get_table_columns_in_schema(conn, source_schema, fs_actual)
            }
            if all(col.lower() in fs_columns for col in required_columns):
                fs_ref = f"{self._sql_ident(source_schema)}.{self._sql_ident(fs_actual)}"
                select_exprs = ["fs.docID AS docID"]
                for col_name in required_columns:
                    actual_col = fs_columns[col_name.lower()]
                    select_exprs.append(
                        f"fs.{self._sql_ident(actual_col)} AS {self._sql_ident(col_name)}"
                    )

                conn.execute(f"DROP TABLE IF EXISTS {self._sql_ident(temp_table_name)}")
                conn.execute(
                    f"""
                    CREATE TEMP TABLE {self._sql_ident(temp_table_name)} AS
                    SELECT
                        {', '.join(select_exprs)}
                    FROM {fs_ref} fs
                    WHERE fs.docID IS NOT NULL
                    """
                )
                self._create_index_if_not_exists(conn, "temp", temp_table_name, ["docID"])
                return

        docs_actual = self._resolve_table_name_in_schema(conn, source_schema, "statement_documents")
        facts_actual = self._resolve_table_name_in_schema(conn, source_schema, "statement_facts")
        if not docs_actual or not facts_actual:
            raise RuntimeError(
                "Source database does not contain canonical FinancialStatements columns or "
                "legacy statement_documents/statement_facts; "
                f"cannot derive {logical_table} metrics from taxonomy-backed statements."
            )

        table_mappings = canonical_mappings.get(logical_table, {}) or {}
        docs_ref = f"{self._sql_ident(source_schema)}.{self._sql_ident(docs_actual)}"
        facts_ref = f"{self._sql_ident(source_schema)}.{self._sql_ident(facts_actual)}"
        select_exprs = ["d.docID AS docID"]

        for col_name in required_columns:
            expr = self._build_fact_value_case_expr(
                table_mappings.get(col_name),
                facts_alias="f",
                value_column="value_numeric",
                text_column="raw_value_text",
            )
            select_exprs.append(f"{expr} AS {self._sql_ident(col_name)}")

        conn.execute(f"DROP TABLE IF EXISTS {self._sql_ident(temp_table_name)}")
        conn.execute(
            f"""
            CREATE TEMP TABLE {self._sql_ident(temp_table_name)} AS
            SELECT
                {', '.join(select_exprs)}
            FROM {docs_ref} d
            LEFT JOIN {facts_ref} f ON f.docID = d.docID
            GROUP BY d.docID
            """
        )
        self._create_index_if_not_exists(conn, "temp", temp_table_name, ["docID"])

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
        referenced_base_columns = self._collect_generate_ratios_base_columns(definitions)

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

            base_refs = {}
            fs_actual = self._resolve_table_name_in_schema(conn, source_schema, "FinancialStatements")
            if not fs_actual:
                raise RuntimeError(
                    "Source table 'FinancialStatements' not found in source database; required for Generate Ratios."
                )
            base_refs["FinancialStatements"] = (
                f"{self._sql_ident(source_schema)}.{self._sql_ident(fs_actual)}"
            )

            canonical_mappings = None
            for table_name in ("IncomeStatement", "BalanceSheet", "CashflowStatement"):
                required_columns = referenced_base_columns.get(table_name, [])
                if not required_columns:
                    continue

                actual = self._resolve_table_name_in_schema(conn, source_schema, table_name)
                if actual:
                    actual_columns = {
                        col.lower(): col for col in self._get_table_columns_in_schema(conn, source_schema, actual)
                    }
                    if "concept_qname" not in actual_columns and all(
                        required_col.lower() in actual_columns for required_col in required_columns
                    ):
                        base_refs[table_name] = (
                            f"{self._sql_ident(source_schema)}.{self._sql_ident(actual)}"
                        )
                        continue

                if canonical_mappings is None:
                    canonical_config = self._canonical_metrics_config_path()
                    if not os.path.exists(canonical_config):
                        raise RuntimeError(
                            "Canonical metrics config not found; cannot derive ratio inputs from taxonomy-backed statements."
                        )
                    canonical_mappings = self._load_financial_statement_mappings(canonical_config)

                temp_table_name = f"_tmp_ratio_{table_name}"
                self._create_ratio_statement_metrics_temp_table(
                    conn,
                    source_schema,
                    table_name,
                    temp_table_name,
                    required_columns,
                    canonical_mappings,
                )
                base_refs[table_name] = self._sql_ident(temp_table_name)

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
                f"LEFT JOIN {self._sql_ident('PerShare')} ps ON ps.docID = fs.docID "
                f"LEFT JOIN {self._sql_ident('Valuation')} va ON va.docID = fs.docID "
                f"LEFT JOIN {self._sql_ident('Quality')} qu ON qu.docID = fs.docID"
            )
            if "IncomeStatement" in base_refs:
                join_sql += f" LEFT JOIN {base_refs['IncomeStatement']} is1 ON is1.docID = fs.docID"
            if "BalanceSheet" in base_refs:
                join_sql += f" LEFT JOIN {base_refs['BalanceSheet']} bs ON bs.docID = fs.docID"
            if "CashflowStatement" in base_refs:
                join_sql += f" LEFT JOIN {base_refs['CashflowStatement']} cs ON cs.docID = fs.docID"

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
        """Create or expand the FinancialStatements table and its docID index."""
        index_names = {
            "FinancialStatements": "ux_fs_docid",
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
            if table_name == "FinancialStatements":
                self._create_index_if_not_exists(conn, "main", table_name, ["edinetCode", "periodEnd"])

        return added_columns

    def _statement_primary_period(self, table_name):
        if table_name == "BalanceSheet":
            return "CurrentYearInstant"
        return "CurrentYearDuration"

    def _statement_line_items_table_name(self):
        return "statement_line_items"

    def _taxonomy_levels_table_name(self):
        return "taxonomy_levels"

    def _ensure_statement_line_items_table(self, conn):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._sql_ident(self._statement_line_items_table_name())} (
                statement_family TEXT NOT NULL,
                concept_qname TEXT NOT NULL,
                column_name TEXT,
                display_label TEXT,
                concept_name TEXT,
                taxonomy_release_id INTEGER,
                role_uri TEXT,
                presentation_parent_qname TEXT,
                parent_column_name TEXT,
                line_order REAL,
                line_depth INTEGER,
                period_key TEXT,
                value_type TEXT,
                is_abstract INTEGER,
                is_required_metric INTEGER,
                PRIMARY KEY (statement_family, concept_qname)
            )
            """
        )
        self._create_index_if_not_exists(
            conn,
            "main",
            self._statement_line_items_table_name(),
            ["statement_family", "line_depth", "line_order"],
        )
        self._create_index_if_not_exists(
            conn,
            "main",
            self._statement_line_items_table_name(),
            ["statement_family", "column_name"],
        )

    def _normalize_statement_label(self, value):
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        return text

    def _statement_value_storage_type(self, concept_qname=None, data_type=None):
        qname = str(concept_qname or "")
        dtype = str(data_type or "").lower()
        if qname.endswith("TextBlock"):
            return "TEXT"
        if any(token in dtype for token in ("text", "string", "textblock")):
            return "TEXT"
        return "REAL"

    def _statement_column_name(self, display_label, concept_name, concept_qname, used_names):
        base_label = self._normalize_statement_label(display_label)
        if not base_label:
            base_label = self._normalize_statement_label(concept_name)
        if not base_label:
            base_label = self._normalize_statement_label(concept_qname)

        candidate = base_label or "Metric"
        suffix = self._normalize_statement_label(concept_name) or self._normalize_statement_label(concept_qname)
        if suffix and suffix.lower() == candidate.lower():
            suffix = self._normalize_statement_label(concept_qname)

        if candidate.lower() not in used_names:
            used_names.add(candidate.lower())
            return candidate

        if suffix:
            candidate = f"{base_label} [{suffix}]"
        counter = 2
        unique_candidate = candidate
        while unique_candidate.lower() in used_names:
            unique_candidate = f"{candidate} ({counter})"
            counter += 1
        used_names.add(unique_candidate.lower())
        return unique_candidate

    def _collect_required_statement_terms(self, mappings):
        required = {table_name: set() for table_name in self._statement_table_names()}
        for table_name in self._statement_table_names():
            for mapping in (mappings.get(table_name, {}) or {}).values():
                for term in mapping.get("Terms", []) or []:
                    normalized = self._normalise_taxonomy_term(term) or term
                    if normalized:
                        required[table_name].add(normalized)
        return required

    def _load_statement_catalog(self, conn, mappings, max_line_depth=None):
        catalog = {table_name: [] for table_name in self._statement_table_names()}
        mapped_statement_terms = self._collect_required_statement_terms(mappings)
        taxonomy_available = False
        required_terms = {table_name: set() for table_name in self._statement_table_names()}
        term_to_family = {
            term: family
            for family, terms in mapped_statement_terms.items()
            for term in terms
        }
        rows_by_family = {table_name: {} for table_name in self._statement_table_names()}

        if self._table_exists(conn, self._taxonomy_levels_table_name()):
            taxonomy_available = True
            family_list_sql = ", ".join(self._sql_literal(name) for name in self._statement_table_names())
            rows = conn.execute(
                f"""
                SELECT
                    tl.release_id,
                    tl.concept_qname,
                    COALESCE(tc.concept_name, taxonomy_local_name(tl.concept_qname)) AS concept_name,
                    tl.statement_family,
                    tc.primary_role_uri,
                    tl.parent_concept_qname,
                    tc.primary_line_order,
                    tl.level AS line_depth,
                    COALESCE(tc.primary_label_en, tl.primary_label_en, tc.primary_label, tc.concept_name, taxonomy_local_name(tl.concept_qname)) AS primary_label,
                    COALESCE(tc.is_abstract, 0) AS is_abstract,
                    tl.data_type
                FROM {self._sql_ident(self._taxonomy_levels_table_name())} tl
                LEFT JOIN {self._sql_ident('taxonomy_concepts')} tc
                  ON tc.release_id = tl.release_id
                 AND tc.namespace_prefix = tl.namespace_prefix
                 AND tc.concept_qname = tl.concept_qname
                WHERE tl.statement_family IN ({family_list_sql})
                ORDER BY
                    COALESCE(tl.release_id, 0) DESC,
                    CASE WHEN tc.primary_role_uri IS NULL OR tc.primary_role_uri = '' THEN 1 ELSE 0 END,
                    CASE WHEN tc.primary_line_order IS NULL THEN 1 ELSE 0 END,
                    COALESCE(tl.level, 999999),
                    COALESCE(tc.primary_line_order, 999999999.0),
                    tl.concept_qname
                """
            ).fetchall()

            for row in rows:
                concept_qname = self._normalise_taxonomy_term(row[1]) or row[1]
                family = row[3] or term_to_family.get(concept_qname)
                if family not in rows_by_family:
                    continue
                if concept_qname in rows_by_family[family]:
                    continue
                rows_by_family[family][concept_qname] = {
                    "statement_family": family,
                    "concept_qname": concept_qname,
                    "concept_name": row[2] or self._taxonomy_local_name(concept_qname),
                    "display_label": row[8] or row[2] or self._taxonomy_local_name(concept_qname),
                    "taxonomy_release_id": row[0],
                    "role_uri": row[4] or None,
                    "presentation_parent_qname": self._normalise_taxonomy_term(row[5]) or row[5],
                    "line_order": row[6],
                    "line_depth": row[7],
                    "period_key": self._statement_primary_period(family),
                    "value_type": self._statement_value_storage_type(concept_qname, row[10]),
                    "is_abstract": 1 if row[9] else 0,
                }

        elif self._table_exists(conn, "taxonomy_concepts"):
            taxonomy_available = True
            family_list_sql = ", ".join(self._sql_literal(name) for name in self._statement_table_names())
            required_sql = ""
            all_required_terms = sorted(term_to_family)
            if all_required_terms:
                required_sql = (
                    " OR concept_qname IN (" + ", ".join(self._sql_literal(term) for term in all_required_terms) + ")"
                )

            rows = conn.execute(
                f"""
                SELECT
                    release_id,
                    concept_qname,
                    concept_name,
                    statement_family_default,
                    primary_role_uri,
                    primary_parent_concept_qname,
                    primary_line_order,
                    primary_line_depth,
                    COALESCE(primary_label_en, primary_label, concept_name, taxonomy_local_name(concept_qname)) AS primary_label,
                    is_abstract,
                    data_type
                FROM taxonomy_concepts
                WHERE statement_family_default IN ({family_list_sql})
                   {required_sql}
                ORDER BY
                    COALESCE(release_id, 0) DESC,
                    CASE WHEN primary_role_uri IS NULL THEN 1 ELSE 0 END,
                    CASE WHEN primary_line_order IS NULL THEN 1 ELSE 0 END,
                    COALESCE(primary_line_depth, 999999),
                    COALESCE(primary_line_order, 999999999.0),
                    concept_qname
                """
            ).fetchall()

            for row in rows:
                concept_qname = self._normalise_taxonomy_term(row[1]) or row[1]
                family = row[3] or term_to_family.get(concept_qname)
                if family not in rows_by_family:
                    continue
                if concept_qname in rows_by_family[family]:
                    continue
                rows_by_family[family][concept_qname] = {
                    "statement_family": family,
                    "concept_qname": concept_qname,
                    "concept_name": row[2] or self._taxonomy_local_name(concept_qname),
                    "display_label": row[8] or row[2] or self._taxonomy_local_name(concept_qname),
                    "taxonomy_release_id": row[0],
                    "role_uri": row[4],
                    "presentation_parent_qname": self._normalise_taxonomy_term(row[5]) or row[5],
                    "line_order": row[6],
                    "line_depth": row[7],
                    "period_key": self._statement_primary_period(family),
                    "value_type": self._statement_value_storage_type(concept_qname, row[10]),
                    "is_abstract": 1 if row[9] else 0,
                }

        if not taxonomy_available:
            required_terms = mapped_statement_terms

        for family, terms in required_terms.items():
            for term in sorted(terms):
                if term in rows_by_family[family]:
                    continue
                local_name = self._taxonomy_local_name(term) or term
                rows_by_family[family][term] = {
                    "statement_family": family,
                    "concept_qname": term,
                    "concept_name": local_name,
                    "display_label": local_name,
                    "taxonomy_release_id": None,
                    "role_uri": None,
                    "presentation_parent_qname": None,
                    "line_order": None,
                    "line_depth": None,
                    "period_key": self._statement_primary_period(family),
                    "value_type": self._statement_value_storage_type(term, None),
                    "is_abstract": 0,
                }

        for family, family_rows in rows_by_family.items():
            include = set()
            for concept_qname, entry in family_rows.items():
                required = concept_qname in required_terms.get(family, set())
                depth = entry.get("line_depth")
                within_depth = (
                    max_line_depth is None
                    or depth is None
                    or int(depth) <= int(max_line_depth)
                )
                if required or (within_depth and not entry.get("is_abstract")):
                    include.add(concept_qname)

            frontier = list(include)
            while frontier:
                child_qname = frontier.pop()
                parent_qname = family_rows.get(child_qname, {}).get("presentation_parent_qname")
                if parent_qname and parent_qname in family_rows and parent_qname not in include:
                    include.add(parent_qname)
                    frontier.append(parent_qname)

            if not include:
                continue

            ordered_qnames = sorted(
                include,
                key=lambda qname: (
                    str(family_rows[qname].get("role_uri") or ""),
                    float(family_rows[qname].get("line_order"))
                    if family_rows[qname].get("line_order") is not None
                    else float("inf"),
                    int(family_rows[qname].get("line_depth") or 0),
                    str(family_rows[qname].get("display_label") or family_rows[qname].get("concept_name") or qname),
                    qname,
                ),
            )

            used_names = set()
            qname_to_column = {}
            entries = []
            for concept_qname in ordered_qnames:
                entry = dict(family_rows[concept_qname])
                entry["is_required_metric"] = 1 if concept_qname in required_terms.get(family, set()) else 0
                if not entry.get("is_abstract"):
                    entry["column_name"] = self._statement_column_name(
                        entry.get("display_label"),
                        entry.get("concept_name"),
                        entry.get("concept_qname"),
                        used_names,
                    )
                    qname_to_column[concept_qname] = entry["column_name"]
                else:
                    entry["column_name"] = None
                entries.append(entry)

            for entry in entries:
                parent_qname = entry.get("presentation_parent_qname")
                entry["parent_column_name"] = qname_to_column.get(parent_qname)
            catalog[family] = entries

        return catalog

    def _statement_table_column_specs(self, catalog_entries):
        specs = [("docID", "TEXT PRIMARY KEY")]
        for entry in catalog_entries:
            column_name = entry.get("column_name")
            if not column_name:
                continue
            specs.append((column_name, entry.get("value_type") or "REAL"))
        return specs

    def _statement_table_shape_matches(self, conn, table_name, column_specs):
        if not self._table_exists(conn, table_name):
            return False
        rows = conn.execute(f"PRAGMA table_info({self._sql_ident(table_name)})").fetchall()
        existing_columns = [row[1] for row in rows]
        expected_columns = [col_name for col_name, _col_type in column_specs]
        return existing_columns == expected_columns

    def _ensure_wide_statement_tables(self, conn, catalog, overwrite=False):
        desired_specs = {
            table_name: self._statement_table_column_specs(catalog.get(table_name, []))
            for table_name in self._statement_table_names()
        }

        rebuild_schema = bool(overwrite)
        for table_name, column_specs in desired_specs.items():
            if len(column_specs) - 1 > 1900:
                raise RuntimeError(
                    f"{table_name} would require {len(column_specs) - 1} taxonomy columns. "
                    "Reduce generate_financial_statements max_line_depth before rerunning."
                )
            if rebuild_schema:
                continue
            if not self._statement_table_shape_matches(conn, table_name, column_specs):
                rebuild_schema = True

        if rebuild_schema:
            for table_name in self._statement_table_names():
                conn.execute(f"DROP TABLE IF EXISTS {self._sql_ident(table_name)}")

        for table_name, column_specs in desired_specs.items():
            cols_sql = ",\n              ".join(
                f"{self._sql_ident(col_name)} {col_type}"
                for col_name, col_type in column_specs
            )
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self._sql_ident(table_name)} (\n"
                f"              {cols_sql}\n"
                f"            )"
            )

        return rebuild_schema

    def _refresh_statement_line_items(self, conn, catalog):
        table_name = self._statement_line_items_table_name()
        conn.execute(f"DELETE FROM {self._sql_ident(table_name)}")
        payload = []
        for family in self._statement_table_names():
            for entry in catalog.get(family, []):
                payload.append(
                    (
                        family,
                        entry.get("concept_qname"),
                        entry.get("column_name"),
                        entry.get("display_label"),
                        entry.get("concept_name"),
                        entry.get("taxonomy_release_id"),
                        entry.get("role_uri"),
                        entry.get("presentation_parent_qname"),
                        entry.get("parent_column_name"),
                        entry.get("line_order"),
                        entry.get("line_depth"),
                        entry.get("period_key"),
                        entry.get("value_type"),
                        int(bool(entry.get("is_abstract"))),
                        int(bool(entry.get("is_required_metric"))),
                    )
                )
        if not payload:
            return
        conn.executemany(
            f"""
            INSERT INTO {self._sql_ident(table_name)} (
                statement_family,
                concept_qname,
                column_name,
                display_label,
                concept_name,
                taxonomy_release_id,
                role_uri,
                presentation_parent_qname,
                parent_column_name,
                line_order,
                line_depth,
                period_key,
                value_type,
                is_abstract,
                is_required_metric
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

    def _drop_legacy_statement_storage(self, conn):
        conn.executescript(
            """
            DROP TABLE IF EXISTS statement_documents;
            DROP TABLE IF EXISTS statement_contexts;
            DROP TABLE IF EXISTS statement_facts;
            DROP TABLE IF EXISTS statement_fact_dimensions;
            """
        )

    def _delete_statement_output_rows(self, conn, temp_docids):
        for table_name in ("FinancialStatements",) + self._statement_table_names():
            if not self._table_exists(conn, table_name):
                continue
            conn.execute(
                f"DELETE FROM {self._sql_ident(table_name)} WHERE docID IN (SELECT docID FROM {temp_docids})"
            )

    def _register_release_resolution_functions(self, conn, release_rows):
        def _resolve_release_id(submit_datetime, period_end):
            release_id, _method, _note = self._resolve_document_taxonomy_release(
                release_rows,
                submit_datetime,
                period_end,
            )
            return release_id

        def _resolve_release_method(submit_datetime, period_end):
            _release_id, method, _note = self._resolve_document_taxonomy_release(
                release_rows,
                submit_datetime,
                period_end,
            )
            return method

        def _resolve_release_note(submit_datetime, period_end):
            _release_id, _method, note = self._resolve_document_taxonomy_release(
                release_rows,
                submit_datetime,
                period_end,
            )
            return note

        conn.create_function("resolve_taxonomy_release_id", 2, _resolve_release_id)
        conn.create_function("resolve_taxonomy_release_method", 2, _resolve_release_method)
        conn.create_function("resolve_taxonomy_release_note", 2, _resolve_release_note)

    def _upsert_base_financial_statements_from_source(
        self,
        conn,
        source_ref,
        temp_docids,
        col_names,
        fs_metric_mappings,
        fs_column_specs,
        company_ref,
        prices_ref,
    ):
        doc_col = col_names.get("docID", "docID")
        submit_expr = self._source_column_expr("s", col_names.get("submitDateTime"))
        period_start_expr = self._source_column_expr("s", col_names.get("periodStart"))
        period_end_expr = self._source_column_expr("s", col_names.get("periodEnd"))

        cte_select = [
            f"s.{self._sql_ident(doc_col)} AS docID",
            f"MAX(s.{self._sql_ident(col_names.get('edinetCode', 'edinetCode'))}) AS edinetCode",
            f"MAX(s.{self._sql_ident(col_names.get('docTypeCode', 'docTypeCode'))}) AS docTypeCode",
            f"MAX(CAST({submit_expr} AS TEXT)) AS submitDateTime",
            f"MIN(CAST({period_start_expr} AS TEXT)) AS periodStart",
            f"MAX(CAST({period_end_expr} AS TEXT)) AS periodEnd",
        ]

        for col_name, _col_type in fs_column_specs:
            expr = self._build_amount_case_expr(
                fs_metric_mappings.get(col_name),
                source_alias="s",
                col_accounting_term=col_names.get("AccountingTerm", "AccountingTerm"),
                col_period=col_names.get("Period", "Period"),
                col_amount=col_names.get("Amount", "Amount"),
                value_type=self._mapping_storage_type(fs_metric_mappings.get(col_name)),
            )
            cte_select.append(f"{expr} AS {self._sql_ident(col_name)}")

        insert_columns = [
            "edinetCode",
            "docID",
            "docTypeCode",
            "periodStart",
            "periodEnd",
            "taxonomy_release_id",
            "release_resolution_method",
            "release_resolution_note",
        ]
        select_columns = [
            "d.edinetCode AS edinetCode",
            "d.docID AS docID",
            "d.docTypeCode AS docTypeCode",
            "d.periodStart AS periodStart",
            "d.periodEnd AS periodEnd",
            "resolve_taxonomy_release_id(d.submitDateTime, d.periodEnd) AS taxonomy_release_id",
            "resolve_taxonomy_release_method(d.submitDateTime, d.periodEnd) AS release_resolution_method",
            "resolve_taxonomy_release_note(d.submitDateTime, d.periodEnd) AS release_resolution_note",
        ]
        update_columns = [
            "edinetCode",
            "docTypeCode",
            "periodStart",
            "periodEnd",
            "taxonomy_release_id",
            "release_resolution_method",
            "release_resolution_note",
        ]

        for col_name, _col_type in fs_column_specs:
            insert_columns.append(col_name)
            select_columns.append(f"d.{self._sql_ident(col_name)} AS {self._sql_ident(col_name)}")
            update_columns.append(col_name)

        insert_columns.append("SharePrice")
        select_columns.append(
            "("
            "SELECT sp.Price "
            f"FROM {prices_ref} sp "
            f"JOIN {company_ref} c ON c.Company_Ticker = sp.Ticker "
            "WHERE c.EdinetCode = d.edinetCode "
            "  AND sp.Date <= d.periodEnd "
            "ORDER BY sp.Date DESC "
            "LIMIT 1"
            ") AS SharePrice"
        )
        update_columns.append("SharePrice")

        insert_sql = ", ".join(self._sql_ident(column) for column in insert_columns)
        update_sql = ", ".join(
            f"{self._sql_ident(column)} = excluded.{self._sql_ident(column)}"
            for column in update_columns
        )

        conn.execute(
            f"""
            WITH doc_base AS (
                SELECT
                    {', '.join(cte_select)}
                FROM {source_ref} s
                INNER JOIN {temp_docids} t ON t.docID = s.{self._sql_ident(doc_col)}
                WHERE s.{self._sql_ident(doc_col)} IS NOT NULL
                GROUP BY s.{self._sql_ident(doc_col)}
            )
            INSERT INTO {self._sql_ident('FinancialStatements')} ({insert_sql})
            SELECT
                {', '.join(select_columns)}
            FROM doc_base d
            WHERE 1 = 1
            ON CONFLICT(docID) DO UPDATE SET
                {update_sql}
            """
        )

    def _materialize_wide_statement_table_batch(
        self,
        conn,
        source_ref,
        temp_docids,
        col_names,
        table_name,
        catalog_entries,
    ):
        column_entries = [entry for entry in catalog_entries if entry.get("column_name")]
        insert_columns = ["docID"] + [entry["column_name"] for entry in column_entries]
        update_columns = [entry["column_name"] for entry in column_entries]

        if not column_entries:
            conn.execute(
                f"INSERT OR REPLACE INTO {self._sql_ident(table_name)} ({self._sql_ident('docID')}) "
                f"SELECT docID FROM {temp_docids}"
            )
            return

        doc_col = col_names.get("docID", "docID")
        term_expr = self._source_column_expr("s", col_names.get("AccountingTerm"))
        period_expr = self._source_column_expr("s", col_names.get("Period"))
        relative_year_expr = self._source_column_expr("s", col_names.get("RelativeYear"))
        consolidation_expr = self._source_column_expr("s", col_names.get("Consolidation"))
        amount_expr = self._source_column_expr("s", col_names.get("Amount"))

        primary_period = self._statement_primary_period(table_name)
        period_prefix = f"{primary_period}\\_%"
        period_filter_sql = (
            f"(CAST({period_expr} AS TEXT) = {self._sql_literal(primary_period)} "
            f"OR CAST({period_expr} AS TEXT) LIKE {self._sql_literal(period_prefix)} ESCAPE '\\')"
        )
        concept_list_sql = ", ".join(
            self._sql_literal(entry["concept_qname"])
            for entry in column_entries
        )
        consolidation_text = f"LOWER(COALESCE(CAST({consolidation_expr} AS TEXT), ''))"

        select_exprs = ["t.docID AS docID"]
        for entry in column_entries:
            value_expr = "r.raw_value_text" if entry.get("value_type") == "TEXT" else "r.value_numeric"
            select_exprs.append(
                f"MAX(CASE WHEN r.concept_qname = {self._sql_literal(entry['concept_qname'])} "
                f"THEN {value_expr} END) AS {self._sql_ident(entry['column_name'])}"
            )

        update_sql = ", ".join(
            f"{self._sql_ident(column)} = excluded.{self._sql_ident(column)}"
            for column in update_columns
        )

        conn.execute(
            f"""
            WITH ranked AS (
                SELECT
                    s.{self._sql_ident(doc_col)} AS docID,
                    normalize_taxonomy_term({term_expr}) AS concept_qname,
                    CAST({amount_expr} AS TEXT) AS raw_value_text,
                    try_real({amount_expr}) AS value_numeric,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.{self._sql_ident(doc_col)}, normalize_taxonomy_term({term_expr})
                        ORDER BY
                            CASE
                                WHEN CAST({period_expr} AS TEXT) = {self._sql_literal(primary_period)} THEN 0
                                WHEN CAST({period_expr} AS TEXT) LIKE {self._sql_literal(period_prefix)} ESCAPE '\\' THEN 1
                                ELSE 9
                            END,
                            CASE
                                WHEN CAST({relative_year_expr} AS TEXT) = 'CurrentYear' THEN 0
                                WHEN {relative_year_expr} IS NULL OR TRIM(CAST({relative_year_expr} AS TEXT)) = '' THEN 1
                                ELSE 2
                            END,
                            CASE
                                WHEN {consolidation_text} LIKE '%連結%' OR {consolidation_text} LIKE '%consolidated%' THEN 0
                                WHEN {consolidation_text} = '' THEN 1
                                WHEN {consolidation_text} LIKE '%個別%' OR {consolidation_text} LIKE '%nonconsolidated%' OR {consolidation_text} LIKE '%non-consolidated%' THEN 2
                                ELSE 3
                            END,
                            CASE WHEN try_real({amount_expr}) IS NOT NULL THEN 0 ELSE 1 END,
                            CAST({amount_expr} AS TEXT) DESC
                    ) AS rn
                FROM {source_ref} s
                INNER JOIN {temp_docids} t ON t.docID = s.{self._sql_ident(doc_col)}
                WHERE s.{self._sql_ident(doc_col)} IS NOT NULL
                  AND {term_expr} IS NOT NULL
                  AND normalize_taxonomy_term({term_expr}) IN ({concept_list_sql})
                  AND {period_filter_sql}
            )
            INSERT INTO {self._sql_ident(table_name)} ({', '.join(self._sql_ident(column) for column in insert_columns)})
            SELECT
                {', '.join(select_exprs)}
            FROM {temp_docids} t
            LEFT JOIN ranked r
              ON r.docID = t.docID
             AND r.rn = 1
            GROUP BY t.docID
            ON CONFLICT(docID) DO UPDATE SET
                {update_sql}
            """
        )

    def _taxonomy_statement_table_columns(self):
        return [
            ("docID", "TEXT"),
            ("edinetCode", "TEXT"),
            ("periodEnd", "TEXT"),
            ("context_ref", "TEXT"),
            ("taxonomy_release_id", "INTEGER"),
            ("concept_qname", "TEXT"),
            ("concept_namespace", "TEXT"),
            ("concept_name", "TEXT"),
            ("display_label", "TEXT"),
            ("role_uri", "TEXT"),
            ("presentation_parent_qname", "TEXT"),
            ("line_order", "REAL"),
            ("line_depth", "INTEGER"),
            ("unit_ref", "TEXT"),
            ("unit_label", "TEXT"),
            ("raw_value_text", "TEXT"),
            ("value_numeric", "REAL"),
            ("source_period", "TEXT"),
            ("source_relative_year", "TEXT"),
            ("source_consolidation", "TEXT"),
            ("is_text_block", "INTEGER"),
        ]

    def _statement_table_names(self):
        return ("IncomeStatement", "BalanceSheet", "CashflowStatement")

    def _is_taxonomy_statement_table_shape(self, conn, table_name):
        if not self._table_exists(conn, table_name):
            return False
        info = conn.execute(f"PRAGMA table_info({self._sql_ident(table_name)})").fetchall()
        existing_columns = [row[1] for row in info]
        expected_columns = [col_name for col_name, _col_type in self._taxonomy_statement_table_columns()]
        return existing_columns == expected_columns

    def _ensure_taxonomy_statement_tables(self, conn, overwrite=False):
        """Create or migrate taxonomy-shaped statement tables."""
        rebuild_schema = bool(overwrite) or not all(
            self._is_taxonomy_statement_table_shape(conn, table_name)
            for table_name in self._statement_table_names()
        )

        if rebuild_schema:
            for table_name in self._statement_table_names():
                conn.execute(f"DROP TABLE IF EXISTS {self._sql_ident(table_name)}")

        cols_sql = ",\n              ".join(
            f"{self._sql_ident(col_name)} {col_type}"
            for col_name, col_type in self._taxonomy_statement_table_columns()
        )
        for table_name in self._statement_table_names():
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self._sql_ident(table_name)} (\n"
                f"              {cols_sql}\n"
                f"            )"
            )
            self._create_index_if_not_exists(conn, "main", table_name, ["docID"])
            self._create_index_if_not_exists(conn, "main", table_name, ["edinetCode", "periodEnd"])
            self._create_index_if_not_exists(conn, "main", table_name, ["docID", "line_order"])
            self._create_index_if_not_exists(conn, "main", table_name, ["docID", "concept_qname"])

        return rebuild_schema

    def _materialize_taxonomy_statement_tables(self, conn):
        """Rebuild taxonomy-shaped statement tables from normalized statement facts."""
        if not self._table_exists(conn, "statement_facts") or not self._table_exists(conn, "statement_documents"):
            return

        insert_columns = [
            "docID",
            "edinetCode",
            "periodEnd",
            "context_ref",
            "taxonomy_release_id",
            "concept_qname",
            "concept_namespace",
            "concept_name",
            "display_label",
            "role_uri",
            "presentation_parent_qname",
            "line_order",
            "line_depth",
            "unit_ref",
            "unit_label",
            "raw_value_text",
            "value_numeric",
            "source_period",
            "source_relative_year",
            "source_consolidation",
            "is_text_block",
        ]
        insert_sql = ", ".join(self._sql_ident(column) for column in insert_columns)

        for table_name in self._statement_table_names():
            conn.execute(f"DELETE FROM {self._sql_ident(table_name)}")
            conn.execute(
                f"""
                INSERT INTO {self._sql_ident(table_name)} ({insert_sql})
                SELECT
                    d.docID,
                    d.edinetCode,
                    d.periodEnd,
                    f.context_ref,
                    f.taxonomy_release_id,
                    f.concept_qname,
                    f.concept_namespace,
                    f.concept_name,
                    COALESCE(f.display_label, f.concept_name, f.concept_qname) AS display_label,
                    f.role_uri,
                    f.presentation_parent_qname,
                    f.line_order,
                    f.line_depth,
                    f.unit_ref,
                    f.unit_label,
                    f.raw_value_text,
                    f.value_numeric,
                    f.source_period,
                    f.source_relative_year,
                    f.source_consolidation,
                    f.is_text_block
                FROM statement_facts f
                INNER JOIN statement_documents d ON d.docID = f.docID
                WHERE f.statement_family = ?
                ORDER BY
                    d.docID,
                    COALESCE(f.role_uri, ''),
                    COALESCE(f.context_ref, ''),
                    COALESCE(f.line_order, 999999999.0),
                    COALESCE(f.concept_qname, '')
                """,
                (table_name,),
            )

    def _create_statement_storage_tables(self, conn):
        """Create normalized taxonomy-shaped statement storage tables."""
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS statement_documents (
                docID TEXT PRIMARY KEY,
                edinetCode TEXT,
                docTypeCode TEXT,
                submitDateTime TEXT,
                periodStart TEXT,
                periodEnd TEXT,
                taxonomy_release_id INTEGER,
                release_resolution_method TEXT,
                release_resolution_note TEXT
            );

            CREATE TABLE IF NOT EXISTS statement_contexts (
                docID TEXT NOT NULL,
                context_ref TEXT NOT NULL,
                relative_year_label TEXT,
                consolidation_kind TEXT,
                period_instant_kind TEXT,
                dimension_signature TEXT,
                is_primary_statement_context INTEGER,
                context_start_date TEXT,
                context_end_date TEXT,
                context_instant_date TEXT,
                PRIMARY KEY (docID, context_ref)
            );

            CREATE TABLE IF NOT EXISTS statement_facts (
                fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
                docID TEXT NOT NULL,
                context_ref TEXT,
                taxonomy_release_id INTEGER,
                concept_qname TEXT,
                concept_namespace TEXT,
                concept_name TEXT,
                statement_family TEXT,
                role_uri TEXT,
                display_label TEXT,
                presentation_parent_qname TEXT,
                line_order REAL,
                line_depth INTEGER,
                unit_ref TEXT,
                unit_label TEXT,
                raw_value_text TEXT,
                value_numeric REAL,
                decimals TEXT,
                is_text_block INTEGER,
                source_element_id TEXT,
                source_item_name TEXT,
                source_period TEXT,
                source_relative_year TEXT,
                source_consolidation TEXT
            );

            CREATE TABLE IF NOT EXISTS statement_fact_dimensions (
                fact_id INTEGER NOT NULL,
                axis_qname TEXT NOT NULL,
                member_qname TEXT NOT NULL,
                dimension_order INTEGER,
                PRIMARY KEY (fact_id, axis_qname, member_qname)
            );

            CREATE INDEX IF NOT EXISTS idx_statement_documents_edinet_period
                ON statement_documents(edinetCode, periodEnd);
            CREATE INDEX IF NOT EXISTS idx_statement_documents_release
                ON statement_documents(taxonomy_release_id);
            CREATE INDEX IF NOT EXISTS idx_statement_contexts_doc_period
                ON statement_contexts(docID, period_instant_kind, consolidation_kind);
            CREATE INDEX IF NOT EXISTS idx_statement_facts_doc_context
                ON statement_facts(docID, context_ref);
            CREATE INDEX IF NOT EXISTS idx_statement_facts_doc_concept
                ON statement_facts(docID, concept_qname);
            CREATE INDEX IF NOT EXISTS idx_statement_facts_release_concept
                ON statement_facts(taxonomy_release_id, concept_qname);
            CREATE INDEX IF NOT EXISTS idx_statement_facts_family_concept
                ON statement_facts(statement_family, concept_qname);
            CREATE INDEX IF NOT EXISTS idx_statement_facts_doc_role_order
                ON statement_facts(docID, role_uri, line_order);
            CREATE INDEX IF NOT EXISTS idx_statement_facts_doc_family_order
                ON statement_facts(docID, statement_family, line_order);
            """
        )

    def _load_taxonomy_release_rows(self, conn):
        if not self._table_exists(conn, "taxonomy_releases"):
            return []
        return taxonomy_processing.load_release_rows(conn)

    def _resolve_document_taxonomy_release(self, release_rows, submit_datetime, period_end):
        release_id, method, note = taxonomy_processing.resolve_release_for_reference_date(
            release_rows,
            submit_datetime,
        )
        if release_id is not None:
            return release_id, method, note

        release_id, _, note = taxonomy_processing.resolve_release_for_reference_date(
            release_rows,
            period_end,
        )
        if release_id is not None:
            fallback_note = note or f"Resolved using periodEnd {str(period_end or '')[:10]}"
            return release_id, "period_end_fallback", fallback_note
        return None, None, None

    def _delete_statement_storage_rows(self, conn, temp_docids):
        conn.execute(
            f"DELETE FROM statement_fact_dimensions WHERE fact_id IN ("
            f"SELECT fact_id FROM statement_facts WHERE docID IN (SELECT docID FROM {temp_docids})"
            f")"
        )
        for table_name in ("statement_facts", "statement_contexts", "statement_documents"):
            conn.execute(
                f"DELETE FROM {self._sql_ident(table_name)} WHERE docID IN (SELECT docID FROM {temp_docids})"
            )

    def _refresh_statement_documents(self, conn, source_ref, temp_docids, col_names=None, release_rows=None):
        """Refresh one-row-per-document metadata used by normalized statement storage."""
        col_names = col_names or {}
        release_rows = release_rows or []

        submit_expr = self._source_column_expr("s", col_names.get("submitDateTime"))
        period_start_expr = self._source_column_expr("s", col_names.get("periodStart"))
        period_end_expr = self._source_column_expr("s", col_names.get("periodEnd"))

        rows = conn.execute(
            f"""
            SELECT
                s.{self._sql_ident(col_names.get('docID', 'docID'))} AS docID,
                MAX(s.{self._sql_ident(col_names.get('edinetCode', 'edinetCode'))}) AS edinetCode,
                MAX(s.{self._sql_ident(col_names.get('docTypeCode', 'docTypeCode'))}) AS docTypeCode,
                MAX(CAST({submit_expr} AS TEXT)) AS submitDateTime,
                MIN(CAST({period_start_expr} AS TEXT)) AS periodStart,
                MAX(CAST({period_end_expr} AS TEXT)) AS periodEnd
            FROM {source_ref} s
            INNER JOIN {temp_docids} t ON t.docID = s.{self._sql_ident(col_names.get('docID', 'docID'))}
            WHERE s.{self._sql_ident(col_names.get('docID', 'docID'))} IS NOT NULL
            GROUP BY s.{self._sql_ident(col_names.get('docID', 'docID'))}
            """
        ).fetchall()

        payload = []
        for doc_id, edinet_code, doc_type_code, submit_datetime, period_start, period_end in rows:
            release_id, resolution_method, resolution_note = self._resolve_document_taxonomy_release(
                release_rows,
                submit_datetime,
                period_end,
            )
            payload.append(
                (
                    doc_id,
                    edinet_code,
                    doc_type_code,
                    submit_datetime,
                    period_start,
                    period_end,
                    release_id,
                    resolution_method,
                    resolution_note,
                )
            )

        if not payload:
            return

        conn.executemany(
            """
            INSERT INTO statement_documents (
                docID,
                edinetCode,
                docTypeCode,
                submitDateTime,
                periodStart,
                periodEnd,
                taxonomy_release_id,
                release_resolution_method,
                release_resolution_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(docID) DO UPDATE SET
                edinetCode = excluded.edinetCode,
                docTypeCode = excluded.docTypeCode,
                submitDateTime = excluded.submitDateTime,
                periodStart = excluded.periodStart,
                periodEnd = excluded.periodEnd,
                taxonomy_release_id = excluded.taxonomy_release_id,
                release_resolution_method = excluded.release_resolution_method,
                release_resolution_note = excluded.release_resolution_note
            """,
            payload,
        )

    def _refresh_statement_contexts(self, conn, source_ref, temp_docids, col_names=None):
        """Insert normalized context rows for the current document batch."""
        col_names = col_names or {}
        doc_col = col_names.get("docID", "docID")
        period_expr = self._source_column_expr("s", col_names.get("Period"))
        relative_year_expr = self._source_column_expr("s", col_names.get("RelativeYear"))
        consolidation_expr = self._source_column_expr("s", col_names.get("Consolidation"))
        period_type_expr = self._source_column_expr("s", col_names.get("PeriodType"))

        conn.execute(
            f"""
            INSERT OR REPLACE INTO statement_contexts (
                docID,
                context_ref,
                relative_year_label,
                consolidation_kind,
                period_instant_kind,
                dimension_signature,
                is_primary_statement_context,
                context_start_date,
                context_end_date,
                context_instant_date
            )
            SELECT DISTINCT
                s.{self._sql_ident(doc_col)} AS docID,
                CAST({period_expr} AS TEXT) AS context_ref,
                CAST({relative_year_expr} AS TEXT) AS relative_year_label,
                CAST({consolidation_expr} AS TEXT) AS consolidation_kind,
                CAST({period_type_expr} AS TEXT) AS period_instant_kind,
                NULL AS dimension_signature,
                CASE WHEN {period_expr} IS NOT NULL THEN 1 ELSE 0 END AS is_primary_statement_context,
                NULL AS context_start_date,
                NULL AS context_end_date,
                NULL AS context_instant_date
            FROM {source_ref} s
            INNER JOIN {temp_docids} t ON t.docID = s.{self._sql_ident(doc_col)}
            WHERE s.{self._sql_ident(doc_col)} IS NOT NULL
              AND {period_expr} IS NOT NULL
            """
        )

    def _refresh_statement_facts(self, conn, source_ref, temp_docids, col_names=None, statement_family_fallbacks=None):
        """Insert taxonomy-shaped fact rows for the current document batch."""
        col_names = col_names or {}
        statement_family_fallbacks = statement_family_fallbacks or {}
        doc_col = col_names.get("docID", "docID")
        term_expr = self._source_column_expr("s", col_names.get("AccountingTerm"))
        item_expr = self._source_column_expr("s", col_names.get("ItemName"))
        period_expr = self._source_column_expr("s", col_names.get("Period"))
        relative_year_expr = self._source_column_expr("s", col_names.get("RelativeYear"))
        consolidation_expr = self._source_column_expr("s", col_names.get("Consolidation"))
        currency_expr = self._source_column_expr("s", col_names.get("Currency"))
        unit_name_expr = self._source_column_expr("s", col_names.get("UnitName"))
        amount_expr = self._source_column_expr("s", col_names.get("Amount"))

        taxonomy_join = ""
        concept_name_expr = f"taxonomy_local_name({term_expr})"
        concept_namespace_expr = f"taxonomy_prefix({term_expr})"
        fallback_family_expr = "NULL"
        if statement_family_fallbacks:
            fallback_when_sql = " ".join(
                f"WHEN normalize_taxonomy_term({term_expr}) = {self._sql_literal(term)} THEN {self._sql_literal(family)}"
                for term, family in sorted(statement_family_fallbacks.items())
            )
            fallback_family_expr = f"(CASE {fallback_when_sql} END)"
        statement_family_expr = (
            f"COALESCE({fallback_family_expr}, CASE WHEN taxonomy_prefix({term_expr}) = 'jpcrp_cor' THEN 'Disclosure' ELSE NULL END)"
        )
        role_uri_expr = "NULL"
        display_label_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({item_expr} AS TEXT)), ''), {concept_name_expr})"
        )
        parent_expr = "NULL"
        order_expr = "NULL"
        depth_expr = "NULL"

        if self._table_exists(conn, "taxonomy_concepts"):
            taxonomy_join = (
                "LEFT JOIN taxonomy_concepts tc "
                "ON tc.release_id = sd.taxonomy_release_id "
                f"AND tc.concept_qname = normalize_taxonomy_term({term_expr})"
            )
            concept_name_expr = f"COALESCE(tc.concept_name, taxonomy_local_name({term_expr}))"
            concept_namespace_expr = f"COALESCE(tc.namespace_prefix, taxonomy_prefix({term_expr}))"
            statement_family_expr = (
                f"COALESCE(tc.statement_family_default, {fallback_family_expr}, CASE WHEN taxonomy_prefix({term_expr}) = 'jpcrp_cor' THEN 'Disclosure' ELSE NULL END)"
            )
            role_uri_expr = "tc.primary_role_uri"
            display_label_expr = (
                f"COALESCE(tc.primary_label, NULLIF(TRIM(CAST({item_expr} AS TEXT)), ''), {concept_name_expr})"
            )
            parent_expr = "tc.primary_parent_concept_qname"
            order_expr = "tc.primary_line_order"
            depth_expr = "tc.primary_line_depth"

        conn.execute(
            f"""
            INSERT INTO statement_facts (
                docID,
                context_ref,
                taxonomy_release_id,
                concept_qname,
                concept_namespace,
                concept_name,
                statement_family,
                role_uri,
                display_label,
                presentation_parent_qname,
                line_order,
                line_depth,
                unit_ref,
                unit_label,
                raw_value_text,
                value_numeric,
                decimals,
                is_text_block,
                source_element_id,
                source_item_name,
                source_period,
                source_relative_year,
                source_consolidation
            )
            SELECT
                s.{self._sql_ident(doc_col)} AS docID,
                CAST({period_expr} AS TEXT) AS context_ref,
                sd.taxonomy_release_id,
                normalize_taxonomy_term({term_expr}) AS concept_qname,
                {concept_namespace_expr} AS concept_namespace,
                {concept_name_expr} AS concept_name,
                {statement_family_expr} AS statement_family,
                {role_uri_expr} AS role_uri,
                {display_label_expr} AS display_label,
                {parent_expr} AS presentation_parent_qname,
                {order_expr} AS line_order,
                {depth_expr} AS line_depth,
                CAST({currency_expr} AS TEXT) AS unit_ref,
                CAST({unit_name_expr} AS TEXT) AS unit_label,
                CAST({amount_expr} AS TEXT) AS raw_value_text,
                try_real({amount_expr}) AS value_numeric,
                NULL AS decimals,
                CASE WHEN normalize_taxonomy_term({term_expr}) LIKE '%TextBlock' THEN 1 ELSE 0 END AS is_text_block,
                CAST({term_expr} AS TEXT) AS source_element_id,
                CAST({item_expr} AS TEXT) AS source_item_name,
                CAST({period_expr} AS TEXT) AS source_period,
                CAST({relative_year_expr} AS TEXT) AS source_relative_year,
                CAST({consolidation_expr} AS TEXT) AS source_consolidation
            FROM {source_ref} s
            INNER JOIN {temp_docids} t ON t.docID = s.{self._sql_ident(doc_col)}
            INNER JOIN statement_documents sd ON sd.docID = s.{self._sql_ident(doc_col)}
            {taxonomy_join}
            WHERE s.{self._sql_ident(doc_col)} IS NOT NULL
              AND {term_expr} IS NOT NULL
            """
        )

    def _upsert_base_financial_statements_from_facts(
        self,
        conn,
        temp_docids,
        mappings,
        fs_column_specs,
        company_ref,
        prices_ref,
    ):
        """Materialize the FinancialStatements compatibility table from statement_facts."""
        fs_map = mappings.get("FinancialStatements", {})
        insert_columns = ["edinetCode", "docID", "docTypeCode", "periodStart", "periodEnd"]
        select_columns = [
            "d.edinetCode AS edinetCode",
            "d.docID AS docID",
            "d.docTypeCode AS docTypeCode",
            "d.periodStart AS periodStart",
            "d.periodEnd AS periodEnd",
        ]

        update_columns = [
            "edinetCode",
            "docTypeCode",
            "periodStart",
            "periodEnd",
        ]

        for col_name, _col_type in fs_column_specs:
            expr = self._build_fact_value_case_expr(
                fs_map.get(col_name),
                facts_alias="f",
                value_column="value_numeric",
                text_column="raw_value_text",
            )
            select_columns.append(f"{expr} AS {self._sql_ident(col_name)}")
            insert_columns.append(col_name)
            update_columns.append(col_name)

        insert_columns.append("SharePrice")
        select_columns.append(
            "("
            "SELECT sp.Price "
            f"FROM {prices_ref} sp "
            f"JOIN {company_ref} c ON c.Company_Ticker = sp.Ticker "
            "WHERE c.EdinetCode = d.edinetCode "
            "  AND sp.Date <= d.periodEnd "
            "ORDER BY sp.Date DESC "
            "LIMIT 1"
            ") AS SharePrice"
        )
        update_columns.append("SharePrice")

        insert_sql = ", ".join(self._sql_ident(column) for column in insert_columns)
        update_sql = ", ".join(
            f"{self._sql_ident(column)} = excluded.{self._sql_ident(column)}"
            for column in update_columns
        )

        conn.execute(
            f"""
            INSERT INTO {self._sql_ident('FinancialStatements')} ({insert_sql})
            SELECT
                {', '.join(select_columns)}
            FROM statement_documents d
            INNER JOIN {temp_docids} t ON t.docID = d.docID
            LEFT JOIN statement_facts f ON f.docID = d.docID
            GROUP BY d.docID
            ON CONFLICT(docID) DO UPDATE SET
                {update_sql}
            """
        )

    def _materialize_statement_table_from_facts(self, conn, temp_docids, table_name, column_specs, mappings):
        """Materialize one compatibility statement table from statement_facts."""
        table_mappings = mappings.get(table_name, {})
        select_exprs = ["d.docID AS docID"]
        insert_columns = ["docID"]
        update_columns = []

        for col_name, _col_type in column_specs:
            expr = self._build_fact_value_case_expr(
                table_mappings.get(col_name),
                facts_alias="f",
                value_column="value_numeric",
                text_column="raw_value_text",
            )
            select_exprs.append(f"{expr} AS {self._sql_ident(col_name)}")
            insert_columns.append(col_name)
            update_columns.append(col_name)

        insert_sql = ", ".join(self._sql_ident(column) for column in insert_columns)
        update_sql = ", ".join(
            f"{self._sql_ident(column)} = excluded.{self._sql_ident(column)}"
            for column in update_columns
        )

        sql = f"""
        INSERT INTO {self._sql_ident(table_name)} ({insert_sql})
        SELECT
            {', '.join(select_exprs)}
        FROM statement_documents d
        INNER JOIN {temp_docids} t ON t.docID = d.docID
        LEFT JOIN statement_facts f ON f.docID = d.docID
        GROUP BY d.docID
        """
        if update_sql:
            sql += f"\nON CONFLICT(docID) DO UPDATE SET\n    {update_sql}"
        else:
            sql += "\nON CONFLICT(docID) DO NOTHING"
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
        max_line_depth=3,
    ):
        """Generate wide taxonomy-backed statement tables and compatibility metrics."""
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
        max_line_depth = None if str(max_line_depth or "").strip() == "" else max(int(max_line_depth), 0)
        mappings = self._load_financial_statement_mappings(mappings_path)
        fs_metric_mappings = self._collect_financial_statement_metric_map(mappings)
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
                "taxonomy_release_id",
                "release_resolution_method",
                "release_resolution_note",
                "DescriptionOfBusiness_EN",
                "SharePrice",
            }
        ]

        same_db = os.path.abspath(source_db) == os.path.abspath(target_db)
        conn = sqlite3.connect(target_db)
        try:
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.create_function("normalize_taxonomy_term", 1, self._normalise_taxonomy_term)
            conn.create_function("taxonomy_prefix", 1, self._taxonomy_prefix)
            conn.create_function("taxonomy_local_name", 1, self._taxonomy_local_name)
            conn.create_function("try_real", 1, self._try_real)

            source_schema = "main"
            if not same_db:
                conn.execute("ATTACH DATABASE ? AS src", (source_db,))
                source_schema = "src"

            source_actual = self._resolve_table_name_in_schema(conn, source_schema, source_tbl) or source_tbl
            source_ref = f"{self._sql_ident(source_schema)}.{self._sql_ident(source_actual)}"

            # Detect actual column names (handles financialdata_full Japanese names)
            col_names = self._resolve_source_col_names(conn, source_schema, source_tbl)
            doc_col_sql = self._sql_ident(col_names.get("docID") or "docID")
            if any(v != k for k, v in col_names.items()):
                logger.info(
                    "generate_financial_statements: source table uses non-standard column names %s",
                    col_names,
                )

            self._create_index_if_not_exists(
                conn,
                source_schema,
                source_actual,
                [col_names.get("docID", "docID")],
            )

            if overwrite:
                logger.info("Overwrite enabled - resetting financial statement tables.")
                conn.executescript(
                    """
                    DROP TABLE IF EXISTS FinancialStatements;
                    DROP TABLE IF EXISTS IncomeStatement;
                    DROP TABLE IF EXISTS BalanceSheet;
                    DROP TABLE IF EXISTS CashflowStatement;
                    DROP TABLE IF EXISTS statement_line_items;
                    """
                )

            self._drop_legacy_statement_storage(conn)

            added_columns = self._create_financial_statement_tables(conn, table_specs)
            schema_expanded = any(cols for cols in added_columns.values())
            if schema_expanded and not overwrite:
                logger.info(
                    "Detected new financial statement columns; reprocessing all relevant documents to backfill them."
                )

            release_rows = self._load_taxonomy_release_rows(conn)
            self._register_release_resolution_functions(conn, release_rows)
            catalog = self._load_statement_catalog(conn, mappings, max_line_depth=max_line_depth)
            self._ensure_statement_line_items_table(conn)
            with conn:
                self._refresh_statement_line_items(conn, catalog)
            statement_tables_rebuilt = self._ensure_wide_statement_tables(conn, catalog, overwrite=overwrite)
            if statement_tables_rebuilt and not overwrite:
                logger.info(
                    "Detected legacy or missing statement-table schema; rebuilding wide taxonomy-backed statement tables."
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

            if schema_expanded or statement_tables_rebuilt:
                pending_sql = f"""
                SELECT DISTINCT s.{doc_col_sql} AS docID
                FROM {source_ref} s
                WHERE s.{doc_col_sql} IS NOT NULL
                ORDER BY s.{doc_col_sql}
                """
            else:
                release_gap_sql = " OR fs.taxonomy_release_id IS NULL" if release_rows else ""
                pending_sql = f"""
                SELECT DISTINCT s.{doc_col_sql} AS docID
                FROM {source_ref} s
                LEFT JOIN FinancialStatements fs ON fs.docID = s.{doc_col_sql}
                LEFT JOIN IncomeStatement is1 ON is1.docID = s.{doc_col_sql}
                LEFT JOIN BalanceSheet bs ON bs.docID = s.{doc_col_sql}
                LEFT JOIN CashflowStatement cs ON cs.docID = s.{doc_col_sql}
                WHERE s.{doc_col_sql} IS NOT NULL
                  AND (
                                        fs.docID IS NULL
                                        {release_gap_sql}
                    OR is1.docID IS NULL
                    OR bs.docID IS NULL
                    OR cs.docID IS NULL
                  )
                ORDER BY s.{doc_col_sql}
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

                    self._delete_statement_output_rows(conn, temp_docids)
                    self._upsert_base_financial_statements_from_source(
                        conn,
                        source_ref,
                        temp_docids,
                        col_names=col_names,
                        fs_metric_mappings=fs_metric_mappings,
                        fs_column_specs=fs_column_specs,
                        company_ref=company_ref,
                        prices_ref=prices_ref,
                    )
                    for table_name in self._statement_table_names():
                        self._materialize_wide_statement_table_batch(
                            conn,
                            source_ref,
                            temp_docids,
                            col_names,
                            table_name,
                            catalog.get(table_name, []),
                        )

                total_docs += len(batch)
                if total_docs % (batch_size * 10) == 0:
                    logger.info("Generate Financial Statements progress: %d docs processed", total_docs)

            logger.info("Generate Financial Statements completed. Processed %d document(s).", total_docs)
        finally:
            conn.close()

    def refresh_statement_hierarchy(
        self,
        target_database,
        mappings_config,
        max_line_depth=3,
    ):
        """Refresh statement_line_items from the current taxonomy_levels projection.

        This is a fast alternative to generate_financial_statements that only
        rewrites the ``statement_line_items`` metadata table without scanning or
        modifying any of the wide statement tables (IncomeStatement, BalanceSheet,
        CashflowStatement, FinancialStatements).  Use it after a taxonomy reparse
        to apply the updated hierarchy without a full regeneration run.
        """
        if not target_database:
            raise ValueError("target_database is required for refresh_statement_hierarchy.")
        if not mappings_config:
            raise ValueError("mappings_config is required for refresh_statement_hierarchy.")

        max_depth = None if str(max_line_depth or "").strip() == "" else max(int(max_line_depth), 0)
        mappings = self._load_financial_statement_mappings(mappings_config)

        conn = sqlite3.connect(target_database)
        try:
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.create_function("normalize_taxonomy_term", 1, self._normalise_taxonomy_term)
            conn.create_function("taxonomy_prefix", 1, self._taxonomy_prefix)
            conn.create_function("taxonomy_local_name", 1, self._taxonomy_local_name)
            conn.create_function("try_real", 1, self._try_real)

            release_rows = self._load_taxonomy_release_rows(conn)
            self._register_release_resolution_functions(conn, release_rows)
            catalog = self._load_statement_catalog(conn, mappings, max_line_depth=max_depth)
            self._ensure_statement_line_items_table(conn)
            with conn:
                self._refresh_statement_line_items(conn, catalog)

            total_rows = conn.execute("SELECT COUNT(*) FROM statement_line_items").fetchone()[0]
            logger.info(
                "refresh_statement_hierarchy: statement_line_items refreshed with %d row(s).",
                total_rows,
            )
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
        slow_provider_warning_seconds = float(
            provider_settings.get("slow_provider_warning_seconds", 10.0) or 10.0
        )
        provider_names = [getattr(provider, "name", type(provider).__name__) for provider in providers]

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

            actual_company = self._resolve_column_name(conn, actual_table, "edinetCode")
            actual_period_end = self._resolve_column_name(conn, actual_table, "periodEnd")

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
                    f"SELECT COUNT(*) FROM {self._sql_ident(actual_table)} "
                    f"WHERE {self._sql_ident(actual_target)} IS NOT NULL "
                    f"AND TRIM(CAST({self._sql_ident(actual_target)} AS TEXT)) <> ''"
                ).fetchone()[0]

            translated_rows = 0
            failed_rows = 0
            processed_rows = 0
            provider_usage = {}
            stopped_early = False
            stop_reason = ""

            attempted_docids = self._sql_ident("_tmp_desc_translation_attempted")
            conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS {attempted_docids} (docID TEXT PRIMARY KEY)")
            conn.execute(f"DELETE FROM {attempted_docids}")

            def _qualified_column(column_name, alias=None):
                qualified = self._sql_ident(column_name)
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
                f"{self._sql_ident(actual_docid)} IS NOT NULL",
                f"{self._sql_ident(actual_source)} IS NOT NULL",
                f"TRIM(CAST({self._sql_ident(actual_source)} AS TEXT)) <> ''",
            ]
            if not overwrite:
                eligible_where_clauses.append(
                    f"({self._sql_ident(actual_target)} IS NULL "
                    f"OR TRIM(CAST({self._sql_ident(actual_target)} AS TEXT)) = '')"
                )
            eligible_row_count = conn.execute(
                f"SELECT COUNT(*) FROM {self._sql_ident(actual_table)} "
                f"WHERE {' AND '.join(eligible_where_clauses)}"
            ).fetchone()[0]
            eligible_company_count = conn.execute(
                f"SELECT COUNT(*) FROM ("
                f"SELECT {company_key_base_expr} AS company_key "
                f"FROM {self._sql_ident(actual_table)} "
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
                    f"t.{self._sql_ident(actual_docid)} IS NOT NULL",
                    f"t.{self._sql_ident(actual_source)} IS NOT NULL",
                    f"TRIM(CAST(t.{self._sql_ident(actual_source)} AS TEXT)) <> ''",
                    f"attempted.docID IS NULL",
                ]
                params = []
                if not overwrite:
                    where_clauses.append(
                        f"(t.{self._sql_ident(actual_target)} IS NULL "
                        f"OR TRIM(CAST(t.{self._sql_ident(actual_target)} AS TEXT)) = '')"
                    )

                base_from = (
                    f"FROM {self._sql_ident(actual_table)} t "
                    f"LEFT JOIN {attempted_docids} attempted "
                    f"ON attempted.docID = t.{self._sql_ident(actual_docid)}"
                )

                if actual_company and actual_period_end:
                    company_key_expr = _company_key_expr("t")
                    period_null_sort_expr = (
                        f"CASE WHEN t.{self._sql_ident(actual_period_end)} IS NULL "
                        f"OR TRIM(CAST(t.{self._sql_ident(actual_period_end)} AS TEXT)) = '' THEN 1 ELSE 0 END"
                    )
                    period_sort_expr = f"CAST(t.{self._sql_ident(actual_period_end)} AS TEXT)"
                    docid_sort_expr = f"CAST(t.{self._sql_ident(actual_docid)} AS TEXT)"
                    sql = f"""
                    WITH company_status AS (
                        SELECT
                            {_company_key_expr("base")} AS company_key,
                            MAX(CASE WHEN {_nonblank_target_expr("base")} THEN 1 ELSE 0 END) AS company_has_translation
                        FROM {self._sql_ident(actual_table)} base
                        GROUP BY company_key
                    ),
                    eligible AS (
                        SELECT
                            t.{self._sql_ident(actual_docid)} AS doc_id,
                            t.{self._sql_ident(actual_source)} AS source_text,
                            t.{self._sql_ident(actual_target)} AS current_target,
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
                        f"SELECT t.{self._sql_ident(actual_docid)}, "
                        f"t.{self._sql_ident(actual_source)}, "
                        f"t.{self._sql_ident(actual_target)}, "
                        f"CAST(t.{self._sql_ident(actual_docid)} AS TEXT) "
                        f"{base_from} "
                        f"WHERE {' AND '.join(where_clauses)} "
                        f"ORDER BY CAST(t.{self._sql_ident(actual_docid)} AS TEXT) DESC "
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
                        f"UPDATE {self._sql_ident(actual_table)} "
                        f"SET {self._sql_ident(actual_target)} = ? "
                        f"WHERE {self._sql_ident(actual_docid)} = ?",
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

    def sync_taxonomy_releases(
        self,
        target_database,
        release_selection="all",
        release_years=None,
        namespaces=None,
        download_dir="assets/taxonomy",
        force_download=False,
        force_reparse=False,
    ):
        """Download and parse EDINET taxonomy releases into normalized tables."""
        return taxonomy_processing.sync_taxonomy_releases(
            target_database=target_database,
            release_selection=release_selection,
            release_years=release_years,
            namespaces=namespaces,
            download_dir=download_dir,
            force_download=force_download,
            force_reparse=force_reparse,
        )

    def import_local_taxonomy_xsd(
        self,
        target_database,
        xsd_file,
        namespace_prefix=None,
        release_label=None,
        release_year=None,
        taxonomy_date=None,
    ):
        """Import a local taxonomy XSD into the normalized taxonomy tables."""
        return taxonomy_processing.import_local_taxonomy_xsd(
            target_database=target_database,
            xsd_file=xsd_file,
            namespace_prefix=namespace_prefix,
            release_label=release_label,
            release_year=release_year,
            taxonomy_date=taxonomy_date,
        )

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
