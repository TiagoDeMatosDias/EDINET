import json
import logging
import re

logger = logging.getLogger("src.data_processing")


class OrchestratorProcessorBase:
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
        """Detect the actual source column names in the raw financial-data table."""
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

        alternative_to_standard = {
            "要素ID": "AccountingTerm",
            "項目名": "ItemName",
            "コンテキストID": "Period",
            "相対年度": "RelativeYear",
            "連結・個別": "Consolidation",
            "期間・時点": "PeriodType",
            "ユニットID": "Currency",
            "単位": "UnitName",
            "値": "Amount",
        }
        reverse_map = {v: k for k, v in alternative_to_standard.items()}

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

    def _build_amount_case_expr(
        self,
        mapping,
        source_alias="s",
        col_accounting_term="AccountingTerm",
        col_period="Period",
        col_amount="Amount",
        value_type="REAL",
    ):
        """Build MAX(CASE WHEN ... THEN CAST(Amount AS REAL) END) SQL expression."""
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
        return f"MAX(CASE WHEN {condition_sql} THEN {value_expr} END)"

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

        return f"MAX(CASE WHEN {' AND '.join(conditions)} THEN {facts_alias}.{self._sql_ident(value_col)} END)"

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
        """Return only doc-level FinancialStatements mappings."""
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