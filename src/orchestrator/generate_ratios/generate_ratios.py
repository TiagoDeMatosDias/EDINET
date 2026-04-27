import ast
import json
import logging
import os
import sqlite3

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common import ratios as ratio_services

logger = logging.getLogger(__name__)

RATIO_DEFINITIONS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "ratios_definitions.json")
)


def _resolve_column_name_in_schema(helper, conn, schema_name, table_name, column_name):
    columns = helper._get_table_columns_in_schema(conn, schema_name, table_name)
    by_lower = {str(col).lower(): str(col) for col in columns}
    return by_lower.get(str(column_name or "").lower())


def _load_ratio_definitions(formulas_path):
    with open(formulas_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    table_specs = raw.get("ratios") if isinstance(raw, dict) else None
    if not isinstance(table_specs, dict) or not table_specs:
        raise ValueError(
            "generate_ratios requires a JSON object with a non-empty 'ratios' mapping."
        )

    normalized = {}
    for target_table, ratio_entries in table_specs.items():
        if not target_table or not isinstance(target_table, str):
            raise ValueError("Each ratio-table key must be a non-empty string.")
        if not isinstance(ratio_entries, list) or not ratio_entries:
            raise ValueError(f"Ratio table '{target_table}' must contain a non-empty list of definitions.")

        normalized_entries = []
        for ratio_entry in ratio_entries:
            if not isinstance(ratio_entry, dict) or len(ratio_entry) != 1:
                raise ValueError(
                    f"Each ratio definition in '{target_table}' must be an object with a single ratio name."
                )

            ratio_name, ratio_spec = next(iter(ratio_entry.items()))
            if not ratio_name or not isinstance(ratio_name, str):
                raise ValueError(f"Ratio names in '{target_table}' must be non-empty strings.")
            if not isinstance(ratio_spec, dict):
                raise ValueError(f"Ratio '{ratio_name}' in '{target_table}' must be an object.")

            formula = ratio_spec.get("formula")
            if not formula or not isinstance(formula, str):
                raise ValueError(f"Ratio '{ratio_name}' in '{target_table}' is missing a formula string.")

            inputs = ratio_spec.get("inputs", [])
            if not isinstance(inputs, list):
                raise ValueError(f"Ratio '{ratio_name}' in '{target_table}' must define 'inputs' as a list.")

            normalized_inputs = []
            seen_input_names = set()
            for input_spec in inputs:
                if not isinstance(input_spec, dict):
                    raise ValueError(
                        f"Ratio '{ratio_name}' in '{target_table}' has an invalid input definition."
                    )

                input_name = input_spec.get("name")
                source_table = input_spec.get("Table")
                source_columns = input_spec.get("Columns", [])
                aggregation = str(input_spec.get("Aggregation") or "sum")

                if not input_name or not isinstance(input_name, str):
                    raise ValueError(
                        f"Ratio '{ratio_name}' in '{target_table}' has an input without a valid name."
                    )
                if input_name in seen_input_names:
                    raise ValueError(
                        f"Ratio '{ratio_name}' in '{target_table}' defines input '{input_name}' more than once."
                    )
                if not input_name.isidentifier():
                    raise ValueError(
                        f"Input '{input_name}' in ratio '{ratio_name}' must be a valid formula identifier."
                    )
                if not source_table or not isinstance(source_table, str):
                    raise ValueError(
                        f"Input '{input_name}' in ratio '{ratio_name}' must define a source table."
                    )
                if not isinstance(source_columns, list) or not source_columns:
                    raise ValueError(
                        f"Input '{input_name}' in ratio '{ratio_name}' must define at least one source column."
                    )

                seen_input_names.add(input_name)
                normalized_inputs.append(
                    {
                        "name": input_name,
                        "table": source_table,
                        "columns": [str(col) for col in source_columns if col],
                        "aggregation": aggregation,
                    }
                )

            normalized_entries.append(
                {
                    "name": ratio_name,
                    "formula": formula,
                    "inputs": normalized_inputs,
                    "skip_nulls": bool(ratio_spec.get("skip_nulls", False)),
                }
            )

        normalized[target_table] = normalized_entries

    return normalized


def _ensure_ratio_table_schema(conn, table_name, ratio_names, overwrite=False, helper=None):
    helper = helper or ratio_services._DB_HELPER
    if overwrite:
        conn.execute(f"DROP TABLE IF EXISTS {helper._sql_ident(table_name)}")

    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {helper._sql_ident(table_name)} ("
        f"{helper._sql_ident('docID')} TEXT PRIMARY KEY"
        f")"
    )

    info = conn.execute(f"PRAGMA table_info({helper._sql_ident(table_name)})").fetchall()
    existing_cols = {row[1] for row in info}
    for ratio_name in ratio_names:
        if ratio_name in existing_cols:
            continue
        conn.execute(
            f"ALTER TABLE {helper._sql_ident(table_name)} "
            f"ADD COLUMN {helper._sql_ident(ratio_name)} REAL"
        )


def _seed_ratio_docids(helper, conn, source_schema, fs_actual, target_tables, fs_docid_col):
    fs_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(fs_actual)}"
    for table_name in target_tables:
        conn.execute(
            f"INSERT OR IGNORE INTO {helper._sql_ident(table_name)} ({helper._sql_ident('docID')}) "
            f"SELECT DISTINCT {helper._sql_ident(fs_docid_col)} FROM {fs_ref} "
            f"WHERE {helper._sql_ident(fs_docid_col)} IS NOT NULL"
        )
    return conn.execute(
        f"SELECT COUNT(DISTINCT {helper._sql_ident(fs_docid_col)}) FROM {fs_ref} "
        f"WHERE {helper._sql_ident(fs_docid_col)} IS NOT NULL"
    ).fetchone()[0]


def _build_input_sql_expression(helper, table_alias, actual_columns, aggregation):
    aggregation_key = str(aggregation or "sum").strip().lower()
    qualified_columns = [f"{table_alias}.{helper._sql_ident(col)}" for col in actual_columns]

    if aggregation_key == "firstnonnull":
        return f"COALESCE({', '.join(qualified_columns)})"

    if aggregation_key == "sum":
        presence_sql = " + ".join(
            f"CASE WHEN {column_sql} IS NOT NULL THEN 1 ELSE 0 END"
            for column_sql in qualified_columns
        )
        sum_sql = " + ".join(f"COALESCE({column_sql}, 0)" for column_sql in qualified_columns)
        return f"(CASE WHEN ({presence_sql}) = 0 THEN NULL ELSE ({sum_sql}) END)"

    raise ValueError(f"Unsupported aggregation '{aggregation}'.")


def _compile_formula_sql(formula, input_sql_by_name):
    try:
        tree = ast.parse(formula, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"Invalid ratio formula '{formula}': {exc.msg}") from exc

    def compile_node(node):
        if isinstance(node, ast.Expression):
            return compile_node(node.body)
        if isinstance(node, ast.Name):
            if node.id not in input_sql_by_name:
                raise ValueError(
                    f"Formula '{formula}' references unknown input '{node.id}'."
                )
            return f"({input_sql_by_name[node.id]})"
        if isinstance(node, ast.Constant):
            if not isinstance(node.value, (int, float)):
                raise ValueError(f"Formula '{formula}' can only use numeric constants.")
            return repr(node.value)
        if isinstance(node, ast.Num):
            return repr(node.n)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            operand_sql = compile_node(node.operand)
            operator_sql = "+" if isinstance(node.op, ast.UAdd) else "-"
            return f"({operator_sql}{operand_sql})"
        if isinstance(node, ast.BinOp):
            left_sql = compile_node(node.left)
            right_sql = compile_node(node.right)
            if isinstance(node.op, ast.Add):
                return f"({left_sql} + {right_sql})"
            if isinstance(node.op, ast.Sub):
                return f"({left_sql} - {right_sql})"
            if isinstance(node.op, ast.Mult):
                return f"({left_sql} * {right_sql})"
            if isinstance(node.op, ast.Div):
                return (
                    f"(CASE WHEN ({right_sql}) IS NULL OR ({right_sql}) = 0 "
                    f"THEN NULL ELSE ({left_sql}) / ({right_sql}) END)"
                )

        raise ValueError(
            f"Formula '{formula}' uses unsupported syntax. Only +, -, *, /, parentheses, and numeric constants are allowed."
        )

    return compile_node(tree)


def _resolve_ratio_query_plan(helper, conn, source_schema, fs_actual, ratio_entries):
    fs_docid_col = _resolve_column_name_in_schema(helper, conn, source_schema, fs_actual, "docID")
    if not fs_docid_col:
        raise RuntimeError("Source table 'FinancialStatements' is missing a docID column.")

    helper._create_index_if_not_exists(conn, source_schema, fs_actual, [fs_docid_col])

    join_specs = []
    source_tables = {}
    ratio_sql_specs = []

    for ratio_entry in ratio_entries:
        input_sql_by_name = {}
        null_checks = []

        for input_spec in ratio_entry["inputs"]:
            declared_table = input_spec["table"]
            actual_table = helper._resolve_table_name_in_schema(conn, source_schema, declared_table)
            if not actual_table:
                raise RuntimeError(
                    f"Source table '{declared_table}' referenced by ratio '{ratio_entry['name']}' was not found."
                )

            if actual_table.lower() == fs_actual.lower():
                table_alias = "fs"
            else:
                table_state = source_tables.get(actual_table)
                if table_state is None:
                    source_docid_col = _resolve_column_name_in_schema(
                        helper,
                        conn,
                        source_schema,
                        actual_table,
                        "docID",
                    )
                    if not source_docid_col:
                        raise RuntimeError(
                            f"Source table '{actual_table}' referenced by ratio '{ratio_entry['name']}' is missing a docID column."
                        )

                    helper._create_index_if_not_exists(conn, source_schema, actual_table, [source_docid_col])
                    table_alias = f"t{len(source_tables)}"
                    table_state = {
                        "alias": table_alias,
                        "docid_col": source_docid_col,
                    }
                    source_tables[actual_table] = table_state
                    join_specs.append(
                        f"LEFT JOIN {helper._sql_ident(source_schema)}.{helper._sql_ident(actual_table)} {table_alias} "
                        f"ON {table_alias}.{helper._sql_ident(source_docid_col)} = fs.{helper._sql_ident(fs_docid_col)}"
                    )
                else:
                    table_alias = table_state["alias"]

            actual_columns = []
            seen_columns = set()
            for requested_column in input_spec["columns"]:
                actual_column = _resolve_column_name_in_schema(
                    helper,
                    conn,
                    source_schema,
                    actual_table,
                    requested_column,
                )
                if not actual_column:
                    continue
                column_key = actual_column.lower()
                if column_key in seen_columns:
                    continue
                seen_columns.add(column_key)
                actual_columns.append(actual_column)

            if not actual_columns:
                raise RuntimeError(
                    f"Ratio '{ratio_entry['name']}' could not resolve any of the configured columns "
                    f"{input_spec['columns']} in source table '{declared_table}'."
                )

            input_sql = _build_input_sql_expression(
                helper,
                table_alias,
                actual_columns,
                input_spec["aggregation"],
            )
            input_sql_by_name[input_spec["name"]] = input_sql
            null_checks.append(f"({input_sql}) IS NULL")

        formula_sql = _compile_formula_sql(ratio_entry["formula"], input_sql_by_name)
        if ratio_entry["skip_nulls"] and null_checks:
            formula_sql = f"(CASE WHEN {' OR '.join(null_checks)} THEN NULL ELSE {formula_sql} END)"

        ratio_sql_specs.append(
            {
                "name": ratio_entry["name"],
                "sql": formula_sql,
            }
        )

    return {
        "fs_docid_col": fs_docid_col,
        "joins": join_specs,
        "ratios": ratio_sql_specs,
    }


def _populate_ratio_table(helper, conn, source_schema, fs_actual, target_table, ratio_entries):
    query_plan = _resolve_ratio_query_plan(helper, conn, source_schema, fs_actual, ratio_entries)
    ratio_names = [ratio_spec["name"] for ratio_spec in query_plan["ratios"]]
    if not ratio_names:
        return

    select_columns_sql = ",\n                ".join(
        f"{ratio_spec['sql']} AS {helper._sql_ident(ratio_spec['name'])}"
        for ratio_spec in query_plan["ratios"]
    )
    insert_columns_sql = ", ".join(
        [helper._sql_ident("docID"), *[helper._sql_ident(name) for name in ratio_names]]
    )
    update_columns_sql = ", ".join(
        f"{helper._sql_ident(name)} = excluded.{helper._sql_ident(name)}"
        for name in ratio_names
    )

    fs_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(fs_actual)}"
    join_sql = "\n            ".join(query_plan["joins"])
    insert_sql = f"""
        INSERT INTO {helper._sql_ident(target_table)} ({insert_columns_sql})
        SELECT
            fs.{helper._sql_ident(query_plan['fs_docid_col'])} AS {helper._sql_ident('docID')},
            {select_columns_sql}
        FROM {fs_ref} fs
            {join_sql}
        WHERE fs.{helper._sql_ident(query_plan['fs_docid_col'])} IS NOT NULL
        ON CONFLICT({helper._sql_ident('docID')}) DO UPDATE SET {update_columns_sql}
    """
    conn.execute(insert_sql)


def generate_ratios(
    database,
    overwrite=False,
    batch_size=5000,
    helper=None,
):
    """Generate configured ratio tables keyed by FinancialStatements.docID."""
    helper = helper or ratio_services._DB_HELPER
    db_path = database
    if not db_path:
        raise ValueError("database is required for generate_ratios.")
    formulas_path = RATIO_DEFINITIONS_PATH
    if not os.path.exists(formulas_path):
        raise FileNotFoundError(f"Ratio definitions file not found: {formulas_path}")

    ratio_definitions = _load_ratio_definitions(formulas_path)
    batch_size = max(int(batch_size or 5000), 1)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        source_schema = "main"

        fs_actual = helper._resolve_table_name_in_schema(conn, source_schema, "FinancialStatements")
        if not fs_actual:
            raise RuntimeError("Source table 'FinancialStatements' not found; required for generate_ratios.")

        target_tables = list(ratio_definitions)
        ratio_count = 0
        for target_table, ratio_entries in ratio_definitions.items():
            ratio_names = [ratio_entry["name"] for ratio_entry in ratio_entries]
            _ensure_ratio_table_schema(
                conn,
                target_table,
                ratio_names,
                overwrite=overwrite,
                helper=helper,
            )
            ratio_count += len(ratio_entries)

        fs_docid_col = _resolve_column_name_in_schema(helper, conn, source_schema, fs_actual, "docID")
        if not fs_docid_col:
            raise RuntimeError("Source table 'FinancialStatements' is missing a docID column.")

        seeded_documents = _seed_ratio_docids(
            helper,
            conn,
            source_schema,
            fs_actual,
            target_tables,
            fs_docid_col,
        )

        for target_table, ratio_entries in ratio_definitions.items():
            _populate_ratio_table(
                helper,
                conn,
                source_schema,
                fs_actual,
                target_table,
                ratio_entries,
            )

        conn.commit()
        logger.info(
            "Generated %d ratio(s) across %d table(s) for %d document(s).",
            ratio_count,
            len(target_tables),
            seeded_documents,
        )
        return {
            "status": "success",
            "documents_seeded": seeded_documents,
            "ratio_count": ratio_count,
            "tables": target_tables,
            "formulas_config": formulas_path,
            "batch_size": batch_size,
        }
    finally:
        conn.close()


def run_generate_ratios(config, overwrite=False):
    logger.info("Generating configured ratio tables...")
    step_cfg = config.get("generate_ratios_config", {})

    return generate_ratios(
        database=step_cfg.get("Database"),
        overwrite=overwrite,
        batch_size=step_cfg.get("batch_size", 5000),
    )


STEP_DEFINITION = StepDefinition(
    name="generate_ratios",
    handler=run_generate_ratios,
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Database", "database", required=True),
        StepFieldDefinition("batch_size", "num", default=5000),
    ),
)