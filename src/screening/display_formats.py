"""Display formats for screening result columns.

Formats come from the sources that define the columns rather than from
column-name patterns:

* ratio definitions declare ``"format"`` for each generated ratio;
* statement-table columns built from the EDINET taxonomy take the XBRL item
  type of their concept (a ``percentItemType`` is shown as a percentage);
* rolling averages inherit their source column's format, and rolling growth
  is a compound annual rate.

Columns without a declared format are plain numbers or text.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Mapping
from functools import lru_cache

from src.orchestrator.common.sqlite import connect_read
from src.orchestrator.generate_ratios.generate_ratios import ratio_display_formats
from src.orchestrator.generate_rolling_metrics.service import (
    parse_rolling_column,
    rolling_source_table,
)

from .screening import _build_result_column_aliases

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _ratio_formats() -> dict[str, str]:
    try:
        return ratio_display_formats()
    except (OSError, ValueError) as exc:
        logger.warning("Ratio display formats unavailable: %s", exc)
        return {}


def taxonomy_formats(db_path: str) -> dict[str, str]:
    """``{"Table.Column": "percent"}`` for taxonomy columns whose concept is a percent item."""
    try:
        conn = connect_read(db_path)
    except (OSError, sqlite3.Error):
        return {}
    try:
        rows = conn.execute(
            """SELECT DISTINCT h.statement_family, h.primary_label_en
                 FROM Statement_Hierarchy h
                 JOIN Taxonomy_Dictionary d ON d.concept_qname = h.concept_qname
                WHERE h.is_column = 1 AND d.item_type LIKE '%:percentItemType'"""
        ).fetchall()
    except sqlite3.Error as exc:
        # The concept dictionary is filled by the taxonomy pipeline step.
        logger.info("Taxonomy column formats unavailable: %s", exc)
        return {}
    finally:
        conn.close()
    return {f"{family}.{column}": "percent" for family, column in rows}


def column_format(reference: str, declared: Mapping[str, str] | None = None) -> str | None:
    """Declared format for one ``Table.Column`` reference, if any."""
    formats = {**_ratio_formats(), **(declared or {})}
    if reference in formats:
        return formats[reference]
    table, _, column = reference.partition(".")
    source_table = rolling_source_table(table)
    parsed = parse_rolling_column(column)
    if source_table is None or parsed is None:
        return None
    metric, kind = parsed
    if kind == "growth":
        return "percent"
    return formats.get(f"{source_table}.{metric}")


def result_column_formats(columns: list[str], db_path: str | None = None) -> dict[str, str]:
    """Formats keyed by the result column names screening returns for ``columns``."""
    references = [column for column in dict.fromkeys(columns) if "." in column]
    declared = taxonomy_formats(db_path) if db_path else {}
    aliases = _build_result_column_aliases(references)
    return {
        aliases[reference]: display_format
        for reference in references
        if (display_format := column_format(reference, declared))
    }


def catalog_column_formats(metrics: Mapping[str, list[str]], db_path: str | None = None) -> dict[str, str]:
    """Declared formats for every ``Table.Column`` the screener offers, keyed by reference."""
    declared = taxonomy_formats(db_path) if db_path else {}
    formats: dict[str, str] = {}
    for table, columns in metrics.items():
        for column in columns:
            reference = f"{table}.{column}"
            display_format = column_format(reference, declared)
            if display_format:
                formats[reference] = display_format
    return formats
