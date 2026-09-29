"""Display formats for screening result columns.

Formats come from the pipelines that define the columns rather than from
column-name patterns: ratio definitions declare ``"format"`` for each ratio,
rolling averages inherit their source column's format, and rolling growth is a
compound annual rate. Columns without a declared format are plain numbers or
text.
"""

from __future__ import annotations

import logging
from functools import lru_cache

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


def column_format(reference: str) -> str | None:
    """Declared format for one ``Table.Column`` reference, if any."""
    formats = _ratio_formats()
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


def result_column_formats(columns: list[str]) -> dict[str, str]:
    """Formats keyed by the result column names screening returns for ``columns``."""
    references = [column for column in dict.fromkeys(columns) if "." in column]
    aliases = _build_result_column_aliases(references)
    return {
        aliases[reference]: display_format
        for reference in references
        if (display_format := column_format(reference))
    }
