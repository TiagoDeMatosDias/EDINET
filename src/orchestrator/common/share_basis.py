"""Which stored per-share figures move with share splits, and how.

Standardized tables keep each filing's per-share figures and share counts as
the issuer reported them: split detection reads those share counts, and they
are what a reader saw at the time. Stored prices are split-adjusted, so every
view puts the figures on that basis with the filing's factors from
``corporate_actions.filing_basis_factors``:

* ``restated``: figures an issuer restates for a split that takes effect
  before it files (EPS, book value per share, shares at the filing date);
* ``fiscal``: figures fixed at the fiscal year end (the year-end share count
  and per-share figures computed from it);
* ``interim``: the interim dividend, paid on the shares held at the half year;
* ``dividend``: the year's dividend, whose interim and final payments can fall
  either side of a split. Toyota's ¥148 for the year to March 2022 is ¥120
  paid before its 5-for-1 split and ¥28 after: ¥52 on today's shares.

Per-share figures are multiplied by their factor and share counts divided by
it. Rolling averages are stored on their filing's own basis and take the
source figure's factor (an average of dividends is on the year-end basis);
rolling growth rates compare figures on one basis and take none.
"""

from __future__ import annotations

import math
from numbers import Real
from typing import Any

from .rolling_columns import ROLLING_WINDOWS, rolling_average_column, rolling_table_name

FACTOR_KINDS = ("restated", "fiscal", "dividend", "interim")

_REPORTED_RULES: dict[str, dict[str, tuple[str, str]]] = {
    "ShareMetrics": {
        "Basic earnings (loss) per share": ("restated", "*"),
        "Diluted earnings per share": ("restated", "*"),
        "Net assets per share": ("restated", "*"),
        "Dividend paid per share": ("dividend", "*"),
        "Interim dividend paid per share": ("interim", "*"),
        "Total number of issued shares": ("fiscal", "/"),
        "Number of issued shares as of fiscal year end": ("fiscal", "/"),
        "Number of issued shares as of filing date": ("restated", "/"),
    },
    "PerShare_Metrics": {
        "Sales Per Share": ("fiscal", "*"),
        "Earnings Per Share": ("fiscal", "*"),
        "Operating Cashflow Per Share": ("fiscal", "*"),
        "Free Cashflow Per Share": ("fiscal", "*"),
        "Net Assets Per Share": ("fiscal", "*"),
        "NCAV Per Share": ("fiscal", "*"),
    },
}

# The basis a rolling average is stored on, when it differs from the figure's.
_AVERAGE_FACTOR = {"dividend": "fiscal"}


def _with_rolling_tables(rules: dict[str, dict[str, tuple[str, str]]]) -> dict[str, dict[str, tuple[str, str]]]:
    out = dict(rules)
    for table, columns in rules.items():
        out[rolling_table_name(table)] = {
            rolling_average_column(column, window): (_AVERAGE_FACTOR.get(factor, factor), operator)
            for column, (factor, operator) in columns.items()
            for window in ROLLING_WINDOWS
        }
    return out


# ``{table: {column: (factor kind, "*" | "/")}}`` for every split-sensitive column.
SHARE_BASIS_COLUMNS: dict[str, dict[str, tuple[str, str]]] = _with_rolling_tables(_REPORTED_RULES)


def share_basis_rule(table: str, column: str) -> tuple[str, str] | None:
    """``(factor kind, operator)`` for a split-sensitive column, else ``None``."""
    return SHARE_BASIS_COLUMNS.get(table, {}).get(column)


def to_adjusted_basis(value: Any, factor: float | None, operator: str) -> Any:
    """Put one stored value on the split-adjusted basis (non-numbers pass through)."""
    if factor is None or factor == 1.0 or isinstance(value, bool) or not isinstance(value, Real):
        return value
    if not math.isfinite(float(value)):
        return value
    return float(value) * factor if operator == "*" else float(value) / factor
