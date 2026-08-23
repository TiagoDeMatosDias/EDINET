"""Orchestrator step that captures TDnet split/consolidation disclosures.

Japanese stock splits are announced through TSE timely disclosure (TDnet),
not EDINET, and TDnet only serves a rolling ~30 day window.  Scheduling this
step daily (or weekly) records every split-related disclosure as an event in
``Tdnet_Disclosures`` (Standardized.db) before it ages out of TDnet's index.

Auto-discovered by ``build_step_registry()`` in
``src/orchestrator/common/__init__.py``.
"""

from __future__ import annotations

import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_db2
from src.utilities.tdnet import (
    DEFAULT_SPLIT_KEYWORDS,
    TDNET_MAX_LOOKBACK_DAYS,
    ensure_tdnet_tables,
    run_tdnet_split_check,
)

logger = logging.getLogger(__name__)


def _parse_keywords(raw) -> tuple[str, ...]:
    """Parse the comma-separated keyword config into a clean tuple."""
    if isinstance(raw, str):
        candidates = raw.split(",")
    elif isinstance(raw, (list, tuple)):
        candidates = list(raw)
    else:
        return DEFAULT_SPLIT_KEYWORDS
    keywords = tuple(kw.strip() for kw in candidates if kw and kw.strip())
    return keywords or DEFAULT_SPLIT_KEYWORDS


def run_check_tdnet_splits(config, overwrite=False, context=None):
    """Handler for the *check_tdnet_splits* orchestrator step.

    Config keys (passed through *config* dict):
        lookback_days (int): How many calendar days back to search TDnet
            (default 7; the service serves at most ~30 days).
        keywords (str): Comma-separated disclosure-title keywords to search
            (default ``"株式分割,株式併合"``).

    Returns:
        dict with ``events_seen``, ``events_new``, ``window_start``,
        ``window_end``, and ``keywords``.
    """
    db2_path = get_db2()
    # Idempotent schema check for the event table.
    ensure_tdnet_tables(db2_path=db2_path)

    lookback_days = int(config.get("lookback_days") or 7)
    if lookback_days > TDNET_MAX_LOOKBACK_DAYS:
        logger.warning(
            "check_tdnet_splits lookback_days=%s exceeds TDnet's rolling window "
            "of %s days; clamping.",
            lookback_days, TDNET_MAX_LOOKBACK_DAYS,
        )
        lookback_days = TDNET_MAX_LOOKBACK_DAYS
    keywords = _parse_keywords(config.get("keywords"))

    results = run_tdnet_split_check(
        db2_path=db2_path,
        lookback_days=lookback_days,
        keywords=keywords,
    )
    logger.info(
        "TDnet check captured %s new split-related disclosure(s) "
        "(%s seen) between %s and %s",
        results["events_new"], results["events_seen"],
        results["window_start"], results["window_end"],
    )
    return results


STEP_DEFINITION = StepDefinition(
    name="check_tdnet_splits",
    handler=run_check_tdnet_splits,
    required_keys=(),
    display_name="Check TDnet splits",
    supports_overwrite=False,
    input_fields=(
        StepFieldDefinition(
            key="lookback_days",
            field_type="num",
            default=7,
            label="Lookback Days",
            description=(
                "How many calendar days back to search TDnet for split "
                "disclosures. Run daily with 7 (or weekly with 30) so the "
                "rolling window is never missed. Maximum 30."
            ),
        ),
        StepFieldDefinition(
            key="keywords",
            field_type="str",
            default="株式分割,株式併合",
            label="Title Keywords",
            description=(
                "Comma-separated Japanese title keywords that identify split "
                "or consolidation announcements (株式分割 = share split, "
                "株式併合 = share consolidation / reverse split)."
            ),
        ),
    ),
)
