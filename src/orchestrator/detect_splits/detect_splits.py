"""Orchestrator step that detects stock splits in price data and cross-validates
them against annual report share counts.

Auto-discovered by ``build_step_registry()`` in
``src/orchestrator/common/__init__.py``.
"""

from __future__ import annotations

import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_db2

logger = logging.getLogger(__name__)


def run_detect_splits(config, overwrite=False, context=None):
    """Handler for the *detect_splits* orchestrator step.

    Config keys (passed through *config* dict):
        mode (str): ``"incremental"`` (default), ``"full"``, or
            ``"verify_pending"``.
        price_drop_threshold (float): Minimum single-day price drop to flag
            as a potential split (default 0.40 = 40%).

    Returns:
        dict with counts for ``new_pending``, ``confirmed``, ``rejected``,
        ``already_known``, and ``tickers_scanned``.
    """
    # Lazy imports: this module is auto-discovered by the orchestrator step
    # registry, which can run while src.portfolio is still partially imported.
    from src.portfolio.split_detection import run_split_detection
    from src.portfolio.split_schema import ensure_split_tables
    from src.portfolio.portfolio_state import _invalidate_split_cache

    db2_path = get_db2()

    # Idempotent schema check
    ensure_split_tables(db2_path)

    mode = str(config.get("mode") or "incremental").strip()
    threshold = float(config.get("price_drop_threshold") or 0.40)

    results = run_split_detection(
        db2_path=db2_path,
        tickers=None,  # all tickers with price data
        mode=mode,
        threshold=threshold,
    )

    # Invalidate the in-process adjustment cache so the next portfolio
    # rebuild (in this same Python process) picks up new splits.
    _invalidate_split_cache()

    return results


STEP_DEFINITION = StepDefinition(
    name="detect_splits",
    handler=run_detect_splits,
    required_keys=(),
    input_fields=(
        StepFieldDefinition(
            key="mode",
            field_type="choice",
            default="incremental",
            choices=("incremental", "full", "verify_pending"),
            label="Detection Mode",
            description=(
                "Incremental: only scan new price data since last known split. "
                "Full: rescan all history for every ticker. "
                "Verify Pending: re-check entries awaiting ShareMetrics confirmation."
            ),
        ),
        StepFieldDefinition(
            key="price_drop_threshold",
            field_type="num",
            default=0.40,
            label="Price Drop Threshold",
            description=(
                "Minimum single-day price drop (0.0–1.0) to flag as a "
                "potential split.  Default 0.40 = 40%."
            ),
        ),
    ),
)
