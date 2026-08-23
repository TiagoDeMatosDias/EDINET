"""Capture TDnet split/consolidation disclosures as pipeline events."""

from src.orchestrator.check_tdnet_splits.check_tdnet_splits import (
    STEP_DEFINITION,
    run_check_tdnet_splits,
)

__all__ = ["STEP_DEFINITION", "run_check_tdnet_splits"]
