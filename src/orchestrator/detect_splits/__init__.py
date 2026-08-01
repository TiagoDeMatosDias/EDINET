"""Detect stock splits via price heuristic with ShareMetrics cross-validation."""

from src.orchestrator.detect_splits.detect_splits import (
    STEP_DEFINITION,
    run_detect_splits,
)

__all__ = ["STEP_DEFINITION", "run_detect_splits"]
