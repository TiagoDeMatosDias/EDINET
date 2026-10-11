"""Read major shareholders and officers from annual reports into the ownership tables."""

from src.orchestrator.update_ownership.update_ownership import STEP_DEFINITION, run_update_ownership

__all__ = ["STEP_DEFINITION", "run_update_ownership"]
