"""FastAPI HTTP layer for the EDINET orchestrator.

Run the standalone API with ``python -m src.api.router``.
"""

from .router import app

__all__ = ["app"]
