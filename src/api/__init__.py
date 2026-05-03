"""
FastAPI HTTP API layer for the EDINET orchestrator.

This module exposes the existing pipeline functionality as REST endpoints.
The underlying orchestrator logic remains unchanged and fully backward compatible.

The server can be started with:
    python -m src.api.server
"""

from .router import app

__all__ = ["app"]
