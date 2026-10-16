"""Screening handler: run one screen on the market database for a user."""

from __future__ import annotations

import logging

from src.orchestrator.common.db_config import get_market_db
from src.screening.job_runner import run_screening_request
from src.worker.context import JobContext, JobError

logger = logging.getLogger(__name__)


def run(context: JobContext) -> dict:
    """Run ``payload["request"]`` with the owner's tags shadowing ``Company_Tags``.

    The result is the body ``POST /api/screening/run`` returns (without
    ``last_run_at``, which the server's finalizer adds).
    """
    payload = context.payload
    request = payload.get("request")
    if not isinstance(request, dict):
        raise JobError("The screening request is missing.")
    tags = [(str(pair[0]), str(pair[1])) for pair in payload.get("company_tags") or [] if len(pair) >= 2]
    context.report_progress(None, "Running the screen")
    context.checkpoint()
    try:
        return run_screening_request(str(get_market_db()), request, tags)
    except ValueError as exc:  # invalid criteria: the user can fix the screen
        raise JobError(str(exc), retryable=False) from exc
