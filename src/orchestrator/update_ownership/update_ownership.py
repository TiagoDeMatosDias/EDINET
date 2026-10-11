"""Orchestrator step that keeps the ownership tables in ``market.db`` current.

Reads the major shareholders (大株主の状況) and officers (役員の状況) in the
annual reports in the filing catalog, then decides which rows are the same
person or organisation across companies and years. The Analysis page's Major
shareholders and Related people sections and Research's People & holders tab
read the result. Run it after ``download_xbrl`` so new reports are stored, and
after ``populate_company_info`` so holders that file with EDINET are named as
Company Analysis names them.

Auto-discovered by ``build_step_registry()`` in
``src/orchestrator/common/__init__.py``.
"""

from __future__ import annotations

import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_filings_db, get_market_db

logger = logging.getLogger(__name__)

_PROGRESS_EVERY = 25


def _flag(value, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, str):
        return value.strip().lower() not in ("0", "false", "no", "off")
    return bool(value)


def run_update_ownership(config, overwrite=False, context=None):
    """Handler for the *update_ownership* orchestrator step."""
    from src.ownership.update import update_ownership

    step_cfg = config.get("update_ownership_config", {}) or {}

    def progress(done: int, total: int, message: str) -> None:
        # Tens of thousands of reports: progress is recorded (and cancellation checked) every few dozen.
        if context is not None and (done % _PROGRESS_EVERY == 0 or done >= total):
            context.report_progress(done, total, message)

    result = update_ownership(
        market_db=get_market_db(),
        filings_db_path=get_filings_db(),
        reparse=bool(overwrite),
        latest_only=not _flag(step_cfg.get("history"), True),
        max_documents=int(step_cfg.get("max_documents") or 0),
        progress=progress,
    )
    logger.info("Ownership update finished: %s", result)
    return result


STEP_DEFINITION = StepDefinition(
    name="update_ownership",
    handler=run_update_ownership,
    display_name="Update ownership",
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition(
            key="history",
            field_type="bool",
            default=True,
            label="Every stored year",
            description=(
                "Read the shareholders and officers in every annual report in the filing catalog, so stakes and boards "
                "can be followed over the years. Off reads only each company's latest report. The first run reads "
                "about a hundred reports a second; later runs read only reports not seen before."
            ),
        ),
        StepFieldDefinition(
            key="max_documents",
            field_type="num",
            default=0,
            label="Max documents",
            description="Read at most this many new reports per run; 0 reads them all.",
        ),
    ),
)
