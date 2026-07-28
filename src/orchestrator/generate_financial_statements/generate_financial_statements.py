import logging
import os

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_db1, get_db2, get_filings_db

from . import service as financial_statement_services

logger = logging.getLogger(__name__)


def run_generate_financial_statements(config, overwrite=False, context=None):
    logger.info("Generating financial statements...")
    step_cfg = config.get("generate_financial_statements_config", {})
    source_mode = str(step_cfg.get("Source_Mode", "csv") or "csv").strip().casefold()
    source_database = get_db1()
    if source_mode == "filings":
        source_database = os.getenv("EDINET_FILINGS_DB") or get_filings_db()

    kwargs = dict(
        source_database=source_database,
        target_database=get_db2(),
        granularity_level=step_cfg.get("Granularity_level", 3),
        overwrite=overwrite,
    )
    if source_mode != "csv":
        kwargs["source_mode"] = source_mode
    if context is not None:
        kwargs["context"] = context
    return financial_statement_services.generate_financial_statements(**kwargs)


STEP_DEFINITION = StepDefinition(
    name="generate_financial_statements",
    handler=run_generate_financial_statements,
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition(
            "Source_Mode",
            "str",
            default="csv",
            label="Source mode",
            description="Use the legacy financial CSV table or the normalized XBRL filings catalog.",
            choices=("csv", "filings"),
        ),
        StepFieldDefinition("Granularity_level", "num", default=3),
    ),
)
