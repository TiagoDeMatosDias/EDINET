import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_db1, get_db2

from . import service as financial_statement_services

logger = logging.getLogger(__name__)


def run_generate_financial_statements(config, overwrite=False, context=None):
    logger.info("Generating financial statements...")
    step_cfg = config.get("generate_financial_statements_config", {})

    kwargs = dict(
        source_database=get_db1(),
        target_database=get_db2(),
        granularity_level=step_cfg.get("Granularity_level", 3),
        overwrite=overwrite,
    )
    if context is not None:
        kwargs["context"] = context
    return financial_statement_services.generate_financial_statements(**kwargs)


STEP_DEFINITION = StepDefinition(
    name="generate_financial_statements",
    handler=run_generate_financial_statements,
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Granularity_level", "num", default=3),
    ),
)
