import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition

from . import service as financial_statement_services

logger = logging.getLogger(__name__)


def run_generate_financial_statements(config, overwrite=False):
    logger.info("Generating financial statements...")
    step_cfg = config.get("generate_financial_statements_config", {})

    return financial_statement_services.generate_financial_statements(
        source_database=step_cfg.get("Source_Database"),
        target_database=step_cfg.get("Target_Database"),
        granularity_level=step_cfg.get("Granularity_level", 3),
        overwrite=overwrite,
    )


STEP_DEFINITION = StepDefinition(
    name="generate_financial_statements",
    handler=run_generate_financial_statements,
    aliases=("Generate Financial Statements",),
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Source_Database", "database", required=True),
        StepFieldDefinition("Target_Database", "database", required=True),
        StepFieldDefinition("Granularity_level", "num", default=3),
    ),
)