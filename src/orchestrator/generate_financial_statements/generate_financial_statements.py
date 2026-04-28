import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition

from . import service as financial_statement_services

logger = logging.getLogger(__name__)


def run_generate_financial_statements(config, overwrite=False):
    logger.info("Generating financial statements...")
    step_cfg = config.get("generate_financial_statements_config", {})
    raw_source = step_cfg.get("Source_Database")
    raw_target = step_cfg.get("Target_Database")
    source_db = config.resolve_db_path(raw_source) if hasattr(config, 'resolve_db_path') else raw_source
    target_db = config.resolve_db_path(raw_target) if hasattr(config, 'resolve_db_path') else raw_target

    return financial_statement_services.generate_financial_statements(
        source_database=source_db,
        target_database=target_db,
        granularity_level=step_cfg.get("Granularity_level", 3),
        overwrite=overwrite,
    )


STEP_DEFINITION = StepDefinition(
    name="generate_financial_statements",
    handler=run_generate_financial_statements,
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Source_Database", "database", required=True),
        StepFieldDefinition("Target_Database", "database", required=True),
        StepFieldDefinition("Granularity_level", "num", default=3),
    ),
)