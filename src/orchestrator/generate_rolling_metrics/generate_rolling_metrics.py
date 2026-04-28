import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition

from . import service as rolling_metrics_services

logger = logging.getLogger(__name__)


def run_generate_rolling_metrics(config, overwrite=False):
    logger.info("Generating rolling metrics tables...")
    step_cfg = config.get("generate_rolling_metrics_config", {})

    raw_source = step_cfg.get("Source_Database")
    raw_target = step_cfg.get("Target_Database")
    source_db = config.resolve_db_path(raw_source) if hasattr(config, 'resolve_db_path') else raw_source
    target_db = config.resolve_db_path(raw_target) if hasattr(config, 'resolve_db_path') else raw_target

    return rolling_metrics_services.generate_rolling_metrics(
        source_database=source_db,
        target_database=target_db,
        overwrite=overwrite,
    )


STEP_DEFINITION = StepDefinition(
    name="generate_rolling_metrics",
    handler=run_generate_rolling_metrics,
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Source_Database", "database", required=True),
        StepFieldDefinition("Target_Database", "database", required=True),
    ),
)
