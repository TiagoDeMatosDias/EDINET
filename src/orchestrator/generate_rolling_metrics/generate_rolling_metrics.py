import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition

from . import service as rolling_metrics_services

logger = logging.getLogger(__name__)


def run_generate_rolling_metrics(config, overwrite=False):
    logger.info("Generating rolling metrics tables...")
    step_cfg = config.get("generate_rolling_metrics_config", {})

    return rolling_metrics_services.generate_rolling_metrics(
        source_database=step_cfg.get("Source_Database"),
        target_database=step_cfg.get("Target_Database"),
        overwrite=overwrite,
    )


STEP_DEFINITION = StepDefinition(
    name="generate_rolling_metrics",
    handler=run_generate_rolling_metrics,
    aliases=("Generate Rolling Metrics",),
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Source_Database", "database", required=True),
        StepFieldDefinition("Target_Database", "database", required=True),
    ),
)
