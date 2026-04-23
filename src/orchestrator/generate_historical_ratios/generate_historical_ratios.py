import logging

from src.orchestrator.common import StepDefinition
from src.orchestrator.common import ratios as ratio_services

logger = logging.getLogger(__name__)


def run_generate_historical_ratios(config, overwrite=False):
    logger.info(
        "Generating historical ratios tables (Pershare_Historical / Quality_Historical / Valuation_Historical)..."
    )
    step_cfg = config.get("generate_historical_ratios_config", {})

    return ratio_services.generate_historical_ratios(
        source_database=step_cfg.get("Source_Database"),
        target_database=step_cfg.get("Target_Database"),
        overwrite=overwrite,
        company_batch_size=step_cfg.get("company_batch_size", 200),
    )


STEP_DEFINITION = StepDefinition(
    name="generate_historical_ratios",
    handler=run_generate_historical_ratios,
    aliases=("Generate Historical Ratios",),
    required_config_fields=(
        ("generate_historical_ratios_config", "Source_Database"),
        ("generate_historical_ratios_config", "Target_Database"),
    ),
)