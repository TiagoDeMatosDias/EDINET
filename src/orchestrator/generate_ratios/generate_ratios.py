import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common import ratios as ratio_services

logger = logging.getLogger(__name__)


def run_generate_ratios(config, overwrite=False):
    logger.info("Generating ratios tables (PerShare / Valuation / Quality)...")
    step_cfg = config.get("generate_ratios_config", {})

    return ratio_services.generate_ratios(
        source_database=step_cfg.get("Source_Database"),
        target_database=step_cfg.get("Target_Database"),
        formulas_config=step_cfg.get(
            "Formulas_Config",
            "config/reference/generate_ratios_formulas_config.json",
        ),
        overwrite=overwrite,
        batch_size=step_cfg.get("batch_size", 5000),
    )


STEP_DEFINITION = StepDefinition(
    name="generate_ratios",
    handler=run_generate_ratios,
    aliases=("Generate Ratios",),
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Source_Database", "database", required=True),
        StepFieldDefinition("Target_Database", "database", required=True),
        StepFieldDefinition(
            "Formulas_Config",
            "file",
            default="config/reference/generate_ratios_formulas_config.json",
        ),
        StepFieldDefinition("batch_size", "num", default=5000),
    ),
)