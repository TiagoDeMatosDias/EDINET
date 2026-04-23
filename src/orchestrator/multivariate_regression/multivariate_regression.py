import logging

from src.orchestrator.common import StepDefinition

from . import analysis

logger = logging.getLogger(__name__)


def run_multivariate_regression(config, overwrite=False):
    logger.info("Running multivariate regression...")
    step_cfg = config.get("Multivariate_Regression_config", {})

    return analysis.multivariate_regression(
        step_cfg,
        step_cfg.get("Source_Database"),
        company_table=config.get("DB_COMPANY_INFO_TABLE"),
    )


STEP_DEFINITION = StepDefinition(
    name="Multivariate_Regression",
    handler=run_multivariate_regression,
    required_config_fields=(("Multivariate_Regression_config", "Source_Database"),),
)