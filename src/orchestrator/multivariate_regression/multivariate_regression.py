import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition

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
    display_name="Multivariate Regression",
    input_fields=(
        StepFieldDefinition("Source_Database", "database", required=True),
        StepFieldDefinition(
            "Output",
            "file",
            default="data/ols_results/ols_results_summary.txt",
        ),
        StepFieldDefinition(
            "winsorize_thresholds",
            "json",
            default={"lower": 0.05, "upper": 0.95},
            label="winsorize_thresholds (JSON)",
        ),
        StepFieldDefinition("SQL_Query", "text", height=6, required=True),
    ),
)