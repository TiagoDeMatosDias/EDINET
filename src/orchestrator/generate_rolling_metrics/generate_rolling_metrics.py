import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_db2

from . import service as rolling_metrics_services

logger = logging.getLogger(__name__)


def run_generate_rolling_metrics(config, overwrite=False):
    logger.info("Generating rolling metrics tables...")
    step_cfg = config.get("generate_rolling_metrics_config", {})
    db2 = get_db2()

    return rolling_metrics_services.generate_rolling_metrics(
        source_database=db2,
        target_database=db2,
        overwrite=overwrite,
    )


STEP_DEFINITION = StepDefinition(
    name="generate_rolling_metrics",
    handler=run_generate_rolling_metrics,
    supports_overwrite=True,
    input_fields=(),
)
