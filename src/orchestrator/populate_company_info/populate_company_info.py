import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.edinet import Edinet

logger = logging.getLogger(__name__)


def run_populate_company_info(config, overwrite=False):
    logger.info("Populating company info table...")
    step_cfg = config.get("populate_company_info_config", {})
    raw_target = step_cfg.get("Target_Database")
    target_database = config.resolve_db_path(raw_target) if hasattr(config, 'resolve_db_path') else raw_target

    edinet = Edinet(
        base_url=config.get("baseURL", ""),
        api_key=config.get("API_KEY", ""),
        db_path=target_database,
        company_info_table="CompanyInfo",
    )
    edinet.store_edinetCodes(step_cfg.get("csv_file"), target_database=target_database)


STEP_DEFINITION = StepDefinition(
    name="populate_company_info",
    handler=run_populate_company_info,
    required_keys=(),
    input_fields=(
        StepFieldDefinition(
            "csv_file",
            "file",
            default="",
            label="csv_file (optional)",
        ),
        StepFieldDefinition("Target_Database", "database", required=True),
    ),
)