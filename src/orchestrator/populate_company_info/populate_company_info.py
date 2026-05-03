import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_db2
from src.orchestrator.common.edinet import Edinet, EDINET_BASE_URL

logger = logging.getLogger(__name__)


def run_populate_company_info(config, overwrite=False):
    logger.info("Populating company info table...")
    step_cfg = config.get("populate_company_info_config", {})
    db2 = get_db2()

    edinet = Edinet(
        base_url=EDINET_BASE_URL,
        api_key=config.get("API_KEY", ""),
        db_path=db2,
        company_info_table="CompanyInfo",
    )
    edinet.store_edinetCodes(step_cfg.get("csv_file"), target_database=db2)


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
    ),
)