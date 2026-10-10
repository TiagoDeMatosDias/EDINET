import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.company_names import fill_english_names
from src.orchestrator.common.db_config import get_filings_db, get_market_db
from src.orchestrator.common.edinet import EDINET_BASE_URL, Edinet
from src.settings import edinet_api_key

logger = logging.getLogger(__name__)


def run_populate_company_info(config, overwrite=False, context=None):
    logger.info("Populating company info table...")
    step_cfg = config.get("populate_company_info_config", {})
    db2 = get_market_db()

    edinet = Edinet(
        base_url=EDINET_BASE_URL,
        api_key=edinet_api_key(),
        db_path=db2,
        company_info_table="CompanyInfo",
    )
    if context is not None:
        context.report_progress(0, 1, "Importing company information")
    edinet.store_edinetCodes(step_cfg.get("csv_file"), target_database=db2)
    # The code list has no English name for most filers; their own annual reports do.
    try:
        named = fill_english_names(db2, get_filings_db())
        logger.info("English company names from annual reports: %s", named)
    except Exception as exc:  # noqa: BLE001 - the import itself succeeded; names are filled again by the next run
        logger.warning("English company names could not be filled in from annual reports: %s", exc)
    if context is not None:
        context.report_progress(1, 1, "Company information import complete")


STEP_DEFINITION = StepDefinition(
    name="populate_company_info",
    handler=run_populate_company_info,
    input_fields=(
        StepFieldDefinition(
            "csv_file",
            "file",
            default="",
            label="csv_file (optional)",
        ),
    ),
)
