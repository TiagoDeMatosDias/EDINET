import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.utilities import stock_prices

logger = logging.getLogger(__name__)

stockprice_api = stock_prices


def run_update_stock_prices(config, overwrite=False):
    logger.info("Updating stock prices...")
    step_cfg = config.get("update_stock_prices_config", {})

    return stockprice_api.update_all_stock_prices(
        db_name=step_cfg.get("Target_Database"),
        Company_Table=config.get("DB_COMPANY_INFO_TABLE"),
        prices_table=config.get("DB_STOCK_PRICES_TABLE"),
        standardized_table=config.get("DB_FINANCIAL_DATA_TABLE"),
    )


STEP_DEFINITION = StepDefinition(
    name="update_stock_prices",
    handler=run_update_stock_prices,
    required_keys=("DB_COMPANY_INFO_TABLE", "DB_STOCK_PRICES_TABLE", "DB_FINANCIAL_DATA_TABLE"),
    input_fields=(
        StepFieldDefinition("Target_Database", "database", required=True),
    ),
)