import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.utilities import stock_prices

logger = logging.getLogger(__name__)

stockprice_api = stock_prices


def run_import_stock_prices_csv(config, overwrite=False):
    logger.info("Importing stock prices from CSV...")
    step_cfg = config.get("import_stock_prices_csv_config", {})

    return stockprice_api.import_stock_prices_csv(
        db_name=step_cfg.get("Target_Database"),
        prices_table=config.get("DB_STOCK_PRICES_TABLE"),
        csv_path=step_cfg.get("csv_file", ""),
        default_ticker=step_cfg.get("default_ticker", step_cfg.get("ticker", "")),
        default_currency=step_cfg.get("default_currency", step_cfg.get("currency", "JPY")),
        date_column=step_cfg.get("date_column", "Date"),
        price_column=step_cfg.get("price_column", "Price"),
        ticker_column=step_cfg.get("ticker_column", ""),
        currency_column=step_cfg.get("currency_column", ""),
    )


STEP_DEFINITION = StepDefinition(
    name="import_stock_prices_csv",
    handler=run_import_stock_prices_csv,
    display_name="Import Stock Prices (CSV)",
    required_keys=("DB_STOCK_PRICES_TABLE",),
    input_fields=(
        StepFieldDefinition("Target_Database", "database", required=True),
        StepFieldDefinition("csv_file", "file", required=True),
        StepFieldDefinition("default_ticker", "str"),
        StepFieldDefinition("default_currency", "str", default="JPY"),
        StepFieldDefinition("date_column", "str", default="Date"),
        StepFieldDefinition("price_column", "str", default="Price"),
        StepFieldDefinition("ticker_column", "str"),
        StepFieldDefinition("currency_column", "str"),
    ),
)