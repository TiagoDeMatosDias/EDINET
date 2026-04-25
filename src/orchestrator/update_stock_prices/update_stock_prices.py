import logging
import sqlite3

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.utilities import stock_prices

logger = logging.getLogger(__name__)

stockprice_api = stock_prices


def update_all_stock_prices(db_name, Company_Table, prices_table, standardized_table=None):
    """Fetch and store the latest stock prices for tickers selected by this step."""
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        stockprice_api._create_prices_table(conn, prices_table)
        conn.commit()

        if standardized_table:
            logger.info("Filtering to only companies in '%s' table", standardized_table)
            query = f"""
                SELECT DISTINCT c.Company_Ticker
                FROM {Company_Table} c
                INNER JOIN {standardized_table} s ON c.edinetCode = s.edinetCode
                WHERE c.Company_Ticker IS NOT NULL
            """
            cursor.execute(query)
        else:
            cursor.execute(
                f"SELECT Company_Ticker FROM {Company_Table} where Company_Ticker is not null"
            )

        tickers = cursor.fetchall()

        logger.info("Found %s tickers to update stock prices for", len(tickers))

        provider_available = True
        for ticker in tickers:
            if not provider_available:
                logger.warning(
                    "Skipping remaining stock price updates because the price provider is unavailable."
                )
                break
            provider_available = stockprice_api.load_ticker_data(ticker[0], prices_table, conn)

    except Exception as exc:
        logger.error("An error occurred: %s", exc, exc_info=True)

    finally:
        if conn:
            conn.close()


def run_update_stock_prices(config, overwrite=False):
    logger.info("Updating stock prices...")
    step_cfg = config.get("update_stock_prices_config", {})

    return update_all_stock_prices(
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