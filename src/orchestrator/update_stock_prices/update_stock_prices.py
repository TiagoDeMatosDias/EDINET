import logging
import sqlite3

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_db2
from src.utilities import stock_prices

logger = logging.getLogger(__name__)

stockprice_api = stock_prices


def get_tickers_from_prices(conn, table_name="CompanyInfo"):
    """Return a list of distinct, non-null, non-empty ticker values from *table_name*.

    Looks for a column named ``Company_Ticker`` (when *table_name* is ``CompanyInfo``)
    or ``Ticker`` (when *table_name* is ``Stock_Prices``) and returns every distinct
    non-null / non-whitespace value.

    Returns an empty list when the table does not exist or contains no matching rows.
    """
    cursor = conn.cursor()
    column = "Company_Ticker" if table_name == "CompanyInfo" else "Ticker"
    try:
        cursor.execute(
            f"SELECT DISTINCT {column} FROM [{table_name}] "
            f"WHERE {column} IS NOT NULL AND TRIM({column}) != ''"
        )
        rows = cursor.fetchall()
        return [r[0] for r in rows]
    except sqlite3.OperationalError:
        return []


def update_all_stock_prices(db_name, Company_Table="CompanyInfo", prices_table="Stock_Prices"):
    """Fetch and store the latest stock prices for tickers present in the database.

    Args:
        db_name: Path to the SQLite database.
        Company_Table: Name of the company-info table (default ``CompanyInfo``).
        prices_table: Name of the stock-prices table (default ``Stock_Prices``).
    """
    conn = None
    try:
        conn = sqlite3.connect(db_name)

        # Try the prices table first (tickers that already have some price history).
        tickers = get_tickers_from_prices(conn, table_name=prices_table)

        # Ensure the prices table exists before attempting updates.
        stockprice_api._create_prices_table(conn, prices_table)
        conn.commit()

        # If the prices table was empty (or just created), fall back to CompanyInfo.
        if not tickers:
            tickers = get_tickers_from_prices(conn, table_name=Company_Table)

        logger.info("Found %s tickers to update stock prices for", len(tickers))

        provider_available = True
        for ticker in tickers:
            if not provider_available:
                logger.warning(
                    "Skipping remaining stock price updates because the price provider is unavailable."
                )
                break
            provider_available = stockprice_api.load_ticker_data(ticker, prices_table, conn)

    except Exception as exc:
        logger.error("An error occurred: %s", exc, exc_info=True)

    finally:
        if conn:
            conn.close()


def run_update_stock_prices(config, overwrite=False):
    """Handler that resolves the target database path and runs the updater."""
    logger.info("Updating stock prices...")

    return update_all_stock_prices(
        get_db2(),
        Company_Table="CompanyInfo",
        prices_table="Stock_Prices",
    )


STEP_DEFINITION = StepDefinition(
    name="update_stock_prices",
    handler=run_update_stock_prices,
    required_keys=(),
    input_fields=(),
)
