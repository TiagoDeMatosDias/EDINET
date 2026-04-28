import logging
import sqlite3

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.utilities import stock_prices

logger = logging.getLogger(__name__)

stockprice_api = stock_prices


def get_tickers_from_prices(conn):
    """Return a list of distinct, non-null, non-empty Company_Ticker values from `prices_table`.

    This helper uses the provided SQLite connection and will return an empty list if the
    table does not exist.
    """
    cursor = conn.cursor()
    # Check table exists to avoid OperationalError when the table is missing
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        ("CompanyInfo",),
    )
    if not cursor.fetchone():
        return []

    cursor.execute(
        f"SELECT DISTINCT Company_Ticker FROM CompanyInfo "
        "WHERE Company_Ticker IS NOT NULL AND TRIM(Company_Ticker) != ''"
    )
    rows = cursor.fetchall()
    return [r[0] for r in rows]


def update_all_stock_prices(db_name):
    """Fetch and store the latest stock prices for tickers present in `Stock_Prices`.

    This function takes only a database path as its input.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        prices_table = "Stock_Prices"

        # First, try to get tickers already listed in the prices table (if it exists).
        tickers = get_tickers_from_prices(conn)

        # Ensure the prices table exists before attempting updates.
        stockprice_api._create_prices_table(conn, prices_table)
        conn.commit()

        # If the table was created above and we found no tickers earlier, try again.
        if not tickers:
            tickers = get_tickers_from_prices(conn)

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
    # Expect a Config-like object here; fall back to dict-like access.
    step_cfg = config.get("update_stock_prices_config", {}) if hasattr(config, 'get') else (config or {}).get("update_stock_prices_config", {})
    raw_target = step_cfg.get("Target_Database")
    db_name = config.resolve_db_path(raw_target) if hasattr(config, 'resolve_db_path') else raw_target

    return update_all_stock_prices(db_name)


STEP_DEFINITION = StepDefinition(
    name="update_stock_prices",
    handler=run_update_stock_prices,
    required_keys=(),
    input_fields=(
        StepFieldDefinition("Target_Database", "database", required=True),
    ),
)
