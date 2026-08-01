import logging
import sqlite3

from src.orchestrator.common import StepDefinition
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


def _delete_ticker_price_rows(conn, prices_table: str, ticker: str) -> int:
    """Delete cached prices for one ticker and return the deleted row count."""
    cursor = conn.execute(
        f"DELETE FROM [{prices_table}] WHERE Ticker = ?",
        (ticker,),
    )
    return max(cursor.rowcount, 0)


def _update_ticker(
    conn,
    prices_table: str,
    ticker: str,
    *,
    overwrite: bool,
    savepoint_id: int,
) -> bool:
    """Update one ticker, optionally replacing its cached history atomically."""
    if not overwrite:
        result = stockprice_api.load_ticker_data(ticker, prices_table, conn)
        if result:
            conn.commit()
        else:
            conn.rollback()
        return result

    savepoint = f"stock_price_overwrite_{savepoint_id}"
    conn.execute(f"SAVEPOINT {savepoint}")
    deleted_rows = 0
    try:
        deleted_rows = _delete_ticker_price_rows(conn, prices_table, ticker)
        result = stockprice_api.load_ticker_data(ticker, prices_table, conn)
        replacement_exists = conn.execute(
            f"SELECT 1 FROM [{prices_table}] WHERE Ticker = ? LIMIT 1",
            (ticker,),
        ).fetchone() is not None
        if not result or not replacement_exists:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            conn.rollback()
            logger.warning(
                "Keeping %s existing price rows for %s because overwrite "
                "did not produce replacement data",
                deleted_rows,
                ticker,
            )
            return False

        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        conn.commit()
        logger.info(
            "Replaced %s existing price rows for %s with freshly downloaded data",
            deleted_rows,
            ticker,
        )
        return True
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            conn.rollback()
        except sqlite3.Error:
            logger.exception("Could not roll back overwrite for ticker %s", ticker)
        raise


def update_all_stock_prices(
    db_name,
    Company_Table="CompanyInfo",
    prices_table="Stock_Prices",
    context=None,
    overwrite=False,
):
    """Fetch and store the latest stock prices for tickers present in the database.

    Japanese tickers use the JPX quote historical page first; the shared
    provider helper falls back to Stooq/Yahoo when JPX cannot cover the
    requested history.

    Args:
        db_name: Path to the SQLite database.
        Company_Table: Name of the company-info table (default ``CompanyInfo``).
        prices_table: Name of the stock-prices table (default ``Stock_Prices``).
        overwrite: Delete each ticker's cached prices before downloading a
            complete replacement. Failed or empty downloads are rolled back.
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

        if overwrite:
            logger.warning(
                "Stock-price overwrite enabled; replacing data for %s tickers",
                len(tickers),
            )

        failed_tickers = []
        for index, ticker in enumerate(tickers):
            if context is not None:
                context.report_progress(
                    index,
                    len(tickers),
                    f"Updating ticker {index + 1} of {len(tickers)}",
                )
            updated = _update_ticker(
                conn,
                prices_table,
                ticker,
                overwrite=overwrite,
                savepoint_id=index,
            )
            if not updated:
                failed_tickers.append(ticker)
        if failed_tickers:
            logger.warning(
                "Stock-price updates failed for %s of %s tickers; remaining "
                "tickers were still attempted",
                len(failed_tickers),
                len(tickers),
            )
        if context is not None and tickers:
            context.report_progress(
                len(tickers),
                len(tickers),
                "Stock price update complete",
            )

    except Exception as exc:
        logger.error("An error occurred: %s", exc, exc_info=True)
        raise

    finally:
        if conn:
            conn.close()


def run_update_stock_prices(config, overwrite=False, context=None):
    """Handler that resolves the target database path and runs the updater."""
    logger.info("Updating stock prices...")

    kwargs = dict(
        Company_Table="CompanyInfo",
        prices_table="Stock_Prices",
    )
    if context is not None:
        kwargs["context"] = context
    if overwrite:
        kwargs["overwrite"] = True
    return update_all_stock_prices(get_db2(), **kwargs)


STEP_DEFINITION = StepDefinition(
    name="update_stock_prices",
    handler=run_update_stock_prices,
    required_keys=(),
    supports_overwrite=True,
    input_fields=(),
)
