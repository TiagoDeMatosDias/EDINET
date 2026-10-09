import logging
import random
import sqlite3
import uuid

from src.orchestrator.common import StepDefinition
from src.orchestrator.common.db_config import get_market_db
from src.orchestrator.common.sqlite import connect_write, quote_identifier
from src.utilities import stock_prices

logger = logging.getLogger(__name__)

stockprice_api = stock_prices

# Commit (and release the WAL write lock) every N ticker updates instead of
# after every single ticker.  Per-ticker commits are slow; batching keeps the
# lock release frequent enough that concurrent writers are not blocked for the
# whole multi-minute fetch loop.
COMMIT_BATCH_SIZE = 100


def get_tickers_from_prices(conn, table_name="CompanyInfo"):
    """Return canonical, distinct ticker values from *table_name*.

    ``CompanyInfo``-style tables use ``Company_Ticker``; price tables use
    ``Ticker``. Values are trimmed before deduplication so provider symbols and
    stored identifiers cannot diverge because of import whitespace.
    """
    cursor = conn.cursor()
    column = "Ticker" if str(table_name).casefold() == "stock_prices" else "Company_Ticker"
    quoted_table = quote_identifier(table_name)
    quoted_column = quote_identifier(column)
    try:
        cursor.execute(
            f"SELECT DISTINCT {quoted_column} FROM {quoted_table} "
            f"WHERE {quoted_column} IS NOT NULL AND TRIM({quoted_column}) != ''"
        )
        rows = cursor.fetchall()
    except sqlite3.OperationalError:
        return []

    tickers = []
    seen = set()
    for row in rows:
        ticker = str(row[0]).strip()
        if ticker and ticker not in seen:
            seen.add(ticker)
            tickers.append(ticker)
    return tickers


def _is_auxiliary_ticker(ticker: str) -> bool:
    """Return whether a Stock_Prices ticker is not an equity instrument."""
    normalized = ticker.strip().upper()
    return normalized == "EUR" or normalized.startswith("INFLATION_")


def _get_ticker_currency(conn, prices_table: str, ticker: str) -> str | None:
    """Return the existing currency for a ticker, or None to let the fetch decide.

    ``load_ticker_data`` repairs mislabelled rows before it reads the stored
    currency and prefers the provider's own report, so a guess here would
    only override better information.
    """
    table = quote_identifier(prices_table)
    try:
        row = conn.execute(
            f"SELECT Currency FROM {table} "
            "WHERE Ticker = ? AND Currency IS NOT NULL AND TRIM(Currency) != '' "
            "ORDER BY Date DESC LIMIT 1",
            (ticker,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    return str(row[0]).strip() if row and row[0] else None


def _delete_ticker_price_rows(conn, prices_table: str, ticker: str) -> int:
    """Delete cached prices for one ticker and return the deleted row count."""
    cursor = conn.execute(
        f"DELETE FROM {quote_identifier(prices_table)} WHERE Ticker = ?",
        (ticker,),
    )
    return max(cursor.rowcount, 0)


def _table_columns(conn, table_name: str) -> list[str]:
    return [
        str(row[1]) for row in conn.execute(f"PRAGMA table_info({quote_identifier(table_name)})")
    ]


def _copy_staged_ticker_rows(
    conn,
    source_table: str,
    target_table: str,
    ticker: str,
) -> int:
    """Copy staged rows into the target using their shared schema columns."""
    source_columns = set(_table_columns(conn, source_table))
    target_columns = set(_table_columns(conn, target_table))
    preferred_columns = (
        "Date",
        "Ticker",
        "Currency",
        "Price",
        "Price_Basis",
        "Provider",
        "Source_Id",
        "Source_Revision",
        "Adjustment_Factor",
        "Split_Adjustment_Factor",
        "Adjusted_Price",
        "Retrieved_At",
    )
    columns = [
        name for name in preferred_columns if name in source_columns and name in target_columns
    ]
    if not {"Date", "Ticker", "Currency", "Price"}.issubset(columns):
        raise RuntimeError("Staged price table is missing required columns")
    quoted_columns = ", ".join(quote_identifier(name) for name in columns)
    target = quote_identifier(target_table)
    source = quote_identifier(source_table)
    cursor = conn.execute(
        f"INSERT INTO {target} ({quoted_columns}) "
        f"SELECT {quoted_columns} FROM {source} WHERE Ticker = ?",
        (ticker,),
    )
    return max(cursor.rowcount, 0)


def _overwrite_ticker(
    conn,
    prices_table: str,
    ticker: str,
    *,
    currency: str | None,
) -> bool:
    """Replace a ticker's cached history through an isolated staging table.

    Returns True when replacement rows were stored.  Does not commit; the
    caller's transaction controls the commit boundary.
    """
    staging_table = f"__stock_price_stage_{uuid.uuid4().hex}"
    try:
        stockprice_api._create_prices_table(conn, staging_table)
        result = stockprice_api.load_ticker_data(
            ticker,
            staging_table,
            conn,
            currency=currency,
        )
        replacement_exists = (
            conn.execute(
                f"SELECT 1 FROM {quote_identifier(staging_table)} WHERE Ticker = ? LIMIT 1",
                (ticker,),
            ).fetchone()
            is not None
        )
        if not result or not replacement_exists:
            logger.warning(
                "Keeping existing price rows for %s because overwrite "
                "did not produce replacement data",
                ticker,
            )
            return False

        deleted_rows = _delete_ticker_price_rows(conn, prices_table, ticker)
        inserted_rows = _copy_staged_ticker_rows(
            conn,
            staging_table,
            prices_table,
            ticker,
        )
        if inserted_rows == 0:
            logger.warning(
                "Keeping existing price rows for %s because staged data was empty",
                ticker,
            )
            return False
        logger.info(
            "Replaced %s existing price rows for %s with %s freshly downloaded rows",
            deleted_rows,
            ticker,
            inserted_rows,
        )
        return True
    finally:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {quote_identifier(staging_table)}")
        except sqlite3.Error:
            logger.exception("Could not remove staging table for ticker %s", ticker)


def _update_ticker(
    conn,
    prices_table: str,
    ticker: str,
    *,
    currency: str | None,
    overwrite: bool,
    savepoint_id: int,
) -> bool:
    """Update one ticker, replacing history through an isolated staging table.

    The ticker's changes stay in the caller's open transaction; a savepoint
    keeps the update atomic so a failure rolls back only this ticker's changes
    and leaves the surrounding batch intact.  The caller commits at the batch
    boundary (see ``COMMIT_BATCH_SIZE``).
    """
    savepoint = f"stock_price_update_{savepoint_id}"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        if not overwrite:
            result = stockprice_api.load_ticker_data(
                ticker,
                prices_table,
                conn,
                currency=currency,
            )
        else:
            result = _overwrite_ticker(
                conn,
                prices_table,
                ticker,
                currency=currency,
            )
        if result:
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            return True
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        return False
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        except sqlite3.Error:
            logger.exception("Could not roll back update for ticker %s", ticker)
        raise


def update_all_stock_prices(
    db_name,
    Company_Table="CompanyInfo",
    prices_table="Stock_Prices",
    context=None,
    overwrite=False,
):
    """Fetch and store prices for the canonical company universe.

    When the company table is unavailable or empty, existing non-auxiliary
    price tickers are used as a compatibility fallback. FX and inflation
    series are never sent through the equity provider chain.
    """
    conn = None
    try:
        conn = connect_write(db_name)
        stockprice_api._create_prices_table(conn, prices_table)
        conn.commit()

        company_tickers = get_tickers_from_prices(conn, table_name=Company_Table)
        price_tickers = get_tickers_from_prices(conn, table_name=prices_table)
        if company_tickers:
            tickers = company_tickers
        else:
            tickers = [ticker for ticker in price_tickers if not _is_auxiliary_ticker(ticker)]

        random.shuffle(tickers)
        logger.info("Randomized stock-price update order for %s tickers", len(tickers))
        logger.info("Found %s tickers to update stock prices for", len(tickers))

        if overwrite:
            logger.warning(
                "Stock-price overwrite enabled; replacing data for %s tickers",
                len(tickers),
            )

        failed_tickers = []
        attempted_count = 0
        aborted_early = False
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
                currency=_get_ticker_currency(conn, prices_table, ticker),
                overwrite=overwrite,
                savepoint_id=index,
            )
            attempted_count += 1
            if not updated:
                failed_tickers.append(ticker)
                # Yahoo is the final fallback for every ticker.  When it is in
                # cooldown, the remaining tickers cannot be refreshed either;
                # stop instead of repeating failing provider requests per ticker.
                if stockprice_api.primary_cooldown_remaining() > 0:
                    aborted_early = True
                    logger.warning(
                        "Last-resort price provider is cooling down; stopping "
                        "after %s of %s tickers",
                        attempted_count,
                        len(tickers),
                    )
                    break
            # Commit (and release the write lock) at the batch boundary rather
            # than after every single ticker.
            if (index + 1) % COMMIT_BATCH_SIZE == 0:
                conn.commit()
        # Commit any remaining uncommitted changes (last partial batch or an
        # early abort) so the write lock is released.
        conn.commit()
        if failed_tickers:
            logger.warning(
                "Stock-price updates failed for %s of %s attempted tickers",
                len(failed_tickers),
                attempted_count,
            )
        if context is not None and tickers:
            context.report_progress(
                attempted_count,
                len(tickers),
                "Stock price update complete",
            )
        return {
            "attempted": attempted_count,
            "updated": attempted_count - len(failed_tickers),
            "failed": len(failed_tickers),
            "failed_tickers": failed_tickers,
            "aborted_early": aborted_early,
            "skipped": len(tickers) - attempted_count,
        }
    except Exception as exc:
        logger.error("An error occurred: %s", exc, exc_info=True)
        if conn:
            # Commit the current batch so the successful tickers in it are not
            # lost when the connection is closed below.
            try:
                conn.commit()
            except sqlite3.Error:
                pass
        raise
    finally:
        if conn:
            conn.close()


def run_update_stock_prices(config, overwrite=False, context=None):
    """Handler that resolves the target database path and runs the updater."""
    logger.info("Updating stock prices...")
    kwargs: dict = {
        "Company_Table": "CompanyInfo",
        "prices_table": "Stock_Prices",
    }
    if context is not None:
        kwargs["context"] = context
    if overwrite:
        kwargs["overwrite"] = True
    return update_all_stock_prices(get_market_db(), **kwargs)


STEP_DEFINITION = StepDefinition(
    name="update_stock_prices",
    handler=run_update_stock_prices,
    supports_overwrite=True,
    input_fields=(),
)
