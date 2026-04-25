import sqlite3
import pandas as pd
import logging
import requests
from io import StringIO

logger = logging.getLogger(__name__)

_STOOQ_DOWNLOAD_ENDPOINT = "https://stooq.com/q/d/l/"

_STOOQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0 Safari/537.36",
    "Referer": "https://stooq.com/",
}

_YAHOO_CHART_ENDPOINTS = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
    "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
)

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0 Safari/537.36",
}


def _provider_symbol_for_ticker(ticker: str) -> str:
    """Map stored ticker values to the symbol expected by Yahoo Finance."""
    clean_ticker = str(ticker).strip()
    if not clean_ticker:
        return clean_ticker
    if clean_ticker.lower().endswith(".jp"):
        return clean_ticker[:-3] + ".T"
    if "." in clean_ticker:
        return clean_ticker
    if len(clean_ticker) >= 4 and clean_ticker[:4].isdigit():
        return clean_ticker[:4] + ".T"
    return clean_ticker


def _stooq_symbol_for_ticker(ticker: str) -> str:
    """Map stored ticker values to the symbol expected by Stooq."""
    clean_ticker = str(ticker).strip().lower()
    if not clean_ticker:
        return clean_ticker
    if clean_ticker.endswith(".jp"):
        return clean_ticker
    if "." in clean_ticker:
        return clean_ticker
    if len(clean_ticker) >= 4 and clean_ticker[:4].isdigit():
        return clean_ticker[:4] + ".jp"
    return clean_ticker


def _flatten_history_column_name(column_name) -> str:
    """Flatten provider column names so they can be matched consistently."""
    if isinstance(column_name, tuple):
        parts = [str(part).strip() for part in column_name if str(part).strip()]
        return " ".join(parts)
    return str(column_name).strip()


def _column_tokens(column_name: str) -> set[str]:
    """Return lowercase tokens from a history column name."""
    return set(str(column_name).lower().replace("_", " ").split())


def _find_history_column(columns, required_tokens, excluded_tokens=None):
    """Find the first history column whose tokens match the requested set."""
    excluded_tokens = set(excluded_tokens or [])
    for column_name in columns:
        tokens = _column_tokens(column_name)
        if required_tokens.issubset(tokens) and not excluded_tokens.intersection(tokens):
            return column_name
    return None


def _fetch_stooq_history(provider_ticker: str, start_date: str | None = None) -> pd.DataFrame:
    """Fetch daily price history for a ticker from Stooq's CSV endpoint."""
    params = {
        "s": provider_ticker,
        "i": "d",
    }
    if start_date:
        params["d1"] = pd.Timestamp(start_date).strftime("%Y%m%d")
        params["d2"] = pd.Timestamp.today().strftime("%Y%m%d")

    response = requests.get(
        _STOOQ_DOWNLOAD_ENDPOINT,
        params=params,
        headers=_STOOQ_HEADERS,
        timeout=30,
    )
    response.raise_for_status()

    text = response.text.strip()
    if not text:
        return pd.DataFrame(columns=["Date", "Close"])

    lowered_text = text.lower()
    if "write to www@stooq.com" in lowered_text or "exceeded the daily hits limit" in lowered_text:
        raise RuntimeError(text)

    history = pd.read_csv(StringIO(text))
    if history.empty:
        return pd.DataFrame(columns=["Date", "Close"])
    return history


def _fetch_yahoo_history(provider_ticker: str, start_date: str | None = None) -> pd.DataFrame:
    """Fetch daily price history for a ticker from the Yahoo Finance chart API."""
    params = {
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }
    if start_date:
        start_ts = int(pd.Timestamp(start_date).timestamp())
        end_ts = int((pd.Timestamp.utcnow().normalize() + pd.Timedelta(days=1)).timestamp())
        params["period1"] = start_ts
        params["period2"] = end_ts
    else:
        params["range"] = "max"

    last_error = None
    for endpoint in _YAHOO_CHART_ENDPOINTS:
        try:
            response = requests.get(
                endpoint.format(symbol=provider_ticker),
                params=params,
                headers=_YAHOO_HEADERS,
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            chart = payload.get("chart", {})
            if chart.get("error"):
                raise RuntimeError(chart["error"])

            results = chart.get("result") or []
            if not results:
                return pd.DataFrame(columns=["Date", "Close"])

            result = results[0]
            timestamps = result.get("timestamp") or []
            quotes = result.get("indicators", {}).get("quote") or []
            if not timestamps or not quotes:
                return pd.DataFrame(columns=["Date", "Close"])

            close_values = quotes[0].get("close") or []
            row_count = min(len(timestamps), len(close_values))
            if row_count == 0:
                return pd.DataFrame(columns=["Date", "Close"])

            return pd.DataFrame(
                {
                    "Date": pd.to_datetime(timestamps[:row_count], unit="s", utc=True)
                    .tz_convert("Asia/Tokyo")
                    .tz_localize(None),
                    "Close": close_values[:row_count],
                }
            )
        except Exception as exc:
            last_error = exc

    raise RuntimeError(
        f"Failed to fetch Yahoo Finance history for {provider_ticker}: {last_error}"
    ) from last_error


def _normalise_price_history(raw_history: pd.DataFrame) -> pd.DataFrame:
    """Convert provider price history into a Date/Close frame."""
    if raw_history is None or raw_history.empty:
        return pd.DataFrame(columns=["Date", "Close"])

    history = raw_history.copy().reset_index()
    history.columns = [_flatten_history_column_name(column) for column in history.columns]

    date_column = _find_history_column(history.columns, {"date"})
    if date_column is None:
        date_column = _find_history_column(history.columns, {"datetime"})

    close_column = _find_history_column(history.columns, {"close"}, excluded_tokens={"adj"})
    if close_column is None:
        close_column = _find_history_column(history.columns, {"adj", "close"})

    if date_column is None or close_column is None:
        raise ValueError(
            "Price provider response missing expected date/close columns. "
            f"Available columns: {list(history.columns)}"
        )

    out_data = pd.DataFrame(
        {
            "Date": pd.to_datetime(history[date_column], errors="coerce").dt.strftime("%Y-%m-%d"),
            "Close": pd.to_numeric(history[close_column], errors="coerce"),
        }
    )
    out_data = out_data.dropna(subset=["Date", "Close"]).drop_duplicates(subset=["Date"], keep="last")
    return out_data.sort_values("Date").reset_index(drop=True)


def _load_provider_history(ticker: str, start_date: str | None = None) -> tuple[str, pd.DataFrame]:
    """Load normalized price history using Stooq first and Yahoo as fallback."""
    providers = [
        ("Stooq", _fetch_stooq_history, _stooq_symbol_for_ticker(ticker)),
        ("Yahoo Finance chart", _fetch_yahoo_history, _provider_symbol_for_ticker(ticker)),
    ]
    last_error = None

    for provider_name, fetcher, provider_ticker in providers:
        try:
            raw_history = fetcher(provider_ticker, start_date=start_date)
            return provider_name, _normalise_price_history(raw_history)
        except Exception as exc:
            last_error = exc
            logger.warning(
                "%s failed for ticker %s (%s): %s",
                provider_name,
                ticker,
                provider_ticker,
                exc,
            )

    raise RuntimeError(
        f"All price providers failed for ticker {ticker}: {last_error}"
    ) from last_error


def update_all_stock_prices(db_name, Company_Table, prices_table, standardized_table=None):
    """Fetch and store the latest stock prices for tickers in the company table that have financial data.

    Iterates over tickers for companies that appear in the standardized financial data table
    and calls :func:`load_ticker_data` for each one. If a standardized_table is not provided,
    all tickers are processed. If the upstream market-data provider becomes unavailable,
    the loop stops early to avoid unnecessary failed requests.

    Args:
        db_name (str): Path to the SQLite database file.
        Company_Table (str): Name of the table containing company ticker symbols.
        prices_table (str): Name of the table where stock prices are stored.
        standardized_table (str, optional): Name of the standardized financial data table.
            If provided, only companies with data in this table will have prices fetched.

    Returns:
        None
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Create the prices table if it doesn't exist
        _create_prices_table(conn, prices_table)
        conn.commit()

        # Fetch tickers based on whether we filter by standardized data
        if standardized_table:
            logger.info(f"Filtering to only companies in '{standardized_table}' table")
            # Get tickers only for companies that have financial data
            query = f"""
                SELECT DISTINCT c.Company_Ticker 
                FROM {Company_Table} c
                INNER JOIN {standardized_table} s ON c.edinetCode = s.edinetCode
                WHERE c.Company_Ticker IS NOT NULL
            """
            cursor.execute(query)
        else:
            # Get all tickers
            cursor.execute(f"SELECT Company_Ticker FROM {Company_Table} where Company_Ticker is not null")
        
        tickers = cursor.fetchall()

        logger.info(f"Found {len(tickers)} tickers to update stock prices for")

        provider_available = True

        # Update the stock price for each ticker
        for ticker in tickers:
            if not provider_available:
                logger.warning("Skipping remaining stock price updates because the price provider is unavailable.")
                break
            provider_available = load_ticker_data(ticker[0], prices_table, conn)
            

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)

    finally:
        if conn:
            conn.close()


def _create_prices_table(conn, table_name):
    """Create the stock prices table if it doesn't exist using pandas.
    
    Args:
        conn (sqlite3.Connection): Database connection
        table_name (str): Name of the table to create
    """
    # Create an empty DataFrame with the correct schema
    df = pd.DataFrame({
        'Date': pd.Series(dtype='str'),
        'Ticker': pd.Series(dtype='str'),
        'Currency': pd.Series(dtype='str'),
        'Price': pd.Series(dtype='float')
    })
    # Create table if it doesn't exist (append is idempotent for empty df)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    logger.debug(f"Stock prices table '{table_name}' is ready")


def load_ticker_data(ticker, prices_table, conn) -> bool:
    """Download and store historical price data for a single ticker.

    Fetches price data for the given ticker, starting from the last date
    already stored in ``prices_table``. Stooq is used as the primary source,
    with the Yahoo Finance chart API used only as a fallback if Stooq fails
    or returns an invalid payload. If the data is already up to date
    (within 5 days), the function returns early.

    Args:
        ticker (str): The company ticker symbol (e.g. ``'7203'``).
        prices_table (str): Name of the SQLite table where prices are stored.
        conn (sqlite3.Connection): Active database connection.

    Returns:
        bool: ``True`` if data was fetched successfully or was already
        up to date, ``False`` if the upstream provider request failed.
    """
    try:
        last_date_query = f"select max(Date) as Last_Date from {prices_table} where Ticker = '{ticker}'"
        df_last_date = pd.read_sql_query(last_date_query, conn)
        start_date = None

        if df_last_date["Last_Date"][0] is not None:
            last_date = df_last_date["Last_Date"][0]        
            today = pd.Timestamp.today().strftime("%Y-%m-%d")
            days_diff = (pd.to_datetime(today) - pd.to_datetime(last_date)).days
            if days_diff <= 5:
                logger.debug(f"Data for ticker {ticker} is already up to date.")
                return True
            start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        provider_name, out_data = _load_provider_history(ticker, start_date=start_date)
        if out_data.empty:
            logger.warning("No data found for ticker %s after querying %s.", ticker, provider_name)
            return True

        out_data["Ticker"] = ticker
        out_data["Currency"] = "JPY"
        out_data["Price"] = out_data["Close"]  
        out_data = out_data[["Date", "Ticker", "Currency", "Price"]]
        out_data.to_sql(prices_table, conn, if_exists="append", index=False)
        logger.info(
            "Successfully stored %s price records for ticker %s using %s",
            len(out_data),
            ticker,
            provider_name,
        )

        return True

    except Exception as e:
        logger.error(f"Failed to fetch data for ticker {ticker}: {e}", exc_info=True)
        return False