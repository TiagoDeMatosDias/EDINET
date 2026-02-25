import sqlite3
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def update_all_stock_prices(db_name, Company_Table, prices_table, standardized_table=None):
    """Fetch and store the latest stock prices for tickers in the company table that have financial data.

    Iterates over tickers for companies that appear in the standardized financial data table
    and calls :func:`load_ticker_data` for each one. If a standardized_table is not provided,
    all tickers are processed. If the Stooq daily request limit is reached, the loop stops
    early to avoid unnecessary failed requests.

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

        checkstooq = True

        # Update the stock price for each ticker
        for ticker in tickers:
            if checkstooq:
                checkstooq = load_ticker_data(ticker[0], prices_table, conn)
            

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
    """Download and store historical price data for a single ticker from Stooq.

    Fetches price data from ``https://stooq.com`` for the given ticker,
    starting from the last date already stored in ``prices_table``.  If the
    data is already up to date (within 5 days), the function returns early.

    Args:
        ticker (str): The company ticker symbol (e.g. ``'7203'``).
        prices_table (str): Name of the SQLite table where prices are stored.
        conn (sqlite3.Connection): Active database connection.

    Returns:
        bool: ``True`` if data was fetched successfully or was already
        up to date, ``False`` if the Stooq daily request limit was exceeded.
    """
    try:
        baseline_ticker = ticker[:4]
        stooq_ticker = baseline_ticker + ".jp"

        # Fetch stock data from Yahoo Finance
        last_date_query = f"select max(Date) as Last_Date from {prices_table} where Ticker = '{ticker}'"
        df_last_date = pd.read_sql_query(last_date_query, conn)

        if df_last_date["Last_Date"][0] is not None:
            last_date = df_last_date["Last_Date"][0]        
            today = pd.Timestamp.today().strftime("%Y-%m-%d")
            base_url = f"https://stooq.com/q/d/l/?s={stooq_ticker}&f={last_date}&t={today}&i=d"
            days_diff = (pd.to_datetime(today) - pd.to_datetime(last_date)).days
            if days_diff <= 5:
                logger.debug(f"Data for ticker {ticker} is already up to date.")
                return True

        else:
            base_url = f"https://stooq.com/q/d/l/?s={stooq_ticker}&i=d"

        logger.info(f"Fetching stock data for ticker {ticker}...")
        df = pd.read_csv(base_url)
        if df.empty and df.keys()[0]=="Exceeded the daily hits limit":
            logger.warning(f"No data found for ticker {ticker}.")
            logger.warning("Exceeded the daily hits limit for Stooq. Please try again later.")
            
            return False



        out_data = df[["Date", "Close"]]        
        out_data["Ticker"] = ticker
        out_data["Currency"] = "JPY"
        out_data["Price"] = out_data["Close"]  
        out_data = out_data[["Date", "Ticker", "Currency", "Price"]]
        out_data.to_sql(prices_table, conn, if_exists="append", index=False)
        logger.info(f"Successfully stored {len(out_data)} price records for ticker {ticker}")

        return True

    except Exception as e:
        logger.error(f"Failed to fetch data for ticker {ticker}: {e}", exc_info=True)
        return True