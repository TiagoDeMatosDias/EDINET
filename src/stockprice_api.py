import sqlite3
import pandas as pd


def update_all_stock_prices(db_name, Company_Table, prices_table):
    """Fetch and store the latest stock prices for all tickers in the company table.

    Iterates over every non-null ticker in ``Company_Table`` and calls
    :func:`load_ticker_data` for each one.  If the Stooq daily request limit
    is reached, the loop stops early to avoid unnecessary failed requests.

    Args:
        db_name (str): Path to the SQLite database file.
        Company_Table (str): Name of the table containing company ticker symbols.
        prices_table (str): Name of the table where stock prices are stored.

    Returns:
        None
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Fetch all tickers from the table
        cursor.execute(f"SELECT Company_Ticker FROM {Company_Table} where Company_Ticker is not null")
        tickers = cursor.fetchall()

        checkstooq = True

        # Update the stock price for each ticker
        for ticker in tickers:
            if checkstooq:
                checkstooq = load_ticker_data(ticker[0], prices_table, conn)
            

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if conn:
            conn.close()


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
                print(f"Data for ticker {ticker} is already up to date.")
                return True

        else:
            base_url = f"https://stooq.com/q/d/l/?s={stooq_ticker}&i=d"

        df = pd.read_csv(base_url)
        if df.empty and df.keys()[0]=="Exceeded the daily hits limit":
            print(f"No data found for ticker {ticker}.")
            print("Exceeded the daily hits limit for Stooq. Please try again later.")
            
            return False



        out_data = df[["Date", "Close"]]        
        out_data["Ticker"] = ticker
        out_data["Currency"] = "JPY"
        out_data["Price"] = out_data["Close"]  
        out_data = out_data[["Date", "Ticker", "Currency", "Price"]]
        out_data.to_sql(prices_table, conn, if_exists="append", index=False)

        return True

    except Exception as e:
        print(f"Failed to fetch data for ticker {ticker}: {e}")
        return True