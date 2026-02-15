import sqlite3
import pandas as pd


def update_all_stock_prices(db_name, Company_Table = "CompanyInfo", prices_table = "Stock_Prices"):
    try:
        # Connect to the database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Fetch all tickers from the table
        cursor.execute(f"SELECT Company_Ticker FROM {Company_Table} where Company_Ticker is not null")
        tickers = cursor.fetchall()

        # Update the stock price for each ticker
        for ticker in tickers:
            load_ticker_data(ticker[0], prices_table, conn)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if conn:
            conn.close()


def load_ticker_data(ticker, prices_table, conn):
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
                return

        else:
            base_url = f"https://stooq.com/q/d/l/?s={stooq_ticker}&i=d"

        df = pd.read_csv(base_url)
        out_data = df[["Date", "Close"]]        
        out_data["Ticker"] = ticker
        out_data["Currency"] = "JPY"
        out_data["Price"] = out_data["Close"]  
        out_data = out_data[["Date", "Ticker", "Currency", "Price"]]
        out_data.to_sql(prices_table, conn, if_exists="append", index=False)

    except Exception as e:
        print(f"Failed to fetch data for ticker {ticker}: {e}")
        return None