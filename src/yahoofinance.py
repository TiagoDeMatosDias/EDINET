import yfinance as yf
import sqlite3

def update_stock_price(ticker, db_name, table_name = "edinet_codes", column_name = "Yahoo_price"):
    if ticker is None:
        print("Ticker is None, skipping update.")
        return

    try:
        # Connect to the database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Fetch stock data from Yahoo Finance
        stock = yf.Ticker(ticker)
        try:
            stock_history = stock.history(period="1d")
        except Exception as e:
            print(f"Failed to fetch data for ticker {ticker}: {e}")
            return

        if stock_history.empty or 'Close' not in stock_history.columns:
            print(f"Invalid data returned for ticker {ticker}, skipping update.")
            return

        stock_price = stock_history['Close'].iloc[-1]

        # Update the stock price in the specified table and column
        cursor.execute(f"UPDATE {table_name} SET {column_name} = ? WHERE Yahoo_Ticker = ?", (stock_price, ticker))
        conn.commit()

        print(f"Updated {ticker} stock price to {stock_price} in {table_name}.{column_name}")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        if conn:
            conn.close()

def update_all_stock_prices(db_name, table_name = "edinet_codes", column_name = "Yahoo_price", only_update_empty = False):
    try:
        # Connect to the database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Fetch all tickers from the table
        if only_update_empty:
            cursor.execute(f"SELECT Yahoo_Ticker FROM {table_name} WHERE {column_name} IS NULL")
        else:
            cursor.execute(f"SELECT Yahoo_Ticker FROM {table_name}")
        tickers = cursor.fetchall()

        # Update the stock price for each ticker
        for ticker in tickers:
            update_stock_price(ticker[0], db_name, table_name, column_name)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if conn:
            conn.close()

# Example usage
# update_stock_price('AAPL', 'finance.db', 'stocks', 'current_price')