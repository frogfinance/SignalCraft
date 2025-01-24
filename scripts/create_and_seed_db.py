# this script is used to prepare the database
# the script is idempotent and subsequent runs should not cause any issues
# for each ticker in the tickers list, a database file is created
# the database file contains a table for minute data
# the table has columns for timestamp, ticker, open, high, low, close, and volume
# the table is created if it does not exist
# download the data for the last 5 months of minute data for each ticker
# store the data in the database

# use alpaca-py to get minute data for the last 5 months
# store the data in a duckdb database

from datetime import datetime, timedelta
import os
import time

import duckdb
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data import StockBarsRequest, TimeFrame

# api-key and secret-key are the Alpaca API
USE_PAPER = os.getenv('USE_PAPER', '1') == '1'
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY_PAPER' if USE_PAPER else 'ALPACA_API_KEY')
ALPACA_API_SECRET = os.getenv('ALPACA_SECRET_KEY_PAPER' if USE_PAPER else 'ALPACA_SECRET_KEY')
timeframe = TimeFrame.Day

# keys required for stock historical data client
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)

tickers = []
with open('tickers.txt', 'r') as f:
    tickers = f.read().splitlines()
    tickers = [t.strip() for t in tickers if t]

end = datetime.now()
if timeframe is TimeFrame.Minute:
    start = end - timedelta(days=5 * 30)
elif timeframe is TimeFrame.Day:
    start = end - timedelta(days=2 * 365)
else:
    start = end - timedelta(days=365)
get_data_for_tickers = []

for ticker in tickers:
    db_path = f"dbs/{ticker}_{timeframe}_data.db"
    if os.path.exists(db_path):
        print("Database already exists for", ticker)
    else:
        conn = duckdb.connect(db_path)
        conn.sql(f"CREATE TABLE IF NOT EXISTS ticker_data (timestamp TIMESTAMP, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume FLOAT, vwap FLOAT, PRIMARY KEY (timestamp, ticker))")
        conn.close()
        print("Database created for", ticker)
        get_data_for_tickers.append(ticker)


# create trades table
trades_db_paths = ["dbs/trades.db", "dbs/backtest_trades.db"]
for trades_db_path in trades_db_paths:
    if not os.path.exists(trades_db_path):
        conn = duckdb.connect(trades_db_path)
        conn.execute("CREATE TABLE trades (timestamp TIMESTAMP, ticker TEXT, action TEXT, qty INT, price FLOAT, order_id TEXT, strategy TEXT, reason TEXT)")
        conn.close()


def save_to_db(ticker, data):
    db_path = f"dbs/{ticker}_{timeframe}_data.db"
    conn = duckdb.connect(db_path)
    conn.execute(f"INSERT OR IGNORE INTO ticker_data VALUES {data}")
    conn.close()


curr_date = end - timedelta(days=1)
while curr_date > start:
    print("Downloading data for {}\nFrom {}-{}".format(get_data_for_tickers, curr_date, end))
    request = StockBarsRequest(
        symbol_or_symbols=get_data_for_tickers,
        start=curr_date,
        end=end,
        timeframe=timeframe,
    )
    print("Request built -> sending request")
    data = client.get_stock_bars(request)

    # convert the data to duckdb expected format
    # print("received data", data.data)
    if data.data is None or get_data_for_tickers[0] not in data.data.keys():
        print("No data received", data)
    else:    
        for ticker in get_data_for_tickers:
            ticker_data = data.data.get(ticker, [])
            for row in ticker_data:
                row_str = f"('{row.timestamp}', '{ticker}', {row.open}, {row.high}, {row.low}, {row.close}, {row.volume}, {row.vwap})"
                save_to_db(ticker, row_str)
            
            print('Data saved for', ticker)
    end = curr_date
    curr_date = curr_date - timedelta(days=1)
    time.sleep(1)

print("Data saved to database")