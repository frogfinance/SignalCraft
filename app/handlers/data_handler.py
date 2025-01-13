import logging
import duckdb
import time
import yfinance as yf
from datetime import datetime, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data import StockBarsRequest
from alpaca.data import TimeFrame


class DataHandler():
    def __init__(self, tickers, api_key, api_secret, db_base_path, timeframe=TimeFrame.Minute):
        super().__init__()
        self.tickers = tickers  # List of tickers to subscribe to
        self.db_base_path = db_base_path  # Base path for database files
        self.data_store = StockHistoricalDataClient(api_key, api_secret)   
        self.timeframe = timeframe

    def fetch_data(self, start=None, end=None, days=1, use_most_recent=False):
        end = datetime.now() if end is None else end
        start = end - timedelta(days=days) if start is None else start

        get_data_for_tickers = self.tickers
        
        try:
            connection = duckdb.connect(f"{self.db_base_path}/{get_data_for_tickers[0]}_{self.timeframe}_data.db")
            if use_most_recent:
                # get most recent candle from the db
                most_recent_candle_data = connection.sql(
                    f"SELECT * FROM ticker_data ORDER BY timestamp DESC LIMIT 1"
                )
                start = most_recent_candle_data["timestamp"].iloc[0] if not most_recent_candle_data.empty else start
            
            request = StockBarsRequest(
                symbol_or_symbols=get_data_for_tickers,
                start=start,
                end=end,
                timeframe=self.timeframe,
            )
            data = self.data_store.get_stock_bars(request)
            if data.data is None or get_data_for_tickers[0] not in data.data.keys():
                logging.info("No data received", data)
                return None
            else:    
                return data.data
        except Exception as e:
            logging.error(f"Error fetching market data: {e}")
            return None


    def fetch_vix_data(self, start=None, end=None, days=1, use_most_recent=False):
        end = datetime.now() if end is None else end
        start = end - timedelta(days=days) if start is None else start
        
        curr_date = end - timedelta(days=1)
        data = []
        while curr_date > start:
            try:
                connection = duckdb.connect(f"{self.db_base_path}/VIX_{self.timeframe}_data.db")
                if use_most_recent:
                    # get most recent candle from the db
                    most_recent_candle_data = connection.sql(
                        f"SELECT * FROM ticker_data ORDER BY timestamp DESC LIMIT 1"
                    )
                    start = most_recent_candle_data["timestamp"].iloc[0] if not most_recent_candle_data.empty else start
                
                # use yfinance to get VIX daily data
                vix_ticker = yf.Ticker("^VIX")

                # Use the history method to fetch the data
                vix_data = vix_ticker.history(start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'), interval='1d')

                # Resetting the index to have Date as a column
                vix_data.reset_index(inplace=True)

                # Selecting columns and converting column names to lowercase
                vix_data = vix_data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
                for index, row in vix_data.iterrows():
                    vwap = (row['Open'] + row['High'] + row['Low'] + row['Close']) / 4
                    row_data = dict(
                        timestamp=row['Date'],
                        ticker='VIX',
                        open=row['Open'],
                        high=row['High'],
                        low=row['Low'],
                        close=row['Close'],
                        volume=row['Volume'],
                        vwap=vwap
                    )
                    logging.info(f"VIX data: {row_data}")
                    connection.execute(
                        "INSERT OR ignore INTO ticker_data (timestamp, ticker, open, high, low, close, volume, vwap) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (row['Date'], 'VIX', row['Open'], row['High'], row['Low'], row['Close'], row['Volume'], vwap)
                    )
                    data.append(row_data)
                logging.info("VIX data saved for {} -> {}", curr_date.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
            except Exception as e:
                logging.error(f"Error fetching market data: {e}")
                return None
            
            end = curr_date
            curr_date = curr_date - timedelta(days=1)
            time.sleep(0.5)
        return data