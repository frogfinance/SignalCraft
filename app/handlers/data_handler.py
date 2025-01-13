import logging
import duckdb
from datetime import datetime, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data import StockBarsRequest
from alpaca.data import TimeFrame


class DataHandler():
    def __init__(self, tickers, api_key, api_secret, db_base_path):
        super().__init__()
        self.tickers = tickers  # List of tickers to subscribe to
        self.db_base_path = db_base_path  # Base path for database files
        self.data_store = StockHistoricalDataClient(api_key, api_secret)   


    def fetch_data(self, timeframe=TimeFrame.Minute):
        end = datetime.now()
        start = end - timedelta(days=1)

        get_data_for_tickers = self.tickers
        
        try:
            connection = duckdb.connect(f"{self.db_base_path}/{get_data_for_tickers[0]}_1min_data.db")
            # get most recent candle from the db
            most_recent_candle_data = connection.sql(
                f"SELECT * FROM ticker_data ORDER BY timestamp DESC LIMIT 1"
            )
            start = most_recent_candle_data["timestamp"].iloc[0] if not most_recent_candle_data.empty else start
            
            request = StockBarsRequest(
                symbol_or_symbols=get_data_for_tickers,
                start=start,
                end=end,
                timeframe=timeframe,
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
