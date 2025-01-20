import logging
import duckdb
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
            if use_most_recent:
                # find the most recent candle timestart as `start``
                connection = duckdb.connect(f"{self.db_base_path}/{get_data_for_tickers[0]}_{self.timeframe}_data.db")
                # get most recent candle from the db
                most_recent_candle_data = connection.sql(
                    f"SELECT * FROM ticker_data ORDER BY timestamp DESC LIMIT 1"
                )
                connection.close()
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
    
    def get_historical_data(self):
        data = dict()
        for ticker in self.tickers:
            conn = duckdb.connect(f"{self.db_base_path}/{ticker}_{self.timeframe}_data.db")
            ticker_data = conn.sql(f"SELECT * FROM ticker_data order by timestamp ASC").df()
            conn.close()
            data[ticker] = ticker_data
        return data

    def save_market_data(self, data: dict):
        for ticker in data.keys():
            ticker_data = data.get(ticker, [])
            for row in ticker_data:
                row_str = f"('{row.timestamp}', '{ticker}', {row.open}, {row.high}, {row.low}, {row.close}, {row.volume}, {row.vwap})"
                self.save_to_db(ticker, row_str, db_base_path=self.db_base_path, timeframe=self.timeframe)
            
            logging.info('Data saved for ticker', ticker)

    def save_to_db(self, ticker, data):
        db_path = f"{self.db_base_path}/{ticker}_{self.timeframe}_data.db"
        conn = duckdb.connect(db_path)
        conn.execute(f"INSERT OR IGNORE INTO ticker_data VALUES {data}")
        conn.close()