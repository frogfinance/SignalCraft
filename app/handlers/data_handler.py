import logging
import duckdb
from datetime import datetime, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data import StockBarsRequest
from alpaca.data import TimeFrame

logger = logging.getLogger("app")


class DataHandler():
    def __init__(self, tickers, api_key, api_secret, db_base_path, timeframe=TimeFrame.Minute):
        super().__init__()
        self.tickers = tickers  # List of tickers to subscribe to
        self.db_base_path = db_base_path  # Base path for database files
        self.data_store = StockHistoricalDataClient(api_key, api_secret)   
        self.timeframe = timeframe

    def fetch_data(self, start=None, end=None, days=1, use_most_recent=False):
        """
        Fetch candle data for the specified tickers and timeframe.
        start is the start date for the data fetch. If None, it defaults to the current date minus `days`.
        end is the end date for the data fetch. If None, it defaults to the current date.
        days is the number of days to fetch data for. If None, it defaults to 1. start and end if specified will override this.
        use_most_recent is a flag to set start as the most recent candle data timestamp.
        Iterates by the number of days specified in the `days` parameter.
        """
        end = datetime.now() if end is None else end
        start = end - timedelta(days=days) if start is None else start
        
        # set start value to the most recent candle timestamp
        if use_most_recent:
            # find the most recent candle timestart as `start``
            oldest_candle = None
            for ticker in self.tickers:
                
                connection = duckdb.connect(f"{self.db_base_path}/{ticker}_{self.timeframe}_data.db")
                # get most recent candle from the db
                most_recent_candle_data = connection.sql(
                    f"SELECT * FROM ticker_data ORDER BY timestamp DESC LIMIT 1"
                ).df()
                connection.close()
                last_candle = most_recent_candle_data["timestamp"].iloc[0] if not most_recent_candle_data.empty else None
                if oldest_candle and last_candle and last_candle < oldest_candle:
                    oldest_candle = last_candle
                elif not oldest_candle:
                    oldest_candle = last_candle
            start = oldest_candle if oldest_candle else start
        

        try:
            curr_start = start
            curr_end = start + timedelta(days=1)
            while curr_start <= end:
                for ticker in self.tickers:
                    logger.info(f"Fetching data for {ticker} from {curr_start} to {curr_end}")
                    data = None
                    request = StockBarsRequest(
                        symbol_or_symbols=[ticker],
                        start=curr_start,
                        end=curr_end,
                        timeframe=self.timeframe,
                    )
                    try:
                        data = self.data_store.get_stock_bars(request)
                    except Exception as e:
                        logger.error(f"Error fetching market data for ticker:{ticker} error; {e}")
                    
                    if data and data.data is None:
                        logger.info("No data received", data)
                    else:    
                        logger.info(f"Data received for {ticker} from {curr_start} to {curr_end}")
                        self.save_market_data(data.data)
                    logger.info(f"Data saved for {ticker}")
                curr_start = curr_end
                curr_end = curr_start + timedelta(days=1)
        except Exception as e:
            logger.error(f"Error fetching market data: {e}")
            return None
    
    def get_backtest_data(self):
        data = dict()
        for ticker in self.tickers:
            conn = duckdb.connect(f"{self.db_base_path}/{ticker}_{self.timeframe}_data.db")
            ticker_data = conn.sql(f"SELECT * FROM ticker_data ORDER BY timestamp ASC").df()
            conn.close()
            data[ticker] = ticker_data
        return data

    def save_market_data(self, data: dict):
        for ticker in data.keys():
            ticker_data = data.get(ticker, [])
            for row in ticker_data:
                value_str = f"('{row.timestamp}', '{ticker}', {row.open}, {row.high}, {row.low}, {row.close}, {row.volume}, {row.vwap})"
                logger.info(f"candle values for {ticker}: {value_str}")
                self.save_to_db(ticker, value_str)
            
            logger.info('Data saved for ticker {}'.format(ticker))

    def save_to_db(self, ticker, value_str):
        db_path = f"{self.db_base_path}/{ticker}_{self.timeframe}_data.db"
        conn = duckdb.connect(db_path)
        conn.execute(f"INSERT OR IGNORE INTO ticker_data VALUES {value_str}")
        conn.close()