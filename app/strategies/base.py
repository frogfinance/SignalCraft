import duckdb
import logging
from alpaca.data import TimeFrame
from datetime import datetime

logger = logging.getLogger("app")

class BaseStrategy:
    def generate_signal(self, ticker, data):
        raise NotImplementedError("generate_signal method must be implemented in child class")

    def fetch_vxx_data(self, end: datetime=None):
        if end:
            end_timestamp = end.strftime('%Y-%m-%d %H:%M:%S')
            query = f"""
            SELECT timestamp, close as vxx
            FROM ticker_data
            WHERE timestamp < TIMESTAMP '{end_timestamp}'
            ORDER BY timestamp ASC
            """
        else:
            query = """
            SELECT timestamp, close as vxx
            FROM ticker_data
            ORDER BY timestamp ASC
            """
        conn = duckdb.connect(f"{self.db_base_path}/VXX_1Min_data.db")
        df = conn.sql(query).fetchdf()
        conn.close()

        return df


def get_ticker_data(ticker, connection, timeframe=TimeFrame.Minute, db_base_path='dbs'):
    # Query the minute-level data
    connection_str = f"{db_base_path}/{ticker}_{timeframe}_data.db"
    query = f"SELECT * FROM ticker_data ORDER BY timestamp ASC"
    try:
        logger.debug("Connecting to database: %r", connection_str)
        data = connection.sql(query).df()  # Convert to Pandas DataFrame
    except Exception as e:
        connection.close()
        connection_str = f"{db_base_path}/{ticker}_{timeframe}_data.db"
        logger.debug("Reconnecting to database: %r", connection_str)
        try:
            connection = duckdb.connect(connection_str)
            data = connection.sql(query).df()
        except Exception as e:
            logger.error("Error fetching duckdb database. This is usually due to a missing data file for a ticker. Run create_and_seed_db.py to build new ticker databases. data=%r", connection_str, exc_info=e)
            raise e
    return data


def get_ticker_data_by_timeframe(ticker, connection, timeframe=TimeFrame.Minute, db_base_path='dbs', end: datetime = None):
    if end is None:
        raise ValueError("The 'end' parameter cannot be None.")
    
    # Format the `end` timestamp as a string that DuckDB can interpret
    end_timestamp_str = end.strftime('%Y-%m-%d %H:%M:%S')

    query = f"""
        SELECT timestamp, open, high, low, close, volume, vwap 
        FROM ticker_data 
        WHERE timestamp < TIMESTAMP '{end_timestamp_str}' 
        ORDER BY timestamp ASC
    """
    
    try:
        data = connection.sql(query).df()  # Convert to Pandas DataFrame
    except Exception as e:
        connection.close()
        logger.error("Error fetching data", exc_info=e)
        connection = duckdb.connect(f"{db_base_path}/{ticker}_{timeframe}_data.db")
        data = connection.sql(query).df()
    
    return data