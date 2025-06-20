import duckdb
import logging
import numpy as np
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


def get_ticker_data_by_timeframe(ticker, connection, timeframe, db_base_path="dbs", end=None):
    """Get ticker data for a specific timeframe."""
    try:
        # First, check if the table exists
        table_check = connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ticker_data'").fetchall()
        
        if not table_check:
            # Check if there's a table named after the ticker
            alternate_table_check = connection.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{ticker}_data'").fetchall()
            
            if alternate_table_check:
                # The table is named after the ticker
                table_name = f"{ticker}_data"
            else:
                # As a last resort, check what tables are in the database
                all_tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                logger.error(f"Available tables in database: {all_tables}")
                
                # Try one more option
                if len(all_tables) > 0:
                    table_name = all_tables[0][0]
                else:
                    raise ValueError(f"No tables found in database for {ticker} with timeframe {timeframe}")
        else:
            table_name = "ticker_data"
        
        # Now build and execute the query with the correct table name
        query = f"""
        SELECT * FROM {table_name} 
        WHERE ticker = '{ticker}'
        """
        
        if end is not None:
            # Convert end to a string if it's a datetime
            if isinstance(end, datetime):
                end = end.isoformat()
            query += f" AND timestamp <= '{end}'"
            
        query += " ORDER BY timestamp DESC LIMIT 500"
        
        logger.debug(f"Executing query: {query}")
        data = connection.execute(query).fetchdf()
        
        if data.empty:
            logger.warning(f"No data found for {ticker} with timeframe {timeframe}")
            return pd.DataFrame()
            
        # Convert timestamp to datetime if it's not already
        if 'timestamp' in data.columns and not pd.api.types.is_datetime64_any_dtype(data['timestamp']):
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            
        return data
        
    except Exception as e:
        logger.error(f"Error getting data for {ticker} with timeframe {timeframe}: {e}")
        return pd.DataFrame()