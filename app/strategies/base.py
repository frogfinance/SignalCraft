import duckdb
from alpaca.data import TimeFrame
from datetime import datetime

class BaseStrategy:
    def generate_signal(self, ticker, data):
        raise NotImplementedError("generate_signal method must be implemented in child class")

    def fetch_vxx_data(self, end: datetime=None):
        if end:
            query = f"""
            SELECT timestamp, close as vxx
            FROM ticker_data
            WHERE symbol = 'VXX'
            AND timestamp < {end.timestamp()}
            ORDER BY timestamp DESC
            """
        else:
            query = """
            SELECT timestamp, close as vxx
            FROM ticker_data
            ORDER BY timestamp DESC
            """
        conn = duckdb.connect(f"{self.db_base_path}/VXX_1Min_data.db")
        df = conn.sql(query).fetchdf()
        conn.close()

        return df


def get_ticker_data(ticker, connection, timeframe=TimeFrame.Minute, db_base_path='dbs'):
    # Query the minute-level data
    query = f"SELECT * FROM ticker_data ORDER BY timestamp ASC"
    try:
        data = connection.sql(query).df()  # Convert to Pandas DataFrame
    except Exception as e:
        connection.close()
        connection = duckdb.connect(f"{db_base_path}/{ticker}_{timeframe}_data.db")
        data = connection.sql(query).df()
    return data


def get_ticker_data_by_timeframe(ticker, connection, timeframe=TimeFrame.Minute, db_base_path='dbs', end: datetime=None):
    data = None
    # Query the minute-level data
    query = f"SELECT * FROM ticker_data where timestamp < {end.timestamp()} ORDER BY timestamp ASC"
    try:
        data = connection.sql(query).df()  # Convert to Pandas DataFrame
    except Exception as e:
        connection.close()
        connection = duckdb.connect(f"{db_base_path}/{ticker}_{timeframe}_data.db")
        data = connection.sql(query).df()
    return data