import duckdb
from alpaca.data import TimeFrame

class BaseStrategy:
    def generate_signal(self, ticker, data):
        raise NotImplementedError("generate_signal method must be implemented in child class")


def get_ticker_data(ticker, connection, timeframe=TimeFrame.Minute, db_base_path='dbs'):
    # Query the minute-level data
    query = f"SELECT * FROM ticker_data ORDER BY timestamp DESC"
    try:
        data = connection.sql(query).df()  # Convert to Pandas DataFrame
    except Exception as e:
        connection.close()
        connection = duckdb.connect(f"{db_base_path}/{ticker}_{timeframe}_data.db")
        data = connection.sql(query).df()
    return data