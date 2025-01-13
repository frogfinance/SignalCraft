import duckdb
from datetime import datetime, timedelta

def get_next_market_candle_datetime(today=None):
    """Get the next market open time."""
    today = datetime.now() if today is None else today
    if today.weekday() == 5:
        today += timedelta(days=2)
    elif today.weekday() == 6:
        today += timedelta(days=1)
    
    if today.hour < 9 or today.hour == 9 and today.minute < 30:
        return datetime(today.year, today.month, today.day, 9, 30)
    else:
        return today


def get_most_recent_timestamp(ticker, db_base_path="tests/data"):
    """Get the most recent timestamp for the given ticker."""
    conn = duckdb.connect(f"{db_base_path}/{ticker}_1min_data.db")
    timestamp = conn.execute(f"SELECT timestamp FROM ticker_data ORDER BY timestamp DESC LIMIT 1")
    conn.close()
    return timestamp