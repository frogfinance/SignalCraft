import duckdb, logging
from alpaca.data import TimeFrame


def save_market_data(data: dict, timeframe=TimeFrame.Minute, db_base_path="dbs", ):
    for ticker in data.keys():
        ticker_data = data.get(ticker, [])
        for row in ticker_data:
            row_str = f"('{row.timestamp}', '{ticker}', {row.open}, {row.high}, {row.low}, {row.close}, {row.volume}, {row.vwap})"
            save_to_db(ticker, row_str, db_base_path=db_base_path, timeframe=timeframe)
        
        logging.info('Data saved for ticker', ticker)


def save_to_db(ticker, data, db_base_path="dbs", timeframe=TimeFrame.Minute):
    db_path = f"{db_base_path}/{ticker}_{timeframe}_data.db"
    conn = duckdb.connect(db_path)
    conn.execute(f"INSERT OR IGNORE INTO ticker_data VALUES {data}")
    conn.close()
