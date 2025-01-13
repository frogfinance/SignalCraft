import asyncio, os
import logging
from datetime import datetime
from alpaca.data import TimeFrame

from app.handlers.data_handler import DataHandler
from app.handlers.execution_handler import ExecutionHandler
from app.handlers.strategy_handler import StrategyHandler
from app.database import save_market_data


# api-key and secret-key are the Alpaca API
USE_PAPER = os.getenv('USE_PAPER', '1') == '1'
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY_PAPER' if USE_PAPER else 'ALPACA_API_KEY')
ALPACA_API_SECRET = os.getenv('ALPACA_SECRET_KEY_PAPER' if USE_PAPER else 'ALPACA_SECRET_KEY')
timeframe = TimeFrame.Minute


tickers = []
with open('tickers.txt', 'r') as f:
    tickers = f.read().splitlines()
    tickers = [t.strip() for t in tickers if t]


async def run_algo_trader():
    """
    Main function to run the algorithmic trading strategy.
    
    The main steps are:
    1. Fetch market data periodically and save to the database.
    2. Generate signals from the strategy.
    3. Execute trades based on the signals.
    """
    
    execution_handler = ExecutionHandler(ALPACA_API_KEY, ALPACA_API_SECRET, USE_PAPER)    
    is_market_open = execution_handler.is_market_open()
    if not is_market_open:
        logging.info("Market is closed. Skipping data fetch.")
        next_open = execution_handler.get_next_market_open()
        sleep_time = (next_open - datetime.now()).total_seconds()
        await asyncio.sleep(sleep_time)

    strategy_handler = StrategyHandler(tickers)
    logging.info("Starting market data fetch...")
    data_handler = DataHandler()

    while True:
        data = data_handler.fetch_data(tickers)
        tickers = data.keys()
        
        save_market_data(data, db_base_path=data_handler.db_base_path)  # Save to database
        logging.info("Market data saved successfully.")

        # generate signals from strategy
        signal_data = strategy_handler.generate_signals()  # Generate signals from strategy

        execution_handler.handle_execution(signal_data)  # Execute trades

        asyncio.sleep(300)  # Sleep for 300 seconds before running again

