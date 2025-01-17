import asyncio, os
import logging
from datetime import datetime
from alpaca.data import TimeFrame

from app.handlers.data_handler import DataHandler
from app.handlers.execution_handler import ExecutionHandler
from app.handlers.strategy_handler import StrategyHandler


# api-key and secret-key are the Alpaca API
USE_PAPER = os.getenv('USE_PAPER', '1') == '1'
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY_PAPER' if USE_PAPER else 'ALPACA_API_KEY')
ALPACA_API_SECRET = os.getenv('ALPACA_SECRET_KEY_PAPER' if USE_PAPER else 'ALPACA_SECRET_KEY')

tickers = []
with open('tickers.txt', 'r') as f:
    tickers = f.read().splitlines()
    tickers = [t.strip() for t in tickers if t]


class TradingSystem:
    """
    This class represents the main trading system that runs the algorithmic trading strategy.
    The timeframe is set to minute by default, and the backtest_mode flag is set to False by default.
    1-Min timeframes are used to fetch market data and generate signals.
    The Strategy Handlers starts off all registered strategies in the class.
    Each strategy has a timeframe that it operates on.
    """
    def __init__(self, timeframe=TimeFrame.Minute, backtest_mode=False):
        self.strategy_handler = None
        self.data_handler = None
        self.execution_handler = None
        self.timeframe = timeframe
        self.backtest_mode = backtest_mode
        self.trade_results = []  # Store results of backtested trades
        self.backtest_name = ''

    async def run(self):
        if self.backtest_mode:
            await self.run_backtest()
        else:
            await self.run_algo_trader()

    async def run_backtest(self):
        logging.info("Starting backtest mode...")
        historical_data = self.data_handler.get_historical_data()

        for ticker, data in historical_data.items():
            for i in range(len(data) - 1):
                current_data = data.iloc[:i+1]
                signal = self.strategy_handler.generate_signals(historical_data={ticker: current_data})

                if signal['action'] in ['buy', 'sell']:
                    # self.execution_handler.handle_execution(signal, backtest=True)
                    outcome = self.executions_handler.execute_backtest_trade(signal, current_data)
                    self.trade_results.append(outcome)

    async def run_algo_trader(self):
        """
        Main function to run the algorithmic trading strategy.
        
        The main steps are:
        1. Fetch market data periodically and save to the database.
        2. Generate signals from the strategy.
        3. Execute trades based on the signals.
        """
        logging.info("Starting live trading mode...")
        self.execution_handler = ExecutionHandler(ALPACA_API_KEY, ALPACA_API_SECRET, USE_PAPER)    
        self.data_handler = DataHandler()

        is_market_open = self.execution_handler.is_market_open()
        
        if not is_market_open:
            logging.info("Market is closed. Fetch any missing data. Skipping signals.")
            next_open = self.execution_handler.get_next_market_open()
            sleep_time = (next_open - datetime.now()).total_seconds()
            data = self.data_handler.fetch_data(use_most_recent=True)
            self.data_handler.save_market_data(data)  # Save to database
            await asyncio.sleep(sleep_time)

        self.strategy_handler = StrategyHandler(tickers)

        while True:
            logging.info("Running trader & fetching market data...")
            data = self.data_handler.fetch_data(use_most_recent=True)

            self.data_handler.save_market_data(data)  # Save to database
            logging.info("Market data saved successfully.")

            # generate signals from strategy
            signal_data = self.strategy_handler.generate_signals()  # Generate signals from strategy

            self.execution_handler.handle_execution(signal_data)  # Execute trades

            asyncio.sleep(300)  # Sleep for 300 seconds before running again
