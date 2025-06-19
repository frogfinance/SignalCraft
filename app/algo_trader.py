import asyncio, os
import logging
from datetime import datetime
from alpaca.data import TimeFrame
import dotenv

from app.backtester import BacktestingSystem
from app.handlers.data_handler import DataHandler
from app.handlers.execution_handler import ExecutionHandler
from app.handlers.strategy_handler import StrategyHandler
import pytz

dotenv.load_dotenv()

logger = logging.getLogger("app")

# api-key and secret-key are the Alpaca API
USE_PAPER = os.getenv('USE_PAPER', '1') == '1'
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY_PAPER' if USE_PAPER else 'ALPACA_API_KEY')
ALPACA_API_SECRET = os.getenv('ALPACA_SECRET_KEY_PAPER' if USE_PAPER else 'ALPACA_SECRET_KEY')
BACKTEST = os.getenv('BACKTEST', '0') == '1'
logger.info("env data: BACKTEST={}".format(os.getenv('BACKTEST')))

local_tz = pytz.timezone('America/New_York')

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
    def __init__(self, timeframe=TimeFrame.Minute):
        self.strategy_handler = None
        self.data_handler = None
        self.execution_handler = None
        self.timeframe = timeframe
        self.backtest_mode = BACKTEST
        self.trade_results = []  # Store results of backtested trades
        self.backtest_name = ''
        self.backtest_system = None

    async def run(self):
        logger.info("AlgoTrader loop is starting ->")
        if self.backtest_mode:
            logger.info("Running loop in backtest mode...")
            backtest_system = BacktestingSystem(tickers, ALPACA_API_KEY, ALPACA_API_SECRET)
            self.data_handler = backtest_system.data_handler
            self.execution_handler = backtest_system.execution_handler
            self.strategy_handler = backtest_system.strategy_handler
            logger.info("AlgoTrader starting backtest mode ->")
            self.backtest_system = backtest_system
            # self.data_handler.fetch_data()
        else:
            logger.info("Startup loop in live trading mode...")
            self.execution_handler = ExecutionHandler(ALPACA_API_KEY, ALPACA_API_SECRET, db_base_path='dbs', use_paper=USE_PAPER)    
            logger.info("AlgoTrader ExecutionHandler started ->")
            self.data_handler = DataHandler(tickers, ALPACA_API_KEY, ALPACA_API_SECRET, db_base_path='dbs', timeframe=self.timeframe)
            logger.info("AlgoTrader DataHandler started ->")
            self.strategy_handler = StrategyHandler(tickers, db_base_path='dbs', timeframe=self.timeframe)
            logger.info("AlgoTrader StrategyHandler started ->")
            logger.info("AlgoTrader starting live trading mode ->")
            await self.run_algo_trader()


    async def run_algo_trader(self):
        """
        Main function to run the algorithmic trading strategy.
        
        The main steps are:
        1. Fetch market data periodically and save to the database.
        2. Generate signals from the strategy.
        3. Execute trades based on the signals.
        """
        logger.info("Starting live trading mode...")

        while True:
            is_market_open = self.execution_handler.is_market_open()
            
            if not is_market_open:
                logger.info("Market is closed. Fetch any missing data. Skipping signals.")
                next_open = self.execution_handler.get_next_market_open()
                sleep_time = (next_open - datetime.now(tz=local_tz)).total_seconds()
                self.data_handler.fetch_data(use_most_recent=True)
                logger.debug("Market data saved successfully.")
                
                logger.info("Sleeping until market open...")
                await asyncio.sleep(sleep_time)
            else:
                # sync with alpaca account
                self.execution_handler.sync_alpaca_data()
                logger.info("Alpaca Account connected successfully.")

            logger.info("Running trader & fetching market data...")
            if not self.data_handler.is_stream_subscribed:
                await self.data_handler.subscribe_to_data_stream()

            # get most recent price data for position checks
            ticker_to_price_map = self.data_handler.fetch_most_recent_prices()

            # generate signals from strategy
            signal_data = self.strategy_handler.generate_signals()  # Generate signals from strategy

            self.execution_handler.handle_execution(signal_data)  # Execute trades

            # check position at the 1hr interval of the market open using last candle from ticker_to_price_map
            if ticker_to_price_map and tickers:  # Make sure both exist and are not empty
                try:
                    first_ticker = tickers[0]
                    if (first_ticker in ticker_to_price_map and 
                        isinstance(ticker_to_price_map[first_ticker], dict) and
                        'timestamp' in ticker_to_price_map[first_ticker] and
                        hasattr(ticker_to_price_map[first_ticker]['timestamp'], 'minute') and
                        ticker_to_price_map[first_ticker]['timestamp'].minute == 30):
                        
                        logger.info(f"Running position check at 30-minute mark")
                        self.execution_handler.position_manager.check_positions(ticker_to_price_map)
                except Exception as e:
                    logger.error(f"Error checking positions: {e}")
            
            logger.info("Sleeping for 60 seconds...")
            await asyncio.sleep(60)  # Sleep for 300 seconds before running again
