
from app.handlers.data_handler import DataHandler
from app.handlers.execution_handler import ExecutionHandler
from app.handlers.strategy_handler import StrategyHandler
import logging
import asyncio  

logger = logging.getLogger("app")


class BacktestingSystem():

    def __init__(self, tickers, api_key, api_secret):
        self.execution_handler = ExecutionHandler(api_key, api_secret, True, is_backtest=True)    
        self.data_handler = DataHandler(tickers, api_key, api_secret, db_base_path='dbs', timeframe=self.timeframe)
        self.strategy_handler = StrategyHandler(tickers, db_base_path='dbs', timeframe=self.timeframe)
        self.trade_results = []  # Store results of backtested trades

    async def run_backtest(self, start_candle_index=1):
        logger.info("AlgoTrader BacktestingSystem fetching backtest data")
        backtest_data = self.data_handler.get_backtest_data()
        backtest_ticker_data = backtest_data[self.tickers[0]]
        # the backtest start candle timestamp is the second candle in the backtest data
        start_candle_timestamp = backtest_ticker_data['timestamp'].iloc[start_candle_index]
        total_number_candles = len(backtest_ticker_data)
        candle_index = start_candle_index
        backtest_data = {'end': start_candle_timestamp}
        # generate signals is expecting backtest_data with a key 'end' denoting the most recent timestamp
        # get all timestamps for the backtest data ordered by eldest to youngest
        logger.info("AlgoTrader BacktestingSystem begin backtest & signal generation")
        while candle_index <= total_number_candles:
            logger.debug(f"Running backtest for candle {candle_index}/{total_number_candles}")
            backtest_data['end'] = backtest_ticker_data['timestamp'].iloc[candle_index]
            signal_data = self.strategy_handler.generate_signals(is_backtest=True, backtest_data=backtest_data)
            candle_index += 1
            for signal in signal_data.values():
                if signal is None or signal.action is None:
                    continue
                outcome = self.execution_handler.run_backtest_trade(signal)
                self.trade_results.append(outcome)
            await asyncio.sleep(0)
        logger.info("Position Manager stats: {}".format(self.execution_handler.position_manager.stats()))
        logger.info("Backtest completed. Results: {}".format(self.trade_results))