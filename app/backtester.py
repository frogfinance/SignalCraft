
import json
from app.handlers.data_handler import DataHandler
from app.handlers.execution_handler import ExecutionHandler
from app.handlers.strategy_handler import StrategyHandler
import logging
import asyncio  

from alpaca.data import TimeFrame

from app.models.websocket_manager import WebSocketManager

logger = logging.getLogger("app")


class BacktestingSystem():

    def __init__(self, tickers, api_key, api_secret, timeframe=TimeFrame.Minute):
        self.timeframe = timeframe
        self.execution_handler = ExecutionHandler(api_key, api_secret, True, is_backtest=True)    
        self.data_handler = DataHandler(tickers, api_key, api_secret, db_base_path='dbs', timeframe=timeframe, is_backtest=True)
        self.strategy_handler = StrategyHandler(tickers, db_base_path='dbs', timeframe=self.timeframe)
        self.trade_results = []  # Store results of backtested trades
        self.tickers = tickers
        self.registered_websockets = []

        # Initialize WebSocket Manager
        self.ws_manager = WebSocketManager()
        self.task = None

    def is_market_open(self, timestamp):
        if timestamp.weekday() >= 5:
            return False
        if timestamp.hour < 9 or timestamp.hour >= 16:
            return False
        if timestamp.hour == 9 and timestamp.minute < 30:
            return False
        return True
    
    def register_websocket(self, ws):
        # register a websocket to the backtesting system to feed information to the front end
        self.registered_websockets.append(ws)

    async def run_backtest(self, start_candle_index=3090):
        logger.info("AlgoTrader BacktestingSystem fetching backtest data")
        self.data_handler.fetch_data(use_most_recent=True)
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
            if self.task and self.task.cancelled():
                return  # Stop if the task is cancelled
            logger.debug("Running backtest for candle %r / %r", candle_index, total_number_candles)
            backtest_data['end'] = backtest_ticker_data['timestamp'].iloc[candle_index]
            candle_index += 1
            # check if candle data is within market open hours
            if not self.is_market_open(backtest_data['end']):
                continue
            signal_data = self.strategy_handler.generate_signals(is_backtest=True, backtest_data=backtest_data)
            for signal in signal_data.values():
                outcome = self.execution_handler.run_backtest_trade(signal)
                if outcome is not None:
                    self.trade_results.append(outcome)
                    logger.info("Trade outcome: %r", outcome)
                    backtest_data = dict(
                        trade={
                            "timestamp": outcome['timestamp'],
                            "ticker": outcome['ticker'],
                            "price": outcome['price'],
                            "side": outcome['side'],
                            "qty": outcome['qty']
                        },
                        balance=self.execution_handler.position_manager.cash_balance,
                        positions=list(self.execution_handler.position_manager.positions.values()),
                        ticker_data=None
                    )
                    await self.ws_manager.send_message(json.dumps(backtest_data))
            if candle_index % 5 == 0:
                ticker_to_price_map = self.data_handler.fetch_most_recent_prices()
                self.execution_handler.update_backtest_positions(backtest_data['end'], ticker_to_price_map=ticker_to_price_map)
                backtest_data = dict(
                        trade=None,
                        balance=self.execution_handler.position_manager.cash_balance,
                        positions=list(self.execution_handler.position_manager.positions.values()),
                        stats=self.execution_handler.position_manager.stats(),
                        ticker_data=ticker_to_price_map
                    )
                await self.ws_manager.send_message(json.dumps(backtest_data))
            await asyncio.sleep(0)
        logger.info("Position Manager stats: %r", self.execution_handler.position_manager.stats())
        logger.info("Backtest completed. Results: %r", self.trade_results)

    def start_backtest(self):
        """Starts the backtest task in the background."""
        if self.task and not self.task.done():
            self.task.cancel()  # Cancel any existing task before starting a new one
        self.task = asyncio.create_task(self.run_backtest())

    def stop_backtest(self):
        """Stops the running backtest."""
        if self.task and not self.task.done():
            self.task.cancel()
            self.task = None