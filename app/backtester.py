from app.handlers.data_handler import DataHandler
from app.handlers.execution_handler import ExecutionHandler
from app.handlers.strategy_handler import StrategyHandler
import logging
import pandas as pd
import asyncio  

from alpaca.data import TimeFrame

from app.models.websocket_manager import WebSocketManager

logger = logging.getLogger("app")


class BacktestingSystem():

    def __init__(self, tickers, api_key, api_secret, timeframe=TimeFrame.Minute):
        self.timeframe = timeframe
        self.execution_handler = ExecutionHandler(api_key, api_secret, use_paper=True, is_backtest=True)    
        self.data_handler = DataHandler(tickers, api_key, api_secret, db_base_path='dbs', timeframe=timeframe, is_backtest=True)
        self.strategy_handler = StrategyHandler(tickers, db_base_path='dbs', timeframe=self.timeframe)
        self.trade_results = []  # Store results of backtested trades
        self.tickers = tickers
        self.registered_websockets = []
        self.running_backtests = {}

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
    
    @property
    def is_running(self):
        return self.running_backtests
    
    def register_websocket(self, ws):
        # register a websocket to the backtesting system to feed information to the front end
        self.registered_websockets.append(ws)


    def serialize_ticker_data(self, ticker_data):
        """
        Aggregates minute-level ticker data into daily candlesticks.
        """
        if ticker_data.empty:
            return []

        # Copy DataFrame to avoid modifying the original slice
        ticker_data = ticker_data.copy()

        # Ensure timestamp is a datetime object
        ticker_data.loc[:, "timestamp"] = pd.to_datetime(ticker_data["timestamp"])

        # Group data by date
        daily_candles = ticker_data.groupby(ticker_data["timestamp"].dt.date).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last"
        }).reset_index()

        # Convert date back to timestamp format for JSON serialization
        daily_candles["timestamp"] = daily_candles["timestamp"].astype(str)

        return daily_candles.to_dict(orient="records")


    def report_data_period(self, data):
        """
        Report the start and end timestamps of the data period.
        """
        start = data["timestamp"].iloc[0]
        end = data["timestamp"].iloc[-1]
        logger.info("Backtest Data period: %r - %r", start.isoformat(), end.isoformat())


    async def run_backtest(self, start_candle_index=0):
        logger.info("AlgoTrader BacktestingSystem fetching backtest data")

        backtest_data = self.data_handler.get_backtest_data()
        backtest_ticker_data = backtest_data[self.tickers[0]]

        start_candle_timestamp = backtest_ticker_data['timestamp'].iloc[start_candle_index]
        total_number_candles = len(backtest_ticker_data)
        candle_index = start_candle_index

        curr_date = start_candle_timestamp
        daily_candle_index = start_candle_index  # Track first candle of the day

        await self.ws_manager.send_message({
            "message": {"type": "success", "text": f"Backtest has begun for tickers {', '.join(self.tickers)}"}
        })

        logger.info("AlgoTrader BacktestingSystem begin backtest & signal generation")

        self.report_data_period(backtest_ticker_data)

        while candle_index <= total_number_candles:
            if self.task and self.task.cancelled():
                logger.warning("Task cancelled!")
                return  # Stop if the task is cancelled

            logger.debug("Running backtest for candle %r / %r", candle_index, total_number_candles)

            backtest_data["end"] = backtest_ticker_data["timestamp"].iloc[candle_index]
            candle_index += 1

            # Skip if the market is closed
            if not self.is_market_open(backtest_data["end"]):
                continue

            # Generate trading signals
            try:
                signal_data = self.strategy_handler.generate_signals(is_backtest=True, backtest_data=backtest_data)
            except Exception as e:
                logger.exception("Error generating signals", exc_info=e)
                
            for signal in signal_data.values():
                order = None
                try:
                    order = self.execution_handler.run_backtest_trade(signal)
                except Exception as e:
                    logger.exception("Error executing backtest trade", exc_info=e)

                if order is not None:
                    order['timestamp'] = backtest_data["end"].isoformat()
                    self.trade_results.append(order)
                    try:
                        logger.info("Trade outcome: %r", order)
                        trade_message = {
                            "trade": {
                                "timestamp": order["timestamp"],
                                "ticker": order["ticker"],
                                "price": order["price"],
                                "side": order["side"].value,
                                "qty": order["qty"],
                                "direction": order["direction"]
                            },
                            "balance": self.execution_handler.position_manager.cash_balance,
                            "positions": [p.__repr__() for p in self.execution_handler.position_manager.positions.values()],
                            "ticker_data": None,  # No new ticker data yet,
                            "message": dict(type="success", text=f"Trade executed for {order['ticker']}")
                        }
                        await self.ws_manager.send_message(trade_message)
                        logger.info("Trade message sent")
                    except Exception as e:
                        logger.exception("Error sending trade data to WebSocket", exc_info=e)

            # **New day detected, send full-day data**
            if backtest_data["end"].date() != curr_date.date():
                logger.debug("New day detected: %r", backtest_data["end"])
                curr_date = backtest_data["end"]

                try:
                    ticker_to_price_map = self.data_handler.fetch_most_recent_prices()
                    self.execution_handler.update_backtest_positions(backtest_data["end"], ticker_to_price_map=ticker_to_price_map)

                    # Slice full day's data
                    ticker_data = self.serialize_ticker_data(backtest_ticker_data.iloc[daily_candle_index:candle_index])

                    daily_message = {
                        "trade": None,  # No specific trade at day start
                        "balance": self.execution_handler.position_manager.cash_balance,
                        "positions": [p.__repr__() for p in self.execution_handler.position_manager.positions.values()],
                        "stats": self.execution_handler.position_manager.stats(),
                        "ticker_data": ticker_data,  # Send all candles from the last detected day
                    }

                    await self.ws_manager.send_message(daily_message)

                    # **Update daily_candle_index to mark the start of a new day**
                    daily_candle_index = candle_index

                except Exception as e:
                    logger.exception("Error sending daily backtest data to WebSocket", exc_info=e)

            await asyncio.sleep(0)

        logger.info("Position Manager stats: %r", self.execution_handler.position_manager.stats())
        logger.info("Backtest completed. Results: %r", self.trade_results)


    def start_backtest(self):
        """Starts the backtest task in the background."""
        if self.task and not self.task.done():
            self.task.cancel()  # Cancel any existing task before starting a new one
        self.task = asyncio.create_task(self.run_backtest())


    async def start_backtest_for_ticker(self, ticker: str, strategy: str):
        """
        Runs a simulated backtest and sends updates via WebSocket.
        """
        if ticker in self.running_backtests:
            self.running_backtests[ticker].cancel()  # Cancel any existing task
        self.tickers = [ticker]
        task = asyncio.create_task(self.run_backtest())
        self.running_backtests[ticker] = task

    # def start_backtest_for_ticker(self, ticker, strategy):
    #     """Starts the backtest task in the background."""
    #     if self.task and not self.task.done():
    #         self.task.cancel()  # Cancel any existing task before starting a new one
    #     self.tickers = [ticker]
    #     self.strategy_handler.tickers = [ticker]
    #     self.data_handler.tickers = [ticker]
    #     if (strategy in self.strategy_handler.strategies):
    #         self.strategy_handler.strategies = {strategy: self.strategy_handler.strategies[strategy]}
    #     else:
    #         raise ValueError(f"Strategy {strategy} not found in strategy handler")
        
    #     self.task = asyncio.create_task(self.run_backtest())


    def stop_backtest(self):
        """Stops the running backtest."""
        if self.task and not self.task.done():
            self.task.cancel()
            self.task = None