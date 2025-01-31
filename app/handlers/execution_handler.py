import duckdb
import logging
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest
from alpaca.trading.enums import OrderSide, OrderType
from alpaca.trading.models import Clock, Order

from app.models.position_manager import PositionManager
from app.models.signal import Signal

logger = logging.getLogger("app")


class ExecutionHandler():
    def __init__(self, api_key, api_secret, db_base_path="dbs", use_paper=True, is_backtest=False):
        super().__init__()
        self.db_base_path = db_base_path
        self.trading_client = TradingClient(api_key, api_secret, paper=use_paper)
        self.position_manager = PositionManager(self.trading_client, backtest=is_backtest)
        self.is_backtest = is_backtest
        self.target_pct = 0.045
    
    def execute_trade(self, signal: Signal, backtest=False):
        """Execute a trade only during market hours."""
        if backtest:
            return self.run_backtest_trade(signal)
        if not self.is_market_open():
            logger.info(f"Market is closed. Cannot execute trade for {signal['ticker']}.")
            return

        def submit_and_handle_order(order_request):
            order = self.submit_order(order_request)
            logger.info("Order submitted: {}".format(order))
            if order.filled_at is None:
                logger.info(f"Order for {signal['ticker']} is still open.")
                trade_timestamp = order.submitted_at
            else:
                trade_timestamp = order.filled_at
            self.save_trade(signal, order, trade_timestamp)
            return order

        qty, is_good_trade = self.position_manager.calculate_target_position(signal.ticker, signal.price, signal.side, target_pct=self.target_pct)
        # if sell, check if we have shares to sell
        if is_good_trade and signal.side in OrderSide.SELL:
            order_request = LimitOrderRequest(symbol=signal.ticker, 
                                      qty=qty, 
                                      side=OrderSide.SELL, 
                                      type=OrderType.LIMIT,
                                      limit_price=signal.price
                    )
            order = submit_and_handle_order(order_request)
       
        elif is_good_trade and signal.side in OrderSide.BUY:   
            order_request = LimitOrderRequest(symbol=signal.ticker,
                                      qty=qty,
                                      side=OrderSide.BUY,
                                      type=OrderType.LIMIT,
                                      limit_price=signal.price
                                    )
            order = submit_and_handle_order(order_request)
        else:
            should_close_position = self.position_manager.should_close_position(signal.ticker, signal)
            if should_close_position:
                logger.info("Detected signal to close position for {}".format(signal.ticker))
                order_request = MarketOrderRequest(symbol=signal.ticker, qty=qty, side=OrderSide.SELL, type=OrderType.MARKET)
                order = submit_and_handle_order(order_request)
            else:
                logger.info("Trade for {} not executed signal_data={}".format(signal.ticker, signal))
        return order or None
    
    def get_all_positions(self):
        return self.trading_client.get_all_positions()

    def get_buying_power(self):
        return self.trading_client.get_account().buying_power
    
    def get_next_market_open(self):
        """Get the next market open time."""
        clock: Clock = self.trading_client.get_clock()
        return clock.next_open

    def handle_execution(self, signals_map: dict):
        if self.is_backtest:
            for signal in signals_map.values():
                self.run_backtest_trade(signal)
        for signal in signals_map.values():
            # Show initial portfolio status
            self.execute_trade(signal)

    def is_market_open(self):
        """Check if the market is currently open."""
        clock: Clock = self.trading_client.get_clock()
        return clock.is_open
    
    def run_backtest_trade(self, signal: Signal):
        """Simulate trade execution and determine outcome."""
        order = dict()
        order.update(signal.__dict__())
        order_generated = False
        qty, is_good_trade = self.position_manager.calculate_target_position(signal.ticker, signal.price, signal.side, target_pct=self.target_pct)
        should_close_position = self.position_manager.should_close_position(signal.ticker, signal)
        if should_close_position is True:
            logger.info("Detected signal to close position for {}".format(signal.ticker))
            order['qty'] = self.position_manager.positions[signal.ticker]['qty']
            order['side'] = OrderSide.SELL
            order_generated = True
        elif qty <= 0:
            logger.debug(f"Trade for {signal.ticker} not executed, qty={qty}")
            order_generated = False
        elif signal.action == 'buy':
            logger.debug(f'Buy signal detected for {signal.ticker}')
            if is_good_trade:
                order['qty'] = qty
                order['side'] = OrderSide.BUY
                logger.debug(f'Buy order generated for {signal.ticker} qty={qty} price={signal.price}')
                order_generated = True
        elif signal.action == 'sell':
            if is_good_trade:
                order['side'] = OrderSide.SELL
                order['qty'] = qty
                logger.debug(f'Sell order generated for {signal.ticker} qty={qty} price={signal.price}')
                order_generated = True
        
        if order_generated:
            logger.debug('Order generated for ticker {} with signal {}'.format(order['ticker'], signal))
            self.position_manager.update_positions_backtest(order, show_status=False)
            return order
        else:
            return None

    def save_trade(self, signal: Signal, order: Order, trade_timestamp):
        db_table = "trades"
        if self.is_backtest:
            db_table = "backtest_trades"
        conn = duckdb.connect(f"{self.db_base_path}/{db_table}.db")
        conn.execute(f"INSERT INTO trades VALUES ('{trade_timestamp}, {order.symbol}', '{signal.action}', {order.filled_qty}, {order.filled_avg_price}, '{order.client_order_id}')")
        conn.close()
        logger.info('Trade saved for ticker', order.symbol)

    def submit_order(self, order_request):
        """this function exists to mock the order submission"""
        return self.trading_client.submit_order(order_request)
    
    def update_backtest_positions(self, timestamp, ticker_to_price_map):
        """Update backtest positions."""
        self.position_manager.check_positions(ticker_to_price_map)
        self.position_manager.update_backtest_account_position_values(timestamp, ticker_to_price_map)