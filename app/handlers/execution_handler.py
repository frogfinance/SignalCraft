import duckdb
import logging
from alpaca.trading.client import TradingClient, GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide
from alpaca.trading.models import Clock, Order
from pandas import DataFrame

from app.models.position_manager import PositionManager
from app.models.signal import Signal

class ExecutionHandler():
    def __init__(self, api_key, api_secret, db_base_path="dbs", use_paper=True, is_backtest=False):
        super().__init__()
        self.db_base_path = db_base_path
        self.trading_client = TradingClient(api_key, api_secret, paper=use_paper)
        self.position_manager = PositionManager(self.trading_client, backtest=is_backtest)
        self.is_backtest = is_backtest
    
    def execute_trade(self, signal: Signal, backtest=False):
        """Execute a trade only during market hours."""
        if backtest:
            self.run_backtest_trade(signal)
        if not self.is_market_open():
            logging.info(f"Market is closed. Cannot execute trade for {signal['ticker']}.")
            return

        def submit_and_handle_order(order_request):
            order = self.submit_order(order_request)
            logging.info("Order submitted: {}".format(order))
            if order.filled_at is None:
                logging.info(f"Order for {signal['ticker']} is still open.")
                trade_timestamp = order.submitted_at
            else:
                trade_timestamp = order.filled_at
            self.save_trade(signal, order, trade_timestamp)

        qty, is_good_trade = self.position_manager.calculate_target_position(signal.ticker, signal.price, signal.side, target_pct=0.045)
        # if sell, check if we have shares to sell
        if is_good_trade and signal.side in OrderSide.SELL:
            order = LimitOrderRequest(symbol=signal.ticker, 
                                      qty=qty, 
                                      side=OrderSide.SELL, 
                                      limit_price=signal.price
                    )
            submit_and_handle_order(order)
       
        elif is_good_trade and signal.side in OrderSide.BUY:   
            order = LimitOrderRequest(symbol=signal.ticker,
                                      qty=qty,
                                      side=OrderSide.BUY,
                                      limit_price=signal.price
                                    )
            submit_and_handle_order(order)
        else:
            logging.info("Trade for {} not executed signal_data={}".format(signal.ticker, signal))

    def get_all_positions(self):
        return self.trading_client.get_all_positions()

    def get_buying_power(self):
        return self.trading_client.get_account().buying_power
    
    def get_next_market_open(self):
        """Get the next market open time."""
        clock: Clock = self.trading_client.get_clock()
        return clock.next_open

    def handle_execution(self, signal_data: dict, backtest=False):
        if backtest:
            self.run_backtest_trade(signal_data)
        for signal in signal_data.values():
            # Show initial portfolio status
            self.position_manager.update_positions(show_status=True)
            self.execute_trade(signal, backtest)

    def is_market_open(self):
        """Check if the market is currently open."""
        clock: Clock = self.trading_client.get_clock()
        return clock.is_open
    
    def manage_existing_positions(self, analyzer):
        """Manage existing positions"""
        current_positions = self.position_manager.update_positions()
        if not current_positions:
            return
            
        # Check each position
        for symbol in list(current_positions.keys()):
            position = current_positions[symbol]
            side = OrderSide.BUY if float(position.qty) > 0 else OrderSide.SELL
            technical_data = analyzer.analyze_stock(symbol, side)
            
            if technical_data and self.should_exit_position(symbol, technical_data):
                print(f"\nSELL {symbol}: {', '.join(technical_data['exit_signals'])}")
                self.position_manager.close_position(symbol)

    def run_backtest_trade(signal):
        """Simulate trade execution and determine outcome."""
        order = dict()
        if signal['action'] == 'buy':
            order['side'] = OrderSide.BUY
        elif signal['action'] == 'sell':
            order['side'] = OrderSide.SELL
        order['symbol'] = signal.ticker
        return order
    
    def save_trade(self, signal: Signal, order: Order, trade_timestamp):
        conn = duckdb.connect(f"{self.db_base_path}/trades.db")
        conn.execute(f"INSERT INTO trades VALUES ('{trade_timestamp}, {order.symbol}', '{signal.action}', {order.filled_qty}, {order.filled_avg_price}, '{order.client_order_id}')")
        conn.close()

    def submit_order(self, order_request):
        """this function exists to mock the order submission"""
        return self.trading_client.submit_order(order_request)
    