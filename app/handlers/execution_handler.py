import duckdb
import logging
from alpaca.trading.client import TradingClient, GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import OrderRequest
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
        self.pending_closes = set()
        self.pending_orders = []
        self.is_backtest = is_backtest

        # Position sizing parameters
        self.max_position_size = 0.08  # 8% max per position
        self.position_step_size = 0.02  # 2% per trade for gradual building
        self.max_total_exposure = 1.6  # 160% total exposure (80% long + 80% short)
        
        if self.is_backtest is False:
            # Initialize current positions and pending orders
            self.position_manager.update_positions()
            self.position_manager.update_pending_orders()
    
    def execute_trade(self, signal, backtest=False):
        """Execute a trade only during market hours."""
        if backtest:
            self.run_backtest_trade(signal)
        if not self.is_market_open():
            logging.info(f"Market is closed. Cannot execute trade for {signal['ticker']}.")
            return
        
        positions = self.position_manager.positions

        # work on order sizing
        # TODO order sizing here

        # if sell, check if we have shares to sell
        if signal['action'] == 'sell':
            position_found = False
            for position in positions:
                if position.symbol == signal['ticker']:
                    if position.qty < signal['qty']:
                        signal['qty'] = position.qty
                    position_found = True
                    break
            if not position_found:
                logging.info(f"No position found for {signal['ticker']}. Cannot sell.")
                return

        # if buy, check if we have enough cash to buy and see if we have any open orders for the same ticker or positions for the same ticker
        # if we have open orders or positions, and a buy signal do not buy
        if signal['action'] == 'buy':
            for position in positions:
                if position.symbol == signal['ticker']:
                    logging.info(f"Already have a position for {signal['ticker']}. Cannot buy.")
                    return
                
            cash_on_hand = self.get_buying_power()
            if cash_on_hand < signal['qty'] * signal['price']:
                logging.info(f"Not enough cash to buy {signal['qty']} shares of {signal['ticker']} at {signal['price']}.")
                return
            
            # check orders to see if we sold this stock on the previous signal


        action = None
        if signal['action'] == 'buy':
            action = OrderSide.BUY
        elif signal['action'] == 'sell':
            action = OrderSide.SELL

        if action:
            try:
                logging.info(f"Executing {action} order for {signal['ticker']}.")
                order_request = OrderRequest(
                    symbol=signal['ticker'],
                    qty=signal['qty'],
                    type='market',
                    side=action,
                )
                order = self.submit_order(order_request)
                logging.info(f"Order response: {order}")

                # if successful save the information to the duckdb database
                if order.filled_at is None:
                    logging.info(f"Order for {signal['ticker']} is still open.")
                    trade_timestamp = order.submitted_at
                else:
                    trade_timestamp = order.filled_at
                self.save_trade(signal, order, trade_timestamp)
            except Exception as e:
                logging.info(f"Error executing trade: {e}")

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

    def submit_order(self, order_request: OrderRequest):
        return self.trading_client.submit_order(order_request)
    