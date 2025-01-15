import duckdb
import logging
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import OrderRequest
from alpaca.trading.enums import OrderSide
from alpaca.trading.models import Clock, Order

class ExecutionHandler():
    def __init__(self, api_key, api_secret, db_base_path="dbs", use_paper=True):
        super().__init__()
        self.db_base_path = db_base_path
        self.trading_client = TradingClient(api_key, api_secret, paper=use_paper)
        self.positions = {} # {ticker: Position}
        self.pending_closes = set()
        self.pending_orders = []
        
        # Position sizing parameters
        self.max_position_size = 0.08  # 8% max per position
        self.position_step_size = 0.02  # 2% per trade for gradual building
        self.max_total_exposure = 1.6  # 160% total exposure (80% long + 80% short)
        
        # Initialize current positions and pending orders
        self.update_positions()
        self.update_pending_orders()

    def execute_trade(self, signal):
        """Execute a trade only during market hours."""
        if not self.is_market_open():
            logging.info(f"Market is closed. Cannot execute trade for {signal['ticker']}.")
            return
        
        positions = self.get_all_positions()

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

    def handle_execution(self, signal_data: dict):
        for signal in signal_data.values():
            self.execute_trade(signal)

    def is_market_open(self):
        """Check if the market is currently open."""
        clock: Clock = self.trading_client.get_clock()
        return clock.is_open
    
    def save_trade(self, signal, order: Order, trade_timestamp):
        conn = duckdb.connect(f"{self.db_base_path}/trades.db")
        conn.execute(f"INSERT INTO trades VALUES ('{trade_timestamp}, {signal['ticker']}', '{signal['action']}', {signal['qty']}, {signal['price']}, '{order.id}')")
        conn.close()

    def submit_order(self, order_request: OrderRequest):
        return self.trading_client.submit_order(order_request)