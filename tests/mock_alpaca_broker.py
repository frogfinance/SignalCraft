import duckdb
import uuid
from datetime import datetime
from alpaca.trading import Order

class MockAlpacaBroker:
    def __init__(self):
        self.account = {
            'cash': 100000,  # Starting cash balance
            'positions': {}, # Dictionary to hold positions
            'buying_power': 100000 # Buying power
        }
        self.orders = []  # List to hold orders

    def submit_order(self, symbol, qty, side, type='market', time_in_force='gtc'):
        order = MockOrder(symbol, qty, side, type, time_in_force)
        self.orders.append(order)
        self._update_account(order)
        return order

    def _update_account(self, order):
        symbol = order['symbol']
        qty = order['qty']
        side = order['side']
        price = self._get_mock_price(symbol) 

        if side == 'buy':
            cost = qty * price
            if self.account['cash'] >= cost:
                self.account['cash'] -= cost
                if symbol in self.account['positions']:
                    self.account['positions'][symbol] += qty
                else:
                    self.account['positions'][symbol] = qty
        elif side == 'sell':
            if symbol in self.account['positions'] and self.account['positions'][symbol] >= qty:
                self.account['positions'][symbol] -= qty
                self.account['cash'] += qty * price
                if self.account['positions'][symbol] == 0:
                    del self.account['positions'][symbol]
        self.account['buying_power'] = self.account['cash']


    def _get_mock_price(self, symbol):
        # Mock price for simplicity get the last close price from the ticker_data duckdb database
        conn = duckdb.connect(f"dbs/{symbol}_1min_data.db")
        last_price = conn.sql("SELECT close FROM ticker_data ORDER BY timestamp DESC LIMIT 1").fetchone()[0]
        conn.close()
        return last_price
        
    def get_account(self):
        return self.account

    def get_orders(self):
        return self.orders


class MockOrder:
    def __init__(self, symbol, qty, side, type='market', time_in_force='gtc'):
        self.id = uuid.uuid4()
        self.client_order_id = str(uuid.uuid4())
        self.created_at = datetime.now()
        self.updated_at = self.created_at
        self.submitted_at = self.created_at
        self.filled_at = self.created_at
        self.expired_at = None
        self.canceled_at = None
        self.failed_at = None
        self.replaced_at = None
        self.replaced_by = None
        self.replaces = None
        self.asset_id = uuid.uuid4()
        self.symbol = symbol
        self.asset_class = 'us_equity'
        self.notional = None
        self.qty = qty
        self.filled_qty = qty
        self.filled_avg_price = 100  # Mock price
        self.side = side
        self.type = type
        self.time_in_force = time_in_force
        self.status = 'filled'  # For simplicity, assume all orders are filled immediately


# Example usage:
# broker = MockAlpacaBroker()
# broker.submit_order('AAPL', 10, 'buy')
# broker.submit_order('AAPL', 5, 'sell')
# print(broker.get_account())
# print(broker.get_orders())