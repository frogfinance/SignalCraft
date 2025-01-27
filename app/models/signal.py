from datetime import datetime
from alpaca.trading.enums import OrderSide

class Signal:

    def __init__(self, buy=False, sell=False, reason=None, strategy=None, ticker=None, price=None):
        self.action = 'buy' if buy else 'sell' if sell else None
        self.reason = reason
        self.strategy = strategy
        self.price = price
        self.ticker = ticker
        self.timestamp = datetime.now()
        self.momentum = None
        self.score = None

    def __str__(self):
        return f"Signal(action={self.action}, reason={self.reason}, strategy={self.strategy})"
    
    def buy(self):
        self.action = 'buy'
        return self
    
    def sell(self):
        self.action = 'sell'
        return self
    
    @property
    def side(self):
        side = OrderSide.BUY if self.action == 'buy' else OrderSide.SELL if self.action == 'sell' else None
        return side

    def __dict__(self):
        return {
            'action': self.action,
            'price': float(self.price),
            'reason': self.reason,
            'strategy': self.strategy,
            'ticker': self.ticker
        }
    
    def __str__(self):
        return f"Signal(action={self.action}, price={self.price}, reason={self.reason}, strategy={self.strategy}, ticker={self.ticker})"