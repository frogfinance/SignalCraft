
class Signal:

    def __init__(self, buy=False, sell=False, reason=None, strategy=None):
        self.action = 'buy' if buy else 'sell' if sell else None
        self.reason = reason
        self.strategy = strategy
        self.price = None

    def __str__(self):
        return f"Signal(action={self.action}, reason={self.reason}, strategy={self.strategy})"
    
    def buy(self):
        self.action = 'buy'
        return self
    
    def sell(self):
        self.action = 'sell'
        return self

    def __dict__(self):
        return {
            'action': self.action,
            'price': self.price,
            'reason': self.reason,
            'strategy': self.strategy
        }