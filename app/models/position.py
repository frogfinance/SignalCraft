from datetime import datetime
from alpaca.trading.enums import OrderSide

class Position:
    def __init__(self, ticker, qty, entry_price, side, entry_time, direction='LONG'):
        self.ticker = ticker
        self.qty = float(qty)
        self.entry_price = float(entry_price)
        self.side = side
        self.entry_time = entry_time
        self.target_qty = float(qty)  # For gradual position building/reduction
        self.pl_pct = 0  # Current P&L percentage
        self.pl = 0  # Current P&L in dollars
        self.current_price = entry_price
        self.direction = None
        self.is_open = True

    def update_pl(self, current_price):
        """Update position P&L"""
        self.current_price = float(current_price)
        multiplier = 1 if self.side == OrderSide.BUY else -1
        self.pl_pct = ((self.current_price / self.entry_price) - 1) * multiplier
        self.pl = self.current_price - self.entry_price
        
    def get_exposure(self, equity):
        """Calculate position exposure as percentage of equity"""
        position_value = abs(self.qty * float(self.current_price))
        return position_value / equity
        
    def __str__(self):
        return (f"{self.ticker}: {self.qty} shares @ ${self.entry_price:.2f} "
                f"({self.pl_pct:.1%} P&L)")

    def __repr__(self):
        return dict(
            ticker=self.ticker,
            qty=self.qty,
            entry_price=self.entry_price,
            side=self.side,
            entry_time=self.entry_time.isoformat() if isinstance(self.entry_time, datetime) else self.entry_time,
            target_qty=self.target_qty,
            pl_pct=self.pl_pct,
            pl=self.pl,
            current_price=self.current_price,
            is_open=self.is_open,
            direction=self.direction
        )