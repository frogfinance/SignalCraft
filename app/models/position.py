from alpaca.trading.enums import OrderSide

class Position:
    def __init__(self, symbol, qty, entry_price, side, entry_time):
        self.symbol = symbol
        self.qty = float(qty)
        self.entry_price = float(entry_price)
        self.side = side
        self.entry_time = entry_time
        self.target_qty = float(qty)  # For gradual position building/reduction
        self.pl_pct = 0  # Current P&L percentage
        self.current_price = entry_price
        
    def update_pl(self, current_price):
        """Update position P&L"""
        self.current_price = float(current_price)
        multiplier = 1 if self.side == OrderSide.BUY else -1
        self.pl_pct = ((self.current_price / self.entry_price) - 1) * multiplier
        
    def get_exposure(self, equity):
        """Calculate position exposure as percentage of equity"""
        position_value = abs(self.qty * self.current_price)
        return position_value / equity
        
    def __str__(self):
        return (f"{self.symbol}: {self.qty} shares @ ${self.entry_price:.2f} "
                f"({self.pl_pct:.1%} P&L)")
