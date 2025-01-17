from alpaca.trading import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest
from datetime import datetime

from app.models.position import Position

class PositionManager:
    def __init__(self, trading_client: TradingClient):
        self.trading_client = trading_client
        self.positions = {}  # symbol -> Position object
        self.pending_closes = set()  # Symbols with pending close orders
        self.pending_orders = []  # List of pending new position orders
        
        # Position sizing parameters
        self.max_position_size = 0.08  # 8% max per position
        self.position_step_size = 0.02  # 2% per trade for gradual building
        self.max_total_exposure = 1.6  # 160% total exposure (80% long + 80% short)
        
        # Initialize current positions and pending orders
        self.update_positions()
        self.update_pending_orders()
    
    def update_pending_orders(self):
        """Update list of pending orders, removing executed ones"""
        try:
            # Get all open orders
            orders = self.trading_client.get_orders()
            
            # Clear old pending orders
            self.pending_orders = []
            
            # Only track orders that are still pending
            for order in orders:
                if order.status in ['new', 'accepted', 'pending']:
                    self.pending_orders.append({
                        'symbol': order.symbol,
                        'shares': float(order.qty),
                        'side': order.side,
                        'order_id': order.id
                    })
                    
        except Exception as e:
            print(f"Error updating orders: {str(e)}")
    
    def get_account_info(self):
        """Get account information"""
        account = self.trading_client.get_account()
        return {
            'equity': float(account.equity),
            'buying_power': float(account.buying_power),
            'initial_margin': float(account.initial_margin),
            'margin_multiplier': float(account.multiplier),
            'daytrading_buying_power': float(account.daytrading_buying_power)
        }
    
    def update_positions(self, show_status=True):
        """Update position tracking with current market data
        Args:
            show_status: Whether to print current portfolio status
        """
        try:
            alpaca_positions = self.trading_client.get_all_positions()
            current_symbols = set()
            
            # Update existing positions and add new ones
            for p in alpaca_positions:
                symbol = p.symbol
                current_symbols.add(symbol)
                qty = float(p.qty)
                current_price = float(p.current_price)
                entry_price = float(p.avg_entry_price)
                side = OrderSide.BUY if qty > 0 else OrderSide.SELL
                
                if symbol not in self.positions:
                    # New position
                    self.positions[symbol] = Position(
                        symbol, qty, entry_price, side, 
                        datetime.now()  # Approximate entry time for existing positions
                    )
                
                # Update position data
                pos: Position = self.positions[symbol]
                pos.qty = qty
                pos.entry_price = entry_price
                pos.update_pl(current_price)
            
            # Remove closed positions
            self.positions = {s: p for s, p in self.positions.items() if s in current_symbols}
            
            # Calculate total exposure excluding pending closes
            account = self.get_account_info()
            active_positions = {s: p for s, p in self.positions.items() 
                              if s not in self.pending_closes}
            total_exposure = sum(p.get_exposure(account['equity']) 
                               for p in active_positions.values())
            
            if show_status:
                print("\nCurrent Portfolio Status:")
                print(f"Total Exposure: {total_exposure:.1%}")
                for pos in active_positions.values():
                    exposure = pos.get_exposure(account['equity'])
                    print(f"{pos} ({exposure:.1%} exposure)")
                
                if self.pending_closes:
                    print("\nPending Close Orders:")
                    for symbol in self.pending_closes:
                        print(f"- {symbol}")
                
                if self.pending_orders:
                    print("\nPending New Orders:")
                    for order in self.pending_orders:
                        print(f"- {order['symbol']} ({order['side']})")
                
            return self.positions
            
        except Exception as e:
            print(f"Error updating positions: {str(e)}")
            return {}
    
    def calculate_target_position(self, symbol, price, side, target_pct=None):
        """
        Calculate target position size considering existing positions
        Args:
            symbol: Stock symbol
            price: Current price
            side: OrderSide.BUY or OrderSide.SELL
            target_pct: Target position size as % of equity (e.g. 0.08 for 8%)
        Returns target shares and whether to allow the trade
        """
        account = self.get_account_info()
        equity = account['equity']
        
        # Calculate current total exposure excluding pending closes
        active_positions = {s: p for s, p in self.positions.items() 
                          if s not in self.pending_closes}
        total_exposure = sum(p.get_exposure(equity) for p in active_positions.values())
        
        # Check if we're already at max exposure
        if total_exposure >= self.max_total_exposure:
            print(f"Maximum total exposure reached: {total_exposure:.1%}")
            return 0, False
        
        # Use provided target_pct or default max_position_size
        position_size = target_pct if target_pct is not None else self.max_position_size
        target_position_value = equity * position_size
        current_position = active_positions.get(symbol)
        
        if current_position:
            # Position exists - check if we should add more
            current_exposure = current_position.get_exposure(equity)
            
            # Don't add if already at target size
            if current_exposure >= position_size:
                print(f"Target position size reached for {symbol}: {current_exposure:.1%}")
                return 0, False
            
            # Don't add if position moving against us
            if current_position.pl_pct < -0.02:  # -2% loss threshold
                print(f"Position moving against us: {current_position.pl_pct:.1%} P&L")
                return 0, False
            
            # Calculate remaining size to reach target
            remaining_size = target_position_value - (current_position.qty * price)
            return int(remaining_size / price), True
            
        else:
            # New position - use full target size
            target_shares = int(target_position_value / price)
            print(f"New {position_size:.1%} position: {target_shares} shares @ ${price:.2f}")
            return target_shares, True
    
    def should_close_position(self, symbol, technical_data):
        """Determine if a position should be closed based on technical analysis"""
        position = self.positions.get(symbol)
        if not position:
            return False
            
        # Get current exposure
        account = self.get_account_info()
        total_exposure = sum(p.get_exposure(float(account['equity'])) 
                           for p in self.positions.values())
        
        # Close if any of these conditions are met:
        reasons = []
        
        # 1. Significant loss
        if position.pl_pct < -0.05:  # -5% stop loss
            reasons.append(f"Stop loss hit: {position.pl_pct:.1%} P&L")
        
        # 2. Technical score moves against position
        technical_score = technical_data['score']
        if position.side == OrderSide.BUY and technical_score < 0.4:
            reasons.append(f"Weak technical score for long: {technical_score:.2f}")
        elif position.side == OrderSide.SELL and technical_score > 0.6:
            reasons.append(f"Strong technical score for short: {technical_score:.2f}")
        
        # 3. Momentum moves against position
        momentum = technical_data['momentum']
        if position.side == OrderSide.BUY and momentum < -0.02:  # -2% momentum for longs
            reasons.append(f"Negative momentum for long: {momentum:.1f}%")
        elif position.side == OrderSide.SELL and momentum > 0.02:  # +2% momentum for shorts
            reasons.append(f"Positive momentum for short: {momentum:.1f}%")
        
        # 4. Over exposure - close weakest positions
        if total_exposure > self.max_total_exposure:
            # Close positions with weak technicals when over-exposed
            if (position.side == OrderSide.BUY and technical_score < 0.5) or \
               (position.side == OrderSide.SELL and technical_score > 0.5):
                reasons.append(f"Reducing exposure ({total_exposure:.1%} total)")
        
        # 5. Mediocre performance with significant age
        position_age = (datetime.now() - position.entry_time).days
        if position_age > 5 and abs(position.pl_pct) < 0.01:
            reasons.append(f"Stagnant position after {position_age} days")
        
        if reasons:
            reason_str = ", ".join(reasons)
            print(f"Closing {symbol} due to: {reason_str}")
            return True
            
        return False
    
    def place_order(self, symbol, shares, side=OrderSide.BUY):
        """Place a market order"""
        if shares <= 0:
            return None
            
        order_details = MarketOrderRequest(
            symbol=symbol,
            qty=shares,
            side=side,
            time_in_force=TimeInForce.DAY
        )
        
        try:
            # Place order and track status
            order = self.trading_client.submit_order(order_details)
            if order.status in ['new', 'accepted', 'pending']:
                self.pending_orders.append({
                    'symbol': symbol,
                    'shares': shares,
                    'side': side,
                    'order_id': order.id
                })
                print(f"Order queued: {shares} shares of {symbol}")
            else:
                print(f"Order executed: {shares} shares of {symbol}")
            
            return order
        except Exception as e:
            print(f"\nError placing order:")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            return None
    
    def check_position_available(self, symbol):
        """Check if position is available to close"""
        try:
            # Get all positions
            positions = self.trading_client.get_all_positions()
            
            # Find this position
            for pos in positions:
                if pos.symbol == symbol:
                    if float(pos.qty_available) == 0:
                        print(f"Skipping {symbol} - all shares held for orders")
                        return False
                    return True
                    
            print(f"Position not found: {symbol}")
            return False
            
        except Exception as e:
            print(f"Error checking position {symbol}: {str(e)}")
            return False
    
    def close_position(self, symbol):
        """Close an existing position"""
        # Skip if already pending close or shares held
        if symbol in self.pending_closes:
            print(f"Skipping {symbol} - close order already pending")
            return None
        
        if not self.check_position_available(symbol):
            return None
            
        try:
            order = self.trading_client.close_position(symbol)
            if order.status == 'accepted':
                self.pending_closes.add(symbol)
                print(f"Close order queued: {symbol}")
                return order
                
        except Exception as e:
            print(f"\nError closing position in {symbol}:")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            return None