import duckdb
import logging
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest, TakeProfitRequest, StopLossRequest
from alpaca.trading.enums import OrderSide, OrderType, OrderClass
from alpaca.trading.models import Clock, Order, TimeInForce

from app.agents.market_analyzer_agent import MarketAnalyzerAgent
from app.models.archiver import Archiver
from app.models.position_manager import PositionManager
from app.models.signal import Signal

logger = logging.getLogger("app")


class ExecutionHandler():
    def __init__(self, api_key, api_secret, db_base_path="dbs", use_paper=True, is_backtest=False, market_analyzer=None):
        super().__init__()
        self.db_base_path = db_base_path
        self.trading_client = TradingClient(api_key, api_secret, paper=use_paper)
        self.position_manager = PositionManager(self.trading_client, backtest=is_backtest)
        self.archiver = Archiver(self.trading_client, backtest=is_backtest)
        self.market_analyzer_agent = market_analyzer
        self.is_backtest = is_backtest
        self.target_pct = 0.045
    
    def execute_trade(self, signal: Signal, backtest=False):
        """Execute a trade only during market hours."""
        order = None
        if backtest:
            return self.run_backtest_trade(signal)
        if not self.is_market_open():
            logger.info(f"Market is closed. Cannot execute trade for {signal.ticker}.")
            return

        def submit_and_handle_order(order_request):
            order = self.submit_order(order_request)
            logger.info("Order submitted: {}".format(order))
            if order.filled_at is None:
                logger.info("Order for %r is still open.", signal.ticker)
                trade_timestamp = order.submitted_at
            else:
                trade_timestamp = order.filled_at
            self.save_trade(signal, order, trade_timestamp)
            return order

        qty, is_good_trade = self.position_manager.calculate_target_position(signal.ticker, signal.price, signal.side, target_pct=self.target_pct)
        
        # Create take_profit and stop_loss requests if provided in the signal
        take_profit = None
        stop_loss = None
        order_class = None
        
        # Only set up bracket orders for entries, not exits
        if signal.take_profit is not None or signal.stop_loss is not None:
            # If both stop loss and take profit are present, use bracket order
            if signal.take_profit is not None and signal.stop_loss is not None:
                order_class = OrderClass.BRACKET
                take_profit = TakeProfitRequest(limit_price=float(signal.take_profit))
                stop_loss = StopLossRequest(stop_price=float(signal.stop_loss))
                logger.info(f"Using bracket order with TP: {signal.take_profit} and SL: {signal.stop_loss}")
            # If only take profit, use one-cancels-other order
            elif signal.take_profit is not None:
                order_class = OrderClass.OCO
                take_profit = TakeProfitRequest(limit_price=float(signal.take_profit))
                logger.info(f"Using OCO order with TP: {signal.take_profit}")
            # If only stop loss, use one-cancels-other order
            elif signal.stop_loss is not None:
                order_class = OrderClass.OCO
                stop_loss = StopLossRequest(stop_price=float(signal.stop_loss))
                logger.info(f"Using OCO order with SL: {signal.stop_loss}")
        
        try:
            # If selling shares we already own
            if is_good_trade and signal.action == 'sell' and qty > 0:
                order_request = LimitOrderRequest(
                    symbol=signal.ticker, 
                    qty=qty, 
                    side=OrderSide.SELL, 
                    type=OrderType.LIMIT,
                    limit_price=float(signal.price),
                    time_in_force=TimeInForce.DAY,
                    order_class=order_class,
                    take_profit=take_profit,
                    stop_loss=stop_loss
                )
                order = submit_and_handle_order(order_request)
            
            # If buying shares
            elif is_good_trade and signal.action == 'buy' and qty > 0:   
                order_request = LimitOrderRequest(
                    symbol=signal.ticker,
                    qty=qty,
                    side=OrderSide.BUY,
                    type=OrderType.LIMIT,
                    limit_price=float(signal.price),
                    time_in_force=TimeInForce.DAY,
                    order_class=order_class,
                    take_profit=take_profit,
                    stop_loss=stop_loss
                )
                order = submit_and_handle_order(order_request)
            else:
                # Handle position closing logic
                should_close_position = self.position_manager.should_close_position(signal.ticker, signal)
                if should_close_position:
                    logger.info("Detected signal to close position for %r", signal.ticker)
                    
                    # Get the actual position quantity
                    position_qty = 0
                    try:
                        if signal.ticker in self.position_manager.positions:
                            position_qty = abs(self.position_manager.positions[signal.ticker].qty)
                        else:
                            positions = self.trading_client.get_all_positions()
                            for position in positions:
                                if position.symbol == signal.ticker:
                                    position_qty = abs(float(position.qty))
                                    break
                    except Exception as e:
                        logger.error(f"Error getting position quantity for {signal.ticker}: {e}")
                        return None
                        
                    if position_qty <= 0:
                        logger.warning(f"Cannot close position for {signal.ticker}: No shares held")
                        return None
                        
                    # For position closings, we don't use bracket orders (we're already exiting)
                    order_request = MarketOrderRequest(
                        symbol=signal.ticker, 
                        qty=position_qty, 
                        side=OrderSide.SELL, 
                        type=OrderType.MARKET, 
                        time_in_force=TimeInForce.DAY
                    )
                    order = submit_and_handle_order(order_request)
                else:
                    logger.info("Trade for %r not executed signal_data=%r", signal.ticker, signal)
        except Exception as e:
            logger.error(f"Error executing trade for %r", signal.ticker, exc_info=e)
            logger.info("Signal from: symbol=%r qty=%r action=%r price=%r", signal.ticker, qty, signal.action, signal.price)
            return None
        return order
    
    def get_all_positions(self):
        if self.is_backtest:
            return self.position_manager.positions
        else:
            return self.trading_client.get_all_positions()

    def get_buying_power(self):
        return self.trading_client.get_account().buying_power
    
    def get_next_market_open(self):
        """Get the next market open time."""
        clock: Clock = self.trading_client.get_clock()
        return clock.next_open
    
    def get_trades(self):
        return self.archiver.get_trades()
    
    def get_trade_markers(self, ticker):
        db_table = "trades"
        trades = None
        if self.is_backtest:
            db_table = "backtest_trades"
        try:
            conn = duckdb.connect(f"{self.db_base_path}/{db_table}.db")
            trades = conn.sql(f"SELECT datetime, price, trade_type FROM trades WHERE ticker = '{ticker}'").fetchdf()
        except Exception as e:
            logger.exception("Error fetching trades", exc_info=e)
        finally:
            conn.close()
        return trades

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
        
        # Include stop loss and take profit in the backtest order
        if signal.stop_loss is not None:
            order['stop_loss'] = signal.stop_loss
        if signal.take_profit is not None:
            order['take_profit'] = signal.take_profit
            
        if should_close_position is True:
            logger.info("Detected signal to close position for %r", signal.ticker)
            order['qty'] = self.position_manager.positions[signal.ticker].qty
            order['side'] = OrderSide.SELL
            order_generated = True
        elif qty <= 0:
            logger.debug("Trade for %r not executed, qty=%r", signal.ticker, qty)
            order_generated = False
        elif signal.action == 'buy':
            logger.debug('Buy signal detected for %r', signal.ticker)
            if is_good_trade:
                order['qty'] = qty
                order['side'] = OrderSide.BUY
                logger.debug('Buy order generated for %r qty=%r price=%r', signal.ticker, qty, signal.price)
                order_generated = True
        elif signal.action == 'sell':
            if is_good_trade:
                order['side'] = OrderSide.SELL
                order['qty'] = qty
                logger.debug('Sell order generated for %r qty=%r price=%r', signal.ticker, qty, signal.price)
                order_generated = True
        
        if order_generated:
            logger.debug('Order generated for ticker %r: %r', signal.ticker, order)
            self.position_manager.update_positions_backtest(order, show_status=False)
            return order
        else:
            return None

    def save_trade(self, signal: Signal, order: Order, trade_timestamp):
        db_table = "trades"
        if self.is_backtest:
            db_table = "backtest_trades"
        conn = duckdb.connect(f"{self.db_base_path}/{db_table}.db")
        
        # Include stop loss and take profit in the saved trade data
        stop_loss = signal.stop_loss if hasattr(signal, 'stop_loss') and signal.stop_loss is not None else 'NULL'
        take_profit = signal.take_profit if hasattr(signal, 'take_profit') and signal.take_profit is not None else 'NULL'
        
        # Update SQL to include stop_loss and take_profit columns
        conn.execute(f"""
            INSERT INTO trades 
            VALUES (
                '{trade_timestamp}', 
                '{order.symbol}', 
                '{signal.action}', 
                {order.filled_qty}, 
                {order.filled_avg_price if order.filled_avg_price is not None else 0}, 
                '{order.client_order_id}', 
                '{signal.strategy}', 
                '{signal.reason}',
                {stop_loss if stop_loss != 'NULL' else 'NULL'},
                {take_profit if take_profit != 'NULL' else 'NULL'}
            )
        """)
        
        conn.close()
        logger.debug('Trade saved for ticker: %r', order.symbol)
    
    def submit_order(self, order_request):
        """this function exists to mock the order submission"""
        return self.trading_client.submit_order(order_request)
    
    def sync_alpaca_data(self):
        """Sync Alpaca account with the latest positions and pending orders."""
        self.position_manager.update_positions()
        self.position_manager.update_pending_orders()
        self.archiver.get_trades()

    def update_backtest_positions(self, timestamp, ticker_to_price_map):
        """Update backtest positions."""
        self.position_manager.update_backtest_account_position_values(timestamp, ticker_to_price_map)

    def process_signal(self, signal):
        """Process and execute a trading signal."""
        logger.info("Processing signal: %s", signal)
        
        # Calculate the number of shares to trade
        shares, is_valid_trade = self.position_manager.calculate_target_position(signal.ticker, signal.price, signal.side, target_pct=self.target_pct)
        
        if not is_valid_trade:
            logger.warning("Invalid trade signal for %s: qty=%s", signal.ticker, shares)
            return None
        
        # Create the order
        order = {
            'ticker': signal.ticker,
            'side': signal.side,
            'qty': shares,
            'price': signal.price,
            'direction': signal.direction
        }
        
        # Add stop loss and take profit if present in the signal
        if hasattr(signal, 'stop_loss') and signal.stop_loss:
            order['stop_loss'] = signal.stop_loss
        
        if hasattr(signal, 'take_profit') and signal.take_profit:
            order['take_profit'] = signal.take_profit
        
        # Submit the order
        try:
            if signal.action == 'buy':
                order_request = LimitOrderRequest(
                    symbol=signal.ticker,
                    qty=shares,
                    side=OrderSide.BUY,
                    type=OrderType.LIMIT,
                    limit_price=float(signal.price),
                    time_in_force=TimeInForce.DAY
                )
                self.submit_order(order_request)
                logger.info("Buy order submitted: %s", order)
            elif signal.action == 'sell':
                order_request = LimitOrderRequest(
                    symbol=signal.ticker,
                    qty=shares,
                    side=OrderSide.SELL,
                    type=OrderType.LIMIT,
                    limit_price=float(signal.price),
                    time_in_force=TimeInForce.DAY
                )
                self.submit_order(order_request)
                logger.info("Sell order submitted: %s", order)
        except Exception as e:
            logger.error("Error submitting order for %s: %s", signal.ticker, e)
            return None
        
        return order
