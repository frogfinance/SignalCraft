from alpaca.trading import TradingClient
from alpaca.trading.enums import OrderStatus
import logging
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetPortfolioHistoryRequest
from alpaca.data import TimeFrame
import duckdb
from datetime import datetime

logger = logging.getLogger("app")


class Archiver:
    def __init__(self, trading_client: TradingClient, backtest=False):
        self.backtest = backtest
        self.trading_client = trading_client
        logger.info(f"Archiver initialized with backtest={self.backtest}")
        self.account_history = self.get_account_history(trading_client)
        logger.info(f"Account history fetched: {len(self.account_history.equity)} records")
        orders = self.get_trades(trading_client=trading_client)
        logger.info(f"Orders fetched: {len(orders)} records")
        self.orders = orders if orders else []

    def get_account_history(self, trading_client: TradingClient):
        if self.backtest:
            db_table = "backtest_account_history"
            conn = None
            try:
                conn = duckdb.connect(f"{self.db_base_path}/{db_table}.db")
                account_history = conn.sql("SELECT * FROM account_history").df()
            except Exception as e:
                logger.exception("Error fetching account history", exc_info=e)
                return []
            finally:
                if conn is not None:
                    conn.close()
            return account_history
        else:
            get_portfolio_history_data = GetPortfolioHistoryRequest(
                period="12M",  # Use "1M" for one month (not "1Month")
                timeframe="1D",  # Use "1D" for daily data (minute timeframes not supported)
                limit=1000
            )
            
            try:
                account_history = trading_client.get_portfolio_history(
                    get_portfolio_history_data
                )

                return account_history
            except Exception as e:
                logger.error(f"Error fetching portfolio history: {e}")
                # Return an empty PortfolioHistory to avoid crashes
                from alpaca.trading.models import PortfolioHistory
                return PortfolioHistory(
                    timestamp=[], 
                    equity=[], 
                    profit_loss=[], 
                    profit_loss_pct=[], 
                    base_value=0.0,
                    timeframe="1D"
                )

    def get_trades(self, trading_client: TradingClient = None):
        """Fetch trades from the Alpaca account or backtest data."""
        if self.backtest:
            db_table = "backtest_trades"
            trades = None
            conn = None
            try:
                conn = duckdb.connect(f"{self.db_base_path}/{db_table}.db")
                trades = conn.sql("SELECT * FROM trades").df()
            except Exception as e:
                logger.exception("Error fetching trades", exc_info=e)
            finally:
                if conn is not None:
                    conn.close()
            return trades
    
        try:
            # Get the last 1000 orders including take profit and stop loss orders
            get_orders_data = GetOrdersRequest(
                status=QueryOrderStatus.CLOSED,
                limit=1000,
                nested=True  # This ensures child orders (stop loss/take profit) are included
            )

            orders = self.trading_client.get_orders(filter=get_orders_data)
            logger.info(f"Retrieved {len(orders)} orders from Alpaca")
            
            # Log order details for debugging
            for order in orders:
                logger.debug(f"Order {order.id}: {order.symbol} {order.side} - Status: {order.status} - Type: {getattr(order, 'type', 'N/A')} - Filled: {order.filled_qty}")
            
            # Only convert orders that have been filled (have execution data)
            trades = []
            for order in orders:
                if order.status == OrderStatus.FILLED and order.filled_qty:
                    trade = self._convert_trade(order)
                    if trade:
                        trades.append(trade)
            
            logger.info(f"Converted {len(trades)} orders to trades")
            self.orders = trades  # Store the trades in the instance variable
            return trades
        except Exception as e:
            logger.error(f"Failed to fetch trades: {e}")
            return []

    def _convert_trade(self, order):
        """Convert Alpaca Order object to a trade dictionary.
        
        Args:
            order: An Alpaca Order object
            
        Returns:
            dict: A dictionary representing a trade
        """
        # Only convert if the order has been filled
        if not order.filled_qty or not order.filled_avg_price:
            logger.debug(f"Skipping order {order.id} - not filled or missing price")
            return None
            
        try:
            # Get order type (market, limit, stop, etc.)
            order_type = getattr(order, 'type', 'unknown')
            
            # Determine if this was a stop loss or take profit
            # These are typically child orders with a parent order ID
            is_stop_loss = False
            is_take_profit = False
            
            if hasattr(order, 'order_class') and order.order_class == 'bracket':
                # For bracket orders
                is_take_profit = getattr(order, 'take_profit_price', None) is not None
                is_stop_loss = getattr(order, 'stop_loss_price', None) is not None
            elif hasattr(order, 'type'):
                # For stop/limit orders
                is_stop_loss = order.type in ['stop', 'stop_limit']
                is_take_profit = order.type in ['limit'] and order.side == 'sell'
                
            # Add parent order ID if available
            parent_order_id = getattr(order, 'parent_order_id', None)
                
            return {
                'id': str(order.id),  # Order ID as trade ID
                'ticker': order.symbol,
                'qty': float(order.filled_qty),  # Use filled quantity
                'side': order.side.name if hasattr(order.side, 'name') else str(order.side),
                'price': float(order.filled_avg_price),  # Use the filled average price
                'timestamp': order.filled_at.isoformat() if order.filled_at else datetime.now().isoformat(),
                'order_id': str(order.id),
                'status': order.status.name if hasattr(order.status, 'name') else str(order.status),
                'order_type': order_type,
                'is_stop_loss': is_stop_loss,
                'is_take_profit': is_take_profit,
                'parent_order_id': str(parent_order_id) if parent_order_id else None
            }
        except Exception as e:
            logger.error(f"Error converting order to trade: {e}, order: {order}")
            return None