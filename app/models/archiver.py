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
            # Get the last 1000 filled orders
            get_orders_data = GetOrdersRequest(
                status=QueryOrderStatus.CLOSED,
                limit=1000,
                nested=True  
            )

            orders = self.trading_client.get_orders(filter=get_orders_data)
            # Only convert orders that have been filled (have execution data)
            trades = [self._convert_trade(order) for order in orders if order.status == OrderStatus.FILLED and order.filled_qty]
            self.orders = trades  # Store the trades in the instance variable
        except Exception as e:
            logger.error(f"Failed to fetch trades: {e}")
            orders = []

        return self.orders

        
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
            return {
                'id': str(order.id),  # Order ID as trade ID
                'ticker': order.symbol,
                'qty': float(order.filled_qty),  # Use filled quantity
                'side': order.side.name if hasattr(order.side, 'name') else str(order.side),
                'price': float(order.filled_avg_price),  # Use the filled average price
                'timestamp': order.filled_at.isoformat() if order.filled_at else datetime.now().isoformat(),
                'order_id': str(order.id),
                'status': order.status.name if hasattr(order.status, 'name') else str(order.status)
            }
        except Exception as e:
            logger.error(f"Error converting order to trade: {e}, order: {order}")
            return None