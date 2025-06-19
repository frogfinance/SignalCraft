import duckdb
import logging
import threading
from datetime import datetime

from app.models.signal import Signal
from app.strategies.base import get_ticker_data, get_ticker_data_by_timeframe
from app.strategies.market_profile_strategy import MarketProfileStrategy
from app.strategies.markov_prediction_strategy import MarkovPredictionStrategy
from app.strategies.support_resistance_strategy import SupportResistanceStrategy
from alpaca.data import TimeFrame

from app.strategies.trend_following_strategy import TrendFollowingStrategy

logger = logging.getLogger("app")


class StrategyHandler():
    def __init__(self, tickers, db_base_path="dbs", timeframe=TimeFrame.Minute):
        super().__init__()
        self.db_base_path = db_base_path
        self.tickers = tickers
        self.timeframe = timeframe
        self.markov_prediction = MarkovPredictionStrategy(db_base_path=self.db_base_path)
        self.market_profile_strategy = MarketProfileStrategy(timeframe=self.timeframe)
        self.support_resistance_strategy = SupportResistanceStrategy()
        self.trend_following_strategy = TrendFollowingStrategy()
        self.supported_strategies = ['support_resistance', 'market_profile', 'trend_following']
        self.strategies = {
            'support_resistance': self.support_resistance_strategy,
            # 'markov': self.markov_prediction,
            # 'market_profile': self.market_profile_strategy
        }

        # Initialize signal tracking
        self.recent_signals = []
        self.signal_id_counter = 0
        self.signal_lock = threading.Lock()  # For thread safety

    def generate_signals(self, is_backtest=False, backtest_data=None):
        signal_data = dict()

        for ticker in self.tickers:
            if ticker in ['VXX']:
                continue
            connection = duckdb.connect(f"{self.db_base_path}/{ticker}_{self.timeframe}_data.db")
            if is_backtest:
                logger.debug("get backtest data: %r", backtest_data.get('end'))
                ticker_data = get_ticker_data_by_timeframe(ticker, connection, timeframe=self.timeframe, db_base_path=self.db_base_path, end=backtest_data['end'])
                # logger.info(f"Backtest data for {ticker}: {ticker_data.head()}")
            else:
                ticker_data = get_ticker_data(ticker, connection, timeframe=self.timeframe, db_base_path=self.db_base_path)    
                logger.debug('most recent ticker %r timestamp: %r', ticker, ticker_data['timestamp'].iloc[-1])
            connection.close()
            for strategy in self.strategies.values():
                if ticker_data.empty:
                    continue
                signal: Signal = strategy.generate_signal(ticker, ticker_data)
                if signal is not None and signal.action is not None:
                    logger.debug("Signal generated for %r: %r", ticker, signal)
                    signal_data[ticker] = signal
                    # After generating signals, store them
                    self.add_signal(signal)
            
        return signal_data

    def add_signal(self, signal):
        """Store a signal in the recent signals list."""
        with self.signal_lock:
            # Add unique ID and timestamp if not present
            if not hasattr(signal, 'id'):
                self.signal_id_counter += 1
                signal.id = self.signal_id_counter
            
            if not hasattr(signal, 'timestamp'):
                signal.timestamp = datetime.now().isoformat()
            
            # Convert to dict for storage
            signal_dict = {
                'id': signal.id,
                'ticker': signal.ticker,
                'side': signal.side.name if hasattr(signal.side, 'name') else str(signal.side),
                'price': float(signal.price),
                'timestamp': signal.timestamp,
                'strategy': signal.strategy,
                'metadata': getattr(signal, 'metadata', {})
            }
            
            # Add to recent signals
            self.recent_signals.append(signal_dict)
            
            # Keep only the latest N signals
            if len(self.recent_signals) > 100:
                self.recent_signals = self.recent_signals[-100:]

    def get_recent_signals(self, limit=20):
        """Get the most recent signals."""
        with self.signal_lock:
            return sorted(self.recent_signals, key=lambda x: x['id'], reverse=True)[:limit]

    def get_signals_since(self, last_id):
        """Get signals with ID greater than last_id."""
        with self.signal_lock:
            return [s for s in self.recent_signals if s['id'] > last_id]

    def get_strategies(self):
        strategies = [strat.to_dict() for strat in self.strategies.values()]
        return strategies