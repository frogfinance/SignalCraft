import duckdb

from app.strategies.base import get_ticker_data
from app.strategies.market_profile_strategy import MarketProfileStrategy
from app.strategies.markov_prediction_strategy import MarkovPredictionStrategy
from alpaca.data import TimeFrame


class StrategyHandler():
    def __init__(self, tickers, db_base_path="dbs", timeframe=TimeFrame.Minute):
        super().__init__()
        self.db_base_path = db_base_path
        self.tickers = tickers
        self.timeframe = timeframe
        self.markov_prediction = MarkovPredictionStrategy(db_base_path=self.db_base_path)
        self.market_profile_strategy = MarketProfileStrategy()
        self.strategies = {
            'markov': self.markov_prediction
        }

    def generate_signals(self, historical_data=None):
        signal_data = dict()
        for ticker in self.tickers:
            if historical_data is None:
                connection = duckdb.connect(f"{self.db_base_path}/{ticker}_{self.timeframe}_data.db")
                ticker_data = get_ticker_data(ticker, connection, timeframe=self.timeframe, db_base_path=self.db_base_path)
                connection.close()
            else:
                ticker_data = historical_data[ticker]
            most_recent_ticker_datetime = ticker_data['timestamp'].max()
            for _, strategy in self.strategies.items():
                signal_data = strategy.generate_signal(ticker, ticker_data)
                signal_data['timestamp'] = most_recent_ticker_datetime
                if signal_data['buy'] or signal_data['sell']:
                    signal_data[ticker] = signal_data
            
        return signal_data
