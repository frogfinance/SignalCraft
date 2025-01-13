from app.strategies.base import BaseStrategy
import numpy as np
import pandas as pd
import duckdb
from alpaca.data import TimeFrame

class MarkovPredictionStrategy(BaseStrategy):

    def __init__(self, db_base_path='dbs'):
        super().__init__()
        self.transition_matrix = None
        self.unique_states = None
        self.db_base_path = db_base_path
        # self.signal_strategy = SignalStrategy()

    def discretize_features(self, data, n_bins=10):
        for col in ['close', 'volume', 'vwap', 'vix']:
            data[col] = pd.qcut(data[col], q=n_bins, labels=False, duplicates="drop")
        return data
    
    def fetch_vix_data(self, timeframe=TimeFrame.Minute):
        query = """
        SELECT timestamp, close as vix
        FROM ticker_data
        ORDER BY timestamp DESC
        """
        conn = duckdb.connect(f"{self.db_base_path}/VIX_{timeframe}_data.db")
        df = conn.sql(query).fetchdf()
        conn.close()
        return df

    def generate_signal(self, ticker_data):
        # based on 15m intervals
        # if this current ticker_data is not a 15m interval, skip signal generation
        timestamp = ticker_data['timestamp'].iloc[-1]
        if timestamp.minute % 15 != 0:
            return {'buy': False, 'sell': False}
        
        vix_data = self.fetch_vix_data()
        ticker_data = pd.merge(ticker_data, vix_data, on='timestamp', how='outer').sort_values(by='timestamp')
        ticker_data.interpolate(method='linear', inplace=True)
        ticker_data.dropna(inplace=True)

        # Resample data into 15-minute intervals
        ticker_data = self.resample_data(ticker_data, interval="15T")
        
        self.train_markov_chain(ticker_data)
        current_state = ticker_data[['close', 'volume', 'vwap', 'vix']].values[-1]
        predicted_state = self.predict_next_state(current_state)
        
        current_close = current_state[0]
        predicted_close = predicted_state[0]
        
        if predicted_close > current_close * 1.01:
            return {'buy': True, 'sell': False, 'reason': 'predicted_close > current_close * 1.01', 'strategy': 'markov'}
        elif predicted_close < current_close * 0.99:
            return {'buy': False, 'sell': True, "reason": 'predicted_close < current_close * 0.99', 'strategy': 'markov'}
        return {'buy': False, 'sell': False, 'reason': 'no signal', 'strategy': 'markov'}

    def predict_next_state(self, current_state, n_steps=1):
        state_index = np.where((self.unique_states == current_state).all(axis=1))[0][0]
        for _ in range(n_steps):
            next_state_index = np.random.choice(range(len(self.unique_states)), p=self.transition_matrix[state_index])
            next_state = self.unique_states[next_state_index]
            state_index = next_state_index
        return next_state

    def resample_data(data, interval="15T"):
        """Resample minute-level data into 15-minute intervals."""
        data['timestamp'] = pd.to_datetime(data['timestamp'])  # Ensure timestamp is datetime
        data.set_index('timestamp', inplace=True)

        aggregated = data.resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'vwap': lambda x: (x * data['volume']).sum() / data['volume'].sum() if data['volume'].sum() > 0 else None
        }).dropna()

        aggregated.reset_index(inplace=True)
        return aggregated

    def train_markov_chain(self, data):
        data = self.discretize_features(data)
        states = data[['close', 'volume', 'vwap', 'vix']].values
        unique_states, indices = np.unique(states, axis=0, return_inverse=True)
        n_states = len(unique_states)
        transition_matrix = np.zeros((n_states, n_states))
        
        for (i, j) in zip(indices, indices[1:]):
            transition_matrix[i, j] += 1
        
        transition_matrix = (transition_matrix + 1) / (transition_matrix.sum(axis=1, keepdims=True) + n_states)
        self.transition_matrix = transition_matrix
        self.unique_states = unique_states
