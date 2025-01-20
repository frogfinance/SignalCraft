from datetime import datetime
from app.models.signal import Signal
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
        self.name = 'markov'
        # self.signal_strategy = SignalStrategy()

    def discretize_features(self, data, n_bins=10):
        for col in ['close', 'volume', 'vwap', 'vxx']:
            data[col] = pd.qcut(data[col], q=n_bins, labels=False, duplicates="drop")
        return data

    def generate_signal(self, ticker_data):
        """
        Generate buy or sell signals based on the Markov prediction model.
        """
        signal = Signal(strategy=self.name, ticker=ticker_data['ticker'].iloc[0])
        # Ensure the timestamp aligns with 15-minute intervals and there are enough data points
        timestamp = ticker_data['timestamp'].iloc[-1]
        if timestamp.minute % 15 != 0 or ticker_data.shape[0] < 15:
            return signal

        current_close, predicted_close = self.make_prediction(ticker_data)

        if predicted_close > current_close * 1.01:
            signal.buy()
            signal.reason = 'Predicted close is significantly higher than current close'
        elif predicted_close < current_close * 0.99:
            signal.sell()
            signal.reason = 'Predicted close is significantly lower than current close'

        return signal

    def make_prediction(self, ticker_data, interval="15T", n_simulations=5000):
        """
        Predict the next close price using Markov chain simulations.
        
        Args:
            ticker_data: DataFrame containing ticker data.
            interval: Resampling interval (e.g., "15T" for 15 minutes).
            n_simulations: Number of simulations to run.
        
        Returns:
            current_close: The current close price.
            predicted_close: The most commonly predicted close price.
        """
        # Fetch and preprocess VXX data
        vxx_data = self.fetch_vxx_data()
        ticker_data = pd.merge(ticker_data, vxx_data, on='timestamp', how='outer').sort_values(by='timestamp')
        ticker_data.interpolate(method='linear', inplace=True)
        ticker_data.dropna(inplace=True)

        # Resample data
        ticker_data = self.resample_data(ticker_data, interval=interval)
        
        # Train Markov chain
        self.train_markov_chain(ticker_data)
        
        # Get the current state
        current_state = ticker_data[['close', 'volume', 'vwap', 'vxx']].values[-1]
        
        # Simulate future states
        predictions = []
        for _ in range(n_simulations):
            state_index = np.where((self.unique_states == current_state).all(axis=1))[0]
            if len(state_index) == 0:
                raise ValueError("Current state not found in unique states.")
            state_index = state_index[0]

            # Predict the next state
            next_state_index = np.random.choice(
                range(len(self.unique_states)),
                p=self.transition_matrix[state_index]
            )
            predictions.append(self.unique_states[next_state_index][0])  # Append the predicted close price

        # Determine the most common predicted close price
        predicted_close = np.mean(predictions)  # Alternatively, use np.median or mode
        current_close = current_state[0]
        return current_close, predicted_close


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
        states = data[['close', 'volume', 'vwap', 'vxx']].values
        unique_states, indices = np.unique(states, axis=0, return_inverse=True)
        n_states = len(unique_states)
        transition_matrix = np.zeros((n_states, n_states))
        
        for (i, j) in zip(indices, indices[1:]):
            transition_matrix[i, j] += 1
        
        transition_matrix = (transition_matrix + 1) / (transition_matrix.sum(axis=1, keepdims=True) + n_states)
        self.transition_matrix = transition_matrix
        self.unique_states = unique_states
