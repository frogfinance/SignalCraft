from datetime import datetime
from app.models.signal import Signal
from app.strategies.base import BaseStrategy
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger("app")


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

    def generate_signal(self, ticker, data) -> Signal:
        """
        Generate buy or sell signals based on the Markov prediction model.
        """
        price = data.iloc[-1]['close']
        last_close = data['close'].iloc[-1]
        last_candle_data = data.iloc[-1]
        logger.info("Price: {}".format(price))
        logger.info(f"Last close price: {last_close}")
        logger.info(f"Last candle data: {last_candle_data}")
        logger.info(f"Generating signal for {ticker} price={price}")
        if price is None or price == 0:
            return Signal(strategy=self.name, ticker=ticker)
        signal = Signal(strategy=self.name, ticker=ticker, price=price)
        logger.info(f"Signal: {signal}")
        if data.empty:
            logger.warning(f"No data available for ticker {ticker}. Skipping signal generation.")
            return signal
        # Ensure the timestamp aligns with 15-minute intervals and there are enough data points
        timestamp = data['timestamp'].iloc[-1]
        if timestamp.minute % 15 != 0 or data.shape[0] < 15:
            return signal
        try:
            current_close, predicted_close = self.make_prediction(data)
            logger.info(f"Predicted close price for {ticker}: {current_close} -> {predicted_close}")
        except ValueError as e:
            logger.error(f"Failed to make prediction for {ticker}: {e}")
            return signal
        if predicted_close > current_close * 1.01:
            signal.buy()
            signal.reason = 'Predicted close is significantly higher than current close'
        elif predicted_close < current_close * 0.99:
            signal.sell()
            signal.reason = 'Predicted close is significantly lower than current close'

        logger.info(f"Signal generated for {ticker}: {signal}")
        return signal

    def make_prediction(self, ticker_data, interval="15min", n_simulations=5000):
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
        vxx_data = self.fetch_vxx_data(end=ticker_data['timestamp'].iloc[-1])
        logger.debug(f"VXX data: {vxx_data.head()}")
        ticker_data = pd.merge(ticker_data, vxx_data, on='timestamp', how='outer').sort_values(by='timestamp')
        ticker_data.infer_objects(copy=False)
        ticker_data.interpolate(method='linear', inplace=True)
        ticker_data.dropna(inplace=True)
        logger.debug(f"Merged data: {ticker_data.head()}")

        # Resample data
        ticker_data = self.resample_data(ticker_data, interval=interval)
        if ticker_data.empty:
            raise ValueError(f"Resampled data is empty. Cannot make predictions for interval {interval}.")

        logger.debug(f"Resampled data: {ticker_data.iloc[-1]}")
        # Train Markov chain
        self.train_markov_chain(ticker_data)
        
        # Get the current state
        current_state = ticker_data[['close', 'volume', 'vwap', 'vxx']].values[-1]
        logger.debug('Current state: {}'.format(current_state))
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
        logger.debug(f"Close price: {ticker_data['close'].iloc[-1]}")
        logger.debug(f"Predicted close price: {predicted_close}")
        current_close = ticker_data['close'].iloc[-1]
        return current_close, predicted_close


    def predict_next_state(self, current_state, n_steps=1):
        state_index = np.where((self.unique_states == current_state).all(axis=1))[0][0]
        for _ in range(n_steps):
            next_state_index = np.random.choice(range(len(self.unique_states)), p=self.transition_matrix[state_index])
            next_state = self.unique_states[next_state_index]
            state_index = next_state_index
        return next_state

    def resample_data(self, data, interval="15min"):
        """Resample minute-level data into 15-minute intervals."""
        data['timestamp'] = pd.to_datetime(data['timestamp'])  # Ensure timestamp is datetime
        data.set_index('timestamp', inplace=True)
        def safe_last(x):
            return x.iloc[-1] if len(x) > 0 else np.nan
        
        aggregated = data.resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'vwap': lambda x: (x * data['volume']).sum() / data['volume'].sum() if data['volume'].sum() > 0 else None,
            'vxx': safe_last  
        }).dropna()

        aggregated.reset_index(inplace=True)
        return aggregated

    def train_markov_chain(self, data):
        logger.info(f"Training Markov chain with data: {data.head()}")
        # data = self.discretize_features(data)
        states = data[['close', 'volume', 'vwap', 'vxx']].values
        unique_states, indices = np.unique(states, axis=0, return_inverse=True)
        n_states = len(unique_states)
        transition_matrix = np.zeros((n_states, n_states))
        
        for (i, j) in zip(indices, indices[1:]):
            transition_matrix[i, j] += 1
        
        transition_matrix = (transition_matrix + 1) / (transition_matrix.sum(axis=1, keepdims=True) + n_states)
        self.transition_matrix = transition_matrix
        self.unique_states = unique_states
