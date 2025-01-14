import os
import duckdb
import numpy as np
import pandas as pd
from alpaca.data import TimeFrame
import logging
from hmmlearn.hmm import GaussianHMM 

class StockPredictor:
    def __init__(self, ticker, db_base_path='tests/data'):
        self.ticker = ticker
        self.db_base_path = db_base_path
        self.transition_matrix = None
        self.unique_states = None
        self.states = None
        self.close_mean = None
        self.close_std = None
        self.volume_mean = None
        self.volume_std = None
        self.vxx_mean = None
        self.vxx_std = None
    
    def fetch_data(self, timeframe=TimeFrame.Minute):
        if not os.path.exists(f"{self.db_base_path}/{self.ticker}_{timeframe}_data.db"):
            raise FileNotFoundError(f"Database file for {self.ticker} not found at {self.db_base_path}/{self.ticker}_{timeframe}_data.db")

        stock_query = f"""
        SELECT timestamp, close, volume
        FROM ticker_data
        ORDER BY timestamp DESC
        """
        
        stock_conn = duckdb.connect(f"{self.db_base_path}/{self.ticker}_{timeframe}_data.db")
        stock_df = stock_conn.sql(stock_query).fetchdf()
        stock_conn.close()
        # print(stock_df)
        return stock_df
    
    def get_vxx_data(self, timeframe=TimeFrame.Minute):
        query = f"""
        SELECT timestamp, close as vxx
        FROM ticker_data
        ORDER BY timestamp DESC
        """
        conn = duckdb.connect(f"{self.db_base_path}/VXX_{timeframe}_data.db")
        df = conn.sql(query).fetchdf()
        conn.close()
        return df
    
    def train_markov_chain(self, data):
        # Fetch and preprocess vxx data
        vxx_data = self.get_vxx_data()
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        vxx_data['timestamp'] = pd.to_datetime(vxx_data['timestamp'])
        data = pd.merge(data, vxx_data, on='timestamp', how='outer').sort_values(by='timestamp')
        data.interpolate(method='linear', inplace=True)
        data.dropna(inplace=True)

        if data.empty:
            raise ValueError("Data is empty after merging and interpolation.")

        # Compute and store mean and std for normalization
        self.close_mean = data['close'].mean()
        self.close_std = data['close'].std()
        self.volume_mean = data['volume'].mean()
        self.volume_std = data['volume'].std()
        self.vxx_mean = data['vxx'].mean()
        self.vxx_std = data['vxx'].std()

        # Normalize features
        data['close_norm'] = (data['close'] - self.close_mean) / self.close_std
        data['volume_norm'] = (data['volume'] - self.volume_mean) / self.volume_std
        data['vxx_norm'] = (data['vxx'] - self.vxx_mean) / self.vxx_std

        # Extract normalized states
        states = data[['close_norm', 'volume_norm', 'vxx_norm']].values

        if states.size == 0:
            raise ValueError("No valid states found after processing the data.")

        unique_states, indices = np.unique(states, axis=0, return_inverse=True)

        n_states = len(unique_states)
        transition_matrix = np.zeros((n_states, n_states))

        for (i, j) in zip(indices, indices[1:]):
            transition_matrix[i, j] += 1

        # Normalize the transition matrix with smoothing
        transition_matrix = (transition_matrix + 1) / (transition_matrix.sum(axis=1, keepdims=True) + n_states)

        self.transition_matrix = transition_matrix
        self.unique_states = unique_states
        self.states = states
        logging.info(f"Training complete. States: {len(states)}, Unique states: {len(unique_states)}")


    def predict(self, n_steps=5):
        """
        Predict the next `n_steps` states using the Markov chain model.

        Args:
            n_steps (int): The number of predictions to generate.

        Returns:
            dict: Predictions containing 'close', 'volume', and 'vxx' for the next `n_steps`.
        """
        if self.states is None or self.transition_matrix is None:
            raise ValueError("Model is not trained. Call `train_markov_chain` first.")

        predictions = {'close': [], 'volume': [], 'vxx': []}

        current_state = self.states[-1]
        state_index = np.where((self.unique_states == current_state).all(axis=1))[0]
        if len(state_index) == 0:
            raise ValueError("Current state not found in unique states.")
        state_index = state_index[0]

        for _ in range(n_steps):
            next_state_index = np.random.choice(
                range(len(self.unique_states)),
                p=self.transition_matrix[state_index]
            )
            next_state = self.unique_states[next_state_index]

            # Append the predictions for each state component
            predictions['close'].append(next_state[0] * self.close_std + self.close_mean)
            predictions['volume'].append(next_state[1] * self.volume_std + self.volume_mean)
            predictions['vxx'].append(next_state[2] * self.vxx_std + self.vxx_mean)

            state_index = next_state_index

        return predictions



# Example usage:
# predictor = StockPredictor('AAPL', '/path/to/your/duckdb/file')
# historical_data = predictor.fetch_data()
# predictor.train_markov_chain(historical_data)
# current_state = historical_data[['close', 'volume', 'vxx']].values[-1]
# predictions = predictor.predict(current_state)
# print(predictions)

class HMMStockPredictor:
    def __init__(self, ticker, db_base_path='tests/data', n_states=3):
        self.ticker = ticker
        self.db_base_path = db_base_path
        self.n_states = n_states
        self.model = None
        self.scaler = None

    def fetch_data(self, timeframe=TimeFrame.Minute):
        if not os.path.exists(f"{self.db_base_path}/{self.ticker}_{timeframe}_data.db"):
            raise FileNotFoundError(f"Database file for {self.ticker} not found at {self.db_base_path}/{self.ticker}_{timeframe}_data.db")

        stock_query = f"""
        SELECT timestamp, close, volume
        FROM ticker_data
        ORDER BY timestamp DESC
        """
        
        stock_conn = duckdb.connect(f"{self.db_base_path}/{self.ticker}_{timeframe}_data.db")
        stock_df = stock_conn.sql(stock_query).fetchdf()
        stock_conn.close()
        # print(stock_df)
        return stock_df

    def train_model(self, data):
        """
        Train an HMM on the historical data.
        """
        # Features: Close price difference, log volume
        data['close_diff'] = data['close'].diff().fillna(0)
        features = data[['close_diff', 'volume']].values

        # Train HMM
        self.model = GaussianHMM(n_components=self.n_states, covariance_type="diag", n_iter=100)
        self.model.fit(features)

    def predict(self, current_state, n_steps=5):
        """
        Predict the next n_steps using the trained HMM.
        """
        if self.model is None:
            raise ValueError("HMM model is not trained. Call `train_model` first.")

        predictions = []
        state_sequence = self.model.predict(current_state.reshape(1, -1))
        
        for _ in range(n_steps):
            next_state = np.random.choice(
                range(self.n_states), 
                p=self.model.transmat_[state_sequence[-1]]
            )
            next_observation = self.model.sample()[0]
            state_sequence = np.append(state_sequence, next_state)
            predictions.append(next_observation)

        # Reverse feature engineering: Derive close price from differences
        predicted_data = {
            'close': [current_state[0] + obs[0] for obs in predictions],
            'volume': [obs[1] * 1e6 for obs in predictions],
        }
        return predicted_data

