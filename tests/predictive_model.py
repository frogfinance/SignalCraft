import os
import duckdb
import numpy as np
import pandas as pd
from alpaca.data import TimeFrame

class StockPredictor:
    def __init__(self, ticker, db_base_path='tests/data'):
        self.ticker = ticker
        self.db_base_path = db_base_path
        self.transition_matrix = None
        self.unique_states = None
        self.states = None
    
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
    
    def get_vix_data(self, timeframe=TimeFrame.Minute):
        query = f"""
        SELECT timestamp, close as vix
        FROM ticker_data
        ORDER BY timestamp DESC
        """
        conn = duckdb.connect(f"{self.db_base_path}/VIX_{timeframe}_data.db")
        df = conn.sql(query).fetchdf()
        conn.close()
        print(df)
        return df
    
    def train_markov_chain(self, data):
        # Fetch and preprocess VIX data
        vix_data = self.get_vix_data()
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        vix_data['timestamp'] = pd.to_datetime(vix_data['timestamp'])
        data = pd.merge(data, vix_data, on='timestamp', how='outer').sort_values(by='timestamp')
        data.interpolate(method='linear', inplace=True)
        data.dropna(inplace=True)

        if data.empty:
            raise ValueError("Data is empty after merging and interpolation.")

        # Extract states
        states = data[['close', 'volume', 'vix']].values

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
        print(f"Training complete. States: {len(states)}, Unique states: {len(unique_states)}")

    def predict(self, n_steps=5):
        predictions = []
        current_state = self.states[-1]
        state_index = np.where((self.unique_states == current_state).all(axis=1))[0][0]
        
        for _ in range(n_steps):
            next_state_index = np.random.choice(range(len(self.unique_states)), p=self.transition_matrix[state_index])
            next_state = self.unique_states[next_state_index]
            predictions.append(next_state)
            state_index = next_state_index
        
        return predictions

# Example usage:
# predictor = StockPredictor('AAPL', '/path/to/your/duckdb/file')
# historical_data = predictor.fetch_data()
# predictor.train_markov_chain(historical_data)
# current_state = historical_data[['close', 'volume', 'vix']].values[-1]
# predictions = predictor.predict(current_state)
# print(predictions)