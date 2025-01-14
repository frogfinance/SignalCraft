import pytest
import os
import pandas as pd
from unittest.mock import patch, MagicMock
from app.handlers.data_handler import DataHandler
from alpaca.data import TimeFrame

# Mock environment variables
os.environ['ALPACA_API_KEY_PAPER'] = 'mock_api_key'
os.environ['ALPACA_SECRET_KEY_PAPER'] = 'mock_secret_key'

@pytest.fixture
def data_handler():
    return DataHandler(['VIX'], os.environ['ALPACA_API_KEY_PAPER'], os.environ['ALPACA_SECRET_KEY_PAPER'], db_base_path='dbs', timeframe=TimeFrame.Day)

@pytest.fixture
def mock_ticker_data():
    data = {
        'timestamp': pd.date_range(start='2023-01-01', periods=5, freq='T'),
        'close': [100, 101, 102, 103, 104],
        'volume': [10, 20, 30, 40, 50],
        'vwap': [100, 101, 102, 103, 104]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_vix_data():
    data = {
        'timestamp': pd.date_range(start='2023-01-01', periods=5, freq='T'),
        'vix': [20, 21, 22, 23, 24]
    }
    return pd.DataFrame(data)

@patch('app.handlers.data_handler.duckdb.connect')
def test_fetch_data(mock_duckdb_connect, data_handler, mock_ticker_data):
    mock_conn = MagicMock()
    mock_conn.sql.return_value.fetchdf.return_value = mock_ticker_data
    mock_duckdb_connect.return_value = mock_conn

    ticker_data = data_handler.fetch_data()
    assert not ticker_data.empty
    assert list(ticker_data.columns) == ['timestamp', 'close', 'volume', 'vwap']

@patch('app.handlers.data_handler.duckdb.connect')
def test_fetch_vix_data(mock_duckdb_connect, data_handler, mock_vix_data):
    mock_conn = MagicMock()
    mock_conn.sql.return_value.fetchdf.return_value = mock_vix_data
    mock_duckdb_connect.return_value = mock_conn

    vix_data = data_handler.fetch_vix_data()
    assert not vix_data.empty
    assert list(vix_data.columns) == ['timestamp', 'vix']

if __name__ == "__main__":
    pytest.main()