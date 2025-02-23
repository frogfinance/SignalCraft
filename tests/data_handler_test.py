import pytest
import os
import pandas as pd
from unittest.mock import patch, MagicMock
from app.handlers.data_handler import DataHandler
from alpaca.data import TimeFrame
from datetime import datetime

# Mock environment variables
os.environ['ALPACA_API_KEY_PAPER'] = 'mock_api_key'
os.environ['ALPACA_SECRET_KEY_PAPER'] = 'mock_secret_key'

@pytest.fixture
def data_handler():
    """Fixture to create a DataHandler instance with mock attributes."""
    handler = DataHandler(['AAPL'], 
                       os.environ['ALPACA_API_KEY_PAPER'],
                        os.environ['ALPACA_SECRET_KEY_PAPER'], 
                        db_base_path='tests/data', 
                        timeframe=TimeFrame.Day)
    handler.data_store = MagicMock()  # Mock data_store API calls
    handler.save_market_data = MagicMock()  # Mock data saving
    return handler

@patch("app.handlers.data_handler.duckdb.connect")
def test_fetch_data_most_recent(mock_duckdb, data_handler):
    """Test fetch_data with use_most_recent=True to get the latest candle timestamp."""
    mock_conn = MagicMock()
    mock_duckdb.return_value = mock_conn

    # Simulate the latest timestamp in the database
    mock_df = MagicMock()
    mock_df.empty = False
    mock_df["timestamp"].iloc[0] = datetime(2024, 2, 1, 14, 30)  # Mock latest timestamp
    mock_conn.sql.return_value.df.return_value = mock_df

    # Run the function with use_most_recent=True
    data_handler.fetch_data(use_most_recent=True)

    # Assert start date was set to the most recent candle timestamp
    assert mock_conn.sql.called
    mock_conn.close.assert_called_once()

@patch("app.handlers.data_handler.duckdb.connect")
def test_fetch_data_normal(mock_duckdb, data_handler):
    """Test fetch_data for a normal date range request."""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 3)

    # Mock API response
    mock_response = MagicMock()
    mock_response.data = dict(data={"AAPL": [{"timestamp": "2024-01-01T09:30:00Z", "open": 150, "close": 155}]})
    data_handler.data_store.get_stock_bars.return_value = mock_response

    # Run function
    data_handler.fetch_data(start=start_date, end=end_date)

    # Assert API request was made
    data_handler.data_store.get_stock_bars.assert_called()
    data_handler.save_market_data.assert_called()

@patch("app.handlers.data_handler.duckdb.connect")
def test_fetch_data_no_data(mock_duckdb, data_handler):
    """Test fetch_data when no data is returned from API."""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 3)

    # Simulate API returning no data
    data_handler.data_store.get_stock_bars.return_value = None

    # Run function
    data_handler.fetch_data(start=start_date, end=end_date)

    # Assert that no data saving was attempted
    data_handler.save_market_data.assert_not_called()

@patch("app.handlers.data_handler.duckdb.connect")
def test_fetch_data_handles_exceptions(mock_duckdb, data_handler):
    """Test fetch_data gracefully handles exceptions."""
    mock_conn = MagicMock()
    mock_duckdb.return_value = mock_conn
    mock_conn.sql.side_effect = Exception("Database error")

    # Run function
    with pytest.raises(Exception):
        data_handler.fetch_data(use_most_recent=True)
