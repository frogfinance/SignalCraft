import pytest
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from app.strategies.support_resistance_strategy import SupportResistanceStrategy
from app.models.signal import Signal
import numpy as np

@pytest.fixture
def strategy():
    return SupportResistanceStrategy()

@pytest.fixture
def sample_data():
    """Load real data from DuckDB for testing"""
    ticker = "AAPL"  # Using AAPL as example
    conn = duckdb.connect(f"dbs/{ticker}_1min_data.db")
    
    # Get a sample of data (e.g., 90 days worth of minute data)
    query = """
    SELECT timestamp, open, high, low, close, volume
    FROM ticker_data
    WHERE timestamp >= (SELECT MAX(timestamp) - INTERVAL '90 days' FROM ticker_data)
    ORDER BY timestamp
    """
    
    df = conn.execute(query).fetchdf()
    conn.close()
    return df

def test_strategy_initialization(strategy):
    """Test strategy initialization and parameters"""
    assert strategy.lookback == 360
    assert strategy.support_threshold == 0.015
    assert strategy.resistance_threshold == 0.02
    assert strategy.name == 'support_resistance'
    assert strategy.display_name == 'Support & Resistance'
    assert strategy.time_interval == "15min"

def test_resample_data(strategy, sample_data):
    """Test data resampling functionality"""
    resampled = strategy.resample_data(sample_data, interval="15min")
    
    assert isinstance(resampled, pd.DataFrame)
    assert all(col in resampled.columns for col in ['open', 'high', 'low', 'close', 'volume'])
    assert len(resampled) < len(sample_data)  # Resampled data should have fewer rows

def test_find_support_resistance(strategy, sample_data):
    """Test support and resistance level identification"""
    resampled = strategy.resample_data(sample_data)
    support_levels, resistance_levels = strategy.find_support_resistance(resampled)
    
    assert len(support_levels) > 0
    assert len(resistance_levels) > 0
    assert all(isinstance(level, (int, float)) for level in support_levels)
    assert all(isinstance(level, (int, float)) for level in resistance_levels)

def test_generate_signal_buy_condition(strategy, sample_data):
    """Test buy signal generation near support levels"""
    ticker = "AAPL"

    # Make sure timestamp is a column, not an index
    if isinstance(sample_data.index, pd.DatetimeIndex):
        sample_data = sample_data.reset_index()
    
    # slice sample_data (90 days) into 10 day chunks where the 0th index is the oldest
    chunks = [sample_data[i:i+72].copy() for i in range(0, len(sample_data), 72)]



    signals = []

    for chunk in chunks:
        # Ensure timestamp column exists and is datetime
        if 'timestamp' not in chunk.columns:
            raise ValueError("DataFrame must have a 'timestamp' column")
            
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'])
        
        resampled = strategy.resample_data(chunk)
        support_levels, _ = strategy.find_support_resistance(resampled)
        
        if len(support_levels) > 0:
            # Set the last close price to be just above a support level
            support_price = support_levels[0]
            # Cast the value to float32 explicitly
            new_price = np.float32(support_price * (1 + strategy.support_threshold/2))
            chunk.iloc[-1, chunk.columns.get_loc('close')] = new_price
            
            signal = strategy.generate_signal(ticker, chunk)
            
            if signal and signal.action == "buy":
                signals.append(signal)

    buy_signals = [signal for signal in signals if signal.action == "buy"]
    if len(buy_signals) == 0:
        assert False, "No buy signals generated"
    for signal in buy_signals:
        assert signal.ticker == ticker
        assert signal.price is not None
        if signal.action is not None:
            assert signal.action in ["buy"]
        if signal.action == "buy":
            assert "support" in signal.reason


def test_generate_signal_sell_condition(strategy, sample_data):
    """Test sell signal generation near resistance levels"""
    ticker = "AAPL"
    
    # Make sure timestamp is a column, not an index
    if isinstance(sample_data.index, pd.DatetimeIndex):
        sample_data = sample_data.reset_index()
    
    # slice sample_data (90 days) into 10 day chunks where the 0th index is the oldest
    chunks = [sample_data[i:i+72].copy() for i in range(0, len(sample_data), 72)]

    signals = []

    for chunk in chunks:
        # Ensure timestamp column exists and is datetime
        if 'timestamp' not in chunk.columns:
            raise ValueError("DataFrame must have a 'timestamp' column")
            
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'])
        
        resampled = strategy.resample_data(chunk)
        _, resistance_levels = strategy.find_support_resistance(resampled)
        
        if len(resistance_levels) > 0:
            # Set the last close price to be just below resistance level
            resistance_price = resistance_levels[0]
            # Cast the value to float32 explicitly
            new_price = np.float32(resistance_price * (1 - strategy.resistance_threshold/2))
            chunk.iloc[-1, chunk.columns.get_loc('close')] = new_price
            
            signal = strategy.generate_signal(ticker, chunk)
            
            if signal:
                signals.append(signal)

    for signal in signals:
        if signal.action is not None:
            assert signal.ticker == ticker
            assert signal.action == "CLOSE"
            assert "resistance" in signal.reason.lower()


def test_generate_signal_no_condition(strategy, sample_data):
    """Test no signal generation when price is not near support/resistance"""
    ticker = "AAPL"
    
    # Modify the last price to be far from support/resistance levels
    resampled = strategy.resample_data(sample_data)
    support_levels, resistance_levels = strategy.find_support_resistance(resampled)
    
    if len(support_levels) > 0 and len(resistance_levels) > 0:
        # Set the last close price to be in the middle of support and resistance
        mid_price = (support_levels[0] + resistance_levels[0]) / 2
        sample_data.iloc[-1, sample_data.columns.get_loc('close')] = mid_price
        
        signal = strategy.generate_signal(ticker, sample_data)
        
        assert signal.ticker == ticker
        assert signal.side is None
        assert signal.reason is None

def test_strategy_to_dict(strategy):
    """Test strategy serialization to dictionary"""
    strategy_dict = strategy.to_dict()
    
    assert strategy_dict["name"] == "support_resistance"
    assert strategy_dict["display_name"] == "Support & Resistance"
    assert strategy_dict["lookback"] == 360
    assert strategy_dict["support_threshold"] == 0.015
    assert strategy_dict["resistance_threshold"] == 0.02
    assert strategy_dict["time_interval"] == "15min"
