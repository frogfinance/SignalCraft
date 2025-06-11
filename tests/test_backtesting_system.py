import pytest
import asyncio
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
from app.backtester import BacktestingSystem
from alpaca.data import TimeFrame


@pytest.fixture
def mock_data():
    """Fixture to create sample backtest data."""
    return pd.DataFrame([
        {"timestamp": datetime(2024, 2, 1, 9, 30), "open": 100, "high": 110, "low": 95, "close": 105},
        {"timestamp": datetime(2024, 2, 1, 10, 30), "open": 106, "high": 112, "low": 100, "close": 108},
        {"timestamp": datetime(2024, 2, 2, 9, 30), "open": 109, "high": 115, "low": 107, "close": 110},
    ])


@pytest.fixture
def backtest_system():
    """Fixture to create a BacktestingSystem instance with mock dependencies."""
    system = BacktestingSystem(
        tickers=["AAPL"], 
        api_key="mock_api_key", 
        api_secret="mock_api_secret", 
        timeframe=TimeFrame.Minute
    )

    # Mock handlers
    system.execution_handler = MagicMock()
    system.data_handler = MagicMock()
    system.strategy_handler = MagicMock()
    system.ws_manager = AsyncMock()
    
    return system


def test_initialization(backtest_system):
    """Test proper initialization of BacktestingSystem."""
    assert backtest_system.tickers == ["AAPL"]
    assert str(backtest_system.timeframe) == str(TimeFrame.Minute)
    assert backtest_system.trade_results == []
    assert backtest_system.task is None
    assert isinstance(backtest_system.running_backtests, dict)


def test_change_tickers(backtest_system):
    """Test changing tickers updates all necessary components."""
    new_tickers = ["TSLA", "GOOGL"]
    backtest_system.change_tickers(new_tickers)
    
    assert backtest_system.tickers == new_tickers
    assert backtest_system.data_handler.tickers == new_tickers
    assert backtest_system.strategy_handler.tickers == new_tickers


def test_is_market_open(backtest_system):
    """Test market hours detection logic."""
    # Test regular market hours
    assert backtest_system.is_market_open(datetime(2024, 2, 5, 10, 0))  # Monday 10:00
    assert backtest_system.is_market_open(datetime(2024, 2, 5, 15, 59))  # Monday 15:59
    
    # Test market closed scenarios
    assert not backtest_system.is_market_open(datetime(2024, 2, 3, 10, 0))  # Saturday
    assert not backtest_system.is_market_open(datetime(2024, 2, 4, 10, 0))  # Sunday
    assert not backtest_system.is_market_open(datetime(2024, 2, 5, 9, 29))  # Before market open
    assert not backtest_system.is_market_open(datetime(2024, 2, 5, 16, 0))  # After market close


def test_serialize_ticker_data(backtest_system, mock_data):
    """Test data serialization for daily candlesticks."""
    result = backtest_system.serialize_ticker_data(mock_data)
    
    assert len(result) == 2  # Two unique dates
    assert result[0]["timestamp"] == "2024-02-01"
    assert result[0]["high"] == 112  # Max high for first day
    assert result[1]["open"] == 109  # Opening price for second day


@pytest.mark.asyncio
async def test_run_backtest_basic_flow(backtest_system):
    """Test basic flow of backtest execution."""
    # Setup mock data with proper datetime objects
    mock_ticker_data = pd.DataFrame({
        "timestamp": [
            datetime(2024, 2, 1, 10, 0),
            datetime(2024, 2, 1, 10, 1),
            datetime(2024, 2, 1, 10, 2)
        ],
        "open": [100, 101, 102],
        "high": [110, 111, 112],
        "low": [95, 96, 97],
        "close": [105, 106, 107]
    })
    
    # Create backtest data dictionary with proper structure
    backtest_data = {
        backtest_system.tickers[0]: mock_ticker_data,
        "end": datetime(2024, 2, 1, 10, 0)  # Initial end time
    }
    
    backtest_system.data_handler.get_backtest_data.return_value = backtest_data
    backtest_system.strategy_handler.generate_signals.return_value = {"AAPL": {"side": "BUY"}}
    backtest_system.data_handler.fetch_most_recent_prices.return_value = {"AAPL": 105.0}
    
    # Mock position manager methods
    backtest_system.execution_handler.position_manager.check_positions.return_value = {}
    backtest_system.execution_handler.position_manager.stats.return_value = {
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0
    }
    
    # Run backtest
    await backtest_system.run_backtest(start_candle_index=0)
    
    # Verify interactions
    backtest_system.data_handler.get_backtest_data.assert_called_once()
    assert backtest_system.strategy_handler.generate_signals.called
    assert backtest_system.ws_manager.send_message.called

    # Verify the initial message was sent
    initial_message = {
        "message": {"type": "success", "text": f"Backtest has begun for tickers {', '.join(backtest_system.tickers)}"}
    }
    backtest_system.ws_manager.send_message.assert_any_call(initial_message)


@pytest.mark.asyncio
async def test_handle_trade(backtest_system):
    """Test trade handling and WebSocket messaging."""
    mock_order = {
        "ticker": "AAPL",
        "price": 150.0,
        "side": MagicMock(value="BUY"),
        "qty": 10,
        "direction": "LONG"
    }
    
    mock_backtest_data = {"end": datetime(2024, 2, 1, 10, 0)}
    
    await backtest_system.handle_trade(mock_backtest_data, mock_order)
    
    assert len(backtest_system.trade_results) == 1
    assert backtest_system.ws_manager.send_message.called
    
    # Verify trade message format
    call_args = backtest_system.ws_manager.send_message.call_args[0][0]
    assert "trade" in call_args
    assert "balance" in call_args
    assert "positions" in call_args


@pytest.mark.asyncio
async def test_process_signals_and_trades(backtest_system):
    """Test processing of trading signals."""
    mock_signals = {
        "AAPL": {"side": "BUY", "price": 150.0}
    }
    
    mock_order = {
        "ticker": "AAPL",
        "price": 150.0,
        "side": MagicMock(value="BUY"),
        "qty": 10,
        "direction": "LONG"
    }
    
    backtest_system.execution_handler.run_backtest_trade.return_value = mock_order
    
    await backtest_system.process_signals_and_trades(
        {"end": datetime(2024, 2, 1, 10, 0)},
        mock_signals
    )
    
    backtest_system.execution_handler.run_backtest_trade.assert_called_with(mock_signals["AAPL"])
    assert len(backtest_system.trade_results) == 1


def test_stop_backtest(backtest_system):
    """Test stopping an active backtest."""
    # Create a mock task
    mock_task = MagicMock()
    mock_task.done.return_value = False
    backtest_system.task = mock_task
    
    backtest_system.stop_backtest()
    
    # Verify that cancel was called before the task was set to None
    mock_task.cancel.assert_called_once()
    assert backtest_system.task is None


@pytest.mark.asyncio
async def test_start_backtest_for_ticker(backtest_system):
    """Test starting a backtest for a specific ticker."""
    ticker = "TSLA"
    strategy = "momentum"
    
    await backtest_system.start_backtest_for_ticker(ticker, strategy)
    
    assert backtest_system.tickers == [ticker]
    assert ticker in backtest_system.running_backtests
