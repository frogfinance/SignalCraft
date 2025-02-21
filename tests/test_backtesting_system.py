import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
from app.backtester import BacktestingSystem
from alpaca.data import TimeFrame


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


def test_is_market_open(backtest_system):
    """Test is_market_open() correctly identifies market hours."""
    assert backtest_system.is_market_open(datetime(2024, 2, 5, 10, 0))  # Market open
    assert not backtest_system.is_market_open(datetime(2024, 2, 4, 10, 0))  # Weekend
    assert not backtest_system.is_market_open(datetime(2024, 2, 5, 8, 59))  # Before market open
    assert not backtest_system.is_market_open(datetime(2024, 2, 5, 16, 30))  # After market close


@pytest.mark.asyncio
async def test_start_backtest(backtest_system):
    """Test starting a backtest schedules the async task."""
    with patch.object(backtest_system, "run_backtest", new_callable=AsyncMock) as mock_run:
        backtest_system.start_backtest()
        await asyncio.sleep(0)  # Allow task scheduling
        mock_run.assert_called_once()


@pytest.mark.asyncio
async def test_stop_backtest(backtest_system):
    """Test stopping an active backtest task."""
    backtest_system.task = asyncio.create_task(asyncio.sleep(1))
    assert backtest_system.task is not None

    backtest_system.stop_backtest()
    assert backtest_system.task is None


def test_serialize_ticker_data(backtest_system):
    """Test serialize_ticker_data() correctly aggregates minute-level data into daily candlesticks."""
    import pandas as pd

    data = pd.DataFrame([
        {"timestamp": datetime(2024, 2, 1, 9, 30), "open": 100, "high": 110, "low": 95, "close": 105},
        {"timestamp": datetime(2024, 2, 1, 10, 30), "open": 106, "high": 112, "low": 100, "close": 108},
        {"timestamp": datetime(2024, 2, 2, 9, 30), "open": 109, "high": 115, "low": 107, "close": 110},
    ])

    result = backtest_system.serialize_ticker_data(data)

    expected = [
        {"timestamp": "2024-02-01", "open": 100, "high": 112, "low": 95, "close": 108},
        {"timestamp": "2024-02-02", "open": 109, "high": 115, "low": 107, "close": 110},
    ]
    
    assert result == expected


@pytest.mark.asyncio
async def test_run_backtest_trades(backtest_system):
    """Test that run_backtest generates signals and executes trades."""
    backtest_system.data_handler.get_backtest_data.return_value = {
        "AAPL": {
            "timestamp": [datetime(2024, 2, 1, 9, 30), datetime(2024, 2, 1, 9, 31)],
        }
    }
    backtest_system.strategy_handler.generate_signals.return_value = {"AAPL": {"side": "BUY"}}
    backtest_system.execution_handler.run_backtest_trade.return_value = {
        "ticker": "AAPL", "price": 100, "side": "BUY", "qty": 10, "direction": "LONG"
    }

    with patch.object(backtest_system, "ws_manager") as mock_ws:
        mock_ws.send_message = AsyncMock()

        await backtest_system.run_backtest(start_candle_index=0)

        # Verify trade execution
        backtest_system.execution_handler.run_backtest_trade.assert_called()
        # Verify WebSocket message was sent
        mock_ws.send_message.assert_called()


@pytest.mark.asyncio
async def test_run_backtest_handles_no_signals(backtest_system):
    """Test that run_backtest continues running when no signals are generated."""
    backtest_system.data_handler.get_backtest_data.return_value = {
        "AAPL": {"timestamp": [datetime(2024, 2, 1, 9, 30)]}
    }
    backtest_system.strategy_handler.generate_signals.return_value = {}  # No signals

    with patch.object(backtest_system, "ws_manager") as mock_ws:
        mock_ws.send_message = AsyncMock()

        await backtest_system.run_backtest(start_candle_index=0)

        # Ensure no trades were executed
        backtest_system.execution_handler.run_backtest_trade.assert_not_called()
        # Ensure WebSocket received at least one message (status update)
        mock_ws.send_message.assert_called()


@pytest.mark.asyncio
async def test_start_backtest_for_ticker(backtest_system):
    """Test starting a backtest for a specific ticker."""
    with patch.object(backtest_system, "run_backtest", new_callable=AsyncMock) as mock_run:
        await backtest_system.start_backtest_for_ticker("AAPL", "momentum")
        assert "AAPL" in backtest_system.running_backtests
        mock_run.assert_called_once()


@pytest.mark.asyncio
async def test_run_backtest_sends_daily_candle_data(backtest_system):
    """Test that backtest detects new trading days and sends daily summary data."""
    backtest_system.data_handler.get_backtest_data.return_value = {
        "AAPL": {
            "timestamp": [
                datetime(2024, 2, 1, 9, 30),
                datetime(2024, 2, 1, 15, 59),  # Last candle of the day
                datetime(2024, 2, 2, 9, 30),  # New day detected
            ]
        }
    }
    
    backtest_system.execution_handler.position_manager.cash_balance = 100000
    backtest_system.execution_handler.position_manager.positions = {"AAPL": MagicMock()}
    backtest_system.execution_handler.position_manager.stats.return_value = {"returns": 5.0}

    with patch.object(backtest_system, "ws_manager") as mock_ws:
        mock_ws.send_message = AsyncMock()

        await backtest_system.run_backtest(start_candle_index=0)

        # Ensure WebSocket received a daily summary
        assert mock_ws.send_message.call_count > 1
        args, kwargs = mock_ws.send_message.call_args
        assert "ticker_data" in args[0]  # Ensure ticker data was sent

