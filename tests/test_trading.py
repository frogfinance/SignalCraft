import pytest, sys, os, duckdb, logging
from datetime import timedelta
from unittest.mock import  Mock

# Add the root directory to the PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import save_market_data
from app.handlers.execution_handler import ExecutionHandler
from app.handlers.data_handler import DataHandler
from app.handlers.strategy_handler import StrategyHandler

from tests import utils
from tests.generate_mock_data import generate_candle_series, generate_trade
from tests.mock_alpaca_broker import MockAlpacaBroker

TICKERS = ['AAPL', 'QQQ', 'VXX']

last_known_real_candle = {ticker: utils.get_most_recent_timestamp(ticker) for ticker in TICKERS}

logging.basicConfig(level=logging.INFO)

@pytest.mark.asyncio
async def test_trading_system(monkeypatch):
    """Test trading system with mocked Alpaca data."""
    broker = MockAlpacaBroker()
    
    # Define mock functions    
    # Generate the next mock for the given ticker
    def generate_next_mock_data():
        data = generate_candle_series(TICKERS)
        return data
    
    # Generate a mock trade for the given signal data
    def mock_handle_trade(signal_data):
        return generate_trade(signal_data, broker)

    # Monkeypatch the DataHandler data fetching method
    # use Markov chain model to generate new data via the mock_get_data function
    monkeypatch.setattr(
        "app.handlers.data_handler.DataHandler.fetch_data",
        Mock(side_effect=generate_next_mock_data),
    )
    # Monkeypatch the Execution trade execution method
    monkeypatch.setattr(
        "app.handlers.execution_handler.ExecutionHandler.submit_order",
        Mock(side_effect=mock_handle_trade),
    )
    # Monkeypatch the ExecutionHandlers
    monkeypatch.setattr(
        "app.handlers.execution_handler.ExecutionHandler.is_market_open",
        Mock(return_value=True),
    )
    monkeypatch.setattr(
        "app.handlers.execution_handler.ExecutionHandler.get_all_positions",
        Mock(return_value=broker.get_account().get('positions')),
    )
    monkeypatch.setattr(
        "app.handlers.execution_handler.ExecutionHandler.get_buying_power",
        Mock(return_value=broker.get_account().get('buying_power')),
    )
    monkeypatch.setattr(
        "app.handlers.execution_handler.ExecutionHandler.get_next_market_open",
        Mock(side_effect=utils.get_next_market_candle_datetime),
    )
    # Initialize the actors
    db_base_path = "tests/data"
    execution_handler = ExecutionHandler("fake_api_key", "fake_secret", db_base_path=db_base_path)
    strategy_handler = StrategyHandler(TICKERS[:2], db_base_path=db_base_path)
    data_handler = DataHandler(TICKERS, "fake_api_key", "fake_secret_key", db_base_path=db_base_path)

    # Run the algo trading system
    days = 30
    # Simulate next market day

    start = utils.get_next_market_candle_datetime()
    end = start + timedelta(days=days)
    curr_date = start
    while curr_date < end:
        # Fetch data - monkeypatch will return `generate_next_mock_data`
        logging.info(f"Fetching data for {curr_date}")
        data = data_handler.fetch_data()
        save_market_data(data, db_base_path=data_handler.db_base_path)
        if data:
            signals = strategy_handler.generate_signals()
            execution_handler.handle_execution(signals)
        curr_date += timedelta(minutes=5)
    
    # Print the mock broker account balances
    logging.info("Mock Broker Account Balances:")
    logging(broker.get_account())

if __name__ == "__main__":
    import asyncio
    from _pytest.monkeypatch import MonkeyPatch

    monkeypatch = MonkeyPatch()
    asyncio.run(test_trading_system(monkeypatch))

    # Reset the databases after the test
    # use last_known_real_candle to reset the database to the last known real candle
    for ticker, timestamp in last_known_real_candle.items():
        conn = duckdb.connect(f"tests/dbs/{ticker}_1min_data.db")
        conn.execute(f"DELETE FROM ticker_data WHERE timestamp > '{timestamp}'")
        conn.close()