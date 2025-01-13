from datetime import datetime, timedelta
import random
from alpaca.data import TimeFrame
import duckdb

from tests import utils
from tests.mock_alpaca_broker import MockAlpacaBroker
from tests.predictive_model import StockPredictor


def map_to_candle_data(data, ticker):
    """Map the raw data to candle data."""
    candles = []
    start_time = utils.get_most_recent_timestamp(ticker)
    for prediction_price in data:
        open_price = prediction_price - random.uniform(0, 5)
        close_price = prediction_price 
        high_price = max(open_price, close_price) + random.uniform(0, 3)
        low_price = min(open_price, close_price) - random.uniform(0, 3)
        volume = random.randint(1000, 10000)

        candles.append({
            "timestamp": start_time,
            "symbol": ticker,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
            "vwap": (open_price + close_price + high_price + low_price) / 4
        })
        start_time += timedelta(minutes=1)
    return candles

def generate_candle_series(tickers):
    """Generate a series of mock candles using the StockPredictor model."""
    print('Generating mock candle series for tickers:', tickers)
    models = [StockPredictor(ticker, "tests/data") for ticker in tickers]
    candles = dict()
    for model in models:
        data = model.fetch_data()
        model.train_markov_chain(data)
        candles = model.predict()
        candles[model.ticker] = map_to_candle_data(candles, model.ticker)
    return candles

def generate_trade(signal_data, broker: MockAlpacaBroker):
    """Generate a mock order."""
    order_data = {
        "timestamp": signal_data["timestamp"],
        "symbol": signal_data["ticker"],
        "qty": signal_data["qty"],
        "action": signal_data["action"],
        "order_id": None,
        "strategy": signal_data.get('strategy', 'markov_prediction'),
        "reason": signal_data.get('reason', 'predicted_price_change')
    }
    broker.submit_order(order_data)
    return order_data