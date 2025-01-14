from datetime import datetime, timedelta
import random
from alpaca.data import TimeFrame
import duckdb
import logging

from tests import utils
from tests.mock_alpaca_broker import MockAlpacaBroker
from tests.predictive_model import StockPredictor

import random
from datetime import timedelta
import logging


def map_to_candle_data(data, ticker, start_time):
    """
    Map raw prediction data to 1-minute candle data.

    Args:
        data (dict): A dictionary containing 'close', 'volume', and 'vxx' predictions for 5 minutes.
        ticker (str): The stock symbol for the ticker.
        start_time (datetime): The starting timestamp for the first candle.

    Returns:
        list: A list of 1-minute candle data dictionaries.
    """
    candles = []
    logging.info('Prediction data: %s', data)

    close_prices = data['close']
    volumes = data['volume']
    vxx_values = data['vxx']

    for i in range(len(close_prices)):
        close_price = close_prices[i]
        volume = volumes[i]

        # Simulate open, high, low, and close prices
        open_price = close_price + random.uniform(-0.5, 0.5)
        high_price = max(open_price, close_price) + random.uniform(0, 0.3)
        low_price = min(open_price, close_price) - random.uniform(0, 0.3)

        # Calculate VWAP using open, high, low, close, and volume
        vwap = (open_price + high_price + low_price + close_price) / 4

        # Create a candle dictionary
        candles.append({
            "timestamp": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "symbol": ticker,
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": int(volume),
            "vwap": round(vwap, 2),
            "vxx": round(vxx_values[i], 2)
        })

        # Increment time by one minute for the next candle
        start_time = start_time + timedelta(minutes=1)

    return candles


def generate_candle_series(tickers):
    """Generate a series of mock candles using the StockPredictor model."""
    logging.info('Generating mock candle series for tickers: {}'.format(tickers))
    models = [StockPredictor(ticker, "tests/data") for ticker in tickers]
    candles = dict()
    for model in models:
        data = model.fetch_data()
        model.train_markov_chain(data)
        candles = model.predict()
        start_time = utils.get_next_market_candle_datetime()
        candles[model.ticker] = map_to_candle_data(candles, model.ticker, start_time)
        logging.info('prediction for ticker: {}'.format(candles[model.ticker]))
    return candles

def generate_trade(signal_data, broker: MockAlpacaBroker):
    logging.info('Generating trade:', signal_data)
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