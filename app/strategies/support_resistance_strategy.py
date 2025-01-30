from app.models.signal import Signal
from app.strategies.base import BaseStrategy
import pandas as pd
import numpy as np
import logging
from scipy.signal import argrelextrema
from typing import Dict
from alpaca.data import TimeFrame

logger = logging.getLogger("app")

class SupportResistanceStrategy(BaseStrategy):
    def __init__(self):
        super().__init__()
        self.lookback = 120  # Number of past intervals to analyze support/resistance
        self.support_threshold = 0.02  # 2% threshold to buy near support
        self.resistance_threshold = 0.02  # 2% threshold to sell near resistance

    def resample_data(self, data: pd.DataFrame, interval="15min") -> pd.DataFrame:
        """Resample minute-level data into 15-minute intervals."""
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data.set_index('timestamp', inplace=True)

        aggregated = data.resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

        aggregated.reset_index(inplace=True)
        return aggregated

    def find_support_resistance(self, data: pd.DataFrame):
        """Identify support and resistance levels using local minima and maxima."""
        # Find local minima (support levels)
        local_mins = argrelextrema(data['low'].values, np.less, order=5)[0]
        support_levels = data.iloc[local_mins]['low'].values

        # Find local maxima (resistance levels)
        local_maxs = argrelextrema(data['high'].values, np.greater, order=5)[0]
        resistance_levels = data.iloc[local_maxs]['high'].values

        return support_levels, resistance_levels

    def generate_signal(self, ticker, data: pd.DataFrame) -> Signal:
        """Generate buy/sell signals based on support and resistance levels."""
        if data.empty:
            logger.debug(f"No data available for ticker {ticker}. Skipping signal generation.")
            return Signal(strategy="support_resistance", ticker=ticker)
        
        # Get the most recent price
        latest_row = data.iloc[-1]
        current_price = latest_row['close']

        signal = Signal(strategy="support_resistance", ticker=ticker, price=current_price)

        timestamp = data['timestamp'].iloc[-1]
        # check every hour on the 29th minute
        if timestamp.minute % 15 != 0 or data.shape[0] < 15 * self.lookback:
            return signal
        
        # Resample data into 15-minute intervals
        data = self.resample_data(data, interval="15min")

        # Ensure enough historical data for support/resistance analysis
        if data.shape[0] < self.lookback:
            return Signal(strategy="support_resistance", ticker=ticker)

        # Identify support and resistance levels
        support_levels, resistance_levels = self.find_support_resistance(data)

        # Buy signal: Price is near support
        for support in support_levels:
            if (current_price <= support * (1 + self.support_threshold)):
                signal.buy()
                signal.reason = f"Price near support at {support:.2f}"
                return signal

        # Sell signal: Price is near resistance
        for resistance in resistance_levels:
            if (current_price >= resistance * (1 - self.resistance_threshold)):
                signal.sell()
                signal.reason = f"Price near resistance at {resistance:.2f}"
                return signal

        return signal
