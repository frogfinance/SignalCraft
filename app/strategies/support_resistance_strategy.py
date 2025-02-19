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
        self.lookback = 360  # Number of past intervals to analyze support/resistance
        self.support_threshold = 0.015  # 1.5% threshold to buy near support
        self.resistance_threshold = 0.02  # 2% threshold to sell near resistance
        self.name = 'support_resistance'
        self.display_name = 'Support & Resistance'
        self.time_interval = "15min"

    def resample_data(self, data: pd.DataFrame, interval="60min") -> pd.DataFrame:
        """Resample minute-level data into 60-minute intervals."""
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
            logger.debug("No data available for ticker %r. Skipping signal generation.", ticker)
            return Signal(strategy="support_resistance", ticker=ticker)
        
        # Get the most recent price
        latest_row = data.iloc[-1]
        current_price = latest_row['close']

        signal = Signal(strategy=self.name, ticker=ticker, price=current_price)

        timestamp = data['timestamp'].iloc[-1]
        # check every hour on the 29th minute
        if timestamp.minute == 0 or data.shape[0] < (60 * self.lookback):
            # convert timestamp to local PST
            if timestamp.tz is None:  # If timestamp is naive, localize it first
                timestamp = timestamp.tz_localize('UTC')

            timestamp = timestamp.tz_convert('US/Pacific')
            logger.debug('Skipping signal generation for %r at %r', ticker, timestamp)
            return signal
        else:
            logger.debug(f"Generating signal for %r at %r", ticker, timestamp)
        
        # Resample data into 15-minute intervals
        data = self.resample_data(data, interval=self.time_interval)

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
                signal.close()
                signal.reason = f"Price near resistance at {resistance:.2f}"
                return signal

        return signal

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "lookback": self.lookback,
            "support_threshold": self.support_threshold,
            "resistance_threshold": self.resistance_threshold,
            "time_interval": self.time_interval
        }