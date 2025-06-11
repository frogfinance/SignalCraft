from app.models.signal import Signal
from app.strategies.base import BaseStrategy
import pandas as pd
import numpy as np
import logging
from scipy.signal import argrelextrema
from typing import Dict

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

    def resample_data(self, data: pd.DataFrame, interval="15min") -> pd.DataFrame:
        """Resample minute-level data into 15m intervals."""
        # Create a copy to avoid modifying the original
        data = data.copy()
        
        # Check if timestamp is in the index
        if isinstance(data.index, pd.DatetimeIndex):
            # Already has timestamp index
            pass
        elif 'timestamp' in data.columns:
            # Convert timestamp column to datetime if it's not already
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            data.set_index('timestamp', inplace=True)
        else:
            raise ValueError("DataFrame must have either a DatetimeIndex or a 'timestamp' column")

        # Perform resampling
        aggregated = data.resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

        # Reset index to get timestamp back as a column
        aggregated.reset_index(inplace=True)
        return aggregated

    def find_support_resistance(self, data: pd.DataFrame):
        """
        Identify support and resistance levels using local minima and maxima.
        Support levels are price levels where the stock tends to stop falling.
        Resistance levels are price levels where the stock tends to stop rising.
        """
        # Convert values to float type
        low_values = data['low'].astype(float).values
        high_values = data['high'].astype(float).values
        
        # Increase the order parameter to find more significant levels
        # order=20 means it looks at 20 points on either side to determine if it's a local min/max
        local_mins = argrelextrema(low_values, np.less, order=20)[0]
        local_maxs = argrelextrema(high_values, np.greater, order=20)[0]
        
        # Get the actual price values at these points
        support_levels = []
        resistance_levels = []
        
        # Only consider levels that have been tested multiple times
        for idx in local_mins:
            price_level = float(data.iloc[idx]['low'])
            # Count how many times price has approached this level
            approaches = np.sum(np.abs(low_values - price_level) <= price_level * 0.01)  # within 1%
            if approaches >= 2:  # Level has been tested at least twice
                support_levels.append(price_level)
        
        for idx in local_maxs:
            price_level = float(data.iloc[idx]['high'])
            # Count how many times price has approached this level
            approaches = np.sum(np.abs(high_values - price_level) <= price_level * 0.01)  # within 1%
            if approaches >= 2:  # Level has been tested at least twice
                resistance_levels.append(price_level)
        
        # Sort levels from lowest to highest
        support_levels = sorted(support_levels)
        resistance_levels = sorted(resistance_levels)
        
        return np.array(support_levels), np.array(resistance_levels)


    def generate_signal(self, ticker, data: pd.DataFrame) -> Signal:
        """Generate buy/sell signals based on support and resistance levels."""
        if data.empty:
            logger.debug("No data available for ticker %r. Skipping signal generation.", ticker)
            return Signal(strategy="support_resistance", ticker=ticker)
        
        # Get the most recent price
        latest_row = data.iloc[-1]
        current_price = float(latest_row['close'])

        signal = Signal(strategy=self.name, ticker=ticker, price=current_price)

        # Get timestamp from index if data is indexed by timestamp, otherwise from column
        if isinstance(data.index, pd.DatetimeIndex):
            timestamp = data.index[-1]
        else:
            timestamp = pd.to_datetime(data['timestamp'].iloc[-1])

        # check every hour on the 29th minute
        if timestamp.minute % 29 == 0:
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

        if len(support_levels) > 0 and len(resistance_levels) > 0:
            logger.debug("Found support levels: %r", support_levels)
            logger.debug("Found resistance levels: %r", resistance_levels)
            logger.debug("Current price: %r", current_price)

            # Find nearest support level below current price
            nearest_support = support_levels[support_levels < current_price][-1] if any(support_levels < current_price) else None
            
            # Find nearest resistance level above current price
            nearest_resistance = resistance_levels[resistance_levels > current_price][0] if any(resistance_levels > current_price) else None

            # Buy signal: Price is near support
            if nearest_support is not None:
                support_threshold_price = nearest_support * (1 + self.support_threshold)
                if current_price <= support_threshold_price:
                    signal.buy()
                    signal.reason = f"Price {current_price:.2f} near support at {nearest_support:.2f}"
                    return signal

            # Sell signal: Price is near resistance
            if nearest_resistance is not None:
                resistance_threshold_price = nearest_resistance * (1 - self.resistance_threshold)
                if current_price >= resistance_threshold_price:
                    signal.close()
                    signal.reason = f"Price {current_price:.2f} near resistance at {nearest_resistance:.2f}"
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