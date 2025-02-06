from app.models.signal import Signal
from app.strategies.base import BaseStrategy
import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional

logger = logging.getLogger("app")

class TrendFollowingStrategy(BaseStrategy):
    def __init__(self):
        super().__init__()
        self.lookback = 360  # Number of past intervals to analyze trends
        self.trend_confirmation_candles = 3  # Number of candles to confirm a trend
        self.stop_loss_multiplier = 0.01  # 1% stop loss from the demand level
        self.take_profit_multiplier = 0.02  # 2% take profit from the recent high

    def resample_data(self, data: pd.DataFrame, interval="15min") -> pd.DataFrame:
        """Resample minute-level data into the specified interval."""
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

    def detect_trend(self, data: pd.DataFrame) -> Optional[Dict[str, any]]:
        """
        Detect uptrend by looking back across the last 200 candles.
        Returns a dictionary with:
        - 'trend': 'uptrend' or None
        - 'demand_zone': {'low': float, 'high': float} (range of the higher low candle)
        - 'previous_high': float (previous high before the higher low)
        """
        if len(data) < 200:
            return None

        highs = data['high'].values
        lows = data['low'].values

        # Look for higher highs followed by a higher low
        higher_highs = []
        higher_lows = []

        for i in range(len(data) - 1):
            if highs[i] > highs[i - 1]:
                higher_highs.append(i)
            if lows[i] > lows[i - 1]:
                higher_lows.append(i)

        # Check if the last candle is a breakout from the higher low
        if len(higher_highs) > 0 and len(higher_lows) > 0:
            # Find the most recent higher low
            higher_low_index = higher_lows[-1]

            # Ensure the higher low is after the last higher high
            if higher_low_index > higher_highs[-1]:
                # Check if the last candle is a breakout (closes above the higher low candle's high)
                breakout_candle = data.iloc[-1]
                higher_low_candle = data.iloc[higher_low_index]

                if breakout_candle['close'] > higher_low_candle['high']:
                    # Define the demand zone as the range of the higher low candle
                    demand_zone = {
                        "low": higher_low_candle['low'],
                        "high": higher_low_candle['high']
                    }

                    # Find the previous high before the higher low
                    previous_high = max(highs[higher_highs[-1]:higher_low_index])

                    return {
                        "trend": "uptrend",
                        "demand_zone": demand_zone,
                        "previous_high": previous_high
                    }

        return None

    def generate_signal(self, ticker, data: pd.DataFrame) -> Signal:
        """Generate buy/sell signals based on trend detection and demand zone."""
        if data.empty:
            logger.debug(f"No data available for ticker {ticker}. Skipping signal generation.")
            return Signal(strategy="trend_following", ticker=ticker)

        # Resample data into 15-minute intervals
        data = self.resample_data(data, interval="15min")

        # Ensure enough historical data for trend analysis
        if data.shape[0] < 200:
            return Signal(strategy="trend_following", ticker=ticker)

        # Get the most recent price
        latest_row = data.iloc[-1]
        current_price = latest_row['close']

        signal = Signal(strategy="trend_following", ticker=ticker, price=current_price)

        # Detect the current trend
        trend_info = self.detect_trend(data)

        if trend_info and trend_info["trend"] == "uptrend":
            demand_zone = trend_info["demand_zone"]
            previous_high = trend_info["previous_high"]

            # Set buy order at the demand zone high (breakout level)
            if current_price >= demand_zone["high"]:
                signal.buy()
                signal.reason = f"Uptrend detected. Buy at demand zone: {demand_zone['low']:.2f} - {demand_zone['high']:.2f}"

                # Set stop loss within the demand zone
                stop_loss = demand_zone["low"] * (1 - self.stop_loss_multiplier)

                # Set take profit at the previous high
                take_profit = previous_high

                signal.stop_loss = stop_loss
                signal.take_profit = take_profit

        return signal