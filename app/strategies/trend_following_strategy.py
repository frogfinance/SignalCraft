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

    def detect_trend(self, data: pd.DataFrame) -> Optional[str]:
        """Detect uptrend or downtrend based on higher highs and lower lows."""
        if len(data) < self.trend_confirmation_candles:
            return None

        highs = data['high'].values
        lows = data['low'].values

        # Check for uptrend (higher highs)
        uptrend = all(highs[-i] > highs[-(i + 1)] for i in range(1, self.trend_confirmation_candles))

        # Check for downtrend (lower lows)
        downtrend = all(lows[-i] < lows[-(i + 1)] for i in range(1, self.trend_confirmation_candles))

        if uptrend:
            return "uptrend"
        elif downtrend:
            return "downtrend"
        else:
            return None

    def find_demand_level(self, data: pd.DataFrame) -> float:
        """Find the demand level (low of the candle before the start of the uptrend)."""
        if len(data) < 2:
            return data['low'].iloc[-1]

        return data['low'].iloc[-2]

    def generate_signal(self, ticker, data: pd.DataFrame) -> Signal:
        """Generate buy/sell signals based on trend detection."""
        if data.empty:
            logger.debug(f"No data available for ticker {ticker}. Skipping signal generation.")
            return Signal(strategy="trend_following", ticker=ticker)

        # Resample data into 15-minute intervals
        data = self.resample_data(data, interval="15min")

        # Ensure enough historical data for trend analysis
        if data.shape[0] < self.lookback:
            return Signal(strategy="trend_following", ticker=ticker)

        # Get the most recent price
        latest_row = data.iloc[-1]
        current_price = latest_row['close']

        signal = Signal(strategy="trend_following", ticker=ticker, price=current_price)

        # Detect the current trend
        trend = self.detect_trend(data)

        if trend == "uptrend":
            # Find the demand level (low of the candle before the start of the uptrend)
            demand_level = self.find_demand_level(data)

            # Set buy order at the demand level
            if current_price >= demand_level:
                signal.buy()
                signal.reason = f"Uptrend detected. Buy at demand level: {demand_level:.2f}"

                # Set stop loss and take profit levels
                stop_loss = demand_level * (1 - self.stop_loss_multiplier)
                take_profit = data['high'].iloc[-1] * (1 + self.take_profit_multiplier)

                signal.stop_loss = stop_loss
                signal.take_profit = take_profit

        elif trend == "downtrend":
            # For downtrends, implement a similar logic for sell/short signals
            pass

        return signal