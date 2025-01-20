from app.models.signal import Signal
from app.strategies.base import BaseStrategy
import pandas as pd
import numpy as np
from typing import Dict
from alpaca.data import TimeFrame

class MarketProfileStrategy(BaseStrategy):
    def __init__(self, timeframe: TimeFrame.Hour):
        super().__init__()
        self.high_rsi_threshold = 64
        self.low_rsi_threshold = 37
        self.timeframe = timeframe

    def calculate_rsi(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index (RSI)."""
        delta = data['close'].diff()
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)

        avg_gain = pd.Series(gain).rolling(window=period, min_periods=1).mean()
        avg_loss = pd.Series(loss).rolling(window=period, min_periods=1).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return pd.Series(rsi, index=data.index)

    def calculate_macd(self, data: pd.DataFrame, short_window: int = 12, long_window: int = 26, signal_window: int = 9) -> pd.DataFrame:
        """Calculate Moving Average Convergence Divergence (MACD)."""
        short_ema = data['close'].ewm(span=short_window, adjust=False).mean()
        long_ema = data['close'].ewm(span=long_window, adjust=False).mean()
        macd = short_ema - long_ema
        signal = macd.ewm(span=signal_window, adjust=False).mean()
        return pd.DataFrame({'macd': macd, 'signal': signal})

    def calculate_vwap(self, data: pd.DataFrame) -> pd.Series:
        """Calculate Volume Weighted Average Price (VWAP)."""
        cumulative_price_volume = (data['close'] * data['volume']).cumsum()
        cumulative_volume = data['volume'].cumsum()
        return cumulative_price_volume / cumulative_volume

    def generate_signal(self, ticker, data: pd.DataFrame) -> Signal:
        """Generate buy/sell signals based on market profile and technical indicators."""
        signal = Signal(strategy='market_profile', ticker=ticker)

        # Ensure there are enough 1-minute candles for aggregation
        required_candles = 60  # For 1-hour aggregation
        if data.shape[0] < required_candles:
            return signal

        # Resample data to the required timeframe (e.g., 1 hour)
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data.set_index('timestamp', inplace=True)
        aggregated_data = data.resample(self.timeframe.value).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        }).dropna()

        # Add VWAP to the aggregated data
        aggregated_data['vwap'] = self.calculate_vwap(aggregated_data)

        # Check if there are enough aggregated intervals for analysis
        if aggregated_data.shape[0] < 90:  # Minimum 90 intervals for reliable signal generation
            return signal

        # Add technical indicators
        aggregated_data['rsi'] = self.calculate_rsi(aggregated_data)
        macd_data = self.calculate_macd(aggregated_data)
        aggregated_data['macd'] = macd_data['macd']
        aggregated_data['signal_line'] = macd_data['signal']

        # Get the most recent row for signal calculation
        latest_row = aggregated_data.iloc[-1]

        # Conditions for a buy signal
        if latest_row['rsi'] < self.low_rsi_threshold and latest_row['close'] > latest_row['vwap'] and latest_row['macd'] > latest_row['signal_line']:
            signal.buy()
            signal.price = latest_row['close']
            signal.reason = f'Oversold (RSI < {self.low_rsi_threshold}), price above VWAP, and MACD bullish crossover.'

        # Conditions for a sell signal
        if latest_row['rsi'] > self.high_rsi_threshold and latest_row['close'] < latest_row['vwap'] and latest_row['macd'] < latest_row['signal_line']:
            signal.sell()
            signal.price = latest_row['close']
            signal.reason = f'Overbought (RSI > {self.high_rsi_threshold}), price below VWAP, and MACD bearish crossover.'

        return signal
