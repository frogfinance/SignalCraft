from app.strategies.base import BaseStrategy
import pandas as pd
import numpy as np
from typing import Dict
from alpaca.data import TimeFrame

class MarketProfileStrategy(BaseStrategy):
    def __init__(self, timeframe: TimeFrame.Hour):
        super().__init__()
        self.high_rsi_threshold = 70
        self.low_rsi_threshold = 30
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

    def generate_signal(self, ticker_data: pd.DataFrame) -> Dict[str, bool]:
        """Generate buy/sell signals based on market profile and technical indicators."""
        # Ensure the data has the required fields
        if not {'close', 'volume', 'high', 'low', 'open', 'vwap' }.issubset(ticker_data.columns):
            raise ValueError("Input data must contain 'close', 'volume', 'high', 'low', 'open', and 'vwap' columns.")

        # Add technical indicators
        ticker_data['rsi'] = self.calculate_rsi(ticker_data)
        macd_data = self.calculate_macd(ticker_data)
        ticker_data['macd'] = macd_data['macd']
        ticker_data['signal_line'] = macd_data['signal']

        # Get the most recent row for signal calculation
        latest_row = ticker_data.iloc[-1]

        # Example logic for buy/sell signals
        signal = {
            'buy': False,
            'sell': False,
            'reason': ''
        }

        # Conditions for a buy signal
        if latest_row['rsi'] < self.low_rsi_threshold and latest_row['close'] > latest_row['vwap'] and latest_row['macd'] > latest_row['signal_line']:
            signal['buy'] = True
            signal['reason'] = f'Oversold (RSI < {self.low_rsi_threshold}), price above VWAP, and MACD bullish crossover.'

        # Conditions for a sell signal
        if latest_row['rsi'] > self.high_rsi_threshold and latest_row['close'] < latest_row['vwap'] and latest_row['macd'] < latest_row['signal_line']:
            signal['sell'] = True
            signal['reason'] = f'Overbought (RSI > {self.high_rsi_threshold}), price below VWAP, and MACD bearish crossover.'

        return signal
s