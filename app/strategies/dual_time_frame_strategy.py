import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any

from app.models.signal import Signal
from app.indicators.dtosc import dtosc
from app.strategies.base import BaseStrategy
from alpaca.data import TimeFrame

logger = logging.getLogger(__name__)

class DualTimeFrameStrategy(BaseStrategy):
    """
    Dual Time Frame Momentum Strategy using DTOSC indicator.
    
    This strategy analyzes momentum across two timeframes:
    1. Higher timeframe for overall trend direction
    2. Lower timeframe for entry/exit timing
    
    Rules:
    1. Higher TF bullish + not OB → Consider longs after smaller TF bullish reversal
    2. Higher TF bullish + OB → No new longs, consider shorts after smaller TF bearish reversal
    3. Higher TF bearish + not OS → Consider shorts after smaller TF bearish reversal
    4. Higher TF bearish + OS → No new shorts, consider longs after smaller TF bullish reversal
    """
    
    def __init__(self, 
             higher_timeframe=TimeFrame.Hour, 
             lower_timeframe=TimeFrame.Minute,
             dtosc_params={'r1': 13, 's1': 8, 's2': 5, 's3': 5},
             risk_reward_ratio=2.0,
             stop_loss_atr_multiple=1.5,
             market_analyzer=None,
             db_base_path="dbs"):
        """
        Initialize the Dual Time Frame Momentum Strategy.
        
        Parameters:
        -----------
        higher_timeframe : TimeFrame
            The higher timeframe for trend direction (default: Hour)
        lower_timeframe : TimeFrame
            The lower timeframe for entry signals (default: Minute)
        dtosc_params : dict
            Parameters for the DTOSC indicator
        risk_reward_ratio : float
            Risk to reward ratio for calculating take profit
        stop_loss_atr_multiple : float
            Multiple of ATR for stop loss calculation
        market_analyzer : MarketAnalyzerAgent
            Agent for market analysis integration
        db_base_path : str
            Base path for database connections
        """
        super().__init__()
        self.name = "Dual Time Frame Momentum"
        
        # Update timeframes
        # For 15min, we'll still use TimeFrame.Minute but adjust aggregation logic
        self.higher_timeframe = TimeFrame.Hour  # Will be treated as 4 hours
        self.lower_timeframe = TimeFrame.Minute  # Will be treated as 15 minutes
        self.higher_minutes = 240  # 4 hours = 240 minutes
        self.lower_minutes = 15    # 15 minutes
    
        self.dtosc_params = dtosc_params
        self.risk_reward_ratio = risk_reward_ratio
        self.stop_loss_atr_multiple = stop_loss_atr_multiple
        self.market_analyzer = market_analyzer
        self.db_base_path = db_base_path
        
        # Cache for higher timeframe data
        self.higher_tf_cache = {}
        self.last_cache_update = {}
        
    def get_higher_timeframe_data(self, ticker, connection, current_data):
        """Get higher timeframe data by aggregating from lower timeframe"""
        # Check if we need to update the cache
        current_time = datetime.now()
        last_update = self.last_cache_update.get(ticker, datetime.min)
        
        # Update cache if it's older than the higher timeframe period
        # For 4 hours timeframe, use 4 hours update interval
        update_interval = timedelta(hours=4)
        
        if ticker not in self.higher_tf_cache or (current_time - last_update) > update_interval:
            try:
                # Instead of trying to load from an hour database, we'll aggregate from minute data
                from app.strategies.base import get_ticker_data
                
                # Get minute data - without the limit parameter
                minute_data = get_ticker_data(ticker, connection, timeframe=TimeFrame.Minute, 
                                              db_base_path=self.db_base_path)
                
                # Apply limit after fetching the data - ensure we have enough data for aggregation
                # For 4 hour bars we'll need at least 4 hours * 60 minutes = 240 minutes of data
                # To have enough 4-hour bars, we'll get more historical data
                if not minute_data.empty and len(minute_data) > 2000:  # Increased from 1000
                    minute_data = minute_data.iloc[-2000:]  # Get the last 2000 records
            
                if minute_data.empty:
                    logger.warning(f"No minute data available for {ticker}")
                    return pd.DataFrame()
                
                # Ensure timestamp is datetime
                if 'timestamp' in minute_data.columns and not pd.api.types.is_datetime64_any_dtype(minute_data['timestamp']):
                    minute_data['timestamp'] = pd.to_datetime(minute_data['timestamp'])
                
                # Set timestamp as index for resampling
                minute_data = minute_data.set_index('timestamp')
                
                # Resample to 15 minutes first (lower timeframe)
                lower_tf_rule = '15min'
                lower_tf_data = minute_data.resample(lower_tf_rule).agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
                
                # Now resample from 15 minutes to 4 hours (higher timeframe)
                higher_tf_rule = '4h'
                higher_tf_data = lower_tf_data.resample(higher_tf_rule).agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
                
                # Reset index to get timestamp as a column again
                higher_tf_data = higher_tf_data.reset_index()
                
                # Add ticker column if it doesn't exist
                if 'ticker' not in higher_tf_data.columns:
                    higher_tf_data['ticker'] = ticker
                
                # Calculate VWAP if it was in the original data
                if 'vwap' in minute_data.columns:
                    # For VWAP we need to recalculate based on the formula
                    higher_tf_data['vwap'] = (higher_tf_data['volume'] * higher_tf_data['close']).cumsum() / higher_tf_data['volume'].cumsum()
            
                if not higher_tf_data.empty:
                    # Calculate DTOSC on higher timeframe
                    higher_tf_data_with_indicators = dtosc(
                        higher_tf_data,
                        **self.dtosc_params
                    )
                    self.higher_tf_cache[ticker] = higher_tf_data_with_indicators
                    self.last_cache_update[ticker] = current_time
                    
                    logger.debug(f"Successfully aggregated to {len(higher_tf_data)} 4-hour bars for {ticker}")
                else:
                    logger.warning(f"Aggregation resulted in empty data for {ticker}")
        
            except Exception as e:
                logger.error(f"Error aggregating timeframe data for {ticker}: {e}")
                return pd.DataFrame()
        
        return self.higher_tf_cache.get(ticker, pd.DataFrame())
    
    def calculate_atr(self, data, period=14):
        """Calculate Average True Range for stop loss determination"""
        high = data['high']
        low = data['low']
        close = data['close'].shift(1)
        
        tr1 = high - low
        tr2 = (high - close).abs()
        tr3 = (low - close).abs()
        
        tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        atr = tr.rolling(period).mean()
        
        return atr
    
    def get_market_condition(self):
        """Get current market condition from market analyzer agent"""
        default_condition = {
            'overall_condition': 'neutral',
            'risk_level': 'moderate',
            'trend': 'neutral',
            'volatility': 'normal'
        }
        
        if not self.market_analyzer:
            return default_condition
            
        # Try to get the latest market analysis
        try:
            # Use the get_market_conditions method that actually exists in MarketAnalyzerAgent
            # This is an async method, so we'll get the data from market_conditions attribute directly
            if hasattr(self.market_analyzer, 'market_conditions'):
                market_conditions = self.market_analyzer.market_conditions
                
                # Map the market analyzer's data structure to our simplified structure
                return {
                    'overall_condition': market_conditions.get('overall_condition', 'neutral'),
                    'risk_level': market_conditions.get('risk_level', 'moderate'),
                    'trend': market_conditions.get('trend', 'neutral'),
                    'volatility': market_conditions.get('volatility', 'normal')
                }
            else:
                logger.warning("Market analyzer doesn't have market_conditions attribute")
                return default_condition
        except Exception as e:
            logger.error(f"Error getting market analysis: {e}")
            return default_condition
    
    def calculate_stop_loss_take_profit(self, data, signal_type, entry_price, atr):
        """
        Calculate stop loss and take profit levels based on ATR and market conditions.
        
        Parameters:
        -----------
        data : pandas.DataFrame
            Price data
        signal_type : str
            'buy' or 'sell'
        entry_price : float
            Entry price for the trade
        atr : float
            Current ATR value
        
        Returns:
        --------
        tuple
            (stop_loss, take_profit)
        """
        # Get market condition
        market = self.get_market_condition()
        
        # Adjust ATR multiple based on volatility
        atr_multiple = self.stop_loss_atr_multiple
        if market['volatility'] == 'high':
            atr_multiple *= 1.5
        elif market['volatility'] == 'low':
            atr_multiple *= 0.8
        
        # Calculate stop loss
        if signal_type == 'buy':
            stop_loss = entry_price - (atr * atr_multiple)
        else:  # sell
            stop_loss = entry_price + (atr * atr_multiple)
        
        # Adjust risk-reward ratio based on market condition
        risk_reward = self.risk_reward_ratio
        if market['overall_condition'] == 'bullish' and signal_type == 'buy':
            risk_reward *= 1.2
        elif market['overall_condition'] == 'bearish' and signal_type == 'sell':
            risk_reward *= 1.2
        
        # Calculate take profit
        stop_distance = abs(entry_price - stop_loss)
        if signal_type == 'buy':
            take_profit = entry_price + (stop_distance * risk_reward)
        else:  # sell
            take_profit = entry_price - (stop_distance * risk_reward)
        
        return round(stop_loss, 2), round(take_profit, 2)
    
    def generate_signal(self, ticker, data):
        """
        Generate trading signals based on the dual timeframe momentum strategy.
        
        Parameters:
        -----------
        ticker : str
            The ticker symbol
        data : pandas.DataFrame
            Price data for the minute timeframe (will be aggregated to 15min)
        
        Returns:
        --------
        Signal or None
            Trading signal if conditions are met, None otherwise
        """
        if data.empty or len(data) < 50:
            logger.warning(f"Not enough data for {ticker}")
            return None
        
        try:
            import duckdb
            # Connect to the minute data database since we'll aggregate from there
            connection = duckdb.connect(f"{self.db_base_path}/{ticker}_{TimeFrame.Minute}_data.db")
            
            # First, aggregate minute data to 15-minute timeframe
            if 'timestamp' in data.columns and not pd.api.types.is_datetime64_any_dtype(data['timestamp']):
                data['timestamp'] = pd.to_datetime(data['timestamp'])
            
            # Set timestamp as index for resampling
            indexed_data = data.set_index('timestamp')
            
            # Resample to 15 minutes
            data_15min = indexed_data.resample('15min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna().reset_index()
            
            # Ensure we have enough 15-minute data
            if len(data_15min) < 50:
                logger.debug(f"Not enough 15-minute data for {ticker}")
                connection.close()
                return None
            
            # Get higher timeframe (4-hour) data
            higher_tf_data = self.get_higher_timeframe_data(ticker, connection, data)
            connection.close()
            
            if higher_tf_data.empty or len(higher_tf_data) < 20:
                logger.debug(f"Not enough 4-hour timeframe data for {ticker}")
                return None
            
            # Calculate DTOSC on 15-minute timeframe
            data_15min = dtosc(data_15min, **self.dtosc_params)
            
            # Calculate ATR for stop loss
            data_15min['atr'] = self.calculate_atr(data_15min)
            
            # Get current values
            current = data_15min.iloc[-1]
            higher_tf_current = higher_tf_data.iloc[-1]
            
            # Create signal object
            signal = Signal(
                strategy="dual_timeframe_momentum",
                ticker=ticker,
                price=current['close'],
                timestamp=current['timestamp'] if isinstance(current['timestamp'], datetime) else 
                          datetime.fromisoformat(current['timestamp']) if isinstance(current['timestamp'], str) else
                          datetime.now()
            )
            
            # Create metadata for the signal - update to show 15min and 4h
            metadata = {
                'higher_timeframe': '4Hour',
                'lower_timeframe': '15Minute',
                'higher_tf_dtosc': float(higher_tf_current['dtosc']),
                'lower_tf_dtosc': float(current['dtosc']),
                'higher_tf_bullish': bool(higher_tf_current['dtosc_bullish']),
                'higher_tf_overbought': bool(higher_tf_current['dtosc_overbought']),
                'higher_tf_oversold': bool(higher_tf_current['dtosc_oversold']),
                'market_condition': self.get_market_condition()['overall_condition'],
                'market_trend': self.get_market_condition()['trend'],
                'market_volatility': self.get_market_condition()['volatility']
            }
            
            # The rest of the signal generation logic remains the same...
            # Define conditions from higher timeframe
            higher_bullish = higher_tf_current['dtosc_bullish']
            higher_bearish = higher_tf_current['dtosc_bearish']
            higher_overbought = higher_tf_current['dtosc_overbought']
            higher_oversold = higher_tf_current['dtosc_oversold']
            
            # Define conditions from lower timeframe
            lower_bullish_reversal = current['dtosc_bullish_reversal']
            lower_bearish_reversal = current['dtosc_bearish_reversal']
            
            # Add market analysis metadata
            market_condition = self.get_market_condition()
            
            # Update the signal metadata
            signal.metadata = metadata
            
            # Rule 1: Higher TF bullish + not OB → Consider longs after smaller TF bullish reversal
            if higher_bullish and not higher_overbought and lower_bullish_reversal:
                stop_loss, take_profit = self.calculate_stop_loss_take_profit(
                    data_15min, 'buy', current['close'], current['atr']
                )
                
                # Check if market condition aligns with signal
                if market_condition['trend'] in ['bullish', 'neutral']:
                    signal.buy()
                    signal.reason = "Rule 1: 4h bullish momentum, 15min bullish reversal"
                    signal.stop_loss = stop_loss
                    signal.take_profit = take_profit
                    return signal
            
            # Rule 2: Higher TF bullish + OB → No new longs, consider shorts after smaller TF bearish reversal
            elif higher_bullish and higher_overbought and lower_bearish_reversal:
                stop_loss, take_profit = self.calculate_stop_loss_take_profit(
                    data_15min, 'sell', current['close'], current['atr']
                )
                
                # Be more cautious with counter-trend trades
                if market_condition['trend'] != 'strongly_bullish':
                    signal.sell_short()
                    signal.reason = "Rule 2: Higher TF bullish but overbought, lower TF bearish reversal"
                    signal.stop_loss = stop_loss
                    signal.take_profit = take_profit
                    return signal
            
            # Rule 3: Higher TF bearish + not OS → Consider shorts after smaller TF bearish reversal
            elif higher_bearish and not higher_oversold and lower_bearish_reversal:
                stop_loss, take_profit = self.calculate_stop_loss_take_profit(
                    data_15min, 'sell', current['close'], current['atr']
                )
                
                # Check if market condition aligns with signal
                if market_condition['trend'] in ['bearish', 'neutral']:
                    signal.sell_short()
                    signal.reason = "Rule 3: Higher TF bearish momentum, lower TF bearish reversal"
                    signal.stop_loss = stop_loss
                    signal.take_profit = take_profit
                    return signal
            
            # Rule 4: Higher TF bearish + OS → No new shorts, consider longs after smaller TF bullish reversal
            elif higher_bearish and higher_oversold and lower_bullish_reversal:
                stop_loss, take_profit = self.calculate_stop_loss_take_profit(
                    data_15min, 'buy', current['close'], current['atr']
                )
                
                # Be more cautious with counter-trend trades
                if market_condition['trend'] != 'strongly_bearish':
                    signal.buy()
                    signal.reason = "Rule 4: Higher TF bearish but oversold, lower TF bullish reversal"
                    signal.stop_loss = stop_loss
                    signal.take_profit = take_profit
                    return signal
            
            return None
            
        except Exception as e:
            logger.error(f"Error generating dual timeframe signal for {ticker}: {e}")
            return None
    
    def to_dict(self):
        """Return strategy details as a dictionary"""
        return {
            "name": self.name,
            "description": "Dual Time Frame Momentum Strategy using DTOSC indicator (15min/4h)",
            "parameters": {
                "higher_timeframe": "4Hour",
                "lower_timeframe": "15Minute",
                "dtosc_params": self.dtosc_params,
                "risk_reward_ratio": self.risk_reward_ratio,
                "stop_loss_atr_multiple": self.stop_loss_atr_multiple
            }
        }