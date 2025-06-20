import pandas as pd
import numpy as np

def dtosc(df, price_col='close', r1=13, s1=8, s2=5, s3=5):
    """
    Dual Time Oscillator (DTOSC) - a momentum oscillator that combines dual time frames
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing price data
    price_col : str
        Column name for price data (default: 'close')
    r1 : int
        First smoothing period (default: 13)
    s1 : int
        Second smoothing period (default: 8)
    s2 : int
        Third smoothing period (default: 5)
    s3 : int
        Fourth smoothing period (default: 5)
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with DTOSC indicator added
    """
    # Make a copy of the DataFrame
    df = df.copy()
    
    # Step 1: Calculate TSI (True Strength Index) components
    # First difference of closing prices
    pc = df[price_col].diff()
    
    # Double smoothed momentum
    ema1 = pc.ewm(span=r1, adjust=False).mean()
    ema2 = ema1.ewm(span=s1, adjust=False).mean()
    
    # Double smoothed absolute momentum
    abs_pc = pc.abs()
    abs_ema1 = abs_pc.ewm(span=r1, adjust=False).mean()
    abs_ema2 = abs_ema1.ewm(span=s1, adjust=False).mean()
    
    # TSI value
    tsi = 100 * (ema2 / abs_ema2)
    
    # Step 2: Calculate signal line
    signal = tsi.ewm(span=s2, adjust=False).mean()
    
    # Step 3: Calculate histogram
    histogram = tsi - signal
    
    # Step 4: Calculate final oscillator values
    dtosc_value = tsi.ewm(span=s3, adjust=False).mean()
    
    # Add indicators to DataFrame
    df['dtosc'] = dtosc_value
    df['dtosc_signal'] = signal
    df['dtosc_histogram'] = histogram
    
    # Add overbought/oversold markers
    df['dtosc_overbought'] = df['dtosc'] > 25
    df['dtosc_oversold'] = df['dtosc'] < -25
    
    # Add bullish/bearish markers
    df['dtosc_bullish'] = df['dtosc'] > 0
    df['dtosc_bearish'] = df['dtosc'] < 0
    
    # Detect reversals (crossing zero line)
    df['dtosc_bullish_reversal'] = (df['dtosc'] > 0) & (df['dtosc'].shift(1) <= 0)
    df['dtosc_bearish_reversal'] = (df['dtosc'] < 0) & (df['dtosc'].shift(1) >= 0)
    
    # Calculate momentum strength
    df['dtosc_momentum'] = df['dtosc'].abs()
    
    return df