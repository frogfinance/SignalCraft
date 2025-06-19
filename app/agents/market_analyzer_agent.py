import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import asyncio
import time

from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from .base_agent import BaseAgent, Message

logger = logging.getLogger(__name__)

class MarketAnalyzerAgent(BaseAgent):
    """
    Agent that analyzes market conditions using VXX and major indices 
    to provide market sentiment and risk assessments.
    """
    
    def __init__(
        self,
        agent_id: str = None,
        name: str = "Market Analyzer",
        openai_api_key: str = None,
        alpaca_api_key: str = None,
        alpaca_secret_key: str = None,
        alpaca_paper: bool = True,
        model: str = "gpt-4o",
        temperature: float = 0.2,
        max_tokens: int = 1000,
        lookback_days: int = 30,
        update_interval: int = 60  # minutes
    ):
        super().__init__(
            agent_id=agent_id,
            name=name,
            openai_api_key=openai_api_key,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens
        )
        
        # Alpaca clients
        self.alpaca_api_key = alpaca_api_key
        self.alpaca_secret_key = alpaca_secret_key
        self.alpaca_paper = alpaca_paper
        self.trading_client = None
        self.data_client = None
        
        if alpaca_api_key and alpaca_secret_key:
            self.trading_client = TradingClient(
                api_key=alpaca_api_key,
                secret_key=alpaca_secret_key,
                paper=alpaca_paper
            )
            self.data_client = StockHistoricalDataClient(
                api_key=alpaca_api_key,
                secret_key=alpaca_secret_key
            )
        
        # Market data settings
        self.indices = ["SPY", "QQQ", "IWM", "DIA", "VXX"]
        self.lookback_days = lookback_days
        self.update_interval = update_interval
        
        # Cache for market data and analysis
        self.market_data = {}
        self.last_analysis = None
        self.last_update_time = None
        self.market_conditions = {
            "timestamp": None,
            "overall_condition": "unknown",
            "volatility": "unknown",
            "trend": "unknown",
            "indices": {},
            "correlations": {},
            "risk_level": "unknown"
        }
        
        logger.info(f"Market Analyzer Agent initialized with lookback of {lookback_days} days")
    
    async def fetch_market_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetch historical market data for VXX and indices from Alpaca.
        
        Returns:
            Dictionary mapping tickers to DataFrames with OHLCV data
        """
        if not self.data_client:
            logger.error("Alpaca data client not initialized")
            return {}
        
        end = datetime.now()
        start = end - timedelta(days=self.lookback_days)
        
        market_data = {}
        
        for ticker in self.indices:
            try:
                # Request historical bars
                request_params = StockBarsRequest(
                    symbol_or_symbols=ticker,
                    timeframe=TimeFrame.Day,
                    start=start,
                    end=end
                )
                
                bars = self.data_client.get_stock_bars(request_params)
                df = bars.df
                
                # If multi-index, convert to single index
                if isinstance(df.index, pd.MultiIndex):
                    df = df.reset_index(level=0)
                
                # Ensure we have expected columns
                if not all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume']):
                    logger.warning(f"Missing expected columns for {ticker}")
                    continue
                
                market_data[ticker] = df
                logger.debug(f"Fetched {len(df)} days of data for {ticker}")
                
            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {str(e)}")
        
        return market_data
    
    def analyze_volatility(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Analyze market volatility using VXX and index price movement.
        
        Args:
            data: Dictionary of DataFrames with market data
            
        Returns:
            Dictionary with volatility metrics
        """
        volatility_metrics = {
            "vxx_level": None,
            "vxx_change": None,
            "vxx_percentile": None,
            "index_volatility": {},
            "assessment": "unknown"
        }
        
        # Extract VXX data if available
        if "VXX" in data and not data["VXX"].empty:
            vxx_df = data["VXX"]
            
            # Current VXX level
            current_vxx = vxx_df['close'].iloc[-1]
            volatility_metrics["vxx_level"] = current_vxx
            
            # VXX 5-day change
            if len(vxx_df) >= 5:
                five_day_ago = vxx_df['close'].iloc[-5]
                vxx_change = (current_vxx / five_day_ago - 1) * 100
                volatility_metrics["vxx_change"] = vxx_change
            
            # VXX percentile rank (where is current VXX in the distribution?)
            vxx_percentile = (vxx_df['close'].rank(pct=True).iloc[-1]) * 100
            volatility_metrics["vxx_percentile"] = vxx_percentile
            
            # Determine volatility regime
            if vxx_percentile > 80:
                assessment = "high"
            elif vxx_percentile > 60:
                assessment = "elevated"
            elif vxx_percentile < 20:
                assessment = "low"
            else:
                assessment = "normal"
            
            volatility_metrics["assessment"] = assessment
        
        # Calculate historical volatility for each index
        for ticker, df in data.items():
            if ticker != "VXX" and not df.empty and len(df) > 10:
                # Calculate 10-day rolling volatility (annualized)
                returns = np.log(df['close'] / df['close'].shift(1))
                vol_10d = returns.rolling(10).std() * np.sqrt(252) * 100  # annualized percentage
                current_vol = vol_10d.iloc[-1]
                
                # Historical context
                vol_percentile = (vol_10d.rank(pct=True).iloc[-1]) * 100
                
                volatility_metrics["index_volatility"][ticker] = {
                    "current": current_vol,
                    "percentile": vol_percentile
                }
        
        return volatility_metrics
    
    def analyze_trend(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Analyze market trends across indices.
        
        Args:
            data: Dictionary of DataFrames with market data
            
        Returns:
            Dictionary with trend metrics
        """
        trend_metrics = {
            "index_trends": {},
            "breadth": None,
            "assessment": "unknown"
        }
        
        # Count indices in uptrend vs downtrend
        uptrends = 0
        downtrends = 0
        
        for ticker, df in data.items():
            if ticker != "VXX" and not df.empty and len(df) > 20:
                # Calculate SMAs
                df['sma20'] = df['close'].rolling(20).mean()
                df['sma50'] = df['close'].rolling(50).mean()
                
                # Current price vs SMAs
                current_price = df['close'].iloc[-1]
                above_sma20 = current_price > df['sma20'].iloc[-1]
                above_sma50 = current_price > df['sma50'].iloc[-1]
                
                # Determine trend
                if above_sma20 and above_sma50:
                    trend = "uptrend"
                    uptrends += 1
                elif not above_sma20 and not above_sma50:
                    trend = "downtrend"
                    downtrends += 1
                elif above_sma20 and not above_sma50:
                    trend = "recovering"
                else:
                    trend = "weakening"
                
                # Calculate 5-day return
                if len(df) >= 5:
                    five_day_return = (df['close'].iloc[-1] / df['close'].iloc[-5] - 1) * 100
                else:
                    five_day_return = None
                
                # Store metrics
                trend_metrics["index_trends"][ticker] = {
                    "trend": trend,
                    "above_sma20": above_sma20,
                    "above_sma50": above_sma50,
                    "five_day_return": five_day_return
                }
        
        # Calculate market breadth
        total_indices = uptrends + downtrends
        if total_indices > 0:
            breadth = uptrends / total_indices
            trend_metrics["breadth"] = breadth
            
            # Overall trend assessment
            if breadth > 0.8:
                assessment = "strong_bullish"
            elif breadth > 0.6:
                assessment = "bullish"
            elif breadth > 0.4:
                assessment = "neutral"
            elif breadth > 0.2:
                assessment = "bearish"
            else:
                assessment = "strong_bearish"
            
            trend_metrics["assessment"] = assessment
        
        return trend_metrics
    
    def calculate_correlations(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Calculate correlations between indices and VXX.
        
        Args:
            data: Dictionary of DataFrames with market data
            
        Returns:
            Dictionary with correlation metrics
        """
        correlation_metrics = {
            "matrix": {},
            "vxx_correlations": {},
            "assessment": "unknown"
        }
        
        # Extract closing prices
        close_prices = {}
        for ticker, df in data.items():
            if not df.empty:
                close_prices[ticker] = df['close']
        
        # Create a DataFrame with all closing prices
        if close_prices:
            df_prices = pd.DataFrame(close_prices)
            
            # Calculate correlation matrix
            corr_matrix = df_prices.corr()
            
            # Convert to dictionary (serialize)
            correlation_metrics["matrix"] = corr_matrix.to_dict()
            
            # Extract VXX correlations specifically
            if "VXX" in corr_matrix.columns:
                for ticker, correlation in corr_matrix["VXX"].items():
                    if ticker != "VXX":
                        correlation_metrics["vxx_correlations"][ticker] = correlation
                
                # Average VXX correlation with indices
                avg_vxx_corr = sum(correlation_metrics["vxx_correlations"].values()) / len(correlation_metrics["vxx_correlations"])
                
                # Assess correlation regime
                if avg_vxx_corr < -0.7:
                    assessment = "strong_negative"
                elif avg_vxx_corr < -0.3:
                    assessment = "negative"
                elif avg_vxx_corr < 0.3:
                    assessment = "weak"
                elif avg_vxx_corr < 0.7:
                    assessment = "positive"
                else:
                    assessment = "strong_positive"
                
                correlation_metrics["assessment"] = assessment
        
        return correlation_metrics
    
    def generate_market_conditions(self) -> Dict[str, Any]:
        """
        Generate a comprehensive market condition assessment.
        
        Returns:
            Dictionary with market condition assessment
        """
        if not self.market_data or len(self.market_data) == 0:
            logger.warning("No market data available for analysis")
            return self.market_conditions
        
        # Analyze volatility
        volatility = self.analyze_volatility(self.market_data)
        
        # Analyze trends
        trend = self.analyze_trend(self.market_data)
        
        # Calculate correlations
        correlations = self.calculate_correlations(self.market_data)
        
        # Determine overall market condition
        overall_condition = "unknown"
        risk_level = "unknown"
        
        # Determine market condition based on trend and volatility
        trend_assessment = trend.get("assessment", "unknown")
        volatility_assessment = volatility.get("assessment", "unknown")
        
        if trend_assessment in ["strong_bullish", "bullish"]:
            if volatility_assessment in ["low", "normal"]:
                overall_condition = "bullish_low_risk"
                risk_level = "low"
            else:
                overall_condition = "bullish_high_risk"
                risk_level = "elevated"
        elif trend_assessment == "neutral":
            if volatility_assessment in ["low", "normal"]:
                overall_condition = "neutral_low_risk"
                risk_level = "moderate"
            else:
                overall_condition = "neutral_high_risk"
                risk_level = "high"
        else:  # bearish or strong_bearish
            if volatility_assessment in ["high", "elevated"]:
                overall_condition = "bearish_high_risk"
                risk_level = "very_high"
            else:
                overall_condition = "bearish_moderate_risk"
                risk_level = "high"
        
        # Compile indices data
        indices_data = {}
        for ticker in self.indices:
            if ticker in self.market_data:
                df = self.market_data[ticker]
                if not df.empty:
                    current_price = df['close'].iloc[-1]
                    previous_close = df['close'].iloc[-2] if len(df) > 1 else None
                    daily_change = ((current_price / previous_close) - 1) * 100 if previous_close else None
                    
                    indices_data[ticker] = {
                        "price": current_price,
                        "daily_change_pct": daily_change,
                        "trend": trend.get("index_trends", {}).get(ticker, {}).get("trend", "unknown")
                    }
        
        # Compile the final market conditions assessment
        market_conditions = {
            "timestamp": datetime.now().isoformat(),
            "overall_condition": overall_condition,
            "volatility": volatility_assessment,
            "trend": trend_assessment,
            "indices": indices_data,
            "correlations": {
                "vxx_correlation": correlations.get("assessment", "unknown"),
                "details": correlations.get("vxx_correlations", {})
            },
            "risk_level": risk_level,
            "vxx_data": {
                "level": volatility.get("vxx_level"),
                "change": volatility.get("vxx_change"),
                "percentile": volatility.get("vxx_percentile")
            }
        }
        
        self.market_conditions = market_conditions
        self.last_analysis = datetime.now()
        
        return market_conditions
    
    async def update_data(self, force: bool = False) -> bool:
        """
        Update market data if needed based on update interval.
        
        Args:
            force: Whether to force update regardless of interval
            
        Returns:
            True if data was updated, False otherwise
        """
        current_time = datetime.now()
        
        # Check if we need to update
        if not force and self.last_update_time:
            time_since_update = (current_time - self.last_update_time).total_seconds() / 60
            if time_since_update < self.update_interval:
                logger.debug(f"Skipping update, last update was {time_since_update:.1f} minutes ago")
                return False
        
        try:
            logger.info("Updating market data...")
            market_data = await self.fetch_market_data()
            
            if market_data and len(market_data) > 0:
                self.market_data = market_data
                self.last_update_time = current_time
                logger.info(f"Updated market data for {len(market_data)} symbols")
                return True
            else:
                logger.warning("No market data retrieved in update")
                return False
                
        except Exception as e:
            logger.error(f"Error updating market data: {str(e)}")
            return False
    
    async def get_market_conditions(self, force_update: bool = False) -> Dict[str, Any]:
        """
        Get the current market conditions, updating data if needed.
        
        Args:
            force_update: Whether to force a data update
            
        Returns:
            Dictionary with market condition assessment
        """
        # Update data if needed
        data_updated = await self.update_data(force=force_update)
        
        # If data was updated or we haven't analyzed yet, generate new conditions
        if data_updated or self.last_analysis is None or force_update:
            self.generate_market_conditions()
            logger.info(f"Generated new market conditions assessment: {self.market_conditions['overall_condition']}")
        
        return self.market_conditions
    
    async def process_message(self, message: Message) -> Optional[Dict]:
        """
        Process incoming messages to generate market condition assessments.
        
        Args:
            message: The message requesting market analysis
            
        Returns:
            Dictionary with market condition assessment
        """
        try:
            # Extract message content
            content = message.content
            metadata = message.metadata or {}
            
            # Check if this is a request for market analysis
            if "analyze_market" in content.lower() or metadata.get("request_type") == "market_analysis":
                # Force update if requested
                force_update = "force_update" in content.lower() or metadata.get("force_update", False)
                
                # Get market conditions
                market_conditions = await self.get_market_conditions(force_update=force_update)
                
                # Generate a summary
                summary = self._generate_market_summary(market_conditions)
                
                return {
                    "formatted_text": summary,
                    "market_conditions": market_conditions
                }
            
            # Handle other types of messages
            return {
                "formatted_text": f"Received message: {content}. To analyze market conditions, please send a message with 'analyze_market'.",
                "market_conditions": None
            }
        
        except Exception as e:
            logger.error(f"Error processing message in Market Analyzer: {str(e)}")
            return {
                "formatted_text": f"Error analyzing market conditions: {str(e)}",
                "error": str(e),
                "market_conditions": None
            }
    
    def _generate_market_summary(self, conditions: Dict[str, Any]) -> str:
        """
        Generate a human-readable summary of market conditions.
        
        Args:
            conditions: Market condition assessment dictionary
            
        Returns:
            Formatted string with market summary
        """
        summary = [
            f"# Market Conditions Summary",
            f"\n## Overall Assessment",
            f"- **Market Condition**: {conditions['overall_condition'].replace('_', ' ').title()}",
            f"- **Risk Level**: {conditions['risk_level'].replace('_', ' ').title()}",
            f"- **Trend**: {conditions['trend'].replace('_', ' ').title()}",
            f"- **Volatility**: {conditions['volatility'].replace('_', ' ').title()}",
            f"\n## Index Performance"
        ]
        
        # Add index data
        for ticker, data in conditions['indices'].items():
            change = data.get('daily_change_pct')
            change_str = f"{change:.2f}%" if change is not None else "N/A"
            trend = data.get('trend', 'unknown').replace('_', ' ').title()
            
            summary.append(f"- **{ticker}**: ${data['price']:.2f} ({change_str}) - {trend}")
        
        # Add VXX data
        vxx_data = conditions.get('vxx_data', {})
        if vxx_data.get('level') is not None:
            summary.append(f"\n## Volatility Indicators")
            summary.append(f"- **VXX Level**: ${vxx_data['level']:.2f}")
            
            if vxx_data.get('change') is not None:
                summary.append(f"- **VXX 5-Day Change**: {vxx_data['change']:.2f}%")
            
            if vxx_data.get('percentile') is not None:
                summary.append(f"- **VXX Percentile**: {vxx_data['percentile']:.1f}th percentile")
        
        # Add correlation data
        summary.append(f"\n## Market Correlations")
        summary.append(f"- **VXX Correlation**: {conditions['correlations']['vxx_correlation'].replace('_', ' ').title()}")
        
        # Add timestamp
        if conditions.get('timestamp'):
            try:
                timestamp = datetime.fromisoformat(conditions['timestamp'])
                formatted_time = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                summary.append(f"\n*Analysis generated at {formatted_time}*")
            except:
                summary.append(f"\n*Analysis generated at {conditions['timestamp']}*")
        
        return "\n".join(summary)
    
    async def run_cycle(self) -> None:
        """Run a single processing cycle for the market analyzer agent."""
        # Process any messages in the inbox
        message = self.get_next_message()
        if message:
            response = await self.process_message(message)
            logger.info(f"Market Analyzer processed message and generated assessment")
        
        # Periodically update market data and analysis
        current_time = datetime.now()
        time_since_update = float('inf')
        
        if self.last_update_time:
            time_since_update = (current_time - self.last_update_time).total_seconds() / 60
        
        if time_since_update >= self.update_interval:
            await self.update_data()
            self.generate_market_conditions()
            logger.info("Market Analyzer performed scheduled market assessment update")