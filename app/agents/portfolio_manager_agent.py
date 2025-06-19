import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from .base_agent import BaseAgent, Message

logger = logging.getLogger(__name__)

class PortfolioManagerAgent(BaseAgent):
    """
    Agent for portfolio management that provides risk-based recommendations 
    for managing trading positions.
    """
    
    def __init__(
        self,
        agent_id: str = None,
        name: str = "Portfolio Manager",
        openai_api_key: str = None,
        model: str = "gpt-4o",
        temperature: float = 0.2,  # Lower temperature for more consistent recommendations
        max_tokens: int = 1500,
        max_loss_pct: float = 7.0,  # Default max loss percentage
        max_portfolio_risk: float = 15.0,  # Default maximum portfolio risk
        risk_per_trade: float = 1.0  # Default risk per trade (% of portfolio)
    ):
        super().__init__(
            agent_id=agent_id,
            name=name,
            openai_api_key=openai_api_key,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens
        )
        
        # Risk management parameters
        self.max_loss_pct = max_loss_pct
        self.max_portfolio_risk = max_portfolio_risk
        self.risk_per_trade = risk_per_trade
        
        # Current portfolio state
        self.current_portfolio = {}
        self.market_data = {}
        self.account_info = {}
        
        logger.info(f"Portfolio Manager Agent initialized with max loss: {max_loss_pct}%, "
                   f"max portfolio risk: {max_portfolio_risk}%, risk per trade: {risk_per_trade}%")
    
    def update_portfolio(self, positions: List[Dict], account_info: Dict = None) -> None:
        """
        Update the agent's view of the current portfolio.
        
        Args:
            positions: List of position dictionaries with details
            account_info: Account information including equity, buying power, etc.
        """
        self.current_portfolio = {p["ticker"]: p for p in positions} if positions else {}
        if account_info:
            self.account_info = account_info
        logger.info(f"Portfolio updated with {len(self.current_portfolio)} positions")
    
    def update_market_data(self, market_data: Dict) -> None:
        """
        Update market data relevant to the portfolio.
        
        Args:
            market_data: Dictionary of market data including price history, indicators, etc.
        """
        self.market_data = market_data
        logger.info(f"Market data updated for {len(market_data)} symbols")
    
    def get_positions_at_risk(self) -> List[Dict]:
        """
        Identify positions that have reached or exceeded the maximum loss threshold.
        
        Returns:
            List of positions that should be considered for exit
        """
        at_risk = []
        for ticker, position in self.current_portfolio.items():
            # Check if position is in a loss greater than max_loss_pct
            if position.get("pl_pct", 0) <= -self.max_loss_pct / 100:
                at_risk.append(position)
        
        return at_risk
    
    def calculate_portfolio_risk(self) -> float:
        """
        Calculate the current portfolio risk as a percentage of account equity.
        
        Returns:
            Total portfolio risk as a percentage
        """
        if not self.account_info.get("equity"):
            return 0
        
        equity = float(self.account_info.get("equity", 0))
        total_exposure = 0
        
        for ticker, position in self.current_portfolio.items():
            position_value = float(position.get("qty", 0)) * float(position.get("current_price", 0))
            total_exposure += position_value
        
        return (total_exposure / equity) * 100 if equity > 0 else 0
    
    def generate_portfolio_recommendations(self) -> Dict[str, Any]:
        """
        Generate comprehensive portfolio recommendations based on current positions,
        risk levels, and market conditions.
        
        Returns:
            Dictionary of recommendations
        """
        # Identify positions at risk (losses exceeding threshold)
        at_risk_positions = self.get_positions_at_risk()
        
        # Calculate current portfolio risk
        current_risk = self.calculate_portfolio_risk()
        
        # Prepare portfolio summary
        portfolio_summary = {
            "total_positions": len(self.current_portfolio),
            "total_value": sum(float(p.get("qty", 0)) * float(p.get("current_price", 0)) 
                              for p in self.current_portfolio.values()),
            "account_equity": self.account_info.get("equity", 0),
            "buying_power": self.account_info.get("buying_power", 0),
            "portfolio_risk": current_risk,
            "positions_at_risk": len(at_risk_positions)
        }
        
        # Generate specific recommendations
        recommendations = {
            "summary": portfolio_summary,
            "exit_recommendations": [],
            "risk_adjustments": [],
            "portfolio_allocation": [],
            "timestamp": datetime.now().isoformat()
        }
        
        # Exit recommendations for positions at risk
        for position in at_risk_positions:
            recommendations["exit_recommendations"].append({
                "ticker": position.get("ticker"),
                "action": "SELL",
                "reason": f"Position has lost {abs(position.get('pl_pct', 0) * 100):.2f}%, "
                         f"exceeding max loss threshold of {self.max_loss_pct}%",
                "urgency": "High" if position.get("pl_pct", 0) <= -self.max_loss_pct * 1.5 / 100 else "Medium"
            })
        
        # Risk adjustment recommendations
        if current_risk > self.max_portfolio_risk:
            recommendations["risk_adjustments"].append({
                "action": "REDUCE_EXPOSURE",
                "reason": f"Portfolio risk ({current_risk:.2f}%) exceeds maximum allowed ({self.max_portfolio_risk}%)",
                "recommendation": "Consider reducing position sizes or hedging to bring overall risk below threshold"
            })
        
        # Use AI to provide more nuanced swing trade recommendations
        if self.openai_api_key:
            ai_recommendations = self._get_ai_recommendations()
            if ai_recommendations:
                recommendations["ai_insights"] = ai_recommendations
        
        return recommendations
    
    def _get_ai_recommendations(self) -> Optional[Dict]:
        """
        Use OpenAI to generate more nuanced portfolio recommendations.
        
        Returns:
            Dictionary of AI-generated insights and recommendations
        """
        if not self.client:
            logger.warning("OpenAI client not configured, skipping AI recommendations")
            return None
        
        # Prepare portfolio context for the AI
        portfolio_context = {
            "positions": list(self.current_portfolio.values()),
            "account": self.account_info,
            "market_conditions": self.market_data,
            "risk_parameters": {
                "max_loss_pct": self.max_loss_pct,
                "max_portfolio_risk": self.max_portfolio_risk,
                "risk_per_trade": self.risk_per_trade
            }
        }
        
        # Create a message for the AI
        messages = [
            {"role": "system", "content": """
            You are a professional portfolio manager with expertise in swing trading and risk management.
            Your task is to analyze the portfolio data and provide actionable recommendations.
            Focus on:
            1. Identifying positions that should be exited due to exceeding risk thresholds
            2. Suggesting position adjustments (size increases/decreases) based on risk
            3. Providing swing trade entry and exit recommendations based on technical analysis
            4. Highlighting overall portfolio allocation concerns
            
            Your recommendations should be specific, actionable, and include clear reasoning.
            """},
            {"role": "user", "content": f"Here is the current portfolio data:\n{portfolio_context}"}
        ]
        
        try:
            response = self.chat_completion(messages, temperature=0.2)
            return {
                "insights": response.choices[0].message.content,
                "generation_time": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error generating AI recommendations: {str(e)}")
            return None
    
    async def process_message(self, message: Message) -> Optional[Dict]:
        """
        Process incoming messages to generate portfolio recommendations.
        
        Args:
            message: The message containing portfolio data or request
            
        Returns:
            Dictionary containing formatted output and structured recommendations
        """
        try:
            # Extract message content
            content = message.content
            metadata = message.metadata or {}
            
            # Check if this is a request for portfolio analysis
            if "analyze_portfolio" in content.lower() or metadata.get("request_type") == "portfolio_analysis":
                # Check if portfolio data is in metadata
                if metadata.get("positions"):
                    self.update_portfolio(metadata.get("positions"), metadata.get("account_info"))
                
                if metadata.get("market_data"):
                    self.update_market_data(metadata.get("market_data"))
                
                # Generate recommendations
                recommendations = self.generate_portfolio_recommendations()
                
                # Create a formatted human-readable string
                formatted_response = self._format_recommendations(recommendations)
                
                # Return both the formatted response and the structured data
                return {
                    "formatted_text": formatted_response,
                    "recommendations": recommendations,
                    "actions": self._extract_actionable_items(recommendations)
                }
            
            # Handle other types of messages
            return {
                "formatted_text": f"Received message: {content}. To analyze your portfolio, please send a message with 'analyze_portfolio' and include position data in metadata.",
                "recommendations": None,
                "actions": None
            }
        
        except Exception as e:
            logger.error(f"Error processing message in Portfolio Manager: {str(e)}")
            return {
                "formatted_text": f"Error analyzing portfolio: {str(e)}",
                "error": str(e),
                "recommendations": None,
                "actions": None
            }

    def _extract_actionable_items(self, recommendations: Dict) -> Dict:
        """
        Extract actionable items from recommendations in a structured format.
        
        Args:
            recommendations: The full recommendations dictionary
            
        Returns:
            Dictionary with actionable items organized by type
        """
        actions = {
            "exit_positions": [],
            "reduce_exposure": False,
            "portfolio_adjustments": [],
            "risk_level": "normal"
        }
        
        # Process exit recommendations
        for rec in recommendations.get("exit_recommendations", []):
            actions["exit_positions"].append({
                "ticker": rec["ticker"],
                "side": "SELL",
                "urgency": rec["urgency"],
                "reason": rec["reason"]
            })
        
        # Process risk adjustments
        for rec in recommendations.get("risk_adjustments", []):
            if rec["action"] == "REDUCE_EXPOSURE":
                actions["reduce_exposure"] = True
        
        # Set overall risk level
        summary = recommendations.get("summary", {})
        portfolio_risk = float(summary.get("portfolio_risk", 0))
        
        if portfolio_risk > self.max_portfolio_risk * 1.2:
            actions["risk_level"] = "critical"
        elif portfolio_risk > self.max_portfolio_risk:
            actions["risk_level"] = "high"
        elif portfolio_risk > self.max_portfolio_risk * 0.7:
            actions["risk_level"] = "elevated"
        else:
            actions["risk_level"] = "normal"
        
        # Add position-specific adjustments from AI insights if available
        if "ai_insights" in recommendations and recommendations["ai_insights"]:
            # This would require parsing the text from AI insights
            # For now, just including the raw insights for manual review
            actions["ai_insights_available"] = True
        
        return actions
    
    async def run_cycle(self) -> None:
        """Run a single processing cycle for the portfolio manager agent."""
        # Process any messages in the inbox
        message = self.get_next_message()
        if message:
            response = await self.process_message(message)
            logger.info(f"Portfolio Manager processed message and generated recommendations")
        
        # Periodically analyze portfolio (even without messages)
        if self.current_portfolio and not self.inbox.qsize():
            # Only do this if we have portfolio data and no pending messages
            current_time = datetime.now()
            last_analysis = self.memory.get("last_analysis_time")
            
            # If we haven't analyzed in the last hour, do it again
            if not last_analysis or (current_time - last_analysis).total_seconds() > 3600:
                recommendations = self.generate_portfolio_recommendations()
                self.memory["last_recommendations"] = recommendations
                self.memory["last_analysis_time"] = current_time
                logger.info("Portfolio Manager performed scheduled portfolio analysis")