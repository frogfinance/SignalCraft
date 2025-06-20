import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import os
from pathlib import Path
import duckdb

# Add these imports
import openai
from openai import OpenAI
from sklearn.preprocessing import normalize
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .base_agent import BaseAgent, Message

logger = logging.getLogger(__name__)

class PortfolioManagerAgent(BaseAgent):
    """
    Agent for portfolio management that provides risk-based recommendations 
    with RAG capabilities for market context and historical insights.
    """
    
    def __init__(
        self,
        agent_id: str = None,
        name: str = "Portfolio Manager",
        openai_api_key: str = None,
        model: str = "gpt-3.5-turbo-16k",
        embedding_model: str = "text-embedding-ada-002",
        temperature: float = 0.2,
        max_tokens: int = 1500,
        max_loss_pct: float = 7.0,
        max_portfolio_risk: float = 15.0,
        risk_per_trade: float = 1.0,
        rag_data_path: str = "./data/portfolio_rag",
        embedding_dim: int = 1536  # Dimension for OpenAI embeddings
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
        
        # RAG system configuration
        self.embedding_model = embedding_model
        self.embedding_dim = embedding_dim
        self.rag_data_path = Path(rag_data_path)
        self.rag_data_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize RAG components
        self.knowledge_sources = []
        self.index = None
        self.embeddings_df = None
        self.embeddings_last_updated = None
        
        # Initialize RAG system
        self._initialize_rag_system()
        
        # Set up scheduler for weekly embedding updates
        self.scheduler = AsyncIOScheduler()
        self.scheduler.add_job(
            self._update_embeddings, 
            'interval', 
            weeks=1,
            id='update_embeddings',
            next_run_time=self._get_next_embedding_update_time()
        )
        self.scheduler.start()
        
        logger.info(f"Portfolio Manager Agent initialized with RAG system at {rag_data_path}")
    
    def _get_next_embedding_update_time(self):
        """Determine when to next update embeddings (weekend during off-hours)"""
        now = datetime.now()
        # Set next update to next Saturday at 2 AM
        days_until_saturday = (5 - now.weekday()) % 7
        if days_until_saturday == 0 and now.hour >= 2:
            days_until_saturday = 7
        
        next_saturday = now + timedelta(days=days_until_saturday)
        next_update = datetime(
            next_saturday.year, 
            next_saturday.month, 
            next_saturday.day, 
            2, 0, 0  # 2:00 AM
        )
        
        return next_update
    
    def _initialize_rag_system(self):
        """Initialize the RAG system, loading existing embeddings if available"""
        # Check for existing embeddings database
        embeddings_path = self.rag_data_path / "embeddings.duckdb"
        # Change this path to match the new approach:
        index_path = self.rag_data_path / "embeddings_metadata.json"
        metadata_path = self.rag_data_path / "metadata.json"
        
        if embeddings_path.exists() and metadata_path.exists():
            # Load existing embeddings
            try:
                # Load embeddings from DuckDB
                conn = duckdb.connect(str(embeddings_path))
                self.embeddings_df = conn.execute("SELECT * FROM embeddings").fetchdf()
                conn.close()
                
                # Don't need to load a separate index since we're using NumPy directly
                self.index = None
                
                # Load metadata
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                    self.embeddings_last_updated = datetime.fromisoformat(metadata.get('last_updated', ''))
                    self.knowledge_sources = metadata.get('sources', [])
                
                logger.info(f"Loaded RAG system with {len(self.embeddings_df)} embeddings last updated on {self.embeddings_last_updated}")
            except Exception as e:
                logger.error(f"Error loading existing RAG system: {e}")
                self._create_new_rag_system()
        else:
            # Create new RAG system
            self._create_new_rag_system()
    
    def _create_new_rag_system(self):
        """Create a new RAG system with empty embeddings"""
        # Create empty embeddings dataframe
        self.embeddings_df = pd.DataFrame({
            'id': [],
            'text': [],
            'source': [],
            'date': [],
            'category': [],
            'ticker': [],
            'embedding': []
        })
        
        # No separate index needed - will use numpy directly
        self.index = None
        
        # Set metadata
        self.embeddings_last_updated = datetime.now()
        self.knowledge_sources = []
        
        # Save empty RAG system
        self._save_rag_system()
        
        logger.info("Created new empty RAG system")
    
    def _save_rag_system(self):
        """Save the current RAG system to disk"""
        try:
            # Save embeddings to DuckDB
            conn = duckdb.connect(str(self.rag_data_path / "embeddings.duckdb"))
            
            # Register the DataFrame with DuckDB first
            conn.register('embeddings_df', self.embeddings_df)
            
            # Now the query will work because embeddings_df is registered
            conn.execute("CREATE TABLE IF NOT EXISTS embeddings AS SELECT * FROM embeddings_df")
            conn.execute("DELETE FROM embeddings")
            conn.execute("INSERT INTO embeddings SELECT * FROM embeddings_df")
            conn.close()
            
            # Save metadata
            metadata = {
                'last_updated': self.embeddings_last_updated.isoformat(),
                'sources': self.knowledge_sources,
                'embedding_count': len(self.embeddings_df)
            }
            
            with open(self.rag_data_path / "metadata.json", 'w') as f:
                json.dump(metadata, f)
            
            logger.info(f"Saved RAG system with {len(self.embeddings_df)} embeddings")
            return True
        except Exception as e:
            logger.error(f"Error saving RAG system: {e}")
            return False
    
    async def _update_embeddings(self):
        """Update embeddings with new data sources (runs weekly)"""
        logger.info("Starting weekly embedding update")
        try:
            # 1. Collect new data sources
            new_sources = await self._collect_new_data_sources()
            
            if not new_sources:
                logger.info("No new data sources to embed")
                return
            
            # 2. Generate embeddings for new sources
            new_embeddings = []
            for source in new_sources:
                try:
                    embedding = self._generate_embedding(source['text'])
                    new_embeddings.append({
                        'id': len(self.embeddings_df) + len(new_embeddings),
                        'text': source['text'],
                        'source': source['source'],
                        'date': source['date'],
                        'category': source.get('category', 'general'),
                        'ticker': source.get('ticker', ''),
                        'embedding': embedding
                    })
                except Exception as e:
                    logger.error(f"Error generating embedding for source {source['source']}: {e}")
            
            # 3. Add new embeddings to dataframe
            if new_embeddings:
                new_df = pd.DataFrame(new_embeddings)
                self.embeddings_df = pd.concat([self.embeddings_df, new_df], ignore_index=True)
                
                # 4. No need to update an index since we're using NumPy directly
            
                # 5. Update metadata
                self.embeddings_last_updated = datetime.now()
                self.knowledge_sources.extend([s['source'] for s in new_sources])
                
                # 6. Save updated RAG system
                self._save_rag_system()
                
                logger.info(f"Added {len(new_embeddings)} new embeddings to RAG system")
        
        except Exception as e:
            logger.error(f"Error updating embeddings: {e}")
    
    async def _collect_new_data_sources(self) -> List[Dict]:
        """
        Collect new data sources for embedding.
        Override this method to implement specific data collection.
        """
        # Example implementation - in practice, you would collect from various sources
        sources = []
        
        # 1. Collect market news and analysis
        # (implementation would depend on your available data sources)
        
        # 2. Collect earnings reports
        # (implementation would depend on your available data sources)
        
        # 3. Collect research reports
        # (implementation would depend on your available data sources)
        
        # 4. Collect historical trade performance
        if hasattr(self, 'memory') and 'historical_trades' in self.memory:
            for trade in self.memory['historical_trades'][-20:]:  # Latest 20 trades
                if trade.get('analysis'):
                    sources.append({
                        'text': trade['analysis'],
                        'source': 'historical_trade',
                        'date': trade['exit_time'],
                        'category': 'trade_analysis',
                        'ticker': trade['ticker']
                    })
        
        # 5. Collect recent portfolio analyses
        if hasattr(self, 'memory') and 'portfolio_analyses' in self.memory:
            for analysis in self.memory['portfolio_analyses'][-5:]:  # Latest 5 analyses
                sources.append({
                    'text': json.dumps(analysis),
                    'source': 'portfolio_analysis',
                    'date': analysis['timestamp'],
                    'category': 'portfolio_analysis'
                })
        
        return sources
    
    def _generate_embedding(self, text: str) -> List[float]:
        """Generate an embedding for the given text using OpenAI"""
        if not self.openai_api_key:
            raise ValueError("OpenAI API key is required for embedding generation")
        
        client = OpenAI(api_key=self.openai_api_key)
        
        # Create embedding
        response = client.embeddings.create(
            model=self.embedding_model,
            input=text
        )
        
        # Extract embedding
        embedding = response.data[0].embedding
        
        return embedding
    
    def _retrieve_relevant_context(self, query: str, top_k: int = 5) -> List[Dict]:
        """
        Retrieve relevant context for a query from the RAG system.
        
        Args:
            query: The query to search for
            top_k: Number of results to return
            
        Returns:
            List of relevant context items with text and metadata
        """
        if len(self.embeddings_df) == 0:
            logger.warning("RAG system not initialized or empty")
            return []
        
        try:
            # Generate embedding for query
            query_embedding = self._generate_embedding(query)
            query_embedding = np.array(query_embedding)
            
            # Get all embeddings as numpy array
            all_embeddings = np.array(self.embeddings_df['embedding'].tolist())
            
            # Calculate cosine similarity
            similarities = np.dot(all_embeddings, query_embedding) / (
                np.linalg.norm(all_embeddings, axis=1) * np.linalg.norm(query_embedding)
            )
            
            # Get top k indices
            top_indices = np.argsort(similarities)[-top_k:][::-1]
            
            # Get relevant context
            results = []
            for idx in top_indices:
                row = self.embeddings_df.iloc[idx]
                results.append({
                    'text': row['text'],
                    'source': row['source'],
                    'date': row['date'],
                    'category': row['category'],
                    'ticker': row['ticker'],
                    'similarity': float(similarities[idx])
                })
            
            return results
        except Exception as e:
            logger.error(f"Error retrieving relevant context: {e}")
            return []
    
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
        Use OpenAI with RAG to generate more nuanced portfolio recommendations.
        
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
            "market_data": self.market_data,
            "risk_parameters": {
                "max_loss_pct": self.max_loss_pct,
                "max_portfolio_risk": self.max_portfolio_risk,
                "risk_per_trade": self.risk_per_trade
            }
        }
        
        # Convert to a string for RAG query
        portfolio_summary = f"""
        Portfolio with {len(self.current_portfolio)} positions. 
        Tickers: {', '.join(self.current_portfolio.keys())}. 
        Current risk: {self.calculate_portfolio_risk():.2f}%.
        Account equity: ${self.account_info.get('equity', 0)}.
        """
        
        # Retrieve relevant context from RAG system
        relevant_context = self._retrieve_relevant_context(portfolio_summary, top_k=5)
        
        # Format retrieved context
        retrieved_knowledge = ""
        if relevant_context:
            retrieved_knowledge = "Relevant market and portfolio insights:\n\n"
            for i, item in enumerate(relevant_context):
                retrieved_knowledge += f"{i+1}. {item['text'][:500]}... (Source: {item['source']}, Date: {item['date']})\n\n"
        
        # Create a message for the AI with RAG context
        messages = [
            {"role": "system", "content": f"""
            You are a professional portfolio manager with expertise in swing trading and risk management.
            Your task is to analyze the portfolio data and provide actionable recommendations.
            
            Focus on:
            1. Identifying positions that should be exited due to exceeding risk thresholds
            2. Suggesting position adjustments (size increases/decreases) based on risk
            3. Providing swing trade entry and exit recommendations based on technical analysis
            4. Highlighting overall portfolio allocation concerns
            
            Consider both the current real-time data and the historical context provided.
            Your recommendations should be specific, actionable, and include clear reasoning.
            
            {retrieved_knowledge}
            """},
            {"role": "user", "content": f"Here is the current portfolio data:\n{portfolio_context}"}
        ]
        
        try:
            response = self.chat_completion(messages, temperature=0.2)
            
            # Store this analysis in memory for future RAG updates
            if hasattr(self, 'memory'):
                if 'portfolio_analyses' not in self.memory:
                    self.memory['portfolio_analyses'] = []
                
                self.memory['portfolio_analyses'].append({
                    'timestamp': datetime.now().isoformat(),
                    'portfolio': portfolio_context,
                    'analysis': response.choices[0].message.content,
                    'relevant_context': [item['text'] for item in relevant_context]
                })
                
                # Limit memory size
                if len(self.memory['portfolio_analyses']) > 50:
                    self.memory['portfolio_analyses'] = self.memory['portfolio_analyses'][-50:]
            
            return {
                "insights": response.choices[0].message.content,
                "generation_time": datetime.now().isoformat(),
                "rag_context_count": len(relevant_context)
            }
        except Exception as e:
            logger.error(f"Error generating AI recommendations: {str(e)}")
            return None
    
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