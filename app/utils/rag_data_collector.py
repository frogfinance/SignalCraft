import logging
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
from typing import List, Dict, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class RagDataCollector:
    """Collects data for RAG system embeddings"""
    
    def __init__(
        self, 
        output_dir: str = "./data/rag_source_data",
        api_keys: Dict = None
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.api_keys = api_keys or {}
    
    async def collect_market_news(self, days_back: int = 7) -> List[Dict]:
        """Collect market news for the past N days"""
        sources = []
        
        # Example: Using a news API (like Alpha Vantage, Polygon, etc.)
        try:
            # Implementation depends on your data sources
            # Example:
            api_key = self.api_keys.get('alpha_vantage')
            if not api_key:
                logger.warning("No API key for news service")
                return sources
                
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Collect news for major indices
            for ticker in ['SPY', 'QQQ', 'IWM', 'DIA']:
                response = requests.get(
                    f"https://www.alphavantage.co/query",
                    params={
                        "function": "NEWS_SENTIMENT",
                        "tickers": ticker,
                        "apikey": api_key,
                        "time_from": start_date.strftime("%Y%m%dT0000"),
                        "time_to": end_date.strftime("%Y%m%dT2359")
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    for item in data.get('feed', []):
                        sources.append({
                            'text': f"Title: {item['title']}\n\nSummary: {item['summary']}",
                            'source': f"news_{item['source']}",
                            'date': item['time_published'],
                            'category': 'market_news',
                            'ticker': ticker
                        })
                
        except Exception as e:
            logger.error(f"Error collecting market news: {e}")
        
        return sources
    
    async def collect_earnings_reports(self, days_back: int = 14) -> List[Dict]:
        """Collect recent earnings reports and analyses"""
        sources = []
        
        # Implementation depends on your data sources
        # Example stub:
        try:
            # Collect earnings reports from your data provider
            pass
        except Exception as e:
            logger.error(f"Error collecting earnings reports: {e}")
        
        return sources
    
    async def collect_research_reports(self) -> List[Dict]:
        """Collect research reports from directories or APIs"""
        sources = []
        
        # Example: Reading PDFs from a research directory
        research_dir = Path("./data/research_reports")
        if research_dir.exists():
            try:
                import PyPDF2
                
                for pdf_file in research_dir.glob("*.pdf"):
                    try:
                        with open(pdf_file, 'rb') as f:
                            reader = PyPDF2.PdfReader(f)
                            text = ""
                            for page in reader.pages:
                                text += page.extract_text() + "\n"
                            
                            # Get metadata from filename or content
                            filename = pdf_file.name
                            date_str = datetime.fromtimestamp(pdf_file.stat().st_mtime).isoformat()
                            
                            sources.append({
                                'text': text[:20000],  # Limit text length
                                'source': f"research_report_{filename}",
                                'date': date_str,
                                'category': 'research_report'
                            })
                    except Exception as e:
                        logger.error(f"Error processing PDF {pdf_file}: {e}")
            except ImportError:
                logger.warning("PyPDF2 not installed, skipping PDF processing")
        
        return sources
    
    async def collect_all_sources(self) -> List[Dict]:
        """Collect all sources for embedding"""
        all_sources = []
        
        # Collect from different sources
        news = await self.collect_market_news()
        earnings = await self.collect_earnings_reports()
        research = await self.collect_research_reports()
        
        all_sources.extend(news)
        all_sources.extend(earnings)
        all_sources.extend(research)
        
        # Save collected sources for reference
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(self.output_dir / f"sources_{timestamp}.json", 'w') as f:
            json.dump(all_sources, f)
        
        logger.info(f"Collected {len(all_sources)} sources for embedding")
        return all_sources