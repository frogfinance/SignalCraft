import duckdb
import pandas as pd
import json
from datetime import datetime
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self, db_path="./dbs/signalcraft.duckdb"):
        # Ensure directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_db()
        
    def _initialize_db(self):
        """Initialize database with required tables if they don't exist"""
        # Create market analysis table - using simpler auto-increment syntax
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS market_analysis (
                id INTEGER PRIMARY KEY,  
                timestamp TEXT,
                overall_condition TEXT,
                volatility TEXT,
                trend TEXT,
                risk_level TEXT,
                indices TEXT,
                vxx_data TEXT,
                correlations TEXT,
                raw_data TEXT
            )
        """)
        
        # Create index on timestamp for faster retrieval
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_analysis_timestamp 
            ON market_analysis(timestamp)
        """)
        
    def save_market_analysis(self, market_conditions):
        """Save market analysis to the database."""
        try:
            # Get the next available ID
            result = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM market_analysis").fetchone()
            next_id = result[0] if result and result[0] is not None else 1
            
            # Convert dict values to JSON strings
            indices_json = json.dumps(market_conditions.get('indices', {}))
            vxx_data_json = json.dumps(market_conditions.get('vxx_data', {}))
            correlations_json = json.dumps(market_conditions.get('correlations', {}))
            raw_data_json = json.dumps(market_conditions)
            
            # Include the ID in the insert statement
            self.conn.execute("""
                INSERT INTO market_analysis 
                (id, timestamp, overall_condition, volatility, trend, risk_level, indices, vxx_data, correlations, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                next_id,
                market_conditions.get('timestamp', datetime.now().isoformat()),
                market_conditions.get('overall_condition', 'unknown'),
                market_conditions.get('volatility', 'unknown'),
                market_conditions.get('trend', 'unknown'),
                market_conditions.get('risk_level', 'unknown'),
                indices_json,
                vxx_data_json,
                correlations_json,
                raw_data_json
            ])
            
            self.conn.commit()
            logger.info(f"Saved market analysis at {market_conditions.get('timestamp')}")
            return True
        except Exception as e:
            logger.error(f"Error saving market analysis: {e}")
            return False
    
    def get_recent_market_analyses(self, limit=50):
        """Get the most recent market analyses"""
        try:
            query = """
                SELECT id, timestamp, overall_condition, volatility, trend, risk_level, 
                       indices, vxx_data, correlations, raw_data
                FROM market_analysis
                ORDER BY timestamp DESC
                LIMIT ?
            """
            result = self.conn.execute(query, [limit]).fetchdf()
            
            # Convert JSON strings back to dictionaries
            if not result.empty:
                for col in ['indices', 'vxx_data', 'correlations', 'raw_data']:
                    result[col] = result[col].apply(json.loads)
            
            return result
        except Exception as e:
            logger.error(f"Error retrieving market analyses: {str(e)}")
            return pd.DataFrame()
    
    def get_latest_market_analysis(self):
        """Get the most recent market analysis"""
        analyses = self.get_recent_market_analyses(limit=1)
        if not analyses.empty:
            return analyses.iloc[0].to_dict()
        return None
    
    def __del__(self):
        """Close connection when object is destroyed"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()