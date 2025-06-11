import asyncio
import logging
import argparse
import sys
from datetime import datetime
from app.backtester import BacktestingSystem
from alpaca.data import TimeFrame
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BacktestRunner:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.backtester = None
        self.results = {
            'trades': [],
            'daily_stats': [],
            'final_stats': None
        }
        
    async def handle_backtest_message(self, message):
        """Handle messages from the backtester"""
        if 'trade' in message and message['trade']:
            # Record trade
            self.results['trades'].append(message['trade'])
            self.print_trade(message['trade'], message['balance'])
            
        if 'stats' in message:
            # Record daily stats
            daily_stat = {
                'timestamp': message.get('trade', {}).get('timestamp', datetime.now().isoformat()),
                'balance': message['balance'],
                'stats': message['stats']
            }
            self.results['daily_stats'].append(daily_stat)
            
        if 'positions' in message:
            self.print_positions(message['positions'])

    def print_trade(self, trade, balance):
        """Print trade information in a formatted way"""
        print("\n=== New Trade Executed ===")
        print(f"Timestamp: {trade['timestamp']}")
        print(f"Ticker: {trade['ticker']}")
        print(f"Side: {trade['side']}")
        print(f"Price: ${trade['price']:.2f}")
        print(f"Quantity: {trade['qty']}")
        print(f"Current Balance: ${balance:.2f}")
        print("========================\n")

    def print_positions(self, positions):
        """Print current positions"""
        if positions:
            print("\nCurrent Positions:")
            for position in positions:
                print(position)
            print()

    def print_final_stats(self):
        """Print final backtest statistics"""
        if not self.results['daily_stats']:
            return
        
        final_stats = self.results['daily_stats'][-1]['stats']
        initial_balance = 100000  # Assuming initial balance of $100,000
        final_balance = self.results['daily_stats'][-1]['balance']
        
        print("\n=== Backtest Results ===")
        print(f"Total Trades: {final_stats['total_trades']}")
        print(f"Winning Trades: {final_stats['winning_trades']}")
        print(f"Losing Trades: {final_stats['losing_trades']}")
        if final_stats['total_trades'] > 0:
            win_rate = (final_stats['winning_trades'] / final_stats['total_trades']) * 100
            print(f"Win Rate: {win_rate:.2f}%")
        print(f"Initial Balance: ${initial_balance:,.2f}")
        print(f"Final Balance: ${final_balance:,.2f}")
        print(f"Total Return: {((final_balance - initial_balance) / initial_balance) * 100:.2f}%")
        print("=====================\n")

    async def validate_strategy(self, strategy: str) -> bool:
        """Validate if the strategy exists"""
        # You may need to adjust this based on how strategies are registered in your system
        valid_strategies = ["momentum", "mean_reversion", "breakout"]  # Add your available strategies here
        if strategy not in valid_strategies:
            return False
        return True
        
    async def validate_ticker_data(self, tickers: list, timeframe: TimeFrame) -> list:
        """Validate if data is available for the given tickers"""
        invalid_tickers = []
        for ticker in tickers:
            # Create a temporary instance to check data availability
            temp_backtester = BacktestingSystem(
                tickers=[ticker],
                api_key=self.api_key,
                api_secret=self.api_secret,
                timeframe=timeframe
            )
            
            # Check if data is available for this ticker
            has_data = await temp_backtester.data_handler.get_historical_data(ticker)
            if not has_data:
                invalid_tickers.append(ticker)
                
        return invalid_tickers

    async def run_backtest(self, tickers: list, strategy: str = None, timeframe: TimeFrame = TimeFrame.Minute):
        """Run a backtest with the specified parameters"""
        self.backtester = BacktestingSystem(
            tickers=tickers,
            api_key=self.api_key,
            api_secret=self.api_secret,
            timeframe=timeframe
        )

        # Validate strategy
        if strategy:
            strategy_valid = strategy in self.backtester.strategy_handler.supported_strategies
            if not strategy_valid:
                raise ValueError("Strategy '%r' does not exist. Please choose from: %r", strategy, self.backtester.strategy_handler.supported_strategies)
        
        # Validate tickers
        invalid_tickers = await self.validate_ticker_data(tickers, timeframe)
        if invalid_tickers:
            raise ValueError("No data available for the following tickers: %r", invalid_tickers)
            
        # Override the WebSocket manager's send_message method
        self.backtester.ws_manager.send_message = self.handle_backtest_message
        
        # Start the backtest
        await self.backtester.start_backtest_for_ticker(tickers[0], strategy)
        
        # Wait for backtest to complete
        while self.backtester.is_running:
            await asyncio.sleep(1)
            
        self.print_final_stats()
        return self.results

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run a backtest with specified parameters')
    parser.add_argument('--strategy', type=str, required=True, help='Trading strategy to use')
    parser.add_argument('--tickers', type=str, required=True, help='Comma-separated list of ticker symbols')
    parser.add_argument('--timeframe', type=str, default='minute', 
                        choices=['minute', 'hour', 'day'], help='Timeframe for the backtest')
    parser.add_argument('--api-key', type=str, required=False, 
                        default="your_api_key", help='Alpaca API key')
    parser.add_argument('--api-secret', type=str, required=False, 
                        default="your_api_secret", help='Alpaca API secret')
    
    args = parser.parse_args()
    
    # Parse tickers from comma-separated string
    tickers = [ticker.strip() for ticker in args.tickers.split(',')]
    
    # Map timeframe string to TimeFrame enum
    timeframe_map = {
        'minute': TimeFrame.Minute,
        'hour': TimeFrame.Hour,
        'day': TimeFrame.Day
    }
    timeframe = timeframe_map[args.timeframe]
    
    # Your Alpaca API credentials
    API_KEY = args.api_key
    API_SECRET = args.api_secret
    
    try:
        # Create backtest runner
        runner = BacktestRunner(API_KEY, API_SECRET)
        
        # Run backtest for specified tickers and strategy
        results = await runner.run_backtest(
            tickers=tickers,
            strategy=args.strategy,
            timeframe=timeframe
        )
        
    except ValueError as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())