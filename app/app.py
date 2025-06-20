from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import json
import asyncio, logging
import logging.config
import plotly.graph_objects as go
from pathlib import Path
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from app.algo_trader import TradingSystem
from app.utils import log_util
from alpaca.trading import OrderSide
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Add these imports at the top of the file
import uuid

# Add DB manager
from app.models.db_manager import DBManager

logging_config = log_util.gen_logging_config()
logging.config.dictConfig(logging_config)

logger = logging.getLogger("app")

# Initialize trading system and DB manager
trading_system = None
db_manager = DBManager()

# Initialize scheduler
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan function to manage startup and shutdown tasks."""
    global trading_system, db_manager, scheduler
    logging.info("Starting the application and initializing resources...")
    trading_system = TradingSystem()
    db_manager = DBManager()

    # Start the market analyzer scheduler
    scheduler.add_job(
        analyze_market_and_store,
        'interval',
        minutes=15,
        id='market_analysis_job',
        replace_existing=True,
        next_run_time=datetime.now() + timedelta(seconds=30)  # Run first time after 30 seconds
    )
    scheduler.start()

    # Start the algorithmic trading task
    loop = asyncio.get_event_loop()
    trader_task = loop.create_task(trading_system.run())
    logging.info("Trading system initialized")
    
    try:
        # Application is running
        yield
    finally:
        # Perform cleanup
        logging.info("Shutting down background tasks...")
        scheduler.shutdown()
        trader_task.cancel()  # Cancel the background trading task
        if trading_system.data_handler is not None:
            trading_system.data_handler.shutdown()
        if trading_system.backtest_system is not None:
            trading_system.backtest_system.stop_backtest()
        try:
            await trader_task
        except asyncio.CancelledError:
            logging.info("Background task successfully cancelled.")
        logging.info("Application shutdown complete.")

app = FastAPI(lifespan=lifespan)

# Set up templates and static files for dashboard
BASE_DIR = Path(__file__).resolve().parent
logger.info("BASE_DIR: %r", BASE_DIR)
app.mount("/static", StaticFiles(directory=BASE_DIR  / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# Set global variables available in all templates
templates.env.globals["current_year"] = datetime.now().year

def format_datetime(value):
    """Format datetime objects or ISO strings for display."""
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return value
    if isinstance(value, datetime):
        return value.strftime("%m/%d/%Y %H:%M:%S")
    return value

templates.env.filters["datetime"] = format_datetime

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    account_info = trading_system.execution_handler.position_manager.get_account_info()
    open_positions = trading_system.execution_handler.position_manager.get_open_positions()
    trade_history = trading_system.execution_handler.get_trades()
    account_history = trading_system.execution_handler.archiver.account_history
    equity_chart = trading_system.data_handler.generate_equity_curve_chart(account_history)
    # Get recent signals from the strategy handler
    recent_signals = trading_system.strategy_handler.get_recent_signals(limit=20) if hasattr(trading_system.strategy_handler, 'get_recent_signals') else []

    if trading_system.backtest_mode:
        strategies = [s for s in trading_system.strategy_handler.strategies.values()]
        return templates.TemplateResponse("backtest_dashboard.html", {
            "request": request,
            "tickers": trading_system.data_handler.tickers,
            "strategies": strategies,
            "account_info": account_info,
            "positions": open_positions,
            "trades": trade_history,
            "backtest_price_data": dict(), # empty dict on page load
            "recent_signals": recent_signals
        })
    else:
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "account": account_info,
            "positions": open_positions,
            "trades": trade_history,
            "equity_chart": equity_chart,
            "recent_signals": recent_signals,
            "account_history": account_history
        })

@app.get("/backtest", response_class=HTMLResponse)
async def backtest_dashboard(request: Request):
    account_info = trading_system.execution_handler.position_manager.get_account_info()
    open_positions = trading_system.execution_handler.position_manager.positions
    trade_history = trading_system.execution_handler.get_trades()

    strategies = [s for s in trading_system.strategy_handler.strategies.values()]
    return templates.TemplateResponse("backtest_dashboard.html", {
        "request": request,
        "tickers": trading_system.data_handler.tickers,
        "strategies": strategies,
        "account_info": account_info,
        "positions": open_positions,
        "trades": trade_history,
        "backtest_price_data": dict(), # empty dict on page load
    })

@app.get("/chart/{ticker}", response_class=HTMLResponse)
async def stock_chart(request: Request, ticker: str):
    start = datetime.now() - timedelta(days=290)
    end = datetime.now()
    data = trading_system.data_handler.get_historical_data(ticker, start, end)
    trades = trading_system.execution_handler.get_trade_markers(ticker)
    
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=data['datetime'],
        open=data['open'],
        high=data['high'],
        low=data['low'],
        close=data['close']
    ))
    
    if not trades.empty:
        fig.add_trace(go.Scatter(
            x=trades['datetime'],
            y=trades['price'],
            mode='markers',
            marker=dict(color=['green' if t == OrderSide.BUY else 'red' for t in trades['trade_type']], size=10),
            name='Trades'
        ))
    
    chart_html = fig.to_html(full_html=False)
    return templates.TemplateResponse("chart.html", {"request": request, "chart": chart_html})


@app.websocket("/ws/trades")
async def websocket_trades(websocket: WebSocket):
    await websocket.accept()
    while True:
        trades = trading_system.execution_handler.get_trades()
        await websocket.send_text(json.dumps(trades))
        await asyncio.sleep(1)


@app.websocket("/ws/backtest")
async def websocket_backtest(websocket: WebSocket):
    """
    Called by the backtest_dashboard JavaScript to register a new backtest & subscribe to data.
    """
    await trading_system.backtest_system.ws_manager.connect(websocket)

    try:
        while True:
            # Expecting JSON message with ticker and strategy
            message = await websocket.receive_json()

            ticker = message.get("ticker")
            strategy = message.get("strategy")

            if not ticker or not strategy:
                await websocket.send_json({"error": "Missing ticker or strategy"})
                continue

            # check if the backtest is already running and if so tell the frontend
            if ticker in trading_system.backtest_system.running_backtests:
                await websocket.send_json({"is_backtest_running": True})
            else:
                # Log the received message
                logger.info(f"Starting backtest for: {ticker} using {strategy}")

                # Start the backtest in a background task
                asyncio.create_task(trading_system.backtest_system.start_backtest_for_ticker(ticker, strategy))

                await websocket.send_json({"message": "Backtest started."})

    except WebSocketDisconnect:
        logger.warning("WebSocket disconnected.")
        trading_system.backtest_system.stop_backtest()
        await trading_system.backtest_system.ws_manager.disconnect(websocket)

@app.websocket("/ws/signals")
async def websocket_signals(websocket: WebSocket):
    await websocket.accept()
    last_signal_id = 0
    
    try:
        while True:
            # Get new signals since last_signal_id
            if hasattr(trading_system.strategy_handler, 'get_signals_since'):
                new_signals = trading_system.strategy_handler.get_signals_since(last_signal_id)
                
                if new_signals:
                    # Serialize signals to ensure datetime objects are properly converted
                    serialized_signals = serialize_for_json(new_signals)
                    await websocket.send_json({"signals": serialized_signals})
                    
                    # Update last_signal_id
                    last_signal_id = max([s["id"] for s in new_signals])
            
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        logger.warning("Signal WebSocket disconnected.")

@app.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    """
    Generic WebSocket endpoint for real-time dashboard updates.
    Sends different types of data as they change.
    """
    await websocket.accept()
    
    # Track last seen IDs/timestamps for each data type
    last_data = {
        "signals": {"id": 0, "timestamp": datetime.now()},
        "positions": {"timestamp": datetime.now()},
        "trades": {"timestamp": datetime.now()},
        "account": {"timestamp": datetime.now()},
        "check_positions": {"timestamp": datetime.now()}
    }
    
    try:
        # Initial data load
        await send_dashboard_data(websocket, "initial", {
            "signals": trading_system.strategy_handler.get_recent_signals(limit=20) 
                      if hasattr(trading_system.strategy_handler, 'get_recent_signals') else [],
            "positions": trading_system.execution_handler.position_manager.get_open_positions(),
            "account": trading_system.execution_handler.position_manager.get_account_info(),
            "trades": trading_system.execution_handler.archiver.get_trades()
        })
        
        # Continuous updates
        while True:
            updates = {}
            
            # Check for new signals
            if hasattr(trading_system.strategy_handler, 'get_signals_since'):
                new_signals = trading_system.strategy_handler.get_signals_since(last_data["signals"]["id"])
                if new_signals:
                    updates["signals"] = new_signals
                    last_data["signals"]["id"] = max([s["id"] for s in new_signals])
            
            # Check for position updates (every 45 seconds)
            if (datetime.now() - last_data["check_positions"]["timestamp"]).total_seconds() > 45:
                positions = trading_system.execution_handler.position_manager.get_open_positions()
                updates["positions"] = positions
            
                # Check for account updates (every 45 seconds)
                account = trading_system.execution_handler.position_manager.get_account_info()
                updates["account"] = account
            
                # Check for new trades (every 45 seconds)
                trades = trading_system.execution_handler.get_trades()
                updates["trades"] = trades
                last_data["check_positions"]["timestamp"] = datetime.now()
            
            # Send any updates
            if updates:
                await send_dashboard_data(websocket, "update", updates)
            
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        logger.warning("Dashboard WebSocket disconnected.")

async def send_dashboard_data(websocket, message_type, data):
    """
    Helper function to send properly serialized dashboard data through the WebSocket.
    
    Args:
        websocket: The WebSocket connection
        message_type: Type of message ("initial", "update", etc.)
        data: Dictionary of data to send
    """
    message = {
        "type": message_type,
        "timestamp": datetime.now().isoformat(),
        "data": data
    }
    
    # Serialize to handle datetime objects
    serialized_message = serialize_for_json(message)
    await websocket.send_json(serialized_message)

import json
from datetime import datetime

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects and custom classes."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        # Handle Position objects
        if hasattr(obj, 'to_dict') and callable(obj.to_dict):
            return obj.to_dict()
        # Handle enum values
        if hasattr(obj, 'name'):
            return obj.name
        return super().default(obj)

# Add a helper function to serialize data with datetimes
def serialize_for_json(data):
    """Convert data to JSON-serializable format."""
    return json.loads(json.dumps(data, cls=DateTimeEncoder))

# Add the agents route
@app.get("/agents", response_class=HTMLResponse)
async def agents_dashboard(request: Request):
    """Render the agents dashboard view."""
    account_info = trading_system.execution_handler.position_manager.get_account_info()
    account_history = trading_system.execution_handler.archiver.account_history
    
    # Get latest market analysis and historical analyses
    latest_analysis = db_manager.get_latest_market_analysis()
    historical_analyses = db_manager.get_recent_market_analyses(limit=50)
    
    return templates.TemplateResponse("agents.html", {
        "request": request,
        "account": account_info,
        "account_history": account_history,
        "latest_analysis": latest_analysis,
        "historical_analyses": historical_analyses
    })

# Add API routes for market analysis
@app.post("/api/analyze-market")
async def api_analyze_market():
    """API endpoint to trigger a market analysis."""
    try:
        # Run analysis and store results
        success = await analyze_market_and_store()
        
        if success:
            return {"success": True}
        else:
            return {"success": False, "error": "Failed to generate market analysis"}
    except Exception as e:
        logger.error(f"Error analyzing market: {str(e)}")
        return {"success": False, "error": str(e)}

@app.get("/api/market-analysis/{analysis_id}")
async def api_get_market_analysis(analysis_id: int):
    """API endpoint to get a specific market analysis by ID."""
    try:
        # Get specific analysis from DB
        query = """
            SELECT id, timestamp, overall_condition, volatility, trend, risk_level, 
                   indices, vxx_data, correlations, raw_data
            FROM market_analysis
            WHERE id = ?
        """
        result = db_manager.conn.execute(query, [analysis_id]).fetchdf()
        
        if result.empty:
            return {"success": False, "error": "Analysis not found"}
        
        # Convert JSON columns
        analysis = result.iloc[0].to_dict()
        for col in ['indices', 'vxx_data', 'correlations', 'raw_data']:
            if isinstance(analysis[col], str):
                analysis[col] = json.loads(analysis[col])
        
        return {"success": True, "analysis": analysis}
    except Exception as e:
        logger.error(f"Error retrieving market analysis: {str(e)}")
        return {"success": False, "error": str(e)}

# Market analysis function to be called by scheduler and API
async def analyze_market_and_store():
    """Run market analysis and store results in the database."""
    try:
        if not hasattr(trading_system, 'market_analyzer'):
            logger.error("Market analyzer not initialized")
            return False
        
        # Create a message to request market analysis
        from app.agents.base_agent import Message
        message = Message(
            id=str(uuid.uuid4()),
            sender_id="system",
            content="analyze_market force_update",
            metadata={"request_type": "market_analysis", "force_update": True}
        )

        logger.info("Requesting market analysis")
        logger.info("trading system.market_analyzer: %r", trading_system.market_analyzer)
        # Process the message with the market analyzer agent
        response = await trading_system.market_analyzer.process_message(message)
        
        # Check if we got market conditions
        if response and "market_conditions" in response:
            market_conditions = response["market_conditions"]
            
            # Store in database
            success = db_manager.save_market_analysis(market_conditions)
            
            logger.info(f"Market analysis completed and stored: {success}")
            return success
        else:
            logger.warning("Market analyzer did not return market conditions")
            return False
    except Exception as e:
        logger.error(f"Error in market analysis: {str(e)}")
        return False