from contextlib import asynccontextmanager
from datetime import datetime
import json
import asyncio, logging
import logging.config
import plotly.graph_objects as go
from pathlib import Path
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from app.algo_trader import TradingSystem
from app.utils import log_util
from alpaca.trading import OrderSide

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

trading_system = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan function to manage startup and shutdown tasks."""
    global trading_system
    logging.info("Starting the application and initializing resources...")
    trading_system = TradingSystem()

    # Start the algorithmic trading task
    loop = asyncio.get_event_loop()
    trader_task = loop.create_task(trading_system.run())
    
    try:
        # Application is running
        yield
    finally:
        # Perform cleanup
        logging.info("Shutting down background tasks...")
        trader_task.cancel()  # Cancel the background trading task
        if trading_system.data_handler is not None:
            trading_system.data_handler.shutdown()
        try:
            await trader_task
        except asyncio.CancelledError:
            logging.info("Background task successfully cancelled.")
        logging.info("Application shutdown complete.")

app = FastAPI(lifespan=lifespan)

logging_config = log_util.gen_logging_config()
logging.config.dictConfig(logging_config)

# Set up templates and static files for dashboard
BASE_DIR = Path(__file__).resolve().parent
logging.info("BASE_DIR: %r", BASE_DIR)
app.mount("/static", StaticFiles(directory=BASE_DIR  / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# Set global variables available in all templates
templates.env.globals["current_year"] = datetime.now().year

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    account_info = trading_system.execution_handler.position_manager.get_account_info()
    open_positions = trading_system.execution_handler.position_manager.positions
    trade_history = trading_system.execution_handler.get_trades()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "account": account_info,
        "positions": open_positions,
        "trades": trade_history
    })

@app.get("/chart/{ticker}", response_class=HTMLResponse)
async def stock_chart(request: Request, ticker: str):
    data = trading_system.data_handler.get_historical_data(ticker)
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
