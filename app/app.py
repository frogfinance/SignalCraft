from contextlib import asynccontextmanager
import asyncio, logging
import logging.config
from fastapi import FastAPI
from app.algo_trader import TradingSystem
from app.utils import log_util

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan function to manage startup and shutdown tasks."""
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

@app.get("/")
def read_root():
    return {"message": "Trading system is running"}
