from contextlib import asynccontextmanager
import asyncio, logging
from fastapi import FastAPI
from app.algo_trader import run_algo_trader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan function to manage startup and shutdown tasks."""
    logging.info("Starting the application and initializing resources...")
    
    # Start the algorithmic trading task
    loop = asyncio.get_event_loop()
    trader_task = loop.create_task(run_algo_trader())
    
    try:
        # Application is running
        yield
    finally:
        # Perform cleanup
        logging.info("Shutting down background tasks...")
        trader_task.cancel()  # Cancel the background trading task
        try:
            await trader_task
        except asyncio.CancelledError:
            logging.info("Background task successfully cancelled.")
        logging.info("Application shutdown complete.")


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"message": "Trading system is running"}
