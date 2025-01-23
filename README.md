SignalCraft - everymans trading system
====

SignalCraft is a Python-based trading system that leverages powerful tools like `backtesting.py`, `duckdb` and the `alpaca-py` [library](https://pypi.org/project/alpaca-py/)

To interact with the Alpaca API. To use this project an alpaca API and SECRET key will need to be obtained.

Get an account at -> https://alpaca.markets

#### Disclaimer

SignalCraft is intended for educational and experimentation purposes only. It does not provide financial, trading, or investment advice. Use this project at your own risk, and consult a licensed financial professional before making any trading decisions. The creators of SignalCraft are not responsible for any financial losses or damages resulting from the use of this software.

### Run the trader!

1. Setup account with Alpaca
2. Update `tickers.txt`
3. Create a new file `.env` & copy the default values from `.env-example`
4. Update the `.env` with Alpaca API
5. Install Docker (if needed)
6. Run `docker compose up -d` to start the Algo Trader

### Getting Started - Local Development & Backtesting

SignalCraft uses [duckdb](https://duckdb.org/) file bases databases to keep our valuable CPU resources limited to our application and not our database. 

1. Install [poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
2. Install project dependencies: Run `poetry install`. 
3. Update the `tickers.txt` file with your desired tickers.
4. Generate the data needed for strategies: Run `poetry run python scripts/create_and_seed_db.py`
5. (Optional) Run the app from the test suite: Run `poetry run pytest`
    - the test suite will simulate live trading by generating fake price data for each ticker
6. (Optional) Implement your strategy and add to the `app/strategy_handler.py`.
7. (Optional) Run it with paper account. Copy the `.env-example` to `.env` and add your API credentials from Alpaca. Then run: `poetry run fastapi dev app/app.py`
8. Run it live with docker. `docker compose up -d`

### Adding strategies

To add a new strategy you should extend the `backtesing.py` Strategy class.

### SignalCraft Markov Test Suite

This test suite provides a predictive model using Markov chains to "predict" the next candle for a stock and apply your strategy to randomized data.

### Backtesing