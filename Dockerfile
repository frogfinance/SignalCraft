FROM python:3.10-slim

WORKDIR /app

# Optional: If jemalloc is unnecessary, avoid installing it
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && apt-get clean

# Set environment variables
ENV PYTHONUNBUFFERED=1

COPY app/pyproject.toml app/poetry.lock tickers.txt /app/

RUN pip install poetry
RUN poetry install --no-root

COPY dbs .
COPY app .

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", ":8000"]
