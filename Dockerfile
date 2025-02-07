# Stage 1: Build Tailwind CSS in a separate Node.js container
FROM node:20-alpine AS tailwind-builder

WORKDIR /app

# Copy only the necessary files to install dependencies
COPY package.json package-lock.json ./

# Install dev deps for building Tailwind
RUN npm install -D

# Copy CSS files
COPY app/static/css ./app/static/css

# Build Tailwind output
RUN npx tailwindcss -i ./app/static/css/styles.css -o ./static/css/output.css -m

# Stage 2: Main Python app
FROM python:3.10-slim AS python-fastapi-app

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y libpq-dev curl gnupg \
    && apt-get clean

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Copy and install Python dependencies
COPY pyproject.toml poetry.lock tickers.txt ./
RUN pip install poetry
RUN poetry install --no-root

# Copy Tailwind output from Stage 1
COPY --from=tailwind-builder /app/static/css/output.css /app/static/css/output.css

# Copy application files
COPY dbs ./dbs
COPY app ./app
COPY .env .

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "8000"]
