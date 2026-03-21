FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt .
COPY requirements_web.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements_web.txt

# Install additional packages
RUN pip install --no-cache-dir \
    telethon \
    cryptg \
    pyaes \
    rsa \
    websocket-client \
    orjson \
    ujson \
    python-multipart \
    aiofiles \
    sqlalchemy \
    psycopg2-binary \
    tweepy \
    feedparser \
    dash \
    plotly \
    dash-core-components \
    dash-html-components \
    dash-table \
    redis \
    gymnasium \
    shimmy \
    tensorflow==2.15.0

# Copy the rest of your application
COPY . .

# Create necessary directories
RUN mkdir -p logs ml_models trained_models training_logs backtest_results

# Expose ports
EXPOSE 5000 8050

# Default command (runs trading bot)
CMD ["python", "trading_system.py", "--mode", "live", "--balance", "30", "--strategy-mode", "voting"]