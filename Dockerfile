FROM python:3.11-slim

WORKDIR /app

# System deps for matplotlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl cron \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY server/ server/
COPY scripts/ scripts/

# Runtime directories (will be volume-mounted with actual data)
RUN mkdir -p data strategies deployments backtest logs

# Cron setup
COPY scripts/cron_setup.sh scripts/cron_setup.sh
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
