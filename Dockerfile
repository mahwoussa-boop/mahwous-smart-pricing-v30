# Streamlit app — optimized for Google Cloud Run deployment.
FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    CHROME_BIN=/usr/bin/chromium \
    CHROME_PATH=/usr/lib/chromium/

# nginx added: acts as a reverse proxy that sets Cache-Control: no-store on
# HTML responses, preventing the browser from caching stale JS chunk hashes
# after a redeployment (root cause of "Failed to fetch dynamically imported
# module" / TypeError errors that appear in the Streamlit UI).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    curl \
    nginx \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run injects PORT=8080; expose the same default
EXPOSE 8080

# Health check hits nginx which answers /_stcore/health immediately
# (nginx starts before Streamlit, so this is always fast)
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl --fail http://localhost:8080/_stcore/health || exit 1

# Entrypoint: starts nginx on $PORT first, then Streamlit on 127.0.0.1:8501
CMD ["python3", "docker_entrypoint.py"]
