# Streamlit app — optimized for Google Cloud Run deployment.
FROM python:3.12-slim-bookworm

WORKDIR /app

# Use the app-local data directory by default. Set DATA_DIR only when you
# intentionally mount persistent storage or want a custom writable path.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    CHROME_BIN=/usr/bin/chromium \
    CHROME_PATH=/usr/lib/chromium/

# Install build dependencies + curl for healthcheck + Chromium for Selenium + nginx as cache-busting proxy
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

# Health check — useful for local Docker and container platforms
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl --fail http://localhost:8080/_stcore/health || exit 1

# Entrypoint restores data files from env vars, then launches Streamlit
CMD ["python3", "docker_entrypoint.py"]
