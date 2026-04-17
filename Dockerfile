# Streamlit app — optimized for Google Cloud Run deployment.
FROM python:3.12-slim-bookworm

WORKDIR /app

# Persistent data directory (Cloud Run: mount a volume here if needed)
ENV DATA_DIR=/data

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install build dependencies + curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    curl \
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
