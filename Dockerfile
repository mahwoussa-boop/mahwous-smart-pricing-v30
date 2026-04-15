# Mahwous Smart Pricing - Google Cloud Run Dockerfile
# Optimized for Cloud Run with Streamlit & Gemini API
# ══════════════════════════════════════════════════════════════

FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DATA_DIR=/data \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /data /data/_scraper_chunks && \
    mkdir -p /app/.streamlit

# Create non-root user for security
RUN useradd -m -u 1000 streamlit && \
    chown -R streamlit:streamlit /app /data

USER streamlit

# Health check for Streamlit on Cloud Run dynamic PORT
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD sh -c 'curl -sf http://localhost:${PORT:-8080}/_stcore/health || exit 1'

EXPOSE 8080

# Run the application through the dynamic-port entrypoint
CMD ["python", "docker_entrypoint.py"]
