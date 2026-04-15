# ══════════════════════════════════════════════════════════════
#  Mahwous — Production Dockerfile (Phase 4)
#  Lean, secure, layer-cached, non-root
# ══════════════════════════════════════════════════════════════
FROM python:3.12-slim-bookworm AS base

# ── Environment ──────────────────────────────────────────────
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DATA_DIR=/data \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_MAX_MESSAGE_SIZE=500 \
    STREAMLIT_SERVER_MAX_UPLOAD_SIZE=1000

WORKDIR /app

# ── OS Dependencies (minimal) ───────────────────────────────
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc g++ libffi-dev ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# ── Python Dependencies (layer-cached) ──────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && find /usr/local/lib/python3.12 -name '__pycache__' -exec rm -rf {} + 2>/dev/null; true

# ── التعديل الهام جداً: تثبيت متصفح Playwright ─────────────────
RUN playwright install --with-deps chromium

# ── Application Source ───────────────────────────────────────
COPY . .

# ── Data & Chunk Directories ────────────────────────────────
RUN mkdir -p /data /data/_scraper_chunks \
    && mkdir -p /app/.streamlit

# ── Non-root User (security) ────────────────────────────────
RUN groupadd -r mahwous && useradd -r -g mahwous -d /app mahwous \
    && chown -R mahwous:mahwous /app /data
USER mahwous

# ── Health Check ─────────────────────────────────────────────
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:${PORT:-8501}/_stcore/health || exit 1

EXPOSE 8501

# ── Entrypoint ───────────────────────────────────────────────
CMD ["python3", "docker_entrypoint.py"]
