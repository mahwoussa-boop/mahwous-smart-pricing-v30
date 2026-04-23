#!/usr/bin/env bash

# نشر آمن إلى Cloud Run باستخدام --source من جذر المستودع الصحيح.
# يقرأ الإعدادات الحساسة من متغيرات البيئة فقط، ولا يكتب الأسرار داخل Git.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
PROJECT_ID="${PROJECT_ID:-${GOOGLE_CLOUD_PROJECT:-$DEFAULT_PROJECT_ID}}"
SERVICE_NAME="${SERVICE_NAME:-$(basename "$SCRIPT_DIR")}"
REGION="${REGION:-us-central1}"
MEMORY="${MEMORY:-4Gi}"
CPU="${CPU:-2}"
TIMEOUT="${TIMEOUT:-3600}"
MAX_INSTANCES="${MAX_INSTANCES:-10}"
MIN_INSTANCES="${MIN_INSTANCES:-0}"
CONCURRENCY="${CONCURRENCY:-80}"
DATA_DIR="${DATA_DIR:-/app/data}"
GCS_DB_BLOB_NAME="${GCS_DB_BLOB_NAME:-vision2030/pricing_v30.db}"
GCS_SYNC_COOLDOWN="${GCS_SYNC_COOLDOWN:-60}"

if ! command -v gcloud >/dev/null 2>&1; then
  echo "❌ gcloud CLI غير مثبت"
  exit 1
fi

if [ -z "$PROJECT_ID" ]; then
  echo "❌ حدّد PROJECT_ID أولاً"
  echo "مثال: PROJECT_ID=sanguine-orb-493713-q6 REGION=northamerica-south1 ./cloud-run-source-deploy.sh"
  exit 1
fi

if [ ! -f "$SCRIPT_DIR/Dockerfile" ]; then
  echo "❌ Dockerfile غير موجود داخل $SCRIPT_DIR"
  exit 1
fi

if [ ! -f "$SCRIPT_DIR/app.py" ]; then
  echo "❌ app.py غير موجود داخل $SCRIPT_DIR"
  exit 1
fi

echo "📦 SOURCE_DIR=$SCRIPT_DIR"
echo "☁️  PROJECT_ID=$PROJECT_ID"
echo "🚀 SERVICE_NAME=$SERVICE_NAME"
echo "🌍 REGION=$REGION"

gcloud config set project "$PROJECT_ID" >/dev/null

ENV_VARS=(
  "STREAMLIT_SERVER_HEADLESS=true"
  "STREAMLIT_BROWSER_GATHER_USAGE_STATS=false"
  "PYTHONUNBUFFERED=1"
  "DATA_DIR=$DATA_DIR"
  "GCP_PROJECT_ID=$PROJECT_ID"
  "GCS_DB_BLOB_NAME=$GCS_DB_BLOB_NAME"
  "GCS_SYNC_COOLDOWN=$GCS_SYNC_COOLDOWN"
)

append_env_if_set() {
  local key="$1"
  local value="${!key:-}"
  if [ -n "$value" ]; then
    ENV_VARS+=("${key}=${value}")
  fi
}

append_env_if_set "GCS_BUCKET_NAME"
append_env_if_set "GEMINI_API_KEY"
append_env_if_set "GEMINI_API_KEYS"
append_env_if_set "OPENROUTER_API_KEY"
append_env_if_set "COHERE_API_KEY"
append_env_if_set "WEBHOOK_UPDATE_PRICES"
append_env_if_set "WEBHOOK_NEW_PRODUCTS"
append_env_if_set "EXTRA_API_KEY"
append_env_if_set "GOOGLE_API_KEY"
append_env_if_set "CLOUD_SQL_CONNECTION_NAME"
append_env_if_set "DB_USER"
append_env_if_set "DB_PASS"
append_env_if_set "DB_NAME"
append_env_if_set "USE_FIRESTORE"

if [ -z "${GCS_BUCKET_NAME:-}" ]; then
  echo "⚠️  GCS_BUCKET_NAME غير مضبوط — سيعمل التطبيق لكن الحفظ سيكون محلياً فقط داخل الحاوية وقد لا يصمد بعد إعادة التشغيل."
else
  echo "✅ GCS_BUCKET_NAME مضبوط: ${GCS_BUCKET_NAME}"
fi

if [ -z "${GEMINI_API_KEY:-${GEMINI_API_KEYS:-}}" ]; then
  echo "⚠️  لا يوجد GEMINI_API_KEY أو GEMINI_API_KEYS — مسارات الذكاء لن تعمل بالكامل."
else
  echo "✅ تم رصد إعدادات الذكاء الاصطناعي من البيئة"
fi

ENV_STRING="$(IFS=,; echo "${ENV_VARS[*]}")"

gcloud run deploy "$SERVICE_NAME" \
  --source "$SCRIPT_DIR" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --platform managed \
  --allow-unauthenticated \
  --memory "$MEMORY" \
  --cpu "$CPU" \
  --timeout "$TIMEOUT" \
  --max-instances "$MAX_INSTANCES" \
  --min-instances "$MIN_INSTANCES" \
  --concurrency "$CONCURRENCY" \
  --set-env-vars "$ENV_STRING"

SERVICE_URL="$(gcloud run services describe "$SERVICE_NAME" --region "$REGION" --format='value(status.url)')"
echo "✅ تم النشر: $SERVICE_URL"
