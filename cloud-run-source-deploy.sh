#!/usr/bin/env bash

# نشر آمن إلى Cloud Run باستخدام --source ولكن من مجلد المستودع الصحيح
# يمكن تشغيله من أي مكان لأنه يحدد SOURCE_DIR تلقائياً من موقع السكربت نفسه.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
SERVICE_NAME="${SERVICE_NAME:-$(basename "$SCRIPT_DIR")}"
REGION="${REGION:-us-central1}"
MEMORY="${MEMORY:-4Gi}"
CPU="${CPU:-2}"
TIMEOUT="${TIMEOUT:-3600}"

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

echo "📦 SOURCE_DIR=$SCRIPT_DIR"
echo "☁️  PROJECT_ID=$PROJECT_ID"
echo "🚀 SERVICE_NAME=$SERVICE_NAME"
echo "🌍 REGION=$REGION"

gcloud config set project "$PROJECT_ID" >/dev/null

gcloud run deploy "$SERVICE_NAME" \
  --source "$SCRIPT_DIR" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --platform managed \
  --allow-unauthenticated \
  --memory "$MEMORY" \
  --cpu "$CPU" \
  --timeout "$TIMEOUT"
