#!/usr/bin/env bash

# نشر آمن لتطبيق Mahwous Smart Pricing على Google Cloud Run باستخدام صورة Docker.
# لا يكتب الأسرار في المستودع، بل يقرأها من البيئة وقت التنفيذ فقط.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEFAULT_PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
PROJECT_ID="${PROJECT_ID:-${GOOGLE_CLOUD_PROJECT:-$DEFAULT_PROJECT_ID}}"
SERVICE_NAME="${SERVICE_NAME:-$(basename "$SCRIPT_DIR")}"
REGION="${REGION:-us-central1}"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
DOCKERFILE="Dockerfile"
MEMORY="${MEMORY:-4Gi}"
CPU="${CPU:-2}"
TIMEOUT="${TIMEOUT:-3600}"
MAX_INSTANCES="${MAX_INSTANCES:-10}"
MIN_INSTANCES="${MIN_INSTANCES:-0}"
CONCURRENCY="${CONCURRENCY:-80}"
DATA_DIR="${DATA_DIR:-/app/data}"
GCS_DB_BLOB_NAME="${GCS_DB_BLOB_NAME:-vision2030/pricing_v30.db}"
GCS_SYNC_COOLDOWN="${GCS_SYNC_COOLDOWN:-60}"
PLATFORM="managed"
SKIP_BUILD="${SKIP_BUILD:-false}"
SKIP_PUSH="${SKIP_PUSH:-false}"
SKIP_DEPLOY="${SKIP_DEPLOY:-false}"

print_header() {
    echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

usage() {
    cat <<EOF
الاستخدام:
  ./cloud-run-deploy.sh

أمثلة:
  PROJECT_ID=sanguine-orb-493713-q6 REGION=northamerica-south1 ./cloud-run-deploy.sh
  PROJECT_ID=sanguine-orb-493713-q6 SERVICE_NAME=mahwous-smart-pricing-v30 REGION=northamerica-south1 GEMINI_API_KEY=AIza... GCS_BUCKET_NAME=my-bucket ./cloud-run-deploy.sh
  SKIP_BUILD=true SKIP_PUSH=true ./cloud-run-deploy.sh

المهم:
  - هذا السكربت ينتقل تلقائياً إلى جذر المستودع: $SCRIPT_DIR
  - الأسرار تُقرأ من البيئة فقط ولا تُكتب داخل Git.
EOF
}

check_requirements() {
    print_header "التحقق من المتطلبات"

    if ! command -v gcloud >/dev/null 2>&1; then
        print_error "gcloud CLI غير مثبت"
        exit 1
    fi
    print_success "gcloud CLI موجود"

    if ! command -v docker >/dev/null 2>&1; then
        print_error "Docker غير مثبت"
        exit 1
    fi
    print_success "Docker موجود"

    if [ ! -f "$DOCKERFILE" ]; then
        print_error "ملف $DOCKERFILE غير موجود داخل $SCRIPT_DIR"
        exit 1
    fi
    print_success "Dockerfile موجود في جذر المشروع"

    if [ ! -f "app.py" ]; then
        print_error "ملف app.py غير موجود داخل $SCRIPT_DIR"
        exit 1
    fi
    print_success "app.py موجود في جذر المشروع"

    if [ -z "$PROJECT_ID" ]; then
        print_error "لم يتم تحديد PROJECT_ID ولم يتم العثور على مشروع فعّال في gcloud"
        echo "مثال: PROJECT_ID=sanguine-orb-493713-q6 REGION=northamerica-south1 ./cloud-run-deploy.sh"
        exit 1
    fi
    print_success "PROJECT_ID = $PROJECT_ID"
}

authenticate_gcloud() {
    print_header "التحقق من إعدادات Google Cloud"
    CURRENT_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"

    if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
        print_info "تعيين المشروع إلى $PROJECT_ID"
        gcloud config set project "$PROJECT_ID" >/dev/null
    fi

    print_success "المشروع الحالي: $PROJECT_ID"
    print_info "الخدمة: $SERVICE_NAME"
    print_info "المنطقة: $REGION"
    print_info "المجلد الفعلي للنشر: $SCRIPT_DIR"
}

build_docker_image() {
    print_header "بناء صورة Docker"
    print_info "بناء الصورة: $IMAGE_NAME:latest"
    docker build -t "$IMAGE_NAME:latest" -f "$DOCKERFILE" .
    print_success "تم بناء الصورة بنجاح"
}

push_docker_image() {
    print_header "دفع الصورة إلى Google Container Registry"
    print_info "تكوين Docker للوصول إلى GCR"
    gcloud auth configure-docker --quiet

    print_info "دفع الصورة: $IMAGE_NAME:latest"
    docker push "$IMAGE_NAME:latest"
    print_success "تم دفع الصورة بنجاح"
}

build_env_string() {
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
        print_warning "GCS_BUCKET_NAME غير مضبوط — لن يكون الحفظ بعد إعادة التشغيل مضموناً بالكامل."
    else
        print_success "تم رصد GCS_BUCKET_NAME"
    fi

    if [ -z "${GEMINI_API_KEY:-${GEMINI_API_KEYS:-}}" ]; then
        print_warning "لا يوجد GEMINI_API_KEY أو GEMINI_API_KEYS — ميزات الذكاء لن تعمل بالكامل."
    else
        print_success "تم رصد إعدادات الذكاء الاصطناعي"
    fi

    ENV_STRING="$(IFS=,; echo "${ENV_VARS[*]}")"
}

deploy_to_cloud_run() {
    print_header "نشر على Google Cloud Run"
    build_env_string

    gcloud run deploy "$SERVICE_NAME" \
        --image "$IMAGE_NAME:latest" \
        --platform "$PLATFORM" \
        --region "$REGION" \
        --memory "$MEMORY" \
        --cpu "$CPU" \
        --timeout "$TIMEOUT" \
        --max-instances "$MAX_INSTANCES" \
        --min-instances "$MIN_INSTANCES" \
        --concurrency "$CONCURRENCY" \
        --allow-unauthenticated \
        --set-env-vars "$ENV_STRING"

    print_success "تم النشر بنجاح على Cloud Run"
}

get_service_url() {
    print_header "معلومات الخدمة"

    SERVICE_URL="$(gcloud run services describe "$SERVICE_NAME" \
        --platform "$PLATFORM" \
        --region "$REGION" \
        --format='value(status.url)')"

    print_success "رابط الخدمة: $SERVICE_URL"
    print_info "لعرض السجلات: gcloud run services logs read $SERVICE_NAME --region $REGION --limit 50"
}

main() {
    if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
        usage
        exit 0
    fi

    print_header "🚀 نشر Mahwous Smart Pricing على Google Cloud Run"
    check_requirements
    authenticate_gcloud

    if [ "$SKIP_BUILD" != "true" ]; then
        build_docker_image
    else
        print_warning "تم تجاوز build لأن SKIP_BUILD=true"
    fi

    if [ "$SKIP_PUSH" != "true" ]; then
        push_docker_image
    else
        print_warning "تم تجاوز push لأن SKIP_PUSH=true"
    fi

    if [ "$SKIP_DEPLOY" != "true" ]; then
        deploy_to_cloud_run
        get_service_url
    else
        print_warning "تم تجاوز deploy لأن SKIP_DEPLOY=true"
    fi

    print_header "✅ اكتمل النشر"
}

main "$@"
