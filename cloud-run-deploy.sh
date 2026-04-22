#!/usr/bin/env bash

# ═══════════════════════════════════════════════════════════════
# Google Cloud Run Deployment Script
# نشر آمن لتطبيق Mahwous Smart Pricing على Google Cloud Run
# هذا السكربت يجبر التنفيذ من مجلد المستودع نفسه حتى لا يتكرر
# خطأ `gcloud run deploy --source .` من مجلد المنزل ~
# ═══════════════════════════════════════════════════════════════

set -euo pipefail

# الألوان للطباعة
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ═══════════════════════════════════════════════════════════════
# الإعدادات القابلة للتخصيص
# يمكن تمريرها كمتغيرات بيئة قبل التشغيل
# ═══════════════════════════════════════════════════════════════

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
PLATFORM="managed"

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
  PROJECT_ID=sanguine-orb-493713-q6 SERVICE_NAME=mahwous-smart-pricing-v30 REGION=northamerica-south1 ./cloud-run-deploy.sh

مهم:
  هذا السكربت ينتقل تلقائياً إلى مجلد المستودع: $SCRIPT_DIR
  وبذلك يمنع رفع مجلد المنزل ~ بالخطأ عند النشر.
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

deploy_to_cloud_run() {
    print_header "نشر على Google Cloud Run"

    gcloud run deploy "$SERVICE_NAME" \
        --image "$IMAGE_NAME:latest" \
        --platform "$PLATFORM" \
        --region "$REGION" \
        --memory "$MEMORY" \
        --cpu "$CPU" \
        --timeout "$TIMEOUT" \
        --max-instances "$MAX_INSTANCES" \
        --min-instances "$MIN_INSTANCES" \
        --allow-unauthenticated \
        --set-env-vars="STREAMLIT_SERVER_HEADLESS=true,STREAMLIT_BROWSER_GATHER_USAGE_STATS=false"

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

set_environment_variables() {
    print_header "تذكير بمتغيرات البيئة"
    print_warning "أضف المتغيرات السرية يدويًا بعد النشر إذا لم تكن موجودة على الخدمة"
    echo "gcloud run services update $SERVICE_NAME \\
  --region $REGION \\
  --update-env-vars GEMINI_API_KEY=YOUR_KEY_HERE"
}

main() {
    if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
        usage
        exit 0
    fi

    print_header "🚀 نشر Mahwous Smart Pricing على Google Cloud Run"
    check_requirements
    authenticate_gcloud

    read -p "هل تريد بناء صورة Docker جديدة؟ (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        build_docker_image
    fi

    read -p "هل تريد دفع الصورة إلى GCR؟ (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        push_docker_image
    fi

    read -p "هل تريد النشر على Cloud Run؟ (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        deploy_to_cloud_run
        get_service_url
    fi

    set_environment_variables
    print_header "✅ اكتمل النشر"
}

main "$@"
