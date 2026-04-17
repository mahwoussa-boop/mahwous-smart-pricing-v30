#!/bin/bash

# ═══════════════════════════════════════════════════════════════
# Google Cloud Run Deployment Script
# لنشر تطبيق Mahwous Smart Pricing على Google Cloud Run
# ═══════════════════════════════════════════════════════════════

set -e

# الألوان للطباعة
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ═══════════════════════════════════════════════════════════════
# إعدادات المشروع
# ═══════════════════════════════════════════════════════════════

PROJECT_ID="mahwous-smart-pricing-v30"
SERVICE_NAME="mahwous-smart-pricing"
REGION="us-central1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
DOCKERFILE="Dockerfile"
MEMORY="2Gi"
CPU="2"
TIMEOUT="3600"
MAX_INSTANCES="10"
MIN_INSTANCES="0"

# ═══════════════════════════════════════════════════════════════
# دوال مساعدة
# ═══════════════════════════════════════════════════════════════

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

# ═══════════════════════════════════════════════════════════════
# التحقق من المتطلبات
# ═══════════════════════════════════════════════════════════════

check_requirements() {
    print_header "التحقق من المتطلبات"
    
    # التحقق من gcloud
    if ! command -v gcloud &> /dev/null; then
        print_error "gcloud CLI غير مثبت. يرجى تثبيت Google Cloud SDK"
        exit 1
    fi
    print_success "gcloud CLI موجود"
    
    # التحقق من docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker غير مثبت"
        exit 1
    fi
    print_success "Docker موجود"
    
    # التحقق من Dockerfile
    if [ ! -f "$DOCKERFILE" ]; then
        print_error "ملف $DOCKERFILE غير موجود"
        exit 1
    fi
    print_success "Dockerfile موجود"
}

# ═══════════════════════════════════════════════════════════════
# تسجيل الدخول إلى Google Cloud
# ═══════════════════════════════════════════════════════════════

authenticate_gcloud() {
    print_header "تسجيل الدخول إلى Google Cloud"
    
    # التحقق من التسجيل الحالي
    CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
    
    if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
        print_info "تعيين المشروع إلى $PROJECT_ID"
        gcloud config set project $PROJECT_ID
    fi
    
    print_success "المشروع: $PROJECT_ID"
}

# ═══════════════════════════════════════════════════════════════
# بناء صورة Docker
# ═══════════════════════════════════════════════════════════════

build_docker_image() {
    print_header "بناء صورة Docker"
    
    print_info "بناء الصورة: $IMAGE_NAME:latest"
    docker build -t $IMAGE_NAME:latest -f $DOCKERFILE .
    
    if [ $? -eq 0 ]; then
        print_success "تم بناء الصورة بنجاح"
    else
        print_error "فشل بناء الصورة"
        exit 1
    fi
}

# ═══════════════════════════════════════════════════════════════
# دفع الصورة إلى Google Container Registry
# ═══════════════════════════════════════════════════════════════

push_docker_image() {
    print_header "دفع الصورة إلى Google Container Registry"
    
    # تكوين Docker للوصول إلى GCR
    print_info "تكوين Docker للوصول إلى GCR..."
    gcloud auth configure-docker
    
    print_info "دفع الصورة: $IMAGE_NAME:latest"
    docker push $IMAGE_NAME:latest
    
    if [ $? -eq 0 ]; then
        print_success "تم دفع الصورة بنجاح"
    else
        print_error "فشل دفع الصورة"
        exit 1
    fi
}

# ═══════════════════════════════════════════════════════════════
# نشر على Cloud Run
# ═══════════════════════════════════════════════════════════════

deploy_to_cloud_run() {
    print_header "نشر على Google Cloud Run"
    
    print_info "جاري النشر..."
    print_info "الخدمة: $SERVICE_NAME"
    print_info "المنطقة: $REGION"
    print_info "الذاكرة: $MEMORY"
    print_info "CPU: $CPU"
    
    gcloud run deploy $SERVICE_NAME \
        --image $IMAGE_NAME:latest \
        --platform managed \
        --region $REGION \
        --memory $MEMORY \
        --cpu $CPU \
        --timeout $TIMEOUT \
        --max-instances $MAX_INSTANCES \
        --min-instances $MIN_INSTANCES \
        --allow-unauthenticated \
        --set-env-vars="STREAMLIT_SERVER_HEADLESS=true,STREAMLIT_BROWSER_GATHER_USAGE_STATS=false"
    
    if [ $? -eq 0 ]; then
        print_success "تم النشر بنجاح على Cloud Run"
    else
        print_error "فشل النشر على Cloud Run"
        exit 1
    fi
}

# ═══════════════════════════════════════════════════════════════
# الحصول على رابط الخدمة
# ═══════════════════════════════════════════════════════════════

get_service_url() {
    print_header "معلومات الخدمة"
    
    SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
        --platform managed \
        --region $REGION \
        --format='value(status.url)')
    
    print_success "رابط الخدمة: $SERVICE_URL"
    echo ""
    print_info "يمكنك الوصول إلى التطبيق من: $SERVICE_URL"
}

# ═══════════════════════════════════════════════════════════════
# إضافة متغيرات البيئة
# ═══════════════════════════════════════════════════════════════

set_environment_variables() {
    print_header "إضافة متغيرات البيئة"
    
    print_warning "لم يتم إضافة متغيرات البيئة تلقائياً"
    print_info "يرجى إضافة المتغيرات التالية يدوياً في Cloud Run:"
    echo ""
    echo "  gcloud run services update $SERVICE_NAME \\"
    echo "    --region $REGION \\"
    echo "    --update-env-vars GEMINI_API_KEY=YOUR_KEY_HERE"
    echo ""
    print_info "أو استخدم Google Cloud Console:"
    echo "  https://console.cloud.google.com/run/detail/$REGION/$SERVICE_NAME/revisions"
}

# ═══════════════════════════════════════════════════════════════
# عرض السجلات
# ═══════════════════════════════════════════════════════════════

view_logs() {
    print_header "عرض السجلات"
    
    print_info "السجلات الأخيرة:"
    gcloud run services describe $SERVICE_NAME \
        --platform managed \
        --region $REGION
    
    print_info "لعرض السجلات المباشرة:"
    echo "  gcloud run services logs read $SERVICE_NAME --region $REGION --limit 50"
}

# ═══════════════════════════════════════════════════════════════
# البرنامج الرئيسي
# ═══════════════════════════════════════════════════════════════

main() {
    print_header "🚀 نشر Mahwous Smart Pricing على Google Cloud Run"
    
    # التحقق من المتطلبات
    check_requirements
    
    # تسجيل الدخول
    authenticate_gcloud
    
    # بناء الصورة
    read -p "هل تريد بناء صورة Docker جديدة؟ (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        build_docker_image
    fi
    
    # دفع الصورة
    read -p "هل تريد دفع الصورة إلى GCR؟ (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        push_docker_image
    fi
    
    # النشر
    read -p "هل تريد النشر على Cloud Run؟ (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        deploy_to_cloud_run
        get_service_url
    fi
    
    # إضافة متغيرات البيئة
    set_environment_variables
    
    print_header "✅ اكتمل النشر!"
}

# تشغيل البرنامج الرئيسي
main
