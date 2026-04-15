# استخدام صورة بايثون الرسمية
FROM python:3.12-slim-bookworm

# تثبيت تبعات النظام الأساسية
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

# تحديد مجلد العمل داخل الحاوية
WORKDIR /app

# نسخ ملف المتطلبات وتثبيتها
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# --- التعديل الهام: تثبيت Playwright مع كافة مكتبات النظام الضرورية ---
RUN playwright install --with-deps chromium

# نسخ بقية ملفات المشروع
COPY . .

# تعيين المنفذ الافتراضي لـ Streamlit
EXPOSE 8501

# تشغيل التطبيق عبر ملف التوجيه الخاص بك
ENTRYPOINT ["python3", "docker_entrypoint.py"]
