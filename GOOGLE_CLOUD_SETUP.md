# 🔗 دليل ربط مشروع Mahwous Smart Pricing بـ Google Cloud

## معلومات المشروع

| المعلومة | القيمة |
|---------|--------|
| **Project ID** | `mahwous-smart-pricing-v30` |
| **Project Number** | `972549703168` |
| **الحساب** | nwtsrash@gmail.com |
| **الرابط** | https://console.cloud.google.com/welcome?project=mahwous-smart-pricing-v30 |

---

## 📋 الخدمات المفعلة حالياً

تم اكتشاف الخدمات التالية مفعلة في المشروع:

### ✅ خدمات الذكاء الاصطناعي (مفعلة)
- **Gemini for Google Cloud API** - 465 طلب (0% أخطاء)
- **Gemini Cloud Assist API** - 52 طلب (0% أخطاء)

### ✅ خدمات النشر والتشغيل (مفعلة)
- **Cloud Run Admin API** - 428 طلب (0% أخطاء)
- **Cloud Build API** - 225 طلب (0% أخطاء)
- **Artifact Registry API** - 22 طلب (9% أخطاء)

### ✅ خدمات البيانات والتخزين (مفعلة)
- **Cloud Storage API** - 16 طلب (0% أخطاء)
- **Cloud Logging API** - 210 طلب (0% أخطاء)

### ✅ خدمات الإدارة (مفعلة)
- **Identity and Access Management (IAM) API** - 12 طلب (0% أخطاء)
- **Cloud Pub/Sub API** - 15 طلب (100% أخطاء - قد تحتاج مراجعة)

---

## 🔑 إعداد Gemini API Key

### الخطوة 1: الوصول إلى Google AI Studio
1. اذهب إلى: https://aistudio.google.com/apikey
2. اختر المشروع: `mahwous-smart-pricing-v30`
3. انقر على **"Create API Key"** أو **"Get API Key"**

### الخطوة 2: نسخ المفتاح
```
المفتاح سيكون بصيغة: AIza...
```

### الخطوة 3: إضافة المفتاح إلى التطبيق

#### الطريقة 1: ملف `.streamlit/secrets.toml` (للتطوير المحلي)
```bash
mkdir -p /home/ubuntu/mahwous-smart-pricing-v30/.streamlit
```

أنشئ الملف `.streamlit/secrets.toml`:
```toml
GEMINI_API_KEY = "YOUR_API_KEY_HERE"
```

#### الطريقة 2: متغيرات البيئة (للنشر على Cloud Run)
```bash
export GEMINI_API_KEY="YOUR_API_KEY_HERE"
```

#### الطريقة 3: متغيرات متعددة (لتجنب Rate Limiting)
```toml
GEMINI_API_KEYS = ["KEY1", "KEY2", "KEY3"]
```

---

## 🚀 نشر التطبيق على Cloud Run

### المتطلبات
- Docker مثبت
- Google Cloud CLI (`gcloud`) مثبت
- الوصول إلى المشروع

### خطوات النشر

#### 1. تسجيل الدخول إلى Google Cloud
```bash
gcloud auth login
gcloud config set project mahwous-smart-pricing-v30
```

#### 2. بناء صورة Docker
```bash
docker build -t gcr.io/mahwous-smart-pricing-v30/mahwous-app:latest .
```

#### 3. دفع الصورة إلى Artifact Registry
```bash
docker push gcr.io/mahwous-smart-pricing-v30/mahwous-app:latest
```

#### 4: نشر على Cloud Run
```bash
gcloud run deploy mahwous-smart-pricing \
  --image gcr.io/mahwous-smart-pricing-v30/mahwous-app:latest \
  --platform managed \
  --region us-central1 \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars GEMINI_API_KEY=YOUR_API_KEY_HERE \
  --allow-unauthenticated
```

---

## 📊 إعدادات Cloud Run الموصى بها

| الإعداد | القيمة | الملاحظة |
|--------|--------|---------|
| **Memory** | 2 GB | كافية لـ Streamlit + Gemini |
| **CPU** | 2 | للمعالجة المتزامنة |
| **Timeout** | 3600 ثانية | ساعة واحدة للعمليات الطويلة |
| **Min Instances** | 0 | توفير التكاليف |
| **Max Instances** | 10 | حماية من الحمل الزائد |
| **Concurrency** | 80 | للطلبات المتزامنة |

---

## 🔐 متغيرات البيئة المطلوبة

أضف المتغيرات التالية في Cloud Run:

```bash
# Gemini API
GEMINI_API_KEY=AIza...

# Make.com Webhooks
WEBHOOK_UPDATE_PRICES=https://hook.eu2.make.com/...
WEBHOOK_NEW_PRODUCTS=https://hook.eu2.make.com/...

# البيانات (إذا لزم الأمر)
DATA_DIR=/data
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
STREAMLIT_SERVER_HEADLESS=true
```

---

## 📁 ملفات البيانات الثابتة

إذا كنت تريد تخزين ملفات البيانات (مثل `competitors_list.json` و `brands.csv`):

### الخيار 1: Cloud Storage
```bash
# إنشاء bucket
gsutil mb gs://mahwous-smart-pricing-data

# رفع الملفات
gsutil cp data/*.json gs://mahwous-smart-pricing-data/
gsutil cp data/*.csv gs://mahwous-smart-pricing-data/
```

### الخيار 2: متغيرات البيئة (Base64)
```bash
# تحويل ملف إلى Base64
base64 -w0 data/competitors_list.json > competitors.b64

# إضافة المتغير في Cloud Run
COMPETITORS_JSON_B64=$(cat competitors.b64)
```

---

## ✅ اختبار الاتصال

### 1. اختبار محلي
```bash
cd /home/ubuntu/mahwous-smart-pricing-v30
streamlit run app.py
```

### 2. اختبار Gemini API
```python
import requests

url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
payload = {
    "contents": [{"parts": [{"text": "مرحبا"}]}],
    "generationConfig": {"maxOutputTokens": 5}
}
headers = {"Content-Type": "application/json"}

response = requests.post(f"{url}?key=YOUR_API_KEY", json=payload, headers=headers)
print(response.json())
```

---

## 🐛 استكشاف الأخطاء

### خطأ: 403 Forbidden
- **السبب**: المفتاح غير صحيح أو لم يتم تفعيل الخدمة
- **الحل**: 
  - تحقق من صحة المفتاح في Google AI Studio
  - تأكد من تفعيل Gemini API في المشروع

### خطأ: 429 Rate Limit
- **السبب**: تجاوز حد الطلبات
- **الحل**:
  - أضف مفاتيح احتياطية
  - استخدم OpenRouter كبديل
  - انتظر 60-120 ثانية

### خطأ: Connection Timeout
- **السبب**: مشكلة في الاتصال بالإنترنت
- **الحل**:
  - تحقق من الاتصال بالإنترنت
  - جرب VPN إذا كان الوصول محظوراً

---

## 📞 الدعم والموارد

- **Google Cloud Console**: https://console.cloud.google.com
- **Gemini API Docs**: https://ai.google.dev/gemini-api/docs
- **Cloud Run Docs**: https://cloud.google.com/run/docs
- **Streamlit Docs**: https://docs.streamlit.io

---

## 📝 ملاحظات مهمة

1. **أمان المفاتيح**: لا تضع مفاتيح API في GitHub - استخدم متغيرات البيئة فقط
2. **التكاليف**: راقب استخدام Gemini API لتجنب رسوم غير متوقعة
3. **النسخ الاحتياطية**: احتفظ بنسخ احتياطية من المفاتيح في مكان آمن
4. **التحديثات**: تحقق من تحديثات Google Cloud API بانتظام

---

**آخر تحديث**: 15 أبريل 2026
**الحالة**: ✅ جاهز للنشر
