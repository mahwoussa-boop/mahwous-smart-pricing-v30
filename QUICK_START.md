# ⚡ البدء السريع (Quick Start)

## 🎯 هدفك في دقيقتين

نشر التطبيق على Google Cloud Run وجعل الكشط والبيانات تعمل بدون مشاكل.

---

## 📝 المتطلبات الأساسية

- حساب Google (Gmail)
- حساب GitHub (لديك بالفعل)
- لا تحتاج لأي خبرة تقنية

---

## 🔑 الخطوة 1: احصل على مفتاح Gemini API (مجاني)

1. اذهب إلى: https://aistudio.google.com/app/apikey
2. اضغط **"Create API Key"**
3. اختر المشروع: **mahwous-smart-pricing-v30**
4. انسخ المفتاح (سيبدأ بـ `AIza...`)
5. احفظه في مكان آمن

---

## ☁️ الخطوة 2: أنشئ Bucket في Google Cloud Storage

1. اذهب إلى: https://console.cloud.google.com/storage/buckets
2. اضغط **"Create"**
3. الاسم: `mahwous-pricing-storage`
4. المنطقة: `us-central1`
5. اضغط **Create**

---

## 🚀 الخطوة 3: انشر التطبيق

### **الطريقة الأسهل: استخدم Cloud Shell**

1. اذهب إلى: https://console.cloud.google.com/cloud-shell/editor
2. انسخ والصق هذا الأمر:

```bash
git clone https://github.com/mahwoussa-boop/mahwous-smart-pricing-v30.git
cd mahwous-smart-pricing-v30
gcloud run deploy mahwous-smart-pricing \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 4Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars \
    GCP_PROJECT_ID=mahwous-smart-pricing-v30,\
    GCS_BUCKET_NAME=mahwous-pricing-storage,\
    GEMINI_API_KEY=YOUR_GEMINI_KEY_HERE
```

3. **استبدل** `YOUR_GEMINI_KEY_HERE` بالمفتاح الذي نسخته في الخطوة 1

4. اضغط Enter وانتظر (قد يستغرق 5-10 دقائق)

---

## ✅ الخطوة 4: تحقق من النشر

1. عندما ينتهي الأمر، سترى رابط مثل:
   ```
   Service URL: https://mahwous-smart-pricing-xxxxxx.a.run.app
   ```

2. انسخ الرابط وافتحه في متصفح جديد

3. إذا رأيت التطبيق يعمل ✅ **تم بنجاح!**

---

## 🧪 اختبر الميزات

### اختبر الكشط:
1. اذهب إلى قسم **"🕷️ كشط المنافسين"**
2. أضف رابط متجر منافس (مثل: `https://www.alkhabeershop.com`)
3. اضغط "ابدأ الكشط"
4. انتظر حتى ينتهي

### تحقق من قاعدة البيانات:
1. اذهب إلى: https://console.cloud.google.com/storage/buckets/mahwous-pricing-storage
2. تحقق من وجود ملف: `vision2030/pricing_v30.db`
3. إذا كان موجود ✅ البيانات تُحفظ بنجاح

---

## 🆘 إذا حدثت مشكلة

### المشكلة: رسالة خطأ عند النشر
**الحل:**
- تأكد من نسخ الأمر بالكامل بدون أخطاء
- تأكد من أن المفتاح صحيح

### المشكلة: التطبيق لا يفتح
**الحل:**
1. اذهب إلى: https://console.cloud.google.com/run
2. اضغط على الخدمة
3. اذهب إلى **Logs** وابحث عن الأخطاء
4. شارك الخطأ معي

### المشكلة: الكشط لا يعمل
**الحل:**
- تأكد من أن الرابط صحيح
- جرب رابط آخر
- تحقق من السجلات

---

## 📊 الخطوات التالية

بعد النشر بنجاح:

1. **أضف متغيرات بيئة إضافية** (اختياري):
   ```
   OPENROUTER_API_KEY=your_key
   WEBHOOK_UPDATE_PRICES=your_webhook
   ```

2. **فعّل النشر التلقائي** من GitHub (Cloud Build)

3. **راقب الأداء** في Cloud Monitoring

---

## 💰 تنبيه التكاليف

- **Cloud Run**: مجاني للـ 2 مليون طلب شهري
- **Google Cloud Storage**: مجاني للـ 5 GB شهري
- **Gemini API**: مجاني للـ 60 طلب في الدقيقة

تأكد من ضبط `max-instances` لتجنب فواتير عالية.

---

**تم! التطبيق الآن جاهز للعمل 🎉**
