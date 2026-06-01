# 🚀 دليل نشر تطبيق مهووس على Google Cloud Run
## (بدون الحاجة لخبرة سابقة)

---

## ✅ ما تم إصلاحه وتجهيزه

تم تحديث التطبيق بنجاح لضمان:
- ✅ **الكشط المتقدم يعمل**: تم تثبيت Chromium و Chromedriver في الحاوية
- ✅ **قاعدة البيانات محفوظة**: تم ربط Google Cloud Storage (GCS) لحفظ البيانات تلقائياً
- ✅ **لا فقدان للبيانات**: عند إعادة تشغيل التطبيق، سيتم استعادة جميع البيانات من السحابة
- ✅ **الكود جاهز**: تم دفع جميع التحديثات إلى GitHub

---

## 📋 الخطوات البسيطة للنشر

### **الخطوة 1: فتح Google Cloud Console**
1. اذهب إلى: https://console.cloud.google.com/run
2. تأكد من أن المشروع الحالي هو: **mahwous-smart-pricing-v30**
3. إذا لم تكن متسجلاً، قم بتسجيل الدخول باستخدام حسابك

### **الخطوة 2: تفعيل الخدمات المطلوبة**
في Cloud Console، قم بتفعيل الخدمات التالية:
```
1. Cloud Run
2. Cloud Build
3. Container Registry (أو Artifact Registry)
4. Cloud Storage (GCS)
```

**كيفية التفعيل:**
- اذهب إلى: https://console.cloud.google.com/apis/dashboard
- ابحث عن كل خدمة واضغط "Enable"

### **الخطوة 3: إنشاء Bucket في Google Cloud Storage (GCS)**
هذا الـ Bucket سيحفظ قاعدة البيانات الخاصة بك:

1. اذهب إلى: https://console.cloud.google.com/storage/buckets
2. اضغط **"Create Bucket"**
3. أدخل الاسم: `mahwous-pricing-storage`
4. اختر المنطقة: `us-central1` (نفس منطقة Cloud Run)
5. اضغط **Create**

### **الخطوة 4: إعداد متغيرات البيئة**
هذه المتغيرات تخبر التطبيق أين يحفظ البيانات:

1. اذهب إلى: https://console.cloud.google.com/run/detail/us-central1/mahwous-smart-pricing/revisions
2. اضغط على **"Edit & Deploy New Revision"**
3. في قسم **"Runtime settings"**، اضغط **"Set environment variables"**
4. أضف المتغيرات التالية:

| المتغير | القيمة | الشرح |
|--------|--------|-------|
| `GCP_PROJECT_ID` | `mahwous-smart-pricing-v30` | معرّف المشروع |
| `GCS_BUCKET_NAME` | `mahwous-pricing-storage` | اسم الـ Bucket الذي أنشأته |
| `GCS_DB_BLOB_NAME` | `vision2030/pricing_v30.db` | مسار قاعدة البيانات في الـ Bucket |
| `GEMINI_API_KEY` | `YOUR_GEMINI_KEY_HERE` | مفتاح Gemini للذكاء الاصطناعي |

### **الخطوة 5: النشر التلقائي من GitHub**

**الطريقة الأولى: Cloud Build (موصى به)**

1. اذهب إلى: https://console.cloud.google.com/cloud-build/triggers
2. اضغط **"Create Trigger"**
3. اختر:
   - **Repository**: `mahwoussa-boop/mahwous-smart-pricing-v30`
   - **Branch**: `master`
   - **Build configuration**: `Cloud Build configuration file (cloudbuild.yaml)`
4. اضغط **Create**

الآن عند كل دفع (push) إلى GitHub، سيتم بناء ونشر التطبيق تلقائياً!

**الطريقة الثانية: النشر اليدوي من Cloud Shell**

1. اذهب إلى: https://console.cloud.google.com/cloud-shell/editor
2. في Terminal، قم بتشغيل:
```bash
cd mahwous-smart-pricing-v30
gcloud run deploy mahwous-smart-pricing \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 4Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars GCP_PROJECT_ID=mahwous-smart-pricing-v30,GCS_BUCKET_NAME=mahwous-pricing-storage
```

### **الخطوة 6: التحقق من النشر**

بعد انتهاء النشر:

1. اذهب إلى: https://console.cloud.google.com/run/detail/us-central1/mahwous-smart-pricing/revisions
2. انتظر حتى يصبح الحالة **"OK"** (أخضر)
3. انسخ الرابط من قسم **"Service URL"**
4. افتح الرابط في متصفح جديد

---

## 🔧 استكشاف الأخطاء

### **المشكلة: التطبيق لا يبدأ**
**الحل:**
1. اذهب إلى: https://console.cloud.google.com/run/detail/us-central1/mahwous-smart-pricing/logs
2. ابحث عن الأخطاء الحمراء
3. تأكد من أن جميع متغيرات البيئة صحيحة

### **المشكلة: الكشط لا يعمل**
**الحل:**
- تأكد من تثبيت Chromium (تم بالفعل في Dockerfile الجديد)
- تحقق من السجلات للبحث عن أخطاء Selenium

### **المشكلة: البيانات تختفي عند إعادة التشغيل**
**الحل:**
- تأكد من أن `GCS_BUCKET_NAME` صحيح
- تحقق من أن الـ Bucket موجود وقابل للوصول
- تأكد من أن الخدمة لديها صلاحيات الوصول إلى GCS

---

## 📊 مراقبة التطبيق

### **عرض السجلات:**
```bash
gcloud run services logs read mahwous-smart-pricing --region us-central1 --limit 50
```

### **التحقق من الأداء:**
اذهب إلى: https://console.cloud.google.com/run/detail/us-central1/mahwous-smart-pricing/metrics

---

## 🎯 الخطوات التالية

بعد النشر بنجاح:

1. **اختبر الكشط**: اذهب إلى قسم "كشط المنافسين" وأضف متجر منافس
2. **تحقق من قاعدة البيانات**: اذهب إلى GCS وتحقق من وجود الملف `vision2030/pricing_v30.db`
3. **راقب الأداء**: استخدم Cloud Monitoring لمراقبة استهلاك الموارد

---

## 💡 نصائح مهمة

- **لا تشارك مفاتيح API**: لا تضع مفاتيح Gemini أو أي مفاتيح سرية في GitHub
- **استخدم متغيرات البيئة**: كل المفاتيح السرية يجب أن تكون في Cloud Run Environment Variables
- **راقب التكاليف**: Cloud Run يفرض رسوم على الموارد المستخدمة، تأكد من ضبط `max-instances` بشكل معقول
- **النسخ الاحتياطية**: تأكد من أن GCS Bucket محمي ولديك نسخ احتياطية

---

## 📞 الدعم

إذا واجهت مشكلة:
1. تحقق من السجلات في Cloud Run
2. اقرأ رسالة الخطأ بعناية
3. تأكد من أن جميع المتغيرات البيئية صحيحة
4. جرب النشر مرة أخرى

---

**تم إعداد كل شيء بواسطة Manus - التطبيق الآن جاهز للعمل! 🎉**
