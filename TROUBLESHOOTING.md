# 🔧 استكشاف الأخطاء والمشاكل الشائعة

---

## ❌ المشكلة 1: "Error: src refspec main does not match any"

### السبب:
الفرع الافتراضي هو `master` وليس `main`.

### الحل:
استخدم:
```bash
git push origin master
```

---

## ❌ المشكلة 2: "Permission denied" عند النشر

### السبب:
لا تملك صلاحيات على المشروع أو لم تسجل الدخول.

### الحل:
```bash
gcloud auth login
gcloud config set project mahwous-smart-pricing-v30
```

---

## ❌ المشكلة 3: التطبيق يبدأ ثم يتوقف مباشرة

### السبب:
غالباً بسبب خطأ في متغيرات البيئة أو مشكلة في الكود.

### الحل:
1. اذهب إلى: https://console.cloud.google.com/run/detail/us-central1/mahwous-smart-pricing/logs
2. ابحث عن رسالة الخطأ
3. تحقق من:
   - `GCS_BUCKET_NAME` صحيح؟
   - `GEMINI_API_KEY` صحيح؟
   - هل الـ Bucket موجود؟

---

## ❌ المشكلة 4: "Chromium not found" في السجلات

### السبب:
Dockerfile لم يثبت Chromium بشكل صحيح.

### الحل:
تأكد من أن Dockerfile يحتوي على:
```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver
```

---

## ❌ المشكلة 5: الكشط لا يعمل

### السبب:
قد يكون الموقع محمي ضد الكشط أو الرابط خاطئ.

### الحل:
1. تأكد من أن الرابط صحيح وموقع حقيقي
2. جرب رابط آخر
3. تحقق من السجلات:
   ```bash
   gcloud run services logs read mahwous-smart-pricing --region us-central1 --limit 100
   ```

---

## ❌ المشكلة 6: "GCS bucket does not exist"

### السبب:
لم تنشئ الـ Bucket أو الاسم خاطئ.

### الحل:
1. اذهب إلى: https://console.cloud.google.com/storage/buckets
2. تأكد من وجود Bucket باسم: `mahwous-pricing-storage`
3. إذا لم يكن موجود، أنشئه:
   - اضغط **Create**
   - الاسم: `mahwous-pricing-storage`
   - المنطقة: `us-central1`
   - اضغط **Create**

---

## ❌ المشكلة 7: "Permission denied" عند الوصول إلى GCS

### السبب:
الخدمة لا تملك صلاحيات الوصول إلى الـ Bucket.

### الحل:
1. اذهب إلى: https://console.cloud.google.com/iam-admin/iam
2. ابحث عن الخدمة: `mahwous-smart-pricing@mahwous-smart-pricing-v30.iam.gserviceaccount.com`
3. أضف الأدوار التالية:
   - `Storage Object Admin`
   - `Storage Bucket Admin`

---

## ❌ المشكلة 8: "GEMINI_API_KEY is invalid"

### السبب:
المفتاح غير صحيح أو منتهي الصلاحية.

### الحل:
1. اذهب إلى: https://aistudio.google.com/app/apikey
2. احذف المفتاح القديم
3. أنشئ مفتاح جديد
4. حدّث متغير البيئة في Cloud Run

---

## ❌ المشكلة 9: "Timeout" عند الكشط

### السبب:
الموقع بطيء جداً أو الاتصال ضعيف.

### الحل:
1. جرب رابط آخر
2. زد قيمة `timeout` في `config.py`:
   ```python
   TIMEOUT = 30  # بدلاً من 15
   ```

---

## ❌ المشكلة 10: البيانات تختفي عند إعادة التشغيل

### السبب:
GCS لم يتم تفعيله أو البيانات لم تُحفظ.

### الحل:
1. تأكد من أن `GCS_BUCKET_NAME` موجود في متغيرات البيئة
2. تحقق من أن الـ Bucket موجود وقابل للوصول
3. تحقق من السجلات للبحث عن أخطاء الحفظ:
   ```bash
   gcloud run services logs read mahwous-smart-pricing --region us-central1 --limit 50 | grep -i "gcs\|storage"
   ```

---

## ✅ كيفية عرض السجلات

### من Cloud Console:
1. اذهب إلى: https://console.cloud.google.com/run/detail/us-central1/mahwous-smart-pricing/logs
2. اختر الفترة الزمنية
3. ابحث عن الأخطاء

### من Command Line:
```bash
# آخر 50 سطر
gcloud run services logs read mahwous-smart-pricing --region us-central1 --limit 50

# آخر 100 سطر مع البحث عن "error"
gcloud run services logs read mahwous-smart-pricing --region us-central1 --limit 100 | grep -i error

# مراقبة السجلات مباشرة
gcloud run services logs read mahwous-smart-pricing --region us-central1 --follow
```

---

## ✅ كيفية إعادة تشغيل الخدمة

```bash
gcloud run services update-traffic mahwous-smart-pricing \
  --to-revisions LATEST=100 \
  --region us-central1
```

---

## ✅ كيفية حذف الخدمة (إذا أردت البدء من جديد)

```bash
gcloud run services delete mahwous-smart-pricing --region us-central1
```

---

## 📞 نصائح للحصول على مساعدة

1. **اقرأ السجلات بعناية**: معظم الأخطاء واضحة في السجلات
2. **ابحث عن الخطأ على Google**: غالباً ستجد حلول
3. **جرب خطوة واحدة في المرة**: لا تغير عدة أشياء في نفس الوقت
4. **احفظ النسخة القديمة**: قبل إجراء تغييرات كبيرة

---

**تذكر: معظم المشاكل يمكن حلها بقراءة السجلات والتحقق من متغيرات البيئة! 🎯**
