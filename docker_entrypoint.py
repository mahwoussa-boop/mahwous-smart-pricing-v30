#!/usr/bin/env python3
"""
تشغيل Streamlit على Railway.
Railway قد يضع STREAMLIT_SERVER_PORT على النص الحرفي '$PORT' — نزيله ثم نمرّر المنفذ رقماً.

قبل تشغيل Streamlit يُعيد هذا الملف بناء مجلد /data من متغيرات البيئة.
هذا يُتيح إرسال ملفات البيانات لـ Railway دون رفعها لـ GitHub.

═══════════════════════════════════════════════════════════════
 طريقة 1: Base64 (للملفات < 64 KB)
═══════════════════════════════════════════════════════════════
  CATEGORIES_CSV_B64   → محتوى categories.csv   مُشفَّر Base64
  OUR_CATALOG_CSV_B64  → محتوى our_catalog.csv  مُشفَّر Base64
  COMPETITORS_JSON_B64 → محتوى competitors_list.json مُشفَّر Base64

  توليد القيمة محلياً:
    PowerShell: [Convert]::ToBase64String([IO.File]::ReadAllBytes("data\\categories.csv"))
    Linux/Mac:  base64 -w0 data/categories.csv

═══════════════════════════════════════════════════════════════
 طريقة 2: URL عام مؤقت (للملفات الكبيرة مثل brands.csv ≈ 400KB)
═══════════════════════════════════════════════════════════════
  BRANDS_CSV_URL → رابط مباشر لملف brands.csv (Google Drive / Dropbox / S3)
  SALLA_BRANDS_URL     → رابط "ماركات مهووس.csv"
  SALLA_CATEGORIES_URL → رابط "تصنيفات مهووس.csv"

  كيف تحصل على الرابط؟
    - Google Drive: شارك الملف (Anyone with link) ← انسخ الـ file_id
      ثم الرابط: https://drive.google.com/uc?export=download&id=<file_id>
    - Dropbox:     شارك الملف، غيّر ?dl=0 إلى ?dl=1 في نهاية الرابط

═══════════════════════════════════════════════════════════════
 طريقة 3: Railway Volume (الأفضل للملفات الثابتة الكبيرة)
═══════════════════════════════════════════════════════════════
  أنشئ Volume في Railway ← Mount path = /data
  ارفع الملفات مرة واحدة عبر: railway run -- bash
  ثم: cp /local/brands.csv /data/brands.csv
"""
import os
import base64
from pathlib import Path
from urllib.request import urlretrieve


# ── Base64: مناسب لـ < 64KB ────────────────────────────────────────────────
_B64_FILES = {
    "CATEGORIES_CSV_B64":    "categories.csv",
    "OUR_CATALOG_CSV_B64":   "our_catalog.csv",
    "COMPETITORS_JSON_B64":  "competitors_list.json",
    "SALLA_BRANDS_B64":      "ماركات مهووس.csv",
    "SALLA_CATEGORIES_B64":  "تصنيفات مهووس.csv",
}

# ── URL: مناسب للملفات الكبيرة (brands.csv ≈ 400KB) ─────────────────────────
_URL_FILES = {
    "BRANDS_CSV_URL":        "brands.csv",
    "SALLA_BRANDS_URL":      "ماركات مهووس.csv",
    "SALLA_CATEGORIES_URL":  "تصنيفات مهووس.csv",
    "OUR_CATALOG_CSV_URL":   "our_catalog.csv",
    "COMPETITORS_JSON_URL":  "competitors_list.json",
}


def _default_data_dir() -> str:
    """المسار الافتراضي الآمن داخل التطبيق بدلاً من الاعتماد على /data."""
    return str((Path(__file__).resolve().parent / "data").resolve())



def _env_truthy(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}



def _resolve_startup_data_dir() -> str:
    requested = (os.environ.get("DATA_DIR") or "").strip()
    data_dir = requested or _default_data_dir()
    try:
        os.makedirs(data_dir, exist_ok=True)
        return data_dir
    except Exception as e:
        fallback = _default_data_dir()
        os.makedirs(fallback, exist_ok=True)
        os.environ["DATA_DIR"] = fallback
        print(f"[entrypoint] ⚠️ تعذّر استخدام DATA_DIR='{data_dir}' بسبب: {e} — سيتم استخدام {fallback}")
        return fallback



def _restore_data_files() -> None:
    """
    يستعيد الملفات من متغيرات البيئة (Base64 أو URL) إلى DATA_DIR.
    آمن: يتخطى الملفات الموجودة مسبقاً (لا يُعيد الكتابة).

    ملاحظة Cloud Run:
    تنزيل الملفات عبر الشبكة قبل بدء Streamlit قد يمنع الحاوية من الاستماع
    على PORT=8080 في الوقت المناسب. لذلك تنزيلات URL أصبحت اختيارية
    ولا تعمل إلا عند تفعيل RESTORE_URL_FILES_ON_STARTUP=1 صراحة.
    """
    data_dir = _resolve_startup_data_dir()

    # ── Base64 ────────────────────────────────────────────────────────────
    for env_key, filename in _B64_FILES.items():
        b64_val = (os.environ.get(env_key) or "").strip()
        if not b64_val:
            continue
        dest = os.path.join(data_dir, filename)
        if os.path.exists(dest):
            print(f"[entrypoint] ℹ️ موجود (تخطي): {filename}")
            continue
        try:
            with open(dest, "wb") as fh:
                fh.write(base64.b64decode(b64_val))
            sz = os.path.getsize(dest)
            print(f"[entrypoint] ✅ Base64 → {filename} ({sz:,} bytes)")
        except Exception as e:
            print(f"[entrypoint] ❌ فشل Base64 {env_key}: {e}")

    # ── URL ───────────────────────────────────────────────────────────────
    if not _env_truthy("RESTORE_URL_FILES_ON_STARTUP"):
        if any((os.environ.get(k) or "").strip() for k in _URL_FILES):
            print("[entrypoint] ℹ️ تم تجاهل تنزيلات URL عند الإقلاع. فعّل RESTORE_URL_FILES_ON_STARTUP=1 إذا أردت استعادتها قبل البدء.")
        return

    for env_key, filename in _URL_FILES.items():
        url = (os.environ.get(env_key) or "").strip()
        if not url:
            continue
        dest = os.path.join(data_dir, filename)
        if os.path.exists(dest):
            print(f"[entrypoint] ℹ️ موجود (تخطي): {filename}")
            continue
        try:
            print(f"[entrypoint] ⬇️ تحميل {filename} من URL...")
            urlretrieve(url, dest)
            sz = os.path.getsize(dest)
            print(f"[entrypoint] ✅ URL → {filename} ({sz:,} bytes)")
        except Exception as e:
            print(f"[entrypoint] ❌ فشل تحميل {env_key}: {e}")


def _port() -> int:
    # Cloud Run injects PORT=8080; use that as default instead of 8501
    raw = (os.environ.get("PORT") or "").strip() or "8080"
    try:
        p = int(raw)
        if 1 <= p <= 65535:
            return p
    except ValueError:
        pass
    return 8080


def _strip_broken_streamlit_server_env() -> None:
    for key in list(os.environ):
        if key.startswith("STREAMLIT_SERVER_"):
            os.environ.pop(key, None)


def main() -> None:
    # ── خطوة 1: استعادة ملفات /data ─────────────────────────────────────
    _restore_data_files()

    # ── خطوة 2: تشغيل Streamlit ─────────────────────────────────────────
    p = _port()
    _strip_broken_streamlit_server_env()
    os.execvp(
        "streamlit",
        [
            "streamlit", "run", "app.py",
            "--server.port", str(p),
            "--server.address", "0.0.0.0",
            "--server.headless", "true",
        ],
    )


if __name__ == "__main__":
    main()
