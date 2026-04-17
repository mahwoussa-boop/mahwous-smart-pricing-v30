"""
config.py - الإعدادات المركزية v30.0 (Manus Optimized)
المفاتيح: أولاً os.environ (Railway / Docker)، ثم Streamlit Secrets عند التوفر.
"""
import json as _json
import os as _os

from utils.data_paths import get_data_db_path

# ===== معلومات التطبيق =====
APP_TITLE   = "نظام التسعير الذكي - مهووس"
APP_NAME    = APP_TITLE
APP_VERSION = "v30.0"
APP_ICON    = "🧪"
GEMINI_MODEL = "gemini-2.0-flash"   # النموذج المستقر الموصى به

# ══════════════════════════════════════════════
#  قراءة Secrets بطريقة آمنة 100%
# ══════════════════════════════════════════════
def _s(key, default=""):
    v = _os.environ.get(key, "")
    if v:
        return v
    try:
        import streamlit as st
        v = st.secrets[key]
        if v is not None:
            return str(v) if not isinstance(v, (list, dict)) else v
    except Exception:
        pass
    return default


def _parse_gemini_keys():
    keys = []
    raw = _s("GEMINI_API_KEYS", "")
    if isinstance(raw, list):
        keys = [k for k in raw if k and isinstance(k, str)]
    elif raw and isinstance(raw, str):
        raw = raw.strip()
        if raw.startswith('['):
            try:
                parsed = _json.loads(raw)
                if isinstance(parsed, list):
                    keys = [k for k in parsed if k]
            except Exception:
                clean = raw.strip("[]").replace('"','').replace("'",'')
                keys = [k.strip() for k in clean.split(',') if k.strip()]
        elif raw:
            keys = [raw]
    single = _s("GEMINI_API_KEY", "")
    if single and single not in keys:
        keys.append(single)
    for n in ["GEMINI_KEY_1","GEMINI_KEY_2","GEMINI_KEY_3","GEMINI_KEY_4","GEMINI_KEY_5"]:
        k = _s(n, "")
        if k and k not in keys:
            keys.append(k)
    keys = [k.strip() for k in keys if k and len(k) > 20]
    return keys

# ══════════════════════════════════════════════
#  المفاتيح الفعلية
# ══════════════════════════════════════════════
GEMINI_API_KEYS    = _parse_gemini_keys()
GEMINI_API_KEY     = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else ""
OPENROUTER_API_KEY = _s("OPENROUTER_API_KEY") or _s("OPENROUTER_KEY") or ""
COHERE_API_KEY     = _s("COHERE_API_KEY") or ""
EXTRA_API_KEY      = _s("EXTRA_API_KEY")

def any_ai_provider_configured() -> bool:
    if GEMINI_API_KEYS or (OPENROUTER_API_KEY or "").strip() or (COHERE_API_KEY or "").strip():
        return True
    return False

ANY_AI_PROVIDER_CONFIGURED = any_ai_provider_configured()

# ══════════════════════════════════════════════
#  Make Webhooks
# ══════════════════════════════════════════════
WEBHOOK_UPDATE_PRICES = _s("WEBHOOK_UPDATE_PRICES") or "https://hook.eu2.make.com/8jia6gc7s1cpkeg6catlrvwck768sbfk"
WEBHOOK_NEW_PRODUCTS = _s("WEBHOOK_NEW_PRODUCTS") or "https://hook.eu2.make.com/xvubj23dmpxu8qzilstd25cnumrwtdxm"

# ══════════════════════════════════════════════
#  إعدادات الألوان والمطابقة
# ══════════════════════════════════════════════
COLORS = {"raise": "#dc3545", "lower": "#00C853", "approved": "#28a745", "missing": "#007bff", "review": "#ff9800", "excluded": "#9e9e9e", "primary": "#6C63FF"}
MATCH_THRESHOLD = 85
HIGH_CONFIDENCE = 95
REVIEW_THRESHOLD = 75
PRICE_TOLERANCE = 5

# ══════════════════════════════════════════════
#  أقسام التطبيق
# ══════════════════════════════════════════════
SECTIONS = ["✨ مصنع المنتجات", "📊 لوحة التحكم", "🔴 سعر أعلى", "🟢 سعر أقل", "✅ موافق عليها", "🔍 منتجات مفقودة", "⚠️ تحت المراجعة", "⚪ مستبعد (لا يوجد تطابق)", "✅ تمت المعالجة", "⚡ أتمتة Make", "🔄 الأتمتة الذكية", "🕷️ كشط المنافسين", "🗑️ سلة المحذوفات", "⚙️ الإعدادات"]
SIDEBAR_SECTIONS = SECTIONS
PAGES_PER_TABLE  = 25
DB_PATH          = get_data_db_path("perfume_pricing.db")

# ══════════════════════════════════════════════
#  Google Cloud Platform (GCP) Settings
# ══════════════════════════════════════════════
GCP_PROJECT_ID            = _s("GCP_PROJECT_ID") or "mahwous-smart-pricing-v30"
GCS_BUCKET_NAME           = _s("GCS_BUCKET_NAME") or "mahwous-pricing-storage"
GCS_DB_BLOB_NAME          = _s("GCS_DB_BLOB_NAME") or "vision2030/pricing_v30.db"
CLOUD_SQL_CONNECTION_NAME = _s("CLOUD_SQL_CONNECTION_NAME")
DB_USER                   = _s("DB_USER")
DB_PASS                   = _s("DB_PASS")
DB_NAME                   = _s("DB_NAME") or "vision2030"
USE_FIRESTORE             = _s("USE_FIRESTORE", "false").lower() == "true"

GCP_ENABLED = bool(GCS_BUCKET_NAME or CLOUD_SQL_CONNECTION_NAME or USE_FIRESTORE)
