# -*- coding: utf-8 -*-
import sys, os, json, re, random, hmac, requests as http_req, time
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
try:
    from urllib3.util.retry import Retry
except ImportError:  # توافق مع إصدارات urllib3 الأقدم
    from requests.packages.urllib3.util.retry import Retry
# ضمان ترميز UTF-8 للطباعة (يمنع تعطّل الإيموجي على كونسول Windows cp1256)
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass
from pathlib import Path
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from empathy_engine import mark_synthetic_output

app = Flask(__name__)
# CORS مقيّد على نطاقات متجرك — اضبط ALLOWED_ORIGINS في البيئة (مفصولة بفواصل).
# الافتراضي '*' للتطوير المحلي فقط؛ في الإنتاج ضع نطاقك الفعلي.
_ALLOWED_ORIGINS = [o.strip() for o in os.environ.get('ALLOWED_ORIGINS', '*').split(',') if o.strip()]
CORS(app, resources={r"/api/*": {"origins": _ALLOWED_ORIGINS}})
BASE_DIR = Path(__file__).parent


def _safe_int(value, default, lo, hi):
    """تحويل آمن لعدد صحيح من مدخل غير موثوق مع تقييده ضمن [lo, hi]."""
    try:
        return max(lo, min(int(value), hi))
    except (TypeError, ValueError):
        return default

# ═══════════════════════════════════════════════════════════
# Rate limiting — حماية endpoints الذكاء الاصطناعي من الإساءة/استنزاف رصيد OpenRouter.
# اختياري وآمن: إن لم تكن flask-limiter مثبّتة يصبح rate_limit ديكوريتر بلا أثر،
# فلا يكسر التطوير المحلي. في الإنتاج تُثبَّت تلقائياً من requirements.txt.
# تُضبط الحدود عبر متغيرات البيئة (RATE_LIMIT_DEFAULT / RATE_LIMIT_GENERATE).
# ═══════════════════════════════════════════════════════════
RATE_LIMIT_GENERATE = os.environ.get('RATE_LIMIT_GENERATE', '20 per minute')
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    _limiter = Limiter(
        key_func=get_remote_address,
        app=app,
        default_limits=[os.environ.get('RATE_LIMIT_DEFAULT', '300 per hour')],
        storage_uri=os.environ.get('RATE_LIMIT_STORAGE', 'memory://'),
    )

    def rate_limit(rule):
        return _limiter.limit(rule)
    USE_RATE_LIMIT = True
    print('✅ flask-limiter loaded (rate limiting active: %s)' % RATE_LIMIT_GENERATE)
except Exception as _e:
    USE_RATE_LIMIT = False

    def rate_limit(rule):
        def _decorator(fn):
            return fn
        return _decorator
    print('⚠️ flask-limiter not active (%s) — endpoints unthrottled' % _e)

# المسار الثابت للبيانات المؤقتة — Railway يضبطه على /data (Persistent Volume).
# يبقى بين عمليات النشر. الكود والبيانات الثابتة (catalog/names/.env) تبقى في BASE_DIR.
DATA_DIR = Path(os.environ.get('DATA_DIR', str(BASE_DIR)))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════
# استيراد المحركات الجديدة (مع fallback آمن)
# ═══════════════════════════════════════════════════════════
try:
    from personas_engine import (generate_persona, generate_review_params, build_master_prompt,
                                  set_catalog as pe_set_catalog, ARCHETYPES as NEW_ARCHETYPES)
    USE_NEW_PERSONAS = True
    print('\u2705 personas_engine loaded')
except ImportError:
    USE_NEW_PERSONAS = False
    print('\u26a0\ufe0f personas_engine not found, using legacy')

try:
    from dialects import get_dialect_for_city, get_dialect_examples, apply_typos
    USE_DIALECTS = True
    print('\u2705 dialects loaded')
except ImportError:
    USE_DIALECTS = False
    print('\u26a0\ufe0f dialects not found')

try:
    from review_patterns import (get_ai_directive, get_pattern_description,
                                  REVIEW_PATTERNS, PATTERN_CATEGORIES)
    USE_PATTERNS = True
    print('\u2705 review_patterns V2 loaded (%d patterns, %d categories)' % (len(REVIEW_PATTERNS), len(PATTERN_CATEGORIES)))
except ImportError:
    USE_PATTERNS = False
    print('\u26a0\ufe0f review_patterns not found')

try:
    from anti_repeat import (archive_review as ar_archive_review, archive_batch as ar_archive_batch,
                             get_used_texts as ar_get_used_texts, get_archive_stats as ar_get_archive_stats,
                             clear_archive as ar_clear_archive, format_used_texts_block, is_duplicate,
                             register_text as ar_register_text)
    USE_ANTI_REPEAT = True
    print('\u2705 anti_repeat loaded')
except ImportError:
    USE_ANTI_REPEAT = False
    print('\u26a0\ufe0f anti_repeat not found')

try:
    from semantic_guard import guard_violations, strip_broken_tail
    USE_SEMANTIC_GUARD = True
    print('\u2705 semantic_guard loaded')
except ImportError:
    USE_SEMANTIC_GUARD = False
    print('\u26a0\ufe0f semantic_guard not found')

try:
    import humanizer as _hz
    from humanizer import humanize_output as hz_humanize_output, content_tells as hz_content_tells
    USE_HUMANIZER = True
    print('\u2705 humanizer loaded (%d marketing tells, %d voices)'
          % (len(_hz.AR_MARKETING_TELLS), _hz.stats()['scraped_voices']))
except ImportError:
    USE_HUMANIZER = False
    print('\u26a0\ufe0f humanizer not found')

try:
    from trending import calculate_product_weight, get_trending_names, blend_selection
    USE_TRENDING = True
    print('\u2705 trending loaded')
except ImportError:
    USE_TRENDING = False
    print('\u26a0\ufe0f trending not found')

try:
    import demographic_matcher as _demo
    USE_DEMO_MATCH = True   # \u0645\u0637\u0627\u0628\u0642\u0629 \u062f\u064a\u0645\u0648\u063a\u0631\u0627\u0641\u064a\u0629 \u2014 \u0639\u0644\u0645 \u062a\u062d\u0643\u0651\u0645 (\u0637\u0631\u064a\u0642 \u0639\u0648\u062f\u0629 \u0641\u0648\u0631\u064a)
    print('\u2705 demographic_matcher loaded')
except ImportError:
    USE_DEMO_MATCH = False
    print('\u26a0\ufe0f demographic_matcher not found')

try:
    from thread_generator import generate_thread_data, parse_ai_reply, format_thread_for_display
    USE_THREADS = True
    print('\u2705 thread_generator loaded')
except ImportError:
    USE_THREADS = False
    print('\u26a0\ufe0f thread_generator not found')

try:
    from short_texts_bank import pick_short_text as stb_pick_short
    USE_SHORT_BANK = True
    print('\u2705 short_texts_bank loaded')
except ImportError:
    USE_SHORT_BANK = False
    print('\u26a0\ufe0f short_texts_bank not found')

try:
    from golden_reviews import get_stats as golden_stats  # مثال الذهبي لم يعد يُحقن في مسار المتجر
    USE_GOLDEN = True
    print('\u2705 golden_reviews loaded (%d reviews)' % golden_stats()['total'])
except ImportError:
    USE_GOLDEN = False
    print('\u26a0\ufe0f golden_reviews not found')

# \u062f\u0648\u0627\u0644 \u062a\u062a\u0628\u0639 \u0627\u0644\u062a\u0643\u0631\u0627\u0631 (\u0623\u0646\u0645\u0627\u0637 + \u0635\u0641\u0627\u062a) \u2014 \u062a\u064f\u0633\u062a\u062f\u0639\u0649 \u0628\u0639\u062f \u0643\u0644 \u062a\u0648\u0644\u064a\u062f AI \u0646\u0627\u062c\u062d
try:
    from anti_repeat import track_pattern as ar_track_pattern, track_adjective as ar_track_adjective
    USE_AR_TRACK = True
except ImportError:
    USE_AR_TRACK = False

# \u0642\u0627\u0646\u0648\u0646 4: \u0644\u0627 \u0642\u0648\u0627\u0644\u0628 \u0627\u062d\u062a\u064a\u0627\u0637\u064a\u0629 \u0641\u064a \u0627\u0644\u0645\u0633\u0627\u0631 \u0627\u0644\u062d\u064a \u2014 \u0627\u0644\u0640 AI \u064a\u0643\u062a\u0628 \u0623\u0648 \u0646\u062a\u0648\u0642\u0641 \u0628\u062e\u0637\u0623 \u0648\u0627\u0636\u062d.
# (\u0623\u064f\u0632\u064a\u0644 fallback_gen \u0627\u0644\u0645\u064a\u062a: \u0643\u0627\u0646 \u064a\u064f\u0646\u0634\u0623 \u0648\u0644\u0627 \u064a\u064f\u0633\u062a\u062f\u0639\u0649 \u2014 \u0627\u0644\u0645\u0648\u0644\u0651\u062f \u0627\u0644\u0642\u0627\u0644\u0628\u064a \u0623\u062f\u0627\u0629
#  offline \u0641\u064a generate_audience.py \u062d\u0635\u0631\u0627\u064b.)

try:
    from mahalli_intel import (get_our_products as intel_get_our_products,
                               get_competitors as intel_get_competitors,
                               get_our_rank as intel_get_our_rank,
                               get_priorities as intel_get_priorities,
                               generate_daily_plan as intel_daily_plan,
                               get_dashboard_summary as intel_dashboard,
                               refresh_all_data as intel_refresh)
    USE_INTEL = True
    print('\u2705 mahalli_intel loaded')
except ImportError:
    USE_INTEL = False
    print('\u26a0\ufe0f mahalli_intel not found')

with open(BASE_DIR / 'names.json', 'r', encoding='utf-8') as f:
    NAMES = json.load(f)

# ═══════════════════════════════════════════════════════════
# أرشيف — يستخدم anti_repeat إذا متوفر، وإلا يعمل كالسابق
# ═══════════════════════════════════════════════════════════
ARCHIVE_FILE = DATA_DIR / 'archive.json'

def _load_archive():
    if ARCHIVE_FILE.exists():
        try:
            with open(ARCHIVE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {'reviews':[], 'store_reviews':[], 'personas':[]}
    return {'reviews':[], 'store_reviews':[], 'personas':[]}

def _save_archive(archive):
    # FIFO: حد أقصى 200 تقييم
    if len(archive.get('reviews', [])) > 200:
        archive['reviews'] = archive['reviews'][-200:]
    with open(ARCHIVE_FILE, 'w', encoding='utf-8') as f:
        json.dump(archive, f, ensure_ascii=False, indent=1)

def _get_used_texts(archive, limit=40):
    """آخر 40 نص مستخدم لإرسالها للـ AI"""
    if USE_ANTI_REPEAT:
        return ar_get_used_texts(limit)
    texts = [r.get('text','') for r in archive.get('reviews',[])]
    return texts[-limit:] if len(texts)>limit else texts

def _archive_review(review_text, product_name, persona_name):
    """حفظ تقييم في الأرشيف"""
    if USE_ANTI_REPEAT:
        ar_archive_review(review_text, product_name, persona_name)
        return
    arc = _load_archive()
    arc['reviews'].append({
        'text': review_text,
        'product': product_name,
        'persona': persona_name,
        'ts': int(time.time())
    })
    _save_archive(arc)

def _archive_batch(reviews, persona_name):
    """حفظ مجموعة تقييمات"""
    if USE_ANTI_REPEAT:
        ar_archive_batch(reviews, persona_name)
        return
    arc = _load_archive()
    for rv in reviews:
        arc['reviews'].append({
            'text': rv.get('text',''),
            'product': rv.get('product',''),
            'persona': persona_name,
            'ts': int(time.time())
        })
    _save_archive(arc)

print(f'\U0001f4e6 Archive: {len(_load_archive().get("reviews",[]))} reviews')

# ═══════════════════════════════════════════════════════════
# OpenRouter AI — نصوص فريدة لكل شخصية وعطر
# ═══════════════════════════════════════════════════════════
# حمّل .env المحلي من دون أن يستبدل متغيرات الاستضافة الأعلى أولوية.
_env_path = BASE_DIR / '.env'
load_dotenv(_env_path, override=False)

# ── مفتاح الـ AI ──
# يقبل عدة أسماء شائعة صراحةً، ثم — إن لم يجد — يمسح البيئة عن أي متغيّر يحمل
# مفتاح OpenRouter (قيمته تبدأ بـ sk-or- أو اسمه يحوي OPENROUTER). هذا يعالج
# الحالة التي يسمّي فيها المستخدم المتغيّر في Railway باسم غير متوقّع فيبدو
# «المفتاح متصل» بينما التطبيق لا يقرأه. يُزيل أي علامات اقتباس/مسافات محيطة.
def _clean_key(v):
    return (v or '').strip().strip('"').strip("'").strip()

_KEY_NAMES = ('AI_KEY', 'OPENROUTER_API_KEY', 'OPENROUTER_KEY',
              'OPENROUTER_API', 'OPENROUTER', 'OPEN_ROUTER_API_KEY', 'API_KEY')
AI_KEY = ''
AI_KEY_SOURCE = ''
for _n in _KEY_NAMES:
    _v = _clean_key(os.environ.get(_n))
    if _v:
        AI_KEY, AI_KEY_SOURCE = _v, _n
        break
# مسح احتياطي: أي متغيّر بيئة قيمته تشبه مفتاح OpenRouter أو اسمه يحوي OPENROUTER
if not AI_KEY:
    for _n, _raw in os.environ.items():
        _v = _clean_key(_raw)
        if _v and (_v.startswith('sk-or-') or 'OPENROUTER' in _n.upper()):
            AI_KEY, AI_KEY_SOURCE = _v, _n
            break

AI_URL = os.environ.get('AI_URL', 'https://openrouter.ai/api/v1/chat/completions').strip()
AI_MODEL = os.environ.get('AI_MODEL', 'google/gemini-2.5-flash').strip()  # قابل للتبديل عبر البيئة
AI_TIMEOUT = _safe_int(os.environ.get('AI_TIMEOUT', '45'), 45, 5, 180)
AI_TEMPERATURE = 0.9     # درجة إبداع عالية لضمان تنوع النصوص

# سجل حالة المفتاح وقت الإقلاع — يظهر في لوج Railway حتى تحت gunicorn
# (الطباعة القديمة كانت داخل if __name__=='__main__' ولا تعمل مع gunicorn).
print('🔑 AI_KEY: configured'
      if AI_KEY else
      '🔑 AI_KEY: MISSING — لم يُعثر على أي مفتاح OpenRouter في متغيّرات البيئة',
      flush=True)
print('🤖 AI_MODEL: %s' % AI_MODEL, flush=True)

# لا تُفعّل واجهات التشخيص الإدارية إلا عند ضبط رمز مستقل في بيئة الخادم.
ADMIN_TOKEN = _clean_key(os.environ.get('ADMIN_TOKEN'))


def _is_admin_request():
    """تحقق ثابت الزمن من رمز الإدارة من دون تسجيل قيمته."""
    provided = request.headers.get('X-Admin-Token', '')
    return bool(ADMIN_TOKEN) and hmac.compare_digest(provided, ADMIN_TOKEN)


def _admin_guard():
    """أخفِ مسارات الإدارة عند غياب رمز صحيح لتقليل سطح الاستكشاف."""
    if not _is_admin_request():
        return jsonify({'error': 'not_found'}), 404
    return None

# آخر خطأ فعلي من الـ AI — يُعرض في /api/ai-check وفي رسائل الفشل بدل الرسالة العامة
_LAST_AI_ERROR = {'when': None, 'status': None, 'message': None}


def _record_ai_error(status, message):
    """يخزّن آخر خطأ فعلي من OpenRouter ليُعرض في التشخيص بدل «المفتاح غير متصل»."""
    _LAST_AI_ERROR['status'] = status
    _LAST_AI_ERROR['message'] = str(message)[:300]
    try:
        _LAST_AI_ERROR['when'] = time.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        _LAST_AI_ERROR['when'] = None


class AIUnavailable(Exception):
    """يُرفع عند تعذّر الاتصال بالـ AI أو نفاد الرصيد — التطبيق يتوقف ولا يكتب قوالب."""
    pass

# جلسة HTTP ثابتة مع إعادة محاولة تلقائية — تستقر تحت ضغط الواجهة المباشرة
_ai_session = http_req.Session()
_ai_retry = Retry(
    total=3, connect=3, read=2, backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(['POST']),
)
_ai_session.mount('https://', HTTPAdapter(max_retries=_ai_retry))
_ai_session.headers.update({
    'Authorization': f'Bearer {AI_KEY}',
    'Content-Type': 'application/json',
})


def extract_json(text):
    """استخراج كائن JSON بأمان من رد الـ AI حتى لو تضمّن نصاً إضافياً.

    يبحث عن أول '{' وآخر '}' عبر Regex لمنع تعطّل الكود إذا أرجع النموذج
    نصاً عادياً مع الـ JSON أو غلّفه بـ markdown.
    """
    if not text:
        return None
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        return json.loads(text)
    except Exception:
        return None


def _ai_call(prompt, max_tokens=1200, temperature=AI_TEMPERATURE):
    """استدعاء AI عبر OpenRouter — جلسة ثابتة + Retry. يسجّل آخر خطأ فعلي."""
    if not AI_KEY:
        _record_ai_error('no_key', 'لا يوجد مفتاح OpenRouter في متغيّرات البيئة')
        print('❌ AI: no key configured', flush=True)
        return None
    try:
        r = _ai_session.post(AI_URL, json={
            'model': AI_MODEL,
            'messages': [{'role':'user','content':prompt}],
            'max_tokens': max_tokens,
            'temperature': temperature,
        }, timeout=AI_TIMEOUT)
        if r.status_code != 200:
            try:
                err = r.json().get('error', {}).get('message', r.text[:200])
            except Exception:
                err = r.text[:200]
            _record_ai_error(r.status_code, err)
            print(f'⚠️ AI HTTP {r.status_code}: {err}', flush=True)
            return None
        data = r.json()
        return data['choices'][0]['message']['content'].strip()
    except Exception as e:
        _record_ai_error('exception', e)
        print(f'❌ AI Error: {e}', flush=True)
        return None

def _make_persona_for_arch(arch):
    """شخصية كاملة من archetype — عميقة (7 أبعاد) إن توفّر المحرك الجديد.

    تضمن أن البرومبت المتقدم يجد كل المفاتيح (dialect/mood/expertise...)
    حتى في مسارات regen/add-perfumes التي كانت تمرر شخصية ناقصة.
    """
    if USE_NEW_PERSONAS:
        return generate_persona(arch)
    age = random.randint(*arch['age'])
    city = _city()
    return {
        'name': _name(arch['g']), 'label': arch['label'], 'gender': arch['g'],
        'age': age, 'city': city, 'emoji': arch.get('emoji', ''), 'archId': arch['id'],
        'address': _national_address(city), 'phone': _phone(),
    }


def _build_cross_sell(current_name, perfumes):
    """توجيه Cross-Sell/Layering: يذكر منتجاً ثانياً من نفس المتجر داخل التقييم.

    يرفع قيمة السلة. يُستخدم منتج آخر من نفس سلة الشخصية (طبيعي أكثر).
    يرجع نصاً (قد يكون فارغاً).
    """
    others = [p['name'] for p in perfumes if p.get('name') and p['name'] != current_name]
    if not others or random.random() >= 0.30:
        return ''
    other = random.choice(others)
    if random.random() < 0.5:
        return ('## ترابط منتجات (Cross-Sell):\n'
                f'- اذكر بعفوية أنك طلبت أيضاً "{other}" من نفس المتجر بنفس الطلب وامدحه بإيجاز '
                '(كلمة أو كلمتين) — لا تجعله محور التقييم.')
    return ('## وصفة سرية (Layering):\n'
            f'- اقترح خلطة شخصية اكتشفتها: رشة من هذا العطر وفوقها رشة من "{other}" من نفس المتجر، '
            'وصف النتيجة باختصار كأنها سر مميز.')


def _make_master_prompt(persona, product_name, used_block, extra_block=''):
    """بناء البرومبت المتقدم إجبارياً. يرجع (prompt, params).

    review_patterns اختياري (له بدائل افتراضية داخل personas_engine)، لكن
    لا نتراجع للبرومبت القديم إلا إذا فشل تحميل personas_engine بالكامل.
    """
    if USE_NEW_PERSONAS:
        params = generate_review_params(persona)
        result = build_master_prompt(persona, product_name, params, used_block, extra_block)
        # build_master_prompt returns (prompt, params) tuple
        if isinstance(result, tuple):
            return result
        return result, params
    # personas_engine غير محمّل = المنظومة ناقصة — نتوقف بخطأ واضح (503)
    # بدل مسار برومبت قديم متدنٍّ يعمل بصمت (نفس فلسفة قانون 4).
    raise AIUnavailable('محرك الشخصيات personas_engine غير محمّل — لا توليد بدونه')


# ═══════════════════════════════════════════════════════════
# تنظيف النص — يضمن نصاً بشرياً بلا ترقيم أو رموز (طبقة ضمان نهائية)
# ═══════════════════════════════════════════════════════════

# علامات ترقيم عربية/لاتينية + رموز تُزال لجعل النص يتدفّق طبيعياً كرسالة سريعة
_PUNCT_RE = re.compile(r'[،,؛;:.…·•\-–—_\"“”\'‘’`«»()\[\]{}<>/\\|*#^~=+%&@!؟?]+')
# نطاقات الإيموجي والرموز التعبيرية
_EMOJI_RE = re.compile(
    '[\U0001F000-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF'
    '\U00002190-\U000021FF\U00002B00-\U00002BFF️‍•]+')


# مسار النص النهائي وبوابة التفرّد — مصدر واحد مشترك مع streamlit_app (review_text)
from review_text import (humanize as _humanize, finalize_review_text,
                         write_unique as rt_write_unique)


# ═══════════════════════════════════════════════════════════
# طبقة ضمان التفرّد — كل نص (تقييم/متجر/رد) فريد لا يتكرر
# ═══════════════════════════════════════════════════════════

def _is_dup(text):
    """هل النص مكرر؟ (يعمل فقط عند توفّر anti_repeat)."""
    return bool(USE_ANTI_REPEAT and text and is_duplicate(text))


def _register(text):
    """تسجيل نص مقبول في طبقة الجلسة لمنع تكراره لاحقاً."""
    if USE_ANTI_REPEAT and text:
        try:
            ar_register_text(text)
        except Exception:
            pass


def _ai_unique_json(prompt, max_tokens, finalize, attempts=4, temp_step=0.06):
    """يستدعي AI متعدّد المحاولات حتى يحصل على JSON بنص **نهائي** فريد.

    يرجع (rv_dict، النص النهائي). عند فشل كل المحاولات يرجع أفضل جهد (قد يكون
    مكرراً) — لكن بعد فحص حقيقي على النص المخزَّن لا على المخرج الخام.

    finalize إلزامي: الفحص على نص والتخزين لنص آخر هو الخلل الأصلي بعينه.
    """
    return rt_write_unique(
        call=lambda p, mt, temp: _ai_call(p, max_tokens=mt, temperature=temp),
        parse=extract_json,
        finalize=finalize,
        is_dup=_is_dup,
        prompt=prompt, max_tokens=max_tokens, attempts=attempts,
        base_temp=AI_TEMPERATURE, temp_step=temp_step, max_temp=1.0,
    )


def _ai_unique_text(prompt, max_tokens, finalize, attempts=4, parser=None, temp_step=0.06):
    """مثل _ai_unique_json لكن يرجع نصاً مفرداً (يدعم parser مخصص مثل parse_ai_reply)."""
    parser = parser or (lambda out: ((extract_json(out) or {}).get('text', '') if out else ''))
    _, text = rt_write_unique(
        call=lambda p, mt, temp: _ai_call(p, max_tokens=mt, temperature=temp),
        parse=lambda out: ({'text': parser(out)} if (parser(out) or '').strip() else None),
        finalize=finalize,
        is_dup=_is_dup,
        prompt=prompt, max_tokens=max_tokens, attempts=attempts,
        base_temp=AI_TEMPERATURE, temp_step=temp_step, max_temp=1.0,
    )
    return text


def _ai_write_json(prompt, max_tokens, finalize, attempts=5):
    """يكتب عبر AI فقط (لا قوالب). يرجع (dict، نص نهائي) أو يرفع AIUnavailable.

    إن عاد الـ AI بنص (حتى لو قريب من سابق) نقبله كأفضل جهد للتفرّد.
    None تعني انقطاع الربط أو نفاد الرصيد → نتوقف ولا نكتب.
    """
    rv, text = _ai_unique_json(prompt, max_tokens=max_tokens, finalize=finalize,
                               attempts=attempts)
    if not rv or not text:
        # قانون 4: عند تعذّر الـ AI (انقطاع/نفاد رصيد) نتوقّف ولا نفبرك تقييمًا.
        # المستدعون ومعالجات except AIUnavailable يترجمونه إلى 503 عبر _ai_unavailable_response().
        raise AIUnavailable('AI returned no usable review — stopping (no fabricated fallback, Law 4)')
    return rv, text


def _used_texts_block(limit=30, persona_name=None):
    """كتلة آخر النصوص المستخدمة (لمنع التكرار) — موحّدة لكل المسارات."""
    if USE_ANTI_REPEAT:
        return format_used_texts_block(limit=limit, persona_name=persona_name)
    used = _get_used_texts(_load_archive(), limit=limit + 10)
    return '\n'.join(f'- {t}' for t in used[-limit:]) if used else ''


def _plan_review(persona, pf, perfumes, used_block):
    """مرحلة (التفكير): يبني البرومبت ويحدّد النمط والتوجيه الاستراتيجي — بلا استدعاء AI.

    يرجع (prompt, params) ليتمكّن المسار اللحظي من عرض «ماذا سيكتب ولماذا» قبل الكتابة.
    """
    cross = _build_cross_sell(pf['name'], perfumes)
    
    context_hints = []
    
    ptype = pf.get('product_type', 'عطر')
    if ptype == 'مكياج':
        context_hints.append("هذا المنتج عبارة عن مكياج/أدوات تجميل (ليس عطراً). تحدث عن اللون، التغطية، الثبات، أو الملمس، ولا تستخدم كلمات عطرية مثل 'فوحان' أو 'ريحة'.")
    elif ptype == 'معطر_شعر':
        context_hints.append("هذا المنتج عطر مخصص للشعر. تحدث عن تأثيره على الشعر (ما ينشفه، ريحته تفوح مع الحركة)، ولا تتعامل معه كعطر جسم عادي.")
    elif ptype == 'معطر_جسم':
        context_hints.append("هذا معطر جسم (Body Mist)، ركز على الانتعاش والترطيب وخفة الريحة بعد الشاور.")
    elif ptype == 'بخور':
        context_hints.append("هذا بخور/عود. استخدم مصطلحات مثل 'الكسرة'، 'الدخان'، 'التبخير'، 'يمسك في الثياب/المجلس'.")
    elif ptype == 'عناية_جسم':
        context_hints.append("هذا منتج عناية بالجسم. ركز على الترطيب والنعومة وسرعة الامتصاص.")

    # Gift matching
    is_gift_male = persona.get('id', persona.get('archId')) == 'هدايا_رجل'
    is_gift_female = persona.get('id', persona.get('archId')) == 'هدايا_أنثى'
    
    if is_gift_male and (pf.get('g') == 'نسائي' or ptype == 'مكياج'):
        context_hints.append("مهم جداً: أنت رجل واشتريت هذا المنتج كهدية (لزوجتك/أمك/أختك). تحدث عن إعجابها بالهدية وردة فعلها، ولا تتحدث كأنك تستخدمه شخصياً!")
    elif is_gift_female and pf.get('g') == 'رجالي':
        context_hints.append("مهم جداً: أنتِ امرأة واشتريتِ هذا المنتج كهدية (لزوجك/أخوكِ/أبوكِ). تحدثي عن فخامة الهدية وكيف عجبته، ولا تتحدثي كأنكِ تستخدمينه شخصياً!")

    # توجيه واعٍ بطبيعة العطر (مناسبته/ثقله) — يجعل التقييم يطابق الاستخدام الحقيقي
    if USE_DEMO_MATCH and not (is_gift_male or is_gift_female):
        _dir = _demo.review_directive(persona, pf)
        if _dir:
            context_hints.append(_dir)

    if context_hints:
        cross += "\n\n## تنبيهات سياقية (التزم بها بحذافيرها):\n- " + "\n- ".join(context_hints)

    return _make_master_prompt(persona, pf['name'], used_block, extra_block=cross)


def _make_review_finalizer(persona, allow_words):
    """يبني دالة النص النهائي لهذه الشخصية — نفس بناء streamlit_app حرفيًّا.

    أخطاء الشخصية الإملائية (has_typo) جزء من صوتها وتُطبَّق **داخل** المسار
    النهائي؛ تطبيقها بعد الفحص كان يغيّر المخزَّن عمّا فُحص. ليست أداة تفرّد.
    """
    typo_fn = None
    if USE_DIALECTS and persona.get('has_typo', False):
        typo_fn = lambda t: apply_typos(t, probability=1.0)
    return lambda t: finalize_review_text(
        t,
        allow_words=allow_words,
        humanizer=(lambda x: hz_humanize_output(x, kind='review')) if USE_HUMANIZER else None,
        strip_tail=strip_broken_tail if USE_SEMANTIC_GUARD else None,
        typo_fn=typo_fn,
    )


def _write_review(persona, pf, prompt, params):
    """مرحلة (الكتابة): الـ AI فقط يكتب — لا قوالب احتياطية.

    يرفع AIUnavailable إذا تعذّر الـ AI (انقطاع/نفاد رصيد) فيتوقف التوليد.
    يسجّل النص في طبقة الجلسة لمنع أي تكرار لاحق.
    """
    # سقف توكنز وحدّ كلمات من الطول المعاين من بيانات المنافسين الحقيقية
    # (len_target من generate_review_params) — عند غيابه: السلوك القديم (4)
    mx = params.get('len_target') or 4
    _allow = mx + (1 if mx <= 4 else max(2, mx // 5))  # سماحية تجاوز صغيرة
    max_tokens = 80 + mx * 8
    # مسار النص النهائي — نفس ما سيُخزَّن، فتفحصه بوابة التفرّد على حقيقته.
    # (القصّ عند _allow كان يُوحّد مخرجات مختلفة بعد اجتيازها الفحص الخام.)
    _finalize = _make_review_finalizer(persona, _allow)
    rv, _text = _ai_write_json(prompt, max_tokens=max_tokens,
                               finalize=_finalize, attempts=5)  # يرفع AIUnavailable عند الفشل
    # ══ الحارس الدلالي: نزيف/بتر/تجاوز طول → إعادة توليد بدل القصّ الصامت ══
    if USE_SEMANTIC_GUARD:
        for _k in range(2):
            _viol = guard_violations(_text, max_words=_allow)
            # طبقة الأنسنة: نبرة إعلانية/آلية أو خاتمة عامة → تُعامَل كمخالفة تُعاد
            if USE_HUMANIZER:
                _viol = _viol + hz_content_tells(_text)
            if not _viol:
                break
            _hint = (f'\n\n⚠️ رُفض النص السابق «{_text}» ({"، ".join(_viol)}). '
                     f'اكتب تقييماً جديداً عن العطر نفسه فقط، جملة مكتملة المعنى، {mx} كلمات كحد أقصى.')
            _nxt, _nxt_text = _ai_unique_json(prompt + _hint, max_tokens=max_tokens,
                                              finalize=_finalize, attempts=2)
            if _nxt and _nxt_text:
                rv, _text = _nxt, _nxt_text
            else:
                break
    rv['text'] = _text
    rv['price'] = pf['price']
    rv['brand'] = pf['brand']
    rv['pg'] = pf['g']
    rv['product'] = pf['name']
    rv['pattern'] = params.get('pattern', '')
    rv = mark_synthetic_output(rv)

    if USE_AR_TRACK:
        ar_track_pattern(params.get('pattern', 'default'))
    _register(rv.get('text', ''))
    return rv


def _ai_reviews(persona, perfumes):
    """توليد تقييمات AI فريدة لكل عطر — يفكّر ثم يكتب (دفعة واحدة)."""
    used_block = _used_texts_block(limit=30, persona_name=persona.get('name'))
    all_reviews = []
    for pf in perfumes:
        prompt, params = _plan_review(persona, pf, perfumes, used_block)
        all_reviews.append(_write_review(persona, pf, prompt, params))
    # حفظ في الأرشيف
    _archive_batch(all_reviews, persona.get('name', ''))
    return all_reviews

def _ai_single_review(persona, product):
    """توليد تقييم واحد بالـ AI فقط — يرفع AIUnavailable عند تعذّر الـ AI."""
    used_block = _used_texts_block(limit=15, persona_name=persona.get('name'))
    prompt, params = _make_master_prompt(persona, product['name'], used_block)
    rv = _write_review(persona, product, prompt, params)
    _archive_review(rv.get('text', ''), product['name'], persona.get('name', ''))
    return rv

# ═══════════════════════════════════════════════════════════
# تقييم المتجر — يستورد المنطق المرجعي من الوحدة المشتركة store_review
# (نفس البيانات والبرومبت والحُرّاس التي تستوردها واجهة Streamlit — مصدر واحد)
# ═══════════════════════════════════════════════════════════
from store_review import (
    STORE_ASPECTS, STORE_OPENERS, ASPECT_TOPIC,
    sample_length_target, band_for, build_store_prompt,
    strip_store_vocatives, has_luxury_metaphor, scrub_luxury_metaphor,
    StoreTopicTracker,
)

# أسماء معروفة لحذف نداء الاسم (يمرَّرها كل تطبيق للوحدة المشتركة)
_STORE_NAMES = set(NAMES.get('male', []) + NAMES.get('female', []) + NAMES.get('family_names', []))
# عدّاد مواضيع المتجر لهذه العملية (سقف 20% لكل موضوع عبر الجلسة)
_store_topics = StoreTopicTracker()


def _ai_store_review(persona):
    """تقييم متجر متنوّع: طول مُعايَن + جوانب موزونة بالموضوع + سقف 20% لكل موضوع + حظر استعارات الفخامة.

    لا يُحقن مثال ذهبي (كان يُسحب كله من story_type واحد فيوحّد المخرج ويطبع القالب الطويل)؛
    STORE_ASPECTS المتنوّعة + الشخصية + سقف الموضوع + طول مُعايَن هي ما يقود التنوّع.
    المنطق نفسه موحّد في store_review، يستورده مسار Streamlit كي لا يتباعد المساران.
    """
    # (1) هدف الطول — يُعيَّن قبل البرومبت (35% قصير جدًا · 40% قصير · 25% متوسط)
    lo, hi, length_desc = sample_length_target()
    band = band_for(hi)
    # (2) اختيار الجوانب بعيدًا عن المواضيع المشبعة (≥20%)
    blocked = _store_topics.blocked()
    pool = [a for a in STORE_ASPECTS if ASPECT_TOPIC.get(a) not in blocked]
    if len(pool) < 2:
        pool = list(STORE_ASPECTS)
    aspects = random.sample(pool, k=2)
    opener = random.choice(STORE_OPENERS)
    used_block = _used_texts_block(limit=15, persona_name=persona.get('name'))
    avoid_line = (f"\n- ممنوع الحديث عن: {'، '.join(sorted(blocked))} (تكرّرت كثيرًا)" if blocked else '')

    prompt = build_store_prompt(persona, band, aspects, opener, used_block, avoid_line)

    def _finalize(text):
        """النص النهائي لتقييم المتجر — يشمل سقف الطول كي تفحص البوابة المخزَّن فعلاً."""
        if USE_HUMANIZER:
            text = hz_humanize_output(text, kind='store')  # إزالة إطار المساعد/الماركداون
        text = _humanize(text)  # بلا ترقيم أو رموز
        text = re.sub(r'\s+', ' ', re.sub(r'[0-9٠-٩]+', ' ', text)).strip()  # صفر أرقام (مسار المتجر)
        text = strip_store_vocatives(text, persona.get('name'), _STORE_NAMES)  # منع النداء حتميًّا
        words = text.split()
        if len(words) > hi:
            words = words[:hi]
            if USE_SEMANTIC_GUARD:
                words = strip_broken_tail(words)
        return ' '.join(words)

    rv, text = _ai_write_json(prompt, max_tokens=200, finalize=_finalize,
                              attempts=5)  # يرفع AIUnavailable عند الفشل

    # (3) حارس موحّد: استعارة فخامة / موضوع مشبع / تجاوز الطول → إعادة توليد موجَّهة
    for _k in range(3):
        problems = []
        if has_luxury_metaphor(text):
            problems.append('استعارة فخامة (كنز/صندوق/مجوهرات/ذهب) ممنوعة')
        blocked_now = _store_topics.blocked()
        if _store_topics.classify(text) in blocked_now:
            fresh = [a for a in STORE_ASPECTS if ASPECT_TOPIC.get(a) not in blocked_now]
            alt = random.choice(fresh or STORE_ASPECTS)
            problems.append(f"موضوع «{_store_topics.classify(text)}» تكرّر كثيرًا؛ اكتب عن {alt} بدلًا منه ولا تذكره")
        if len(text.split()) > hi + 2:
            problems.append(f"أطل من المطلوب؛ التزم بـ {length_desc}")
        if USE_HUMANIZER:
            for _tl in hz_content_tells(text, kind='store'):
                problems.append(f'أنسنة: {_tl}')
        if not problems:
            break
        hint = "\n\n⚠️ أعد الكتابة: " + " · ".join(problems)
        nxt, nxt_text = _ai_unique_json(prompt + hint, max_tokens=200,
                                        finalize=_finalize, attempts=2)
        if not (nxt and nxt_text):
            break
        rv, text = nxt, nxt_text

    # (4) ضمان حتمي أخير: صفر استعارة فخامة (سقف الطول مطبَّق داخل _finalize)
    text = scrub_luxury_metaphor(text)
    rv['text'] = text
    rv = mark_synthetic_output(rv)

    # (5) تسجيل الموضوع النهائي في عدّاد الجلسة (بعد التثبيت)
    _store_topics.record(text)

    _register(text)
    if USE_ANTI_REPEAT:
        try:
            ar_archive_review(text, 'متجر مهووس', persona.get('name', ''))
        except Exception:
            pass
    return rv

# ملاحظة: لا توجد كتابة احتياطية بالقوالب — الـ AI وحده يكتب.
# عند تعذّر الـ AI (انقطاع/نفاد رصيد) يرفع _ai_write_json استثناء AIUnavailable
# فتتوقف الكتابة وتُرجع الواجهة رسالة خطأ بدل عرض نص قالب.
# (بنوك النصوص مثل real_reviews_bank/short_texts_bank تبقى مراجع أسلوبية تُحقن
#  في البرومبت ليتعلّم منها النموذج فقط — لا تُعرض كتقييمات.)

# ═══════════ الشخصيات ═══════════
ARCHETYPES = [
    {'id':'شاب_جامعي','g':'male','label':'شاب جامعي','emoji':'🎓','age':(18,23),
     'price':(0,300),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'رجل_أعمال','g':'male','label':'رجل أعمال','emoji':'👔','age':(30,50),
     'price':(400,2000),'prefers':['رجالي','مشترك'],'count':(3,6)},
    {'id':'خبير_عطور','g':'male','label':'خبير عطور','emoji':'🧪','age':(25,40),
     'price':(200,2000),'prefers':['رجالي','مشترك'],'count':(3,7)},
    {'id':'أب_عائلة','g':'male','label':'أب عائلة','emoji':'👨‍👧','age':(35,55),
     'price':(100,500),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'شاب_رياضي','g':'male','label':'شاب رياضي','emoji':'💪','age':(20,30),
     'price':(100,400),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'كبير_سن','g':'male','label':'رجل كبير','emoji':'👴','age':(55,75),
     'price':(100,600),'prefers':['رجالي','مشترك'],'count':(2,3)},
    {'id':'موظف','g':'male','label':'موظف','emoji':'🏢','age':(25,45),
     'price':(150,500),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'بنت_عصرية','g':'female','label':'بنت عصرية','emoji':'💅','age':(20,28),
     'price':(100,600),'prefers':['نسائي','مشترك'],'count':(3,6)},
    {'id':'أم_سعودية','g':'female','label':'أم سعودية','emoji':'👩‍🦱','age':(38,55),
     'price':(100,800),'prefers':['نسائي','مشترك'],'count':(2,5)},
    {'id':'عروس','g':'female','label':'عروس','emoji':'👰','age':(21,30),
     'price':(300,2000),'prefers':['نسائي','مشترك'],'count':(4,8)},
    {'id':'موظفة','g':'female','label':'موظفة','emoji':'👩‍💻','age':(24,40),
     'price':(150,600),'prefers':['نسائي','مشترك'],'count':(2,4)},
    {'id':'خبيرة_تجميل','g':'female','label':'خبيرة تجميل','emoji':'💄','age':(25,38),
     'price':(200,1000),'prefers':['نسائي','مشترك'],'count':(3,6)},
    {'id':'طالبة','g':'female','label':'طالبة جامعية','emoji':'📚','age':(18,23),
     'price':(0,250),'prefers':['نسائي','مشترك'],'count':(2,3)},
    {'id':'جدة','g':'female','label':'سيدة كبيرة','emoji':'👵','age':(55,75),
     'price':(100,500),'prefers':['نسائي','مشترك'],'count':(2,3)},
    {'id':'مقارن','g':'male','label':'مقارن أسعار','emoji':'📊','age':(25,40),
     'price':(100,600),'prefers':['رجالي','مشترك'],'count':(3,5)},
    {'id':'هدايا_رجل','g':'male','label':'يشتري هدايا','emoji':'🎁','age':(25,45),
     'price':(200,1000),'prefers':['نسائي','مشترك'],'count':(2,4)},
    {'id':'هدايا_أنثى','g':'female','label':'تشتري هدايا','emoji':'🎀','age':(22,45),
     'price':(200,800),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'محب_تسوق','g':'male','label':'محب تسوق','emoji':'🛍️','age':(22,35),
     'price':(100,800),'prefers':['رجالي','مشترك'],'count':(3,6)},
    {'id':'محبة_تسوق','g':'female','label':'محبة تسوق','emoji':'👛','age':(22,35),
     'price':(100,800),'prefers':['نسائي','مشترك'],'count':(3,6)},
    # ── شخصيات إضافية (توسعة) — يجب أن تطابق personas_engine.ARCHETYPES ──
    {'id':'مبتعث','g':'male','label':'شاب مبتعث','emoji':'✈️','age':(22,30),
     'price':(150,900),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'عسكري','g':'male','label':'منسوب عسكري','emoji':'🎖️','age':(24,45),
     'price':(150,600),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'معلم','g':'male','label':'معلم','emoji':'🧑‍🏫','age':(28,50),
     'price':(150,500),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'مهندس','g':'male','label':'مهندس','emoji':'👷','age':(26,45),
     'price':(200,800),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'طبيب','g':'male','label':'طبيب','emoji':'🩺','age':(30,52),
     'price':(300,1500),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'متذوق_نيش','g':'male','label':'متذوق نيش','emoji':'🌿','age':(25,45),
     'price':(400,3000),'prefers':['رجالي','مشترك'],'count':(3,6)},
    {'id':'عامل_مقتصد','g':'male','label':'يدوّر العملي','emoji':'🧰','age':(25,50),
     'price':(0,180),'prefers':['رجالي','مشترك'],'count':(2,3)},
    {'id':'وجيه','g':'male','label':'وجيه ومحب عود','emoji':'🕌','age':(45,70),
     'price':(300,2000),'prefers':['رجالي','مشترك'],'count':(2,4)},
    {'id':'بدوي','g':'male','label':'محب البخور والعود','emoji':'🐪','age':(30,60),
     'price':(100,800),'prefers':['رجالي','مشترك'],'count':(2,3)},
    {'id':'مؤثرة','g':'female','label':'مؤثرة عطور','emoji':'📸','age':(22,35),
     'price':(200,1500),'prefers':['نسائي','مشترك'],'count':(3,6)},
    {'id':'معلمة','g':'female','label':'معلمة','emoji':'👩‍🏫','age':(28,50),
     'price':(150,500),'prefers':['نسائي','مشترك'],'count':(2,4)},
    {'id':'طبيبة','g':'female','label':'طبيبة','emoji':'👩‍⚕️','age':(30,50),
     'price':(300,1200),'prefers':['نسائي','مشترك'],'count':(2,4)},
    {'id':'متذوقة_نيش','g':'female','label':'متذوقة نيش','emoji':'🌸','age':(25,45),
     'price':(400,2500),'prefers':['نسائي','مشترك'],'count':(3,5)},
    {'id':'ست_بيت','g':'female','label':'ست بيت','emoji':'🏠','age':(30,55),
     'price':(80,400),'prefers':['نسائي','مشترك'],'count':(2,4)},
    # ── الطبقة 7 — يجب أن تطابق personas_engine.ARCHETYPES (مصيدة #1) ──
    {'id':'كسول','g':'male','label':'كسول ما يحب يكتب','emoji':'😑','age':(18,55),
     'price':(0,999),'prefers':['رجالي','مشترك'],'count':(1,1)},
    {'id':'كسولة','g':'female','label':'كسولة ما تحب تكتب','emoji':'😑','age':(18,55),
     'price':(0,999),'prefers':['نسائي','مشترك'],'count':(1,1)},
    {'id':'ناقد_صريح','g':'male','label':'ناقد يقول رأيه بصراحة','emoji':'🤨','age':(25,50),
     'price':(50,500),'prefers':['رجالي','مشترك'],'count':(1,3)},
    {'id':'ناقدة_صريحة','g':'female','label':'ناقدة تقول رأيها بصراحة','emoji':'🤨','age':(25,50),
     'price':(50,500),'prefers':['نسائي','مشترك'],'count':(1,3)},
    {'id':'متردد','g':'male','label':'متردد كان خايف واكتشف','emoji':'🤔','age':(20,40),
     'price':(30,300),'prefers':['رجالي','مشترك'],'count':(1,2)},
    {'id':'مترددة','g':'female','label':'مترددة كانت خايفة واكتشفت','emoji':'🤔','age':(20,40),
     'price':(30,300),'prefers':['نسائي','مشترك'],'count':(1,2)},
    {'id':'متذمر_لطيف','g':'male','label':'يشتكي من شي تافه ثم يمدح','emoji':'😂','age':(20,45),
     'price':(30,400),'prefers':['رجالي','مشترك'],'count':(1,2)},
    {'id':'متذمرة_لطيفة','g':'female','label':'تشتكي من شي تافه ثم تمدح','emoji':'😂','age':(20,45),
     'price':(30,400),'prefers':['نسائي','مشترك'],'count':(1,2)},
]

# ═══════════ الكتالوج الكامل من ملف المتجر ═══════════
with open(BASE_DIR / 'catalog.json', 'r', encoding='utf-8') as f:
    PRODUCTS = json.load(f)
# مصدر واحد للحقيقة: نُمرّر نفس القائمة لفهرس personas_engine بدل قراءة الملف مرتين
if USE_NEW_PERSONAS:
    try:
        pe_set_catalog(PRODUCTS)
    except Exception as _e:
        print(f'⚠️ set_catalog failed: {_e}')
print(f'📦 الكتالوج: {len(PRODUCTS)} منتج ({sum(1 for p in PRODUCTS if p["g"]=="رجالي")} رجالي | {sum(1 for p in PRODUCTS if p["g"]=="نسائي")} نسائي | {sum(1 for p in PRODUCTS if p["g"]=="مشترك")} مشترك)')

# ═══════════ براندات ترند — الأكثر مبيعاً في السعودية ═══════════
TRENDING_BRANDS = [
    'ديور','شانيل','توم فورد','لطافة','أرماني','بربري','كارولينا',
    'لانكوم','فرساتشي','جوتشي','جان بول','أمواج','نارسيسو',
    'بولغاري','كالفن','مونت بلانك','دولتشي','بوشرون','هوجو',
    'جيرلان','كريد','بيليونير','راشد','عساف','سوفاج','خمرة',
]

def _pick_products(arch):
    """اختيار عطور ذكية حسب الشخصية مع ترجيح الترند المتقدم"""
    pool = []
    is_gift_male = arch['id'] == 'هدايا_رجل'
    is_gift_female = arch['id'] == 'هدايا_أنثى'
    
    for p in PRODUCTS:
        if p['g'] not in arch['prefers']:
            continue
        
        ptype = p.get('product_type', 'عطر')
        # الذكور (غير الهدايا) لا يشترون مكياج أبداً
        if arch['g'] == 'male' and not is_gift_male and ptype in ['مكياج', 'معطر_شعر']:
            continue
            
        # الإناث (غير الهدايا) لا يشترين عطور رجالية أبداً
        if arch['g'] == 'female' and not is_gift_female and p['g'] == 'رجالي':
            continue
            
        if arch['price'][0] <= p['price'] <= arch['price'][1]:
            pool.append(p)
            
    if len(pool) < 3:
        pool = [p for p in PRODUCTS if p['g'] in arch['prefers']]
        # Re-apply strict type filters
        pool = [p for p in pool if not (arch['g'] == 'male' and not is_gift_male and p.get('product_type') in ['مكياج', 'معطر_شعر'])]
    if len(pool) < 3:
        pool = PRODUCTS[:]

    # فلتر المطابقة الديموغرافي: يهذّب المجمّع حسب (عمر/مناسبة/ثقل) الشخصية،
    # يرفض غير المنطقي (شيخ+حلويات، رياضي+عود ثقيل) ويعيد الترشيح. لا يُفرِّغ
    # المجمّع أبداً، ولا يمسّ حقن كربتك/الترند داخل blend_selection.
    if USE_DEMO_MATCH:
        pool = _demo.filter_pool(arch, pool)

    count = random.randint(*arch['count'])

    # خلطة "محلي" الذكية التلقائية: كربتك (أولوية قصوى) + أفضل 100 ترند + تمويه
    # مختلفة في كل سلة. تحترم pool المُفلتر (جنس/سعر/نوع) فلا تكسر التماسك.
    if USE_TRENDING:
        return blend_selection(PRODUCTS, pool, count, arch['prefers'])

    # شبكة أمان (بلا محرك الترند): ترجيح بسيط للبراندات الترند
    trend_lower = [b.lower() for b in TRENDING_BRANDS]
    weights = []
    for p in pool:
        brand = p.get('brand', '').lower()
        name = p.get('name', '').lower()
        is_trend = any(tb in brand or tb in name for tb in trend_lower)
        weights.append(3.0 if is_trend else 1.0)
    count = min(count, len(pool))
    selected, pool_copy, w_copy = [], list(pool), list(weights)
    for _ in range(count):
        if not pool_copy:
            break
        idx = random.choices(range(len(pool_copy)), weights=w_copy, k=1)[0]
        selected.append(pool_copy.pop(idx))
        w_copy.pop(idx)
    return selected

# ═══════════ الأسماء والمدن والعناوين ═══════════
def _name(g):
    key='male' if g=='male' else 'female'
    return random.choice(NAMES.get(key,['مستخدم']))+' '+random.choice(NAMES.get('family_names',['السعودي']))

def _city():
    cities=NAMES.get('cities',[{'name':'الرياض','weight':1}])
    return random.choices(cities,weights=[c.get('weight',1) for c in cities],k=1)[0]['name']

# ═══════════ عنوان وطني سعودي ═══════════
# مصدر واحد للحقيقة: personas_engine. يُستورد منه مباشرة لتفادي تكرار/تباعد القائمتين.
# الـ fallback المحلي يُستخدم فقط إذا تعذّر استيراد المحرك الجديد.
if USE_NEW_PERSONAS:
    from personas_engine import CITY_DATA, _make_address as _national_address
else:
    CITY_DATA = {
        'الرياض': {'districts': [('العليا', 'شارع العليا العام', '122'), ('النخيل', 'شارع الأمير محمد بن سعد', '123'),
            ('الملقا', 'طريق أنس بن مالك', '135'), ('الصحافة', 'طريق أبو بكر الصديق', '133')]},
        'جدة': {'districts': [('الحمراء', 'شارع فلسطين', '233'), ('الروضة', 'شارع الأمير سلطان', '234'),
            ('الشاطئ', 'طريق الكورنيش', '236')]},
        'الدمام': {'districts': [('الشاطئ', 'طريق الكورنيش', '324'), ('الفيصلية', 'شارع الملك فيصل', '322')]},
    }

    def _national_address(city_name):
        cdata = CITY_DATA.get(city_name, {'districts': [('المركز', 'الشارع العام', '114')]})
        dist, street, postal_prefix = random.choice(cdata['districts'])
        building = str(random.randint(2000, 9999))
        postal = f'{postal_prefix}{random.randint(0, 99):02d}'
        extra = str(random.randint(2000, 9999))
        return {
            'building': building, 'street': street, 'district': dist,
            'city': city_name, 'postal': postal, 'extra': extra,
            'short_code': ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(4)) + f'{random.randint(0,9999):04d}',
            'full': f'{building} {street}، حي {dist}، {city_name} {postal} - {extra}'
        }

# ═══════════ رقم جوال سعودي ═══════════
SAUDI_PREFIXES = ['050','053','054','055','056','057','058','059']
def _phone():
    return random.choice(SAUDI_PREFIXES) + ''.join([str(random.randint(0,9)) for _ in range(7)])

# ═══════════ توليد شخصية كاملة ═══════════
def _gen_one():
    """توليد شخصية كاملة — يستخدم المحرك الجديد إذا متوفر"""
    if USE_NEW_PERSONAS:
        # المحرك الجديد: شخصية عميقة بـ 7 أبعاد
        persona = generate_persona()
        arch = next((a for a in ARCHETYPES if a['id'] == persona['archId']), random.choice(ARCHETYPES))
    else:
        # المحرك القديم
        arch = random.choice(ARCHETYPES)
        age = random.randint(*arch['age'])
        city = _city()
        addr = _national_address(city)
        persona = {
            'name': _name(arch['g']), 'city': city, 'gender': arch['g'], 'age': age,
            'label': arch['label'], 'emoji': arch['emoji'], 'archId': arch['id'],
            'address': addr, 'phone': _phone()
        }

    perfumes = _pick_products(arch)
    reviews = _ai_reviews(persona, perfumes)
    store = _ai_store_review(persona)

    return {
        'persona': persona,
        'perfumes': perfumes,
        'reviews': reviews,
        'store': store
    }

@app.route('/')
def index(): return render_template('index.html')

@app.route('/health')
def health():
    """Railway health check — حالة المحركات والبيانات"""
    return jsonify({
        'status': 'ok',
        'engines': {
            'personas': USE_NEW_PERSONAS,
            'dialects': USE_DIALECTS,
            'patterns': USE_PATTERNS,
            'anti_repeat': USE_ANTI_REPEAT,
            'trending': USE_TRENDING,
            'intel': USE_INTEL,
            'rate_limit': USE_RATE_LIMIT,
        },
        'products': len(PRODUCTS),
        'archive': len(_load_archive().get('reviews', [])),
        'ai_key_loaded': bool(AI_KEY),
        'ai_model': AI_MODEL,
    })


@app.route('/api/ai-check')
def ai_check():
    """تشخيص إداري محلي لا ينفذ طلباً خارجياً ولا يعرض أي جزء من المفتاح."""
    if not _is_admin_request():
        return jsonify({'error': 'not_found'}), 404

    info = {
        'key_loaded': bool(AI_KEY),
        'model': AI_MODEL,
        'last_error': _LAST_AI_ERROR,
    }
    if not AI_KEY:
        info['ok'] = False
        info['reason'] = 'لا يوجد مفتاح AI مضبوط في بيئة الخادم.'
        return jsonify(info), 200
    info['ok'] = True
    return jsonify(info), 200


def _ai_unavailable_response():
    """رد موحّد عند تعذّر الـ AI — يكشف السبب الحقيقي بدل رسالة عامة مضلّلة."""
    status = _LAST_AI_ERROR.get('status')
    reason = _LAST_AI_ERROR.get('message')
    if not AI_KEY or status == 'no_key':
        msg = ('مفتاح OpenRouter غير محمّل. اضبط AI_KEY أو OPENROUTER_API_KEY في '
               'بيئة الخادم ثم أعد النشر.')
    elif status == 401:
        msg = 'المفتاح مرفوض من OpenRouter (401) — القيمة غير صحيحة أو منتهية. جدّد المفتاح.'
    elif status == 402:
        msg = 'نفد رصيد OpenRouter (402) — أضِف رصيداً للحساب.'
    elif status == 404:
        msg = f'النموذج «{AI_MODEL}» غير متاح (404) — بدّله عبر متغيّر AI_MODEL.'
    elif reason:
        msg = f'تعذّر التوليد — سبب الـ AI: {reason}.'
    else:
        msg = 'تعذّر الاتصال بالذكاء الاصطناعي أو نفد الرصيد — لم تتم كتابة أي تقييم.'
    return jsonify({
        'error': 'ai_unavailable',
        'message': msg,
        'ai_status': status,
        'ai_error': reason,
    }), 503

@app.route('/api/generate', methods=['POST'])
@rate_limit(RATE_LIMIT_GENERATE)
def generate():
    try:
        return jsonify(_gen_one())
    except AIUnavailable:
        return _ai_unavailable_response()

@app.route('/api/batch', methods=['POST'])
@rate_limit(RATE_LIMIT_GENERATE)
def batch():
    n = _safe_int((request.get_json(silent=True) or {}).get('count', 5), 5, 1, 20)
    try:
        results = [_gen_one() for _ in range(n)]
    except AIUnavailable:
        return _ai_unavailable_response()
    return jsonify({'results': results, 'count': n})

@app.route('/api/regen-review', methods=['POST'])
@rate_limit(RATE_LIMIT_GENERATE)
def regen_review():
    d = request.get_json(silent=True) or {}
    arch = next((a for a in ARCHETYPES if a['id']==d.get('archId','')), random.choice(ARCHETYPES))
    product = {'name':d.get('product',''),'price':d.get('price',0),'brand':d.get('brand',''),'g':d.get('pg','مشترك')}
    persona = _make_persona_for_arch(arch)
    try:
        rv = _ai_single_review(persona, product)
    except AIUnavailable:
        return _ai_unavailable_response()
    return jsonify(rv)

@app.route('/api/add-perfumes', methods=['POST'])
@rate_limit(RATE_LIMIT_GENERATE)
def add_perfumes():
    d = request.get_json(silent=True) or {}
    arch = next((a for a in ARCHETYPES if a['id']==d.get('archId','')), random.choice(ARCHETYPES))
    exclude = set(d.get('existing', []))
    pool = [p for p in PRODUCTS if p['g'] in arch['prefers'] and p['name'] not in exclude
            and arch['price'][0]<=p['price']<=arch['price'][1]]
    if not pool:
        pool = [p for p in PRODUCTS if p['g'] in arch['prefers'] and p['name'] not in exclude]
    if not pool:
        return jsonify({'perfumes':[],'reviews':[]})
    count = random.randint(1, min(3, len(pool)))
    perfumes = random.sample(pool, count)
    persona = _make_persona_for_arch(arch)
    try:
        reviews = _ai_reviews(persona, perfumes)
    except AIUnavailable:
        return _ai_unavailable_response()
    return jsonify({'perfumes': perfumes, 'reviews': reviews})

@app.route('/api/new-phone', methods=['POST'])
def new_phone():
    return jsonify({'phone': _phone()})


@app.route('/api/git-push', methods=['POST'])
def git_push():
    """رفع يدوي محكوم؛ معطّل افتراضياً ولا يعمل بلا رمز إدارة."""
    denied = _admin_guard()
    if denied:
        return denied
    if os.environ.get('ENABLE_GIT_PUSH', '').strip().lower() not in {'1', 'true', 'yes'}:
        return jsonify({'ok': False, 'error': 'git_push_disabled'}), 403
    import subprocess
    try:
        cwd = os.environ.get('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
        subprocess.run(['git', 'add', '.'], cwd=cwd, check=True, capture_output=True, timeout=30)
        subprocess.run(['git', 'commit', '-m', 'auto update'], cwd=cwd, capture_output=True, timeout=30)
        result = subprocess.run(['git', 'push'], cwd=cwd, check=True, capture_output=True, timeout=60)
        return jsonify({'ok': True, 'msg': 'تم الرفع بنجاح'})
    except subprocess.CalledProcessError as e:
        return jsonify({'ok': False, 'error': e.stderr.decode('utf-8', errors='replace')[:200]}), 500
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)[:200]}), 500

@app.route('/api/archive', methods=['GET'])
def archive_stats():
    arc = _load_archive()
    return jsonify({
        'total_reviews': len(arc.get('reviews',[])),
        'last_10': [{'text':r['text'],'product':r.get('product',''),'persona':r.get('persona','')}
                    for r in arc.get('reviews',[])[-10:]]
    })

@app.route('/api/archive/stats', methods=['GET'])
def archive_stats_detail():
    """إحصائيات الأرشيف التفصيلية (العدد الحالي + الحد الأقصى)"""
    if USE_ANTI_REPEAT:
        try:
            return jsonify(ar_get_archive_stats())
        except Exception as e:
            print(f'⚠️ archive stats error: {e}')
    arc = _load_archive()
    return jsonify({'total': len(arc.get('reviews', [])), 'max': 200})

@app.route('/api/archive/clear', methods=['POST'])
def archive_clear():
    denied = _admin_guard()
    if denied:
        return denied
    if USE_ANTI_REPEAT:
        ar_clear_archive()
    else:
        _save_archive({'reviews':[], 'store_reviews':[], 'personas':[]})
    return jsonify({'status':'ok','message':'تم مسح الأرشيف'})

# ═══════════════════════════════════════════════════════════
# API استخبارات محلي (Algolia Intelligence)
# ═══════════════════════════════════════════════════════════

@app.route('/api/intel/our-products', methods=['GET'])
def api_intel_products():
    """جلب منتجاتنا من Algolia"""
    if not USE_INTEL:
        return jsonify({'error': 'mahalli_intel not available'}), 503
    products = intel_get_our_products()
    return jsonify({'products': products, 'count': len(products)})

@app.route('/api/intel/competitors/<path:product_name>', methods=['GET'])
def api_intel_competitors(product_name):
    """جلب المنافسين لمنتج"""
    if not USE_INTEL:
        return jsonify({'error': 'mahalli_intel not available'}), 503
    competitors = intel_get_competitors(product_name)
    return jsonify({'competitors': competitors, 'count': len(competitors)})

@app.route('/api/intel/our-rank/<path:query>', methods=['GET'])
def api_intel_rank(query):
    """ترتيبنا لكلمة بحث"""
    if not USE_INTEL:
        return jsonify({'error': 'mahalli_intel not available'}), 503
    rank, product = intel_get_our_rank(query)
    return jsonify({'rank': rank, 'product': product, 'query': query})

@app.route('/api/intel/priorities', methods=['GET'])
def api_intel_priorities():
    """قائمة الأولويات"""
    if not USE_INTEL:
        return jsonify({'error': 'mahalli_intel not available'}), 503
    priorities = intel_get_priorities()
    return jsonify({'priorities': priorities})

@app.route('/api/intel/daily-plan', methods=['GET'])
def api_intel_daily():
    """خطة اليوم"""
    if not USE_INTEL:
        return jsonify({'error': 'mahalli_intel not available'}), 503
    plan = intel_daily_plan()
    return jsonify(plan)

@app.route('/api/intel/dashboard', methods=['GET'])
def api_intel_dashboard():
    """ملخص لوحة التحكم"""
    if not USE_INTEL:
        return jsonify({'error': 'mahalli_intel not available'}), 503
    summary = intel_dashboard()
    return jsonify(summary)

@app.route('/api/intel/refresh', methods=['POST'])
def api_intel_refresh():
    """تحديث البيانات من Algolia"""
    denied = _admin_guard()
    if denied:
        return denied
    if not USE_INTEL:
        return jsonify({'error': 'mahalli_intel not available'}), 503
    result = intel_refresh()
    return jsonify(result)

# ═══════════════════════════════════════════════════════════
# SSE — مؤشر مباشر لعمل الـ API (Server-Sent Events)
# ═══════════════════════════════════════════════════════════

from flask import Response, stream_with_context

@app.route('/api/generate-stream', methods=['POST'])
@rate_limit(RATE_LIMIT_GENERATE)
def api_generate_stream():
    """توليد لحظي حقيقي: يختار العطور → يفكّر → يكتب أمام المستخدم خطوة بخطوة (SSE).

    خلافاً للنسخة القديمة (تولّد كل شيء ثم تعيد تمثيل الخطوات)، هنا كل حدث
    يُبثّ فور حدوثه فعلاً: استدعاء AI لكل عطر يتم داخل الحلقة ويُعرض ناتجه مباشرة.
    """
    def _sse(evt):
        return f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"

    def generate():
        try:
            # ── 1) اختيار الشخصية ──
            yield _sse({'step': 'persona', 'msg': 'يختار الشخصية المناسبة...', 'icon': '👤'})
            if USE_NEW_PERSONAS:
                persona = generate_persona()
                arch = next((a for a in ARCHETYPES if a['id'] == persona['archId']),
                            random.choice(ARCHETYPES))
            else:
                arch = random.choice(ARCHETYPES)
                city = _city()
                persona = {
                    'name': _name(arch['g']), 'city': city, 'gender': arch['g'],
                    'age': random.randint(*arch['age']), 'label': arch['label'],
                    'emoji': arch['emoji'], 'archId': arch['id'],
                    'address': _national_address(city), 'phone': _phone(),
                }
            yield _sse({'step': 'persona_done', 'icon': '✅',
                        'msg': f"الشخصية: {persona['name']} — {persona['label']} — {persona['city']}",
                        'persona': persona})

            # ── 2) اختيار العطور (منظّم: موزون بالترند وتفضيلات الشخصية ونطاق سعرها) ──
            yield _sse({'step': 'products', 'icon': '📦',
                        'msg': 'يختار عطوراً تناسب الشخصية (ترجيح ذكي لا عشوائي)...'})
            perfumes = _pick_products(arch)
            yield _sse({'step': 'products_done', 'icon': '✅',
                        'msg': f'اختار {len(perfumes)} عطور',
                        'perfumes': [{'name': p['name'], 'price': p['price'],
                                      'brand': p.get('brand', '')} for p in perfumes]})

            # ── 3) لكل عطر: يفكّر (نمط + توجيه استراتيجي) ثم يكتب لحظياً ──
            used_block = _used_texts_block(limit=30)
            reviews = []
            for i, pf in enumerate(perfumes, 1):
                # (أ) التفكير — يُبنى البرومبت ويُحدَّد النمط قبل الكتابة
                prompt, params = _plan_review(persona, pf, perfumes, used_block)
                pattern = params.get('pattern', '')
                directive = get_ai_directive(pattern) if USE_PATTERNS else ''
                desc = get_pattern_description(pattern) if USE_PATTERNS else ''
                yield _sse({'step': 'thinking', 'index': i, 'icon': '🧠',
                            'product': pf['name'],
                            'msg': f"يفكّر في «{pf['name']}» — النمط: {desc or pattern}",
                            'pattern': pattern, 'directive': directive[:160]})

                # (ب) الكتابة — استدعاء AI فعلي الآن وعرض الناتج مباشرة
                rv = _write_review(persona, pf, prompt, params)
                reviews.append(rv)
                yield _sse({'step': 'review', 'index': i, 'icon': '✍️',
                            'product': pf['name'],
                            'msg': f"كتب تقييم «{pf['name']}»",
                            'text': rv.get('text', ''), 'rating': rv.get('rating', 5),
                            'pattern': rv.get('pattern', pattern)})

            # حفظ الدفعة في الأرشيف (مرة واحدة بعد اكتمالها)
            _archive_batch(reviews, persona.get('name', ''))

            # ── 4) تقييم المتجر (متغيّر وغير مكرر) ──
            yield _sse({'step': 'store', 'icon': '🏪', 'msg': 'يكتب تقييماً عاماً للمتجر...'})
            store = _ai_store_review(persona)
            yield _sse({'step': 'store_done', 'icon': '✅', 'msg': 'كتب تقييم المتجر',
                        'text': store.get('text', ''), 'rating': store.get('rating', 5)})

            # ── 5) اكتمل — البيانات الكاملة للواجهة ──
            data = {'persona': persona, 'perfumes': perfumes,
                    'reviews': reviews, 'store': store}
            yield _sse({'step': 'done', 'icon': '🎉', 'msg': 'اكتمل التوليد!', 'data': data})

        except AIUnavailable:
            # تعذّر الـ AI (انقطاع/نفاد رصيد/مفتاح) — نتوقف ونكشف السبب الحقيقي
            _resp = _ai_unavailable_response()[0].get_json()
            yield _sse({'step': 'error', 'icon': '🛑', 'fatal': True,
                        'msg': _resp.get('message', 'تعذّر الاتصال بالذكاء الاصطناعي.'),
                        'ai_status': _resp.get('ai_status')})
        except Exception as e:
            yield _sse({'step': 'error', 'msg': str(e), 'icon': '❌'})

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
    )


# ═══════════════════════════════════════════════════════════
# Thread Generation — توليد محادثات (تقييم + ردود)
# ═══════════════════════════════════════════════════════════

@app.route('/api/generate-thread', methods=['POST'])
@rate_limit(RATE_LIMIT_GENERATE)
def api_generate_thread():
    """توليد محادثة: تقييم رئيسي + ردود من عملاء آخرين (الـ AI فقط)"""
    if not USE_THREADS:
        return jsonify({'error': 'thread_generator not available'}), 503

    body = request.get_json(silent=True) or {}
    product_name = body.get('product_name', '')
    main_review = body.get('main_review', '')

    if not product_name or not main_review:
        return jsonify({'error': 'product_name and main_review required'}), 400

    reply_count = _safe_int(body.get('reply_count', 2), 2, 1, 4)

    # بناء بيانات المحادثة (برومبتات فقط)
    thread_data = generate_thread_data(product_name, main_review, reply_count)

    # استدعاء الـ AI لكل رد — فريد لا يتكرر. عند تعذّر الـ AI نتوقف ولا نكتب قوالب.
    thread_replies = []
    for reply_info in thread_data['replies']:
        # التنظيف داخل بوابة التفرّد لا بعدها (نفس قاعدة مسار التقييم)
        text = _ai_unique_text(reply_info['prompt'], max_tokens=150,
                               finalize=_humanize, attempts=4, parser=parse_ai_reply)
        if not text:
            return _ai_unavailable_response()
        _register(text)
        thread_replies.append({
            'name': reply_info['persona'].get('name', 'عميل'),
            'city': reply_info['persona'].get('city', ''),
            'text': text,
            'type': reply_info['type'],
            'is_answer': reply_info.get('is_answer', False),
        })

    return jsonify({
        'thread_type': thread_data['thread_type'],
        'main_review': main_review,
        'product_name': product_name,
        'replies': thread_replies,
    })


if __name__ == '__main__':
    arc = _load_archive()
    print('='*50)
    print('Perfume Assistant V2 — AI Mode (Railway)')
    print(f'   DATA_DIR: {DATA_DIR}')
    print(f'   Products: {len(PRODUCTS)}')
    print(f'   Archive: {len(arc.get("reviews",[]))} reviews (max 200 FIFO)')
    print(f'   Engines: P={USE_NEW_PERSONAS} D={USE_DIALECTS} R={USE_PATTERNS}')
    print(f'            A={USE_ANTI_REPEAT} T={USE_TRENDING} I={USE_INTEL}')
    print(f'            Threads={USE_THREADS}')
    if USE_PATTERNS:
        print(f'   Patterns: {len(REVIEW_PATTERNS)} in {len(PATTERN_CATEGORIES)} categories')
    print(f'   AI: {AI_MODEL}')
    print(f'   AI_KEY: {"configured" if AI_KEY else "MISSING!"}')
    print('='*50)
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

