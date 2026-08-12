# -*- coding: utf-8 -*-
"""
🌸 مساعدي في شراء العطور — واجهة Streamlit الرئيسية
تطبيق ذكي لمحاكاة شخصيات بحثية وتحليل تجربة شراء العطور بالذكاء الاصطناعي
مع لوحة استخبارات محلي وتصدير CSV
"""
import streamlit as st
import sys, json, random, time, os, csv, io, re
import requests as http_req
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from empathy_engine import SYNTHETIC_DISCLOSURE_AR, mark_synthetic_output

# ضمان ترميز UTF-8 للطباعة (يمنع تعطّل الإيموجي على كونسول Windows cp1256)
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

# ═══════════════════════════════════════════════════════════
#  إعدادات الصفحة (يجب أن تكون أول أمر Streamlit)
# ═══════════════════════════════════════════════════════════
st.set_page_config(
    page_title="🌸 مساعدي في شراء العطور",
    layout="wide",
    initial_sidebar_state="collapsed",
)

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / '.env', override=False)
# مسار ثابت للبيانات — Railway يضبطه على /data ليبقى بين عمليات النشر
DATA_DIR = Path(os.environ.get('DATA_DIR', str(BASE_DIR)))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════
#  استيراد المحركات الجديدة مع try/except
# ═══════════════════════════════════════════════════════════
USE_NEW_PERSONAS = False
USE_DIALECTS = False
USE_REVIEW_PATTERNS = False
USE_ANTI_REPEAT = False
USE_TRENDING = False
USE_MAHALLI = False

try:
    from personas_engine import (
        generate_persona, generate_review_params, build_master_prompt,
        build_context_hints, ARCHETYPES, CITY_DATA as PE_CITY_DATA,
    )
    USE_NEW_PERSONAS = True
except ImportError:
    def build_context_hints(persona, product): return ''

try:
    from dialects import get_dialect_for_city, get_dialect_data, get_dialect_examples, apply_typos
    USE_DIALECTS = True
except ImportError:
    pass

try:
    from review_patterns import pick_pattern, pick_rating, get_pattern_description, REVIEW_PATTERNS
    USE_REVIEW_PATTERNS = True
except ImportError:
    pass

try:
    from anti_repeat import (get_used_texts as ar_get_used_texts, archive_review, MAX_ARCHIVE,
                             is_duplicate as ar_is_duplicate, register_text as ar_register_text,
                             format_used_texts_block as ar_format_used)
    USE_ANTI_REPEAT = True
except ImportError:
    MAX_ARCHIVE = 200
    def ar_is_duplicate(t, threshold=0.6): return False
    def ar_register_text(t): pass
    def ar_format_used(limit=30): return ''

try:
    from trending import get_trending_brands, get_weight_for_product, blend_selection
    USE_TRENDING = True
except ImportError:
    pass

# الحارس الدلالي — نزيف/بتر/تجاوز طول (كان مربوطاً في Flask فقط — سُدّت الفجوة)
USE_SEMANTIC_GUARD = False
try:
    from semantic_guard import guard_violations, strip_broken_tail
    USE_SEMANTIC_GUARD = True
except ImportError:
    pass

USE_HUMANIZER = False
try:
    from humanizer import humanize_output as hz_humanize_output, content_tells as hz_content_tells
    USE_HUMANIZER = True
except ImportError:
    pass

USE_DEMO_MATCH = False
try:
    import demographic_matcher as _demo   # مطابقة ديموغرافية (علم تحكّم)
    USE_DEMO_MATCH = True
except ImportError:
    pass

try:
    from mahalli_intel import (
        get_our_products, get_competitors, get_our_rank, get_priorities,
        generate_daily_plan, get_dashboard_summary, refresh_all_data,
        is_cache_stale, get_cache_age, TOP_SEARCHES, SAFETY_RULES,
    )
    USE_MAHALLI = True
except ImportError:
    pass

# ═══════════════════════════════════════════════════════════
#  تحميل البيانات الأساسية
# ═══════════════════════════════════════════════════════════

@st.cache_data
def load_names():
    """تحميل ملف الأسماء"""
    try:
        with open(BASE_DIR / 'names.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {'male': ['محمد'], 'female': ['نورة'], 'family_names': ['السعودي'],
                'cities': [{'name': 'الرياض', 'weight': 1}]}

@st.cache_data
def load_catalog():
    """تحميل كتالوج المنتجات"""
    try:
        with open(BASE_DIR / 'catalog.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

NAMES = load_names()
PRODUCTS = load_catalog()

# ═══════════════════════════════════════════════════════════
#  مفتاح AI — من البيئة أو .env
# ═══════════════════════════════════════════════════════════
AI_KEY = (os.environ.get('AI_KEY')
          or os.environ.get('OPENROUTER_API_KEY')
          or os.environ.get('OPENROUTER_KEY')
          or os.environ.get('OPENROUTER_API')
          or '').strip()
AI_URL = os.environ.get('AI_URL', 'https://openrouter.ai/api/v1/chat/completions').strip()
AI_MODEL = os.environ.get('AI_MODEL', 'google/gemini-2.5-flash').strip()

# ═══════════════════════════════════════════════════════════
#  دوال AI
# ═══════════════════════════════════════════════════════════

def ai_call(prompt, max_tokens=1200, temperature=1.0):
    """استدعاء API الذكاء الاصطناعي عبر OpenRouter"""
    if not AI_KEY:
        return None
    try:
        r = http_req.post(AI_URL, headers={
            'Authorization': f'Bearer {AI_KEY}',
            'Content-Type': 'application/json',
        }, json={
            'model': AI_MODEL,
            'messages': [{'role': 'user', 'content': prompt}],
            'max_tokens': max_tokens,
            'temperature': temperature,
        }, timeout=60)
        data = r.json()
        if r.status_code != 200:
            return None
        return data['choices'][0]['message']['content'].strip()
    except Exception:
        return None

def clean_json(result):
    """تنظيف JSON من Markdown fencing"""
    result = result.strip()
    if result.startswith('```'):
        result = result.split('\n', 1)[1] if '\n' in result else result[3:]
        if result.endswith('```'):
            result = result[:-3]
    return result.strip()


def _extract_json(result):
    """استخراج كائن JSON بأمان حتى لو غُلّف بنص/Markdown (يطابق app.extract_json)."""
    if not result:
        return None
    try:
        cleaned = clean_json(result)
        m = re.search(r'\{.*\}', cleaned, re.DOTALL)
        return json.loads(m.group(0) if m else cleaned)
    except Exception:
        return None


# تنظيف بشري + مسار النص النهائي — مصدر واحد مشترك مع app.py (review_text)
from review_text import humanize as _humanize, finalize_review_text, write_unique


def ai_write_unique(prompt, max_tokens, finalize, attempts=5, is_store=False,
                    base_temp=0.9, temp_step=0.06):
    """يكتب عبر AI مع بوابة تفرّد تفحص **النص النهائي** (بعد الأنسنة والقصّ).

    يرجع (rv_dict, final_text)؛ (None, '') عند تعذّر الـAI كلياً.

    finalize إلزامي عمداً: تمريره اختيارياً كان يعيد الخلل الأصلي صامتاً —
    فحص نص وتخزين نص آخر.
    """
    return write_unique(
        call=lambda p, mt, temp: ai_call(p, max_tokens=mt, temperature=temp),
        parse=_extract_json,
        finalize=finalize,
        is_dup=(lambda t: bool(USE_ANTI_REPEAT and ar_is_duplicate(t, is_store_review=is_store))),
        prompt=prompt, max_tokens=max_tokens, attempts=attempts,
        base_temp=base_temp, temp_step=temp_step,
    )

# ═══════════════════════════════════════════════════════════
#  أرشيف التقييمات
# ═══════════════════════════════════════════════════════════
ARCHIVE_FILE = DATA_DIR / 'archive.json'

def load_archive():
    """تحميل أرشيف التقييمات"""
    if ARCHIVE_FILE.exists():
        try:
            with open(ARCHIVE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {'reviews': []}
    return {'reviews': []}

def save_archive(arc):
    """حفظ الأرشيف مع حد أقصى"""
    if len(arc.get('reviews', [])) > MAX_ARCHIVE:
        arc['reviews'] = arc['reviews'][-MAX_ARCHIVE:]
    with open(ARCHIVE_FILE, 'w', encoding='utf-8') as f:
        json.dump(arc, f, ensure_ascii=False, indent=1)

def get_used_texts(limit=40):
    """جلب آخر النصوص المستخدمة لمنع التكرار"""
    if USE_ANTI_REPEAT:
        return ar_get_used_texts(limit)
    arc = load_archive()
    texts = [r.get('text', '') for r in arc.get('reviews', [])]
    return texts[-limit:]

def archive_batch(reviews, persona_name):
    """أرشفة دفعة من التقييمات"""
    arc = load_archive()
    for rv in reviews:
        entry = {
            'text': rv.get('text', ''),
            'product': rv.get('product', ''),
            'persona': persona_name,
            'rating': rv.get('rating', 5),
            'ts': int(time.time()),
        }
        arc['reviews'].append(entry)
    save_archive(arc)

def suggest_review_times(count):
    """توزيع التقييمات على ساعات الذروة بشكل طبيعي (HH:MM مرتبة)."""
    peak_hours = [9, 10, 11, 14, 15, 16, 20, 21, 22, 23]
    times = []
    for _ in range(max(0, int(count))):
        hour = random.choice(peak_hours)
        minute = random.randint(0, 59)
        times.append(f'{hour:02d}:{minute:02d}')
    return sorted(times)

# ═══════════════════════════════════════════════════════════
#  Fallback: شخصيات قديمة (عند عدم وجود personas_engine)
# ═══════════════════════════════════════════════════════════

FALLBACK_ARCHETYPES = [
    {'id': 'شاب_جامعي', 'g': 'male', 'label': 'شاب جامعي', 'emoji': '🎓',
     'age': (18, 23), 'price': (0, 300), 'prefers': ['رجالي', 'مشترك'], 'count': (2, 4)},
    {'id': 'رجل_أعمال', 'g': 'male', 'label': 'رجل أعمال', 'emoji': '👔',
     'age': (30, 50), 'price': (400, 2000), 'prefers': ['رجالي', 'مشترك'], 'count': (3, 6)},
    {'id': 'بنت_عصرية', 'g': 'female', 'label': 'بنت عصرية', 'emoji': '💅',
     'age': (20, 28), 'price': (100, 600), 'prefers': ['نسائي', 'مشترك'], 'count': (3, 6)},
    {'id': 'أم_سعودية', 'g': 'female', 'label': 'أم سعودية', 'emoji': '👩‍🦱',
     'age': (38, 55), 'price': (100, 800), 'prefers': ['نسائي', 'مشترك'], 'count': (2, 5)},
    {'id': 'عروس', 'g': 'female', 'label': 'عروس', 'emoji': '👰',
     'age': (21, 30), 'price': (300, 2000), 'prefers': ['نسائي', 'مشترك'], 'count': (4, 8)},
]

FALLBACK_CITY_DATA = {
    'الرياض': {'postal': (11411, 13999),
               'dists': [('العليا', 'شارع العليا'), ('النخيل', 'شارع الأمير محمد')]},
    'جدة': {'postal': (21411, 23999),
             'dists': [('الحمراء', 'شارع فلسطين'), ('الروضة', 'شارع الأمير سلطان')]},
}
PREFIXES = ['050', '053', '054', '055', '056', '057', '058', '059']

TRENDING_BRANDS = [
    'ديور', 'شانيل', 'توم فورد', 'لطافة', 'أرماني', 'بربري',
    'كارولينا', 'لانكوم', 'فرساتشي', 'جوتشي', 'أمواج', 'كريد',
]
TREND_LOW = [b.lower() for b in TRENDING_BRANDS]


def _fallback_pick_city():
    """اختيار مدينة (fallback)"""
    cities = NAMES.get('cities', [{'name': 'الرياض', 'weight': 1}])
    return random.choices(cities, weights=[c.get('weight', 1) for c in cities], k=1)[0]['name']


def _fallback_make_address(city):
    """توليد عنوان (fallback)"""
    cd = FALLBACK_CITY_DATA.get(city, {'postal': (11411, 13999),
                                        'dists': [('المركز', 'شارع العام')]})
    dist, street = random.choice(cd['dists'])
    return f'{random.randint(1000, 9999)} {street}، حي {dist}، {city} {random.randint(*cd["postal"])}'


def _fallback_gen_persona():
    """توليد شخصية بالنظام القديم"""
    arch = random.choice(FALLBACK_ARCHETYPES)
    age = random.randint(*arch['age'])
    city = _fallback_pick_city()
    key = 'male' if arch['g'] == 'male' else 'female'
    name = random.choice(NAMES.get(key, ['مستخدم'])) + ' ' + \
           random.choice(NAMES.get('family_names', ['السعودي']))
    phone = random.choice(PREFIXES) + ''.join([str(random.randint(0, 9)) for _ in range(7)])
    address = _fallback_make_address(city)
    pool = [p for p in PRODUCTS if p['g'] in arch['prefers']
            and arch['price'][0] <= p['price'] <= arch['price'][1]]
    if len(pool) < 3:
        pool = [p for p in PRODUCTS if p['g'] in arch['prefers']]
    if len(pool) < 3:
        pool = PRODUCTS[:]
    if USE_DEMO_MATCH:
        pool = _demo.filter_pool(arch, pool)   # مطابقة ديموغرافية قبل الخلطة
    count = random.randint(*arch['count'])
    # خلطة "محلي" الذكية: كربتك (أولوية) + أفضل 100 ترند + تمويه
    if USE_TRENDING:
        sel = blend_selection(PRODUCTS, pool, count, arch['prefers'])
    else:
        weights = []
        for p in pool:
            nm = (p.get('brand', '') + p.get('name', '')).lower()
            weights.append(3.0 if any(t in nm for t in TREND_LOW) else 1.0)
        count = min(count, len(pool))
        sel, pc, wc = [], list(pool), list(weights)
        for _ in range(count):
            if not pc:
                break
            idx = random.choices(range(len(pc)), weights=wc, k=1)[0]
            sel.append(pc.pop(idx))
            wc.pop(idx)
    persona = {
        'name': name, 'city': city, 'gender': arch['g'], 'age': age,
        'label': arch['label'], 'emoji': arch['emoji'], 'archId': arch['id'],
        'address': address, 'phone': phone,
        # أبعاد فارغة (fallback)
        'mood': 'عادي', 'expertise': 'متوسط', 'writing_style': 'عادي',
        'dialect': 'najdi', 'dialect_name': 'نجدية',
        'mention_product': True, 'use_emoji': False, 'has_typo': False,
    }
    return persona, sel


def gen_persona_full():
    """توليد شخصية كاملة — محرك جديد أو قديم"""
    if USE_NEW_PERSONAS:
        persona = generate_persona()
        # اختيار المنتجات بناءً على archetype
        arch_match = None
        for a in ARCHETYPES:
            if a['id'] == persona.get('archId'):
                arch_match = a
                break
        if arch_match is None:
            arch_match = random.choice(ARCHETYPES)
        pool = [p for p in PRODUCTS if p['g'] in arch_match['prefers']
                and arch_match['price'][0] <= p['price'] <= arch_match['price'][1]]
        if len(pool) < 3:
            pool = [p for p in PRODUCTS if p['g'] in arch_match['prefers']]
        if len(pool) < 3:
            pool = PRODUCTS[:]
        if USE_DEMO_MATCH:
            pool = _demo.filter_pool(arch_match, pool)   # مطابقة ديموغرافية قبل الخلطة
        count = random.randint(*arch_match['count'])
        # خلطة "محلي" الذكية: كربتك (أولوية) + أفضل 100 ترند + تمويه — مختلفة كل مرة
        if USE_TRENDING:
            sel = blend_selection(PRODUCTS, pool, count, arch_match['prefers'])
        else:
            weights = []
            for p in pool:
                nm = (p.get('brand', '') + p.get('name', '')).lower()
                weights.append(3.0 if any(t in nm for t in TREND_LOW) else 1.0)
            count = min(count, len(pool))
            sel, pc, wc = [], list(pool), list(weights)
            for _ in range(count):
                if not pc:
                    break
                idx = random.choices(range(len(pc)), weights=wc, k=1)[0]
                sel.append(pc.pop(idx))
                wc.pop(idx)
        # تحويل العنوان لنص إذا كان dict
        addr = persona.get('address', '')
        if isinstance(addr, dict):
            addr = addr.get('full', str(addr))
        persona['address'] = addr
        return persona, sel
    else:
        return _fallback_gen_persona()


# ═══════════════════════════════════════════════════════════
#  توليد التقييمات بالذكاء الاصطناعي
# ═══════════════════════════════════════════════════════════

def _make_review_finalizer(persona, allow_words):
    """يبني دالة النص النهائي لهذه الشخصية — تُستخدم داخل بوابة التفرّد وخارجها.

    أخطاء الشخصية الإملائية جزء من صوتها (has_typo) وتُطبَّق **داخل** المسار
    النهائي لا بعده؛ تطبيقها بعد الفحص كان يغيّر النص المخزَّن عمّا فُحص.
    ليست أداة تفرّد ولا تُفتعَل لكسر التكرار.
    """
    typo_fn = None
    if USE_DIALECTS and persona.get('has_typo'):
        typo_fn = lambda t: apply_typos(t, 1.0)
    return lambda t: finalize_review_text(
        t,
        allow_words=allow_words,
        humanizer=(lambda x: hz_humanize_output(x, kind='review')) if USE_HUMANIZER else None,
        strip_tail=strip_broken_tail if USE_SEMANTIC_GUARD else None,
        typo_fn=typo_fn,
    )


def gen_reviews(persona, perfumes):
    """توليد تقييمات المنتجات — مُحصّن: تنظيف + منع تكرار + إطار هدية + بلا «ممتاز» وهمية."""
    pname = persona.get('name')
    if USE_ANTI_REPEAT:
        ub = ar_format_used(30, persona_name=pname)
    else:
        used = get_used_texts() or []
        ub = '\n'.join([f'- {t}' for t in used[-30:]])

    if USE_NEW_PERSONAS:
        # === برومبت متقدم لكل منتج (بارتي كامل مع app.py) ===
        all_reviews = []
        for pf in perfumes:
            review_params = generate_review_params(persona)
            # تنبيهات الجنس/الهدية/نوع المنتج — نفس منطق Flask (مكياج/معطر شعر/هدية...)
            extra = build_context_hints(persona, pf)
            # اسم نظيف (بلا لاحقة سعر) ليعمل بحث الكتالوج وبيانات المنتج بدقة
            # build_master_prompt يرجع (prompt, review_params) — عدم فكّها كان يمرر tuple لـ
            # ai_write_unique فينفجر "can only concatenate tuple (not str) to tuple" عند أي محاولة توليد.
            prompt, review_params = build_master_prompt(
                persona=persona,
                product_name=pf['name'],
                review_params=review_params,
                used_texts_block=ub,
                extra_block=extra,
                product=pf,   # بيانات الكتالوج الحقيقية (ماركة/عائلة/مكوّنات)
            )
            # سقف توكنز وحدّ كلمات من الطول المعاين من بيانات المنافسين (مطابق Flask)
            _tgt = review_params.get('len_target') or 4
            _allow = _tgt + (1 if _tgt <= 4 else max(2, _tgt // 5))
            mx = 80 + _tgt * 8
            # مسار النص النهائي — نفس ما سيُخزَّن بالضبط، فتفحصه بوابة التفرّد
            # على حقيقته لا على المخرج الخام (القصّ كان يُوحّد مخرجات مختلفة).
            _finalize = _make_review_finalizer(persona, _allow)
            rv, txt = ai_write_unique(prompt, max_tokens=mx, finalize=_finalize)
            if not txt.strip():
                # قانون 4: لا نص وهمي — نتوقف ونُظهر خطأ بدل الفبركة.
                st.error('تعذّر الاتصال بالذكاء الاصطناعي أو نفد الرصيد — لم تتم كتابة أي تقييم.')
                st.stop()
            # ══ الحارس الدلالي: نزيف/بتر/تجاوز → إعادة توليد (النتيجة تمرّ بنفس البوابة) ══
            if USE_SEMANTIC_GUARD:
                _viol = guard_violations(txt, max_words=_allow)
                if USE_HUMANIZER:
                    _viol = _viol + hz_content_tells(txt)
                if _viol:
                    _hint = (f'\n\n⚠️ رُفض النص السابق «{txt}» ({"، ".join(_viol)}). '
                             f'اكتب تقييماً جديداً عن العطر نفسه فقط، جملة مكتملة المعنى، {_tgt} كلمات كحد أقصى.')
                    _rv2, _t2 = ai_write_unique(prompt + _hint, max_tokens=mx,
                                                finalize=_finalize, attempts=2)
                    if _t2.strip():
                        rv, txt = (_rv2 if isinstance(_rv2, dict) else rv), _t2
            entry = rv if isinstance(rv, dict) else {}
            entry.update({
                'product': pf['name'], 'brand': pf['brand'], 'price': pf['price'],
                'rating': entry.get('rating', 5), 'text': txt,
                'pattern': review_params.get('pattern', ''),
            })
            entry = mark_synthetic_output(entry)
            if USE_ANTI_REPEAT:
                ar_register_text(txt, pname)
            all_reviews.append(entry)
        archive_batch(all_reviews, pname or '')
        return all_reviews
    else:
        # المسار الدفعي القديم أُغلق (توحيد المنظومة): بلا personas_engine
        # لا توليد — نفس فلسفة قانون 4، لا مخرج متدنٍّ يعمل بصمت.
        st.error('محرك الشخصيات personas_engine غير محمّل — لا يمكن توليد تقييمات بدونه.')
        st.stop()


# تقييم المتجر — يستورد المنطق المرجعي من الوحدة المشتركة store_review
# (نفس بيانات وبرومبت وحُرّاس app.py — مصدر واحد، فلا يتباعد المساران)
from store_review import (
    STORE_ASPECTS, STORE_OPENERS, ASPECT_TOPIC,
    sample_length_target, band_for, build_store_prompt,
    strip_store_vocatives, has_luxury_metaphor, scrub_luxury_metaphor,
    StoreTopicTracker,
)

# أسماء معروفة لحذف نداء الاسم
_STORE_NAMES = set(NAMES.get('male', []) + NAMES.get('female', []) + NAMES.get('family_names', []))

# عدّاد مواضيع المتجر (سقف 20%) — مثبّت في session_state ليتراكم السقف عبر دفعات
# الجلسة لا داخل الدفعة الواحدة فقط (module-level كان يُعاد إنشاؤه كل rerun).
_store_topics_fallback = None


def _get_store_topics():
    """يُرجع عدّاد مواضيع المتجر مثبّتًا في session_state (تراكم السقف عبر الدفعات).
    تدرّج آمن لمثيل وحيد خارج سياق Streamlit (استيراد/اختبار)."""
    global _store_topics_fallback
    try:
        if 'store_topics' not in st.session_state:
            st.session_state['store_topics'] = StoreTopicTracker()
        return st.session_state['store_topics']
    except Exception:
        if _store_topics_fallback is None:
            _store_topics_fallback = StoreTopicTracker()
        return _store_topics_fallback


def gen_store_review(persona):
    """تقييم متجر متنوّع وغير مكرر — بالمنطق المرجعي الموحّد من store_review:
    طول مُعايَن (35/40/25)، برومبت متكيّف مع النطاق، سقف موضوعي 20%، وحظر
    استعارات الفخامة وحذف نداء الاسم. نفس مسار app.py حرفيًّا كي يتناغم الطرفان."""
    pname = persona.get('name')
    _store_topics = _get_store_topics()  # مثبّت في session_state — يتراكم عبر الدفعات
    # (1) طول مُعايَن قبل البرومبت  (2) جوانب بعيدًا عن المواضيع المشبعة
    lo, hi, length_desc = sample_length_target()
    band = band_for(hi)
    blocked = _store_topics.blocked()
    pool = [a for a in STORE_ASPECTS if ASPECT_TOPIC.get(a) not in blocked]
    if len(pool) < 2:
        pool = list(STORE_ASPECTS)
    aspects = random.sample(pool, k=2)
    opener = random.choice(STORE_OPENERS)
    ub = ar_format_used(15, persona_name=pname) if USE_ANTI_REPEAT else ''
    avoid_line = (f"\n- ممنوع الحديث عن: {'، '.join(sorted(blocked))} (تكرّرت كثيرًا)" if blocked else '')
    prompt = build_store_prompt(persona, band, aspects, opener, ub, avoid_line)

    def _finalize(text):
        """النص النهائي لتقييم المتجر — يشمل سقف الطول كي تفحص البوابة المخزَّن فعلاً."""
        if USE_HUMANIZER:
            text = hz_humanize_output(text or '', kind='store')  # إزالة إطار المساعد/الماركداون
        text = _humanize(text or '')  # بلا ترقيم أو رموز
        text = re.sub(r'\s+', ' ', re.sub(r'[0-9٠-٩]+', ' ', text)).strip()  # صفر أرقام
        text = strip_store_vocatives(text, pname, _STORE_NAMES)  # منع النداء حتميًّا
        words = text.split()
        if len(words) > hi:
            words = words[:hi]
            if USE_SEMANTIC_GUARD:
                words = strip_broken_tail(words)
        return ' '.join(words)

    rv, txt = ai_write_unique(prompt, max_tokens=200, finalize=_finalize, is_store=True)

    # (3) حارس موحّد: استعارة فخامة / موضوع مشبع / تجاوز الطول → إعادة توليد موجَّهة
    for _k in range(3):
        problems = []
        if has_luxury_metaphor(txt):
            problems.append('استعارة فخامة (كنز/صندوق/مجوهرات/ذهب) ممنوعة')
        blocked_now = _store_topics.blocked()
        if _store_topics.classify(txt) in blocked_now:
            fresh = [a for a in STORE_ASPECTS if ASPECT_TOPIC.get(a) not in blocked_now]
            alt = random.choice(fresh or STORE_ASPECTS)
            problems.append(f"موضوع «{_store_topics.classify(txt)}» تكرّر كثيرًا؛ اكتب عن {alt} بدلًا منه ولا تذكره")
        if len(txt.split()) > hi + 2:
            problems.append(f"أطل من المطلوب؛ التزم بـ {length_desc}")
        if USE_HUMANIZER:
            for _tl in hz_content_tells(txt, kind='store'):
                problems.append(f'أنسنة: {_tl}')
        if not problems:
            break
        hint = "\n\n⚠️ أعد الكتابة: " + " · ".join(problems)
        nxt, nxt_txt = ai_write_unique(prompt + hint, max_tokens=200,
                                       finalize=_finalize, is_store=True)
        if not nxt_txt.strip():
            break
        rv, txt = (nxt if isinstance(nxt, dict) else rv), nxt_txt

    # (4) ضمان حتمي أخير: صفر استعارة فخامة (سقف الطول مطبَّق داخل _finalize)
    txt = scrub_luxury_metaphor(txt)

    if not txt.strip():
        # قانون 4: لا نص وهمي — نتوقف ونُظهر خطأ بدل الفبركة.
        st.error('تعذّر الاتصال بالذكاء الاصطناعي أو نفد الرصيد — لم تتم كتابة أي تقييم.')
        st.stop()

    # (5) تسجيل الموضوع + الأرشيف
    _store_topics.record(txt)
    if USE_ANTI_REPEAT:
        ar_register_text(txt, pname)
        try:
            archive_review(txt, 'متجر مهووس', pname or '')
        except Exception:
            pass
    return mark_synthetic_output({
        'rating': rv.get('rating', 5) if isinstance(rv, dict) else 5,
        'text': txt,
    })


# ═══════════════════════════════════════════════════════════
#  CSS — الثيم الداكن مع ذهبي
# ═══════════════════════════════════════════════════════════

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Tajawal:wght@400;500;700;800&display=swap');

/* === أساسيات === */
* { font-family: 'Tajawal', sans-serif !important; }
.main { direction: rtl; text-align: right; }
h1 {
    background: linear-gradient(135deg, #ddb562, #f4e4b0);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    text-align: center;
    font-weight: 800;
    font-size: 2.2rem;
}

/* === التبويبات === */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    direction: rtl;
    justify-content: center;
    border-bottom: 2px solid rgba(221,181,98,0.15);
}
.stTabs [data-baseweb="tab"] {
    font-family: 'Tajawal', sans-serif !important;
    font-size: 16px;
    font-weight: 700;
    color: #9a9080;
    padding: 12px 28px;
    border-radius: 10px 10px 0 0;
    transition: all 0.3s;
}
.stTabs [aria-selected="true"] {
    color: #ddb562 !important;
    background: rgba(221,181,98,0.08);
    border-bottom: 3px solid #ddb562;
}

/* === بطاقة الشخصية === */
.persona-card {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(221,181,98,0.2);
    border-radius: 14px;
    padding: 18px 20px;
    margin: 12px 0;
}
.persona-card h3 {
    color: #ddb562;
    margin: 0 0 8px 0;
    font-size: 1.3rem;
}

/* === صندوق التقييم === */
.review-box {
    background: rgba(221,181,98,0.08);
    border-right: 3px solid #ddb562;
    padding: 10px 14px;
    margin: 6px 0;
    border-radius: 0 8px 8px 0;
}
.stars { color: #ddb562; font-size: 14px; }

/* === وسوم المنتجات === */
.product-tag {
    display: inline-block;
    background: rgba(221,181,98,0.15);
    color: #ddb562;
    padding: 3px 12px;
    border-radius: 20px;
    font-size: 13px;
    margin: 3px;
}

/* === الأبعاد (Badges) === */
.dim-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 12px;
    margin: 2px;
    font-weight: 500;
}
.dim-mood       { background: rgba(147,112,219,0.2); color: #b79ae0; }
.dim-expertise  { background: rgba(70,180,130,0.2);  color: #5ec49e; }
.dim-style      { background: rgba(70,150,220,0.2);  color: #6ab0e8; }
.dim-dialect    { background: rgba(221,181,98,0.2);   color: #ddb562; }
.dim-mention    { background: rgba(220,120,120,0.15); color: #e09090; }
.dim-emoji      { background: rgba(255,200,60,0.15);  color: #f0c830; }
.dim-typo       { background: rgba(180,180,180,0.15); color: #b0b0b0; }

/* === معلومات === */
.info-row { color: #9a9080; font-size: 14px; margin: 4px 0; }

/* === تقييم المتجر === */
.store-review {
    background: rgba(221,181,98,0.12);
    border: 1px solid rgba(221,181,98,0.3);
    border-radius: 10px;
    padding: 12px;
    margin: 8px 0;
}

/* === لوحة الاستخبارات === */
.intel-card {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(221,181,98,0.15);
    border-radius: 12px;
    padding: 16px;
    margin: 8px 0;
}
.intel-metric {
    text-align: center;
    padding: 14px;
    background: rgba(221,181,98,0.06);
    border-radius: 10px;
    border: 1px solid rgba(221,181,98,0.12);
}
.intel-metric .value {
    font-size: 2rem;
    font-weight: 800;
    color: #ddb562;
    display: block;
}
.intel-metric .label {
    font-size: 13px;
    color: #9a9080;
    margin-top: 2px;
}
.priority-row {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(221,181,98,0.1);
    border-radius: 10px;
    padding: 12px 14px;
    margin: 6px 0;
}
.priority-name {
    color: #ddb562;
    font-weight: 700;
    font-size: 15px;
}
.rank-badge {
    display: inline-block;
    background: rgba(221,181,98,0.2);
    color: #ddb562;
    padding: 2px 10px;
    border-radius: 8px;
    font-size: 13px;
    font-weight: 700;
}
.rank-badge.danger { background: rgba(220,80,80,0.2); color: #e07070; }
.rank-badge.success { background: rgba(70,180,130,0.2); color: #5ec49e; }

/* === قواعد السلامة === */
.safety-rule {
    background: rgba(70,180,130,0.06);
    border-right: 3px solid #5ec49e;
    padding: 8px 14px;
    margin: 4px 0;
    border-radius: 0 8px 8px 0;
    font-size: 14px;
    color: #b0c8b8;
}

/* === إعدادات === */
.engine-status {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 8px;
    font-size: 13px;
    margin: 3px;
    font-weight: 600;
}
.engine-on  { background: rgba(70,180,130,0.2); color: #5ec49e; }
.engine-off { background: rgba(220,80,80,0.15); color: #e07070; }

/* === نص النسخ === */
.copy-text {
    background: rgba(0,0,0,0.3);
    padding: 6px 10px;
    border-radius: 6px;
    font-size: 13px;
    margin: 4px 0;
    direction: ltr;
}

/* === sub header === */
.sub-header {
    text-align: center;
    color: #9a9080;
    font-size: 14px;
    margin-bottom: 18px;
}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════
#  العنوان الرئيسي
# ═══════════════════════════════════════════════════════════

arc_count = len(load_archive().get('reviews', []))
st.markdown("<h1>🌸 مساعدي في شراء العطور</h1>", unsafe_allow_html=True)
st.markdown(
    f"<p class='sub-header'>🧠 ذكاء اصطناعي • {len(PRODUCTS):,} عطر "
    f"• لهجات سعودية • أرشيف ذكي ({arc_count} تقييم)</p>",
    unsafe_allow_html=True,
)
st.warning(SYNTHETIC_DISCLOSURE_AR, icon=":material/science:")

# ═══════════════════════════════════════════════════════════
#  التبويبات الثلاثة
# ═══════════════════════════════════════════════════════════

tab1, tab2, tab3 = st.tabs([
    "✨ مولد الشخصيات",
    "📊 استخبارات محلي",
    "⚙️ الإعدادات",
])

# ═══════════════════════════════════════════════════════════
#  Tab 1: مولد الشخصيات
# ═══════════════════════════════════════════════════════════

with tab1:
    # أزرار سريعة
    lnk1, lnk2, lnk3 = st.columns(3)
    with lnk1:
        st.link_button("📧 بريد مؤقت", "https://boomlify.com/ar/dashboard")
    with lnk2:
        st.link_button("🛒 مهووس", "https://mahwous.com")
    with lnk3:
        st.link_button("📦 محلي", "http://localhost:5000")

    st.divider()

    count = st.selectbox("عدد الشخصيات:", [1, 3, 5, 10], index=0, key="persona_count")

    if st.button("✨ ولّد شخصيات جديدة", type="primary", width="stretch"):
        results = []
        progress = st.progress(0, text="جاري التوليد بالذكاء الاصطناعي...")
        for i in range(count):
            progress.progress(i / count, text=f"🧠 شخصية {i + 1} من {count}...")
            persona, perfumes = gen_persona_full()
            reviews = gen_reviews(persona, perfumes)
            store = gen_store_review(persona)
            results.append({
                'persona': persona,
                'perfumes': perfumes,
                'reviews': reviews,
                'store': store,
            })
        progress.progress(1.0, text=f"✅ {count} شخصية جاهزة!")
        st.session_state['results'] = results

    # زر: ولّد + انسخ الكل (يبني جدولاً جاهزاً للصق في Excel/Sheets)
    if st.button("📋 ولّد + انسخ الكل", width="stretch", key="gen_copy_all"):
        results = []
        progress = st.progress(0, text="جاري التوليد...")
        for i in range(count):
            progress.progress(i / count, text=f"🧠 شخصية {i + 1} من {count}...")
            persona, perfumes = gen_persona_full()
            reviews = gen_reviews(persona, perfumes)
            store = gen_store_review(persona)
            results.append({'persona': persona, 'perfumes': perfumes,
                            'reviews': reviews, 'store': store})
        progress.progress(1.0, text=f"✅ {count} شخصية جاهزة!")
        st.session_state['results'] = results
        # بناء نص مجمّع: صف لكل تقييم (مفصول بـ Tab)
        rows = ["نوع المحتوى\tقابل للنشر\tالاسم\tالجوال\tالعنوان\tالمنتج\tالنجوم\tالمحاكاة"]
        for r in results:
            p = r['persona']
            for rv in r['reviews']:
                text = str(rv.get('text', '')).strip()
                if not text:
                    continue  # لا تسمح بنسخ تقييم فارغ
                rows.append("\t".join([
                    "اصطناعي للبحث الداخلي", "لا",
                    str(p.get('name', '')), str(p.get('phone', '')),
                    str(p.get('address', '')), str(rv.get('product', '')),
                    str(rv.get('rating', 5)), text,
                ]))
        st.session_state['copy_all_text'] = "\n".join(rows) if len(rows) > 1 else ""

    if st.session_state.get('copy_all_text'):
        st.markdown("**📋 كل البيانات (انسخها للصقها في Excel / Google Sheets):**")
        st.code(st.session_state['copy_all_text'], language=None)

    # === عرض النتائج ===
    if 'results' in st.session_state:
        for idx, r in enumerate(st.session_state['results']):
            p = r['persona']
            with st.container():
                # --- بطاقة الشخصية ---
                st.markdown(f"""<div class="persona-card">
<h3>{p['emoji']} {p['name']}</h3>
<div class="info-row">📍 {p['city']} • {p['label']} • {p['age']} سنة</div>
<div class="info-row">📱 {p['phone']}</div>
<div class="info-row">🏠 {p['address']}</div>
</div>""", unsafe_allow_html=True)

                # --- الأبعاد السبعة (badges) ---
                mood = p.get('mood', '')
                expertise = p.get('expertise', '')
                w_style = p.get('writing_style', '')
                dialect_n = p.get('dialect_name', '')
                mention = '✓ ذكر المنتج' if p.get('mention_product') else '✗ بدون ذكر'
                emoji_flag = '✓ إيموجي' if p.get('use_emoji') else '✗ بدون إيموجي'
                typo_flag = '✓ أخطاء' if p.get('has_typo') else '✗ بدون أخطاء'

                st.markdown(f"""
<span class="dim-badge dim-mood">🎭 {mood}</span>
<span class="dim-badge dim-expertise">📈 {expertise}</span>
<span class="dim-badge dim-style">✍️ {w_style}</span>
<span class="dim-badge dim-dialect">🗣️ {dialect_n}</span>
<span class="dim-badge dim-mention">📦 {mention}</span>
<span class="dim-badge dim-emoji">😊 {emoji_flag}</span>
<span class="dim-badge dim-typo">✏️ {typo_flag}</span>
""", unsafe_allow_html=True)

                # --- نسخ البيانات ---
                copy_data = f"{p['name']}\n{p['phone']}\n{p['city']}\n{p['address']}"
                st.code(copy_data, language=None)

                # --- المنتجات ---
                st.markdown(f"**🧴 المنتجات ({len(r['perfumes'])})**")
                for pf in r['perfumes']:
                    g_icon = '👨' if pf['g'] == 'رجالي' else ('👩' if pf['g'] == 'نسائي' else '👤')
                    st.markdown(
                        f"<span class='product-tag'>{g_icon} {pf['name']} — {pf['price']} ر.س</span>",
                        unsafe_allow_html=True,
                    )

                # --- التقييمات ---
                st.markdown("**💬 التقييمات**")
                for rv in r['reviews']:
                    rating = rv.get('rating', 5)
                    stars = '★' * rating + '☆' * (5 - rating)
                    st.markdown(f"""<div class="review-box">
<div style="font-size:13px;color:#9a9080">{rv.get('product', '')}</div>
<div class="stars">{stars}</div>
<div>{rv.get('text', '')}</div>
</div>""", unsafe_allow_html=True)

                # --- تقييم المتجر ---
                s = r['store']
                s_stars = '★' * s.get('rating', 5)
                st.markdown(f"""<div class="store-review">
<b>🏪 تقييم المتجر</b><br>
<span class="stars">{s_stars}</span> {s.get('text', '')}
</div>""", unsafe_allow_html=True)

                st.divider()


# ═══════════════════════════════════════════════════════════
#  Tab 2: استخبارات محلي
# ═══════════════════════════════════════════════════════════

with tab2:
    if not USE_MAHALLI:
        st.warning("⚠️ محرك استخبارات محلي (mahalli_intel.py) غير متاح. تأكد من وجود الملف.")
        st.info("يتطلب هذا التبويب ملف `mahalli_intel.py` في نفس المجلد.")
    else:
        st.markdown("### 📊 لوحة استخبارات محلي")
        st.markdown("<p style='color:#9a9080;font-size:13px'>بيانات تنافسية مباشرة من Algolia — ترتيب ومقارنة وخطة يومية</p>",
                    unsafe_allow_html=True)

        # --- ملخص يومي ---
        try:
            summary = get_dashboard_summary()
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.markdown(f"""<div class="intel-metric">
<span class="value">{summary.get('total_products', 0)}</span>
<span class="label">إجمالي المنتجات</span>
</div>""", unsafe_allow_html=True)
            with m2:
                st.markdown(f"""<div class="intel-metric">
<span class="value">{summary.get('active_products', 0)}</span>
<span class="label">منتجات نشطة</span>
</div>""", unsafe_allow_html=True)
            with m3:
                st.markdown(f"""<div class="intel-metric">
<span class="value">{summary.get('daily_reviews_needed', 0)}</span>
<span class="label">تقييمات مطلوبة اليوم</span>
</div>""", unsafe_allow_html=True)
            with m4:
                cache_age = summary.get('cache_age_minutes', -1)
                age_txt = f"{cache_age} دق" if cache_age >= 0 else "—"
                st.markdown(f"""<div class="intel-metric">
<span class="value">{age_txt}</span>
<span class="label">عمر الكاش</span>
</div>""", unsafe_allow_html=True)
        except Exception as e:
            st.error(f"خطأ في تحميل الملخص: {e}")

        # --- زر التحديث ---
        st.markdown("")
        if st.button("🔄 تحديث الآن", width="stretch", key="refresh_intel"):
            with st.spinner("جاري تحديث البيانات من Algolia..."):
                try:
                    result = refresh_all_data()
                    st.success(
                        f"✅ تم التحديث — {result.get('products_count', 0)} منتج، "
                        f"{result.get('rankings_count', 0)} ترتيب"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"خطأ في التحديث: {e}")

        st.divider()

        # --- قائمة الأولويات ---
        st.markdown("### 🎯 قائمة الأولويات")
        try:
            plan = generate_daily_plan()
            products_plan = plan.get('products', [])
            if not products_plan:
                st.info("🎉 لا توجد منتجات تحتاج تقييمات حالياً — ممتاز!")
            else:
                st.markdown(
                    f"<p style='color:#9a9080;font-size:13px'>📅 خطة اليوم: "
                    f"{plan.get('total_reviews', 0)} تقييم على {len(products_plan)} منتج</p>",
                    unsafe_allow_html=True,
                )
                for item in products_plan:
                    name = item.get('name', 'بدون اسم')
                    our_w = item.get('our_weight', 0)
                    target_w = item.get('target_weight', 1)
                    our_count = item.get('our_count', 0)
                    gap = item.get('gap', 0)
                    quota = item.get('quota', 0)
                    s5 = item.get('stars_5', 0)
                    s4 = item.get('stars_4', 0)
                    progress_pct = min(our_w / target_w, 1.0) if target_w > 0 else 0

                    # المنافس الأول
                    top_comp = item.get('top_competitor')
                    comp_name = ''
                    if top_comp:
                        comp_name = top_comp.get('store_name', top_comp.get('name_ar', ''))[:25]

                    rank_class = 'danger' if gap > 50 else ('success' if gap <= 0 else '')

                    st.markdown(f"""<div class="priority-row">
<div class="priority-name">{name}</div>
<div class="info-row">
    تقييماتنا: <b>{our_count}</b> •
    الوزن: <b>{our_w}</b> / <b>{target_w}</b> •
    الفجوة: <span class="rank-badge {rank_class}">{gap}</span>
</div>
<div class="info-row">
    المنافس الأول: {comp_name if comp_name else '—'} •
    الحصة اليومية: <b>{quota}</b> ({s5}×⭐5 + {s4}×⭐4)
</div>
<div class="info-row">
    ⏰ توقيت مقترح: {' • '.join(suggest_review_times(quota)) if quota else '—'}
</div>
</div>""", unsafe_allow_html=True)

                    # شريط التقدم
                    st.progress(progress_pct,
                                text=f"{int(progress_pct * 100)}% من الهدف")

        except Exception as e:
            st.error(f"خطأ في تحميل الأولويات: {e}")

        st.divider()

        # --- فاحص الترتيب ---
        st.markdown("### 🔍 فاحص ترتيب البحث")
        check_col1, check_col2 = st.columns([3, 1])
        with check_col1:
            search_query = st.text_input(
                "كلمة البحث:", placeholder="مثال: ديور سوفاج", key="rank_query",
            )
        with check_col2:
            st.markdown("<br>", unsafe_allow_html=True)
            check_btn = st.button("🔍 فحص", key="check_rank")

        if check_btn and search_query:
            with st.spinner(f"جاري البحث عن «{search_query}»..."):
                try:
                    rank, product = get_our_rank(search_query, use_cache=False)
                    if rank:
                        prod_name = product.get('name_ar', '') if product else ''
                        rc = 'success' if rank <= 5 else ('danger' if rank > 15 else '')
                        st.markdown(f"""<div class="intel-card">
<b>🔍 نتيجة البحث عن: «{search_query}»</b><br>
ترتيبنا: <span class="rank-badge {rc}">#{rank}</span><br>
المنتج: {prod_name}
</div>""", unsafe_allow_html=True)
                    else:
                        st.warning(f"❌ لم نظهر في أول 50 نتيجة لـ «{search_query}»")
                except Exception as e:
                    st.error(f"خطأ: {e}")

        # --- كلمات البحث الشائعة ---
        with st.expander("📋 كلمات البحث الشائعة"):
            try:
                for kw in TOP_SEARCHES:
                    rank, product = get_our_rank(kw)
                    if rank:
                        rc = 'success' if rank <= 5 else ('danger' if rank > 15 else '')
                        st.markdown(
                            f"<span class='rank-badge {rc}'>#{rank}</span> {kw}",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            f"<span class='rank-badge danger'>—</span> {kw}",
                            unsafe_allow_html=True,
                        )
            except Exception as e:
                st.error(f"خطأ: {e}")

        st.divider()

        # --- قواعد السلامة ---
        st.markdown("### 🛡️ قواعد السلامة")
        try:
            rules = SAFETY_RULES
            rules_display = {
                'max_reviews_per_product_per_day': ('أقصى تقييمات لكل منتج يومياً', '📝'),
                'max_products_per_account_per_day': ('أقصى منتجات لكل حساب يومياً', '📦'),
                'min_days_after_purchase': ('أقل أيام بعد الشراء', '⏳'),
                'max_days_after_purchase': ('أقصى أيام بعد الشراء', '📅'),
                'four_star_percentage': ('نسبة تقييمات 4 نجوم (%)', '⭐'),
                'num_accounts': ('عدد الحسابات', '👤'),
            }
            for key, (label, icon) in rules_display.items():
                val = rules.get(key, '—')
                st.markdown(
                    f'<div class="safety-rule">{icon} {label}: <b>{val}</b></div>',
                    unsafe_allow_html=True,
                )
        except Exception as e:
            st.error(f"خطأ: {e}")


# ═══════════════════════════════════════════════════════════
#  Tab 3: الإعدادات
# ═══════════════════════════════════════════════════════════

with tab3:
    st.markdown("### ⚙️ الإعدادات والأدوات")

    # --- إحصائيات الأرشيف ---
    st.markdown("#### 📦 الأرشيف")
    arc = load_archive()
    arc_reviews = arc.get('reviews', [])
    arc_len = len(arc_reviews)
    st.markdown(f"""<div class="intel-card">
<b>إجمالي التقييمات المحفوظة:</b> {arc_len} / {MAX_ARCHIVE}<br>
<span style="color:#9a9080;font-size:13px">يحتفظ النظام بآخر {MAX_ARCHIVE} تقييم (FIFO)</span>
</div>""", unsafe_allow_html=True)

    if arc_len > 0:
        st.progress(min(arc_len / MAX_ARCHIVE, 1.0),
                    text=f"{arc_len}/{MAX_ARCHIVE} تقييم")

    # مسح الأرشيف
    if st.button("🗑️ مسح الأرشيف", key="clear_archive"):
        save_archive({'reviews': []})
        st.success("✅ تم مسح الأرشيف بنجاح")
        st.rerun()

    st.divider()

    # --- تصدير CSV ---
    st.markdown("#### 📥 تصدير CSV")
    st.markdown("<p style='color:#9a9080;font-size:13px'>تصدير كل الشخصيات والتقييمات المولّدة في الجلسة الحالية</p>",
                unsafe_allow_html=True)

    if 'results' in st.session_state and st.session_state['results']:
        results = st.session_state['results']
        # بناء CSV في الذاكرة
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        # رأس الجدول
        writer.writerow([
            'نوع المحتوى', 'قابل للنشر',
            'اسم الشخصية', 'المدينة', 'الهاتف', 'العنوان', 'العمر',
            'النوع', 'النمط', 'المزاج', 'الخبرة', 'أسلوب الكتابة',
            'اللهجة', 'ذكر المنتج', 'إيموجي', 'أخطاء',
            'اسم المنتج', 'البراند', 'السعر', 'التقييم (نجوم)', 'نص التقييم',
            'تقييم المتجر (نجوم)', 'نص تقييم المتجر',
        ])
        for r in results:
            p = r['persona']
            store = r.get('store', {})
            for rv in r['reviews']:
                writer.writerow([
                    'اصطناعي للبحث الداخلي', 'لا',
                    p.get('name', ''), p.get('city', ''), p.get('phone', ''),
                    p.get('address', ''), p.get('age', ''),
                    'ذكر' if p.get('gender') == 'male' else 'أنثى',
                    p.get('label', ''), p.get('mood', ''), p.get('expertise', ''),
                    p.get('writing_style', ''), p.get('dialect_name', ''),
                    'نعم' if p.get('mention_product') else 'لا',
                    'نعم' if p.get('use_emoji') else 'لا',
                    'نعم' if p.get('has_typo') else 'لا',
                    rv.get('product', ''), rv.get('brand', ''), rv.get('price', ''),
                    rv.get('rating', 5), rv.get('text', ''),
                    store.get('rating', 5), store.get('text', ''),
                ])
        csv_data = output.getvalue()

        st.download_button(
            label="📥 تحميل CSV",
            data=csv_data.encode('utf-8-sig'),
            file_name=f"personas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            width="stretch",
        )
        st.markdown(
            f"<p style='color:#9a9080;font-size:13px'>"
            f"📊 {len(results)} شخصية — "
            f"{sum(len(r['reviews']) for r in results)} تقييم جاهز للتصدير</p>",
            unsafe_allow_html=True,
        )
    else:
        st.info("💡 لا توجد شخصيات مولّدة في الجلسة الحالية. ولّد شخصيات أولاً من تبويب «✨ مولد الشخصيات».")

    st.divider()

    # --- حالة المحركات ---
    st.markdown("#### 🔧 حالة المحركات")
    engines = {
        'personas_engine': USE_NEW_PERSONAS,
        'dialects': USE_DIALECTS,
        'review_patterns': USE_REVIEW_PATTERNS,
        'anti_repeat': USE_ANTI_REPEAT,
        'trending': USE_TRENDING,
        'mahalli_intel': USE_MAHALLI,
    }
    badges_html = ""
    for name, loaded in engines.items():
        cls = "engine-on" if loaded else "engine-off"
        icon = "✅" if loaded else "❌"
        badges_html += f'<span class="engine-status {cls}">{icon} {name}</span> '
    st.markdown(badges_html, unsafe_allow_html=True)

    engine_on = sum(1 for v in engines.values() if v)
    engine_total = len(engines)
    st.markdown(
        f"<p style='color:#9a9080;font-size:13px;margin-top:8px'>"
        f"🔌 {engine_on}/{engine_total} محرك نشط</p>",
        unsafe_allow_html=True,
    )

    st.divider()

    # --- معلومات النظام ---
    st.markdown("#### 💻 معلومات النظام")
    st.markdown(f"""<div class="intel-card">
<b>📁 مسار المشروع:</b> <code style="direction:ltr">{BASE_DIR}</code><br>
<b>💾 مسار البيانات (DATA_DIR):</b> <code style="direction:ltr">{DATA_DIR}</code><br>
<b>🧠 نموذج AI:</b> {AI_MODEL}<br>
<b>🔑 مفتاح AI:</b> {'✅ محمّل' if AI_KEY else '❌ غير موجود'}<br>
<b>📦 المنتجات:</b> {len(PRODUCTS):,} عطر<br>
<b>👤 الأسماء:</b> {len(NAMES.get('male', []))} ذكر + {len(NAMES.get('female', []))} أنثى + {len(NAMES.get('family_names', []))} عائلة<br>
<b>🏙️ المدن:</b> {len(NAMES.get('cities', []))} مدينة<br>
<b>⏰ الوقت:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
</div>""", unsafe_allow_html=True)

    st.divider()

    # --- إحصائيات الأداء ---
    st.markdown("### 📈 إحصائيات الأداء")
    from collections import Counter
    _arc = load_archive()
    _reviews = _arc.get('reviews', [])
    if not _reviews:
        st.info("💡 لا توجد تقييمات في الأرشيف بعد — ولّد بعض الشخصيات لرؤية الإحصائيات.")
    else:
        total = len(_reviews)
        # توزيع النجوم الفعلي
        ratings = Counter(r.get('rating', 5) for r in _reviews if 'rating' in r)
        rated = sum(ratings.values())
        if rated:
            st.markdown("**توزيع النجوم الفعلي:**")
            lines = []
            for star in [5, 4, 3, 2, 1]:
                cnt = ratings.get(star, 0)
                pct = round(cnt / rated * 100)
                bar = '█' * (pct // 2) + '░' * (50 - pct // 2)
                lines.append(f"{star}⭐ {bar} {pct}% ({cnt})")
            st.markdown("```\n" + "\n".join(lines) + "\n```")

        # أكثر المنتجات تقييماً
        products_c = Counter(r.get('product', '') for r in _reviews if r.get('product'))
        if products_c:
            st.markdown("**أكثر المنتجات تقييماً:**")
            for prod, cnt in products_c.most_common(5):
                st.markdown(f"- {prod}: **{cnt}** تقييم")

        # أكثر الشخصيات نشاطاً
        personas_c = Counter(r.get('persona', '') for r in _reviews if r.get('persona'))
        if personas_c:
            st.markdown("**أكثر الشخصيات نشاطاً:**")
            for psn, cnt in personas_c.most_common(5):
                st.markdown(f"- {psn}: **{cnt}** تقييم")

        st.markdown(
            f"<p style='color:#9a9080;font-size:13px;margin-top:8px'>"
            f"📊 الإجمالي: {total} تقييم محفوظ ({rated} منها بنجوم)</p>",
            unsafe_allow_html=True,
        )
