# -*- coding: utf-8 -*-
"""
realism_calibrator.py — معاير الواقعية (Realism Calibrator)
============================================================
يشكّل توزيع أطوال التقييمات المولّدة على «الحمض النووي» الحقيقي لعملاء
المنافسين المرصود في competitor_reviews.json، بدل التوزيع المُخمَّن.

الفلسفة: العميل السعودي الحقيقي يكتب «ممتاز» وينصرف، أو «10/10»، أو «…..»،
أو «💎💎💎» — لا يكتب مقالات. نعاير على شكله بالضبط لا على شكل الذكاء الاصطناعي.

مصدر الحقيقة (452 نصًّا غير فارغ من 1215 تقييمًا عبر 25 متجرًا، مرصودة 2026-07-12
من competitor_reviews_full.json — الكشط الشامل):
  • توزيع الأطوال (كلمات): مبيّن في _FALLBACK_LEN_COUNTS أدناه.
  • 5.1% تقييمات رمزية صرفة بلا حروف عربية («10/10»، «…..»، إيموجي صرف).
  • 13.7% تحوي تمطيطًا (حرف مكرّر 3+): «راااائع»، «واووو».
  • 11.9% تحوي إيموجي.
  • الوسيط = 3 كلمات، المتوسط = 5.37 كلمة، الشرائح: 1ك=27%، 2-4ك=37%، 5-14ك=30.6%، 15+=5.5%.

graceful degradation: إن غاب الملف أو تعذّر تحليله، نعتمد التوزيع المُضمَّن
المرصود سلفًا — فلا يتعطّل التوليد أبدًا (نفس مبدأ الحارس الدلالي).

stdlib فقط + استيراد اختياري لـ short_texts_bank (للتمطيط المشترك).
"""
import os
import re
import json
import random

# تطبيع عربي موحّد لإزالة التكرار — نفس المصدر المستخدم في anti_repeat.py
# (راجع anti_repeat._normalize لتوثيق القرار: توحيد صور الحروف وإسقاط
# التشكيل/غير العربي). مصدر واحد بدل نسخة محلية قد تتباعد.
import anti_repeat as _ar

# ═══════════════════════════════════════════════════════════
#  التوزيع المرصود المُضمَّن — احتياطي إن غاب ملف المنافسين
#  المصدر: competitor_reviews_full.json، 452 نصًّا، أطوال بالكلمات
# ═══════════════════════════════════════════════════════════
_FALLBACK_LEN_COUNTS = {
    1: 122, 2: 71, 3: 46, 4: 50, 5: 35, 6: 28, 7: 20, 8: 15, 9: 8,
    10: 9, 11: 7, 12: 7, 13: 6, 14: 3, 15: 3, 16: 2, 17: 1, 19: 2,
    21: 1, 22: 2, 24: 3, 25: 2, 27: 1, 33: 1, 34: 1, 35: 1, 39: 1,
    63: 1, 66: 1, 76: 1, 89: 1,
}

# نسبة التقييمات الرمزية الصرفة داخل شريحة الكلمة الواحدة (19 من 122)
_SYMBOLIC_SHARE_OF_1WORD = 19 / 122  # ≈ 0.156

# معدلات النسيج المرصودة (حصص من كامل العينة — 452 نصًّا)
TEXTURE = {
    'symbolic_pure': 0.051,   # بلا حروف عربية
    'elongation': 0.137,      # حرف مكرّر 3+
    'emoji_any': 0.119,       # يحوي إيموجي
    'median_words': 3,
    'mean_words': 5.37,
    'median_chars': 16,
}

# سقف الطول المستهدف في المسار الحي: الذيل النادر جدًّا (63-89 كلمة،
# 4 نصوص من 452) يُقصّ إلى 34 — تقييم أطول من ذلك يفقد مصداقية الـ AI.
LIVE_MAX_TARGET = 34

# ═══════════════════════════════════════════════════════════
#  بنك التقييمات الرمزية الصرفة — من رصد المنافسين الحقيقي
#  (هذه أكبر ثغرة «ريحة ذكاء اصطناعي»: المولّد لا ينتج منها شيئًا)
# ═══════════════════════════════════════════════════════════
SYMBOLIC_SHORTS = [
    '10/10', '10/10', '10/10',           # الأكثر تكرارًا عند المنافسين (6×)
    '5/5', '💯',
    '…..', '….', '……',                   # نقاط صرفة (رُصدت 4×)
    '👍', '👍🏻', '👍🏻👍🏻', '👍🏻👍🏻👍🏻',
    '😍', '😍😍😍', '😍😍😍😍😍',
    '💎💎💎', '💎💎💎💎',
    '🔥', '🔥🔥', '❤️', '❤️❤️❤️',
    '👌', '👌🏻', '⭐️⭐️⭐️⭐️⭐️',
    '👍🏻👍🏻🤣', '🌸',
    'Excellent', 'Excellent', 'Nice', 'Top', 'Good',
    'واووو', 'واااو', 'ءءءء', 'ياااه',
]

# ═══════════════════════════════════════════════════════════
#  حروف قابلة للتمطيط — نستورد من البنك المشترك إن توفّر
# ═══════════════════════════════════════════════════════════
try:
    from short_texts_bank import STRETCHABLE_CHARS as _STRETCH
except Exception:
    _STRETCH = {
        'ن': 'ننن', 'ر': 'ررر', 'ل': 'للل', 'ي': 'ييي', 'و': 'ووو',
        'ا': 'ااا', 'ه': 'ههه', 'م': 'مم', 'ب': 'بب', 'ح': 'حح',
    }

_ARABIC_RE = re.compile('[؀-ۿ]')
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# سلسلة مصادر الحقيقة: الكشط الشامل (1215 تقييمًا/25 متجرًا) أولًا،
# ثم الملف القديم، ثم التوزيع المُضمَّن — لا يتعطّل التوليد أبدًا.
_SOURCE_CHAIN = [
    os.path.join(_BASE_DIR, 'competitor_reviews_full.json'),
    os.path.join(_BASE_DIR, 'competitor_reviews.json'),
]


# ═══════════════════════════════════════════════════════════
#  تحميل بركة الأطوال — من الملف الحقيقي أو الاحتياطي
# ═══════════════════════════════════════════════════════════

def _expand(counts):
    pool = []
    for length, n in counts.items():
        pool.extend([int(length)] * int(n))
    return pool


def load_length_pool(path=None):
    """يُرجع قائمة أطوال (عدد كلمات) — عيّنة حقيقية نعاير عليها.

    من competitor_reviews_full.json (الكشط الشامل) إن وُجد، ثم القديم،
    وإلا التوزيع المُضمَّن المرصود. أخذ عيّنة من هذه القائمة يجعل توزيع
    المولّد يقارب توزيع المنافسين بحكم قانون الأعداد الكبيرة —
    مطابقة بالبناء لا بالتقريب.
    """
    for p in ([path] if path else _SOURCE_CHAIN):
        try:
            with open(p, encoding='utf-8') as f:
                data = json.load(f)
            reviews = data.get('reviews', data) if isinstance(data, dict) else data
            pool = []
            for r in reviews:
                txt = (r.get('text') if isinstance(r, dict) else str(r)) or ''
                txt = txt.strip()
                if txt:
                    pool.append(len(txt.split()))
            if len(pool) >= 30:  # عيّنة معقولة
                return pool
        except Exception:
            continue
    return _expand(_FALLBACK_LEN_COUNTS)


def sample_target_length(pool, cap=None):
    """طول مستهدف واحد — عيّنة تمهيدية (bootstrap) من البركة الحقيقية.

    cap (اختياري): سقف للمسار الحي (LIVE_MAX_TARGET) يقصّ الذيل النادر.
    """
    n = random.choice(pool)
    return min(n, cap) if cap else n


def length_directive(target):
    """سطر توجيه عربي للبرومبت حسب الطول المستهدف المعاين من الواقع.

    يُغذّي MASTER_PROMPT في personas_engine — مصدر واحد للصياغة في
    المسارين (Flask وStreamlit) كي لا يتباعدا.
    """
    t = int(target)
    if t <= 1:
        return 'كلمة واحدة فقط (مثل: ممتاز / روووعه / فخم)'
    if t <= 4:
        return f'من 1 إلى {t} كلمات فقط لا أكثر أبداً'
    if t <= 9:
        return f'حوالي {t} كلمات — جملة عفوية واحدة مكتملة المعنى'
    return f'حوالي {t} كلمة — جملتان عفويتان بلا حشو ولا إنشاء'


# ═══════════════════════════════════════════════════════════
#  بنك الأمثلة الحقيقية — مفردات المنافسين لا مفردات الذكاء
#  الكشط الشامل يرتقي من مرجعية أطوال إلى مرجعية مفردات وأنماط:
#  نُعطي البرومبت أمثلة نصية فعلية كتبها عملاء المنافسين قرب الطول
#  المستهدف، بدل قائمة ثابتة مكتوبة بيد المطوّر (أكبر مصدر «ريحة AI»).
# ═══════════════════════════════════════════════════════════

# احتياطي مطابق للقائمة الثابتة القديمة في MASTER_PROMPT — إن غاب الكشط
# يتدرّج السلوك لِما كان عليه تماماً (نفس مبدأ التدرّج الآمن أعلاه).
_FALLBACK_EXEMPLARS = [
    'ممتاز', 'يستاهل كل ريال', 'ريحته ثابتة', 'والله حلو', 'وصل سليم',
    'جربته وارتحت', 'عادي بصراحة', 'شكرا مهووس', 'يثبت طول اليوم',
    'حلو بس خفيف', 'بطلب مره ثانيه', 'ريحة فخمة',
]

_EXEMPLAR_POOL = None  # كاش الوحدة — يُحمَّل مرة واحدة (الملف 292KB)


def load_exemplar_pool(path=None):
    """يُرجع [(عدد_كلمات, نص)] من نصوص المنافسين الحقيقية النظيفة.

    يصفّي: نص غير فارغ، يحوي حروفاً عربية، ويزيل التكرار (بعد التطبيع
    العربي — لا التطابق الحرفي فقط) مع الحفاظ على الترتيب.
    من competitor_reviews_full.json (الكشط الشامل) أولاً ثم القديم، وإلا [].

    التطبيع ضروري هنا لا رفاهية: «اصلي 100%» و«اصلى 100‎%‎» أو
    «جميل جدا/جميل جداً/جميل جدًا» نصوص متطابقة معنى تختلف شكلاً فقط —
    التطابق الحرفي القديم كان يُبقيهما «فريدين»، فيمكن أن يصل كلاهما لنفس
    البرومبت الواحد كمثالين مختلفين (رُصد فعلياً عبر عيّنات عشوائية متعددة).
    """
    for p in ([path] if path else _SOURCE_CHAIN):
        try:
            with open(p, encoding='utf-8') as f:
                data = json.load(f)
            reviews = data.get('reviews', data) if isinstance(data, dict) else data
            seen, pool = set(), []
            for r in reviews:
                txt = (r.get('text') if isinstance(r, dict) else str(r)) or ''
                txt = txt.strip()
                if not txt or not _ARABIC_RE.search(txt):
                    continue
                key = _ar._normalize(txt)
                if not key or key in seen:
                    continue
                seen.add(key)
                pool.append((len(txt.split()), txt))
            if len(pool) >= 30:  # عيّنة معقولة
                return pool
        except Exception:
            continue
    return []


def get_exemplar_pool():
    """بركة الأمثلة مع كاش على مستوى الوحدة (تجنّب قراءة الملف كل نداء AI)."""
    global _EXEMPLAR_POOL
    if _EXEMPLAR_POOL is None:
        _EXEMPLAR_POOL = load_exemplar_pool()
    return _EXEMPLAR_POOL


def sample_exemplars(target_len=None, n=8, pool=None):
    """يُعيد حتى n نصاً حقيقياً منحازةً لشريحة الطول المستهدف.

    عند غياب الكشط (بركة فارغة) يتدرّج إلى القائمة الثابتة القديمة —
    فلا يتعطّل البرومبت أبداً ولا يخسر أمثلته. المنطق:
    نبدأ بنافذة ضيقة حول الطول المعاين ثم نوسّعها حتى نجمع n.
    """
    pool = get_exemplar_pool() if pool is None else pool
    if not pool:
        picks = list(_FALLBACK_EXEMPLARS)
        random.shuffle(picks)
        return picks[:n]

    if target_len is None:
        chosen = random.sample(pool, min(n, len(pool)))
        return [t for _, t in chosen]

    t = int(target_len)
    # نوافذ متوسّعة حول الطول المستهدف: قصير يجاور قصير، طويل يجاور طويل
    picked, used = [], set()
    for half in (1, 2, 4, 8, 999):
        window = [txt for wc, txt in pool if abs(wc - t) <= half and txt not in used]
        random.shuffle(window)
        for txt in window:
            picked.append(txt)
            used.add(txt)
            if len(picked) >= n:
                return picked
    return picked or [t for _, t in random.sample(pool, min(n, len(pool)))]


# ═══════════════════════════════════════════════════════════
#  محقنات النسيج البشري
# ═══════════════════════════════════════════════════════════

def is_symbolic_for_1word():
    """هل تُصدَر شريحة الكلمة-الواحدة كرمز صرف؟ (≈28% منها عند المنافسين)."""
    return random.random() < _SYMBOLIC_SHARE_OF_1WORD


def pick_symbolic():
    return random.choice(SYMBOLIC_SHORTS)


def maybe_elongate(text, probability=None):
    """تمطيط حرف في كلمة عشوائية ليطابق 13.7% المرصودة (راااائع، جميييل).

    يختلف عن short_texts_bank._stretch_text بأنه يمطّ كلمة عشوائية لا الأخيرة
    فقط — أقرب لما رُصد فعليًا (التمطيط يقع في وسط الكلمة عادةً).
    """
    p = TEXTURE['elongation'] if probability is None else probability
    if random.random() > p:
        return text
    words = text.split()
    cand = [i for i, w in enumerate(words) if len(w) >= 3]
    if not cand:
        return text
    idx = random.choice(cand)
    w = words[idx]
    positions = [j for j, ch in enumerate(w) if ch in _STRETCH]
    if not positions:
        return text
    j = random.choice(positions)
    words[idx] = w[:j] + _STRETCH[w[j]] + w[j + 1:]
    return ' '.join(words)


# ═══════════════════════════════════════════════════════════
#  تقرير المطابقة — للتحقق (chi-square goodness of fit)
# ═══════════════════════════════════════════════════════════

# حدود الشرائح للمقارنة (تُطابق مدرّج المالك: 1،2،3،4،>4 + تفصيل الذيل)
BUCKETS = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 7), (8, 14), (15, 999)]


def _bucket_of(w):
    for lo, hi in BUCKETS:
        if lo <= w <= hi:
            return (lo, hi)
    return BUCKETS[-1]


def bucket_histogram(word_counts):
    """يحوّل قائمة أطوال إلى حصص شرائح."""
    total = len(word_counts) or 1
    hist = {b: 0 for b in BUCKETS}
    for w in word_counts:
        hist[_bucket_of(w)] += 1
    return {b: hist[b] / total for b in BUCKETS}, hist


def chi_square(observed_counts, expected_probs, n):
    """χ² للمطابقة: observed أعداد، expected احتمالات، n حجم العيّنة المولّدة."""
    chi = 0.0
    for b in BUCKETS:
        exp = expected_probs[b] * n
        obs = observed_counts.get(b, 0)
        if exp > 0:
            chi += (obs - exp) ** 2 / exp
    return chi


if __name__ == '__main__':
    import sys
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
    pool = load_length_pool()
    print(f'حجم بركة الأطوال: {len(pool)}')
    probs, counts = bucket_histogram(pool)
    for (lo, hi), pct in probs.items():
        label = f'{lo}' if lo == hi else (f'>{lo-1}' if hi == 999 else f'{lo}-{hi}')
        print(f'  {label:>5} كلمة: {pct*100:5.1f}%  ({counts[(lo,hi)]})')
    print(f'رموز صرفة بالبنك: {len(SYMBOLIC_SHORTS)} | حصة رمزية بالكلمة-الواحدة: {_SYMBOLIC_SHARE_OF_1WORD:.1%}')
