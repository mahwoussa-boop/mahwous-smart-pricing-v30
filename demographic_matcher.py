# -*- coding: utf-8 -*-
"""
محرك المطابقة الديموغرافي — Demographic Matching Engine (v1.0)
==============================================================
"من يتحدث، وعن ماذا؟" — قبل أن يُكتب أي حرف في تقييم، يمرّ الثنائي (شخصية/منتج)
بهذا الفلتر البارد:

  1) تحليل المنتج والجمهور  (profile_product):
     يقرأ عائلة العطر (scent_family) + النوتات (ingredients) + التصنيف (category)
     + السعر، ويستنتج جمهوره الحقيقي: فئة عمرية · مناسبة استخدام · ثقل · حلاوة.

  2) فلترة الشخصية        (score / is_valid_pair):
     خوارزمية Scoring تقيس ملاءمة (الجنس + العمر + نمط حياة الشخصية) مع بروفايل
     العطر. الثنائي غير المنطقي (شيخ كبير + عطر حلويات مراهقات، رياضي + عود ثقيل
     للمجالس) يُرفض أو يُخفَّض، و filter_pool يعيد الترشيح.

  3) صياغة واعية بالمنتج  (review_directive):
     توجيه قصير يعكس الاستخدام الحقيقي المطابق لطبيعة العطر (فخم للعزايم / صباحي
     للدوام / منعش للجيم) — يُمرَّر مع تنبيهات البرومبت.

قواعد تصميم:
- الوحدة **نقية** (stdlib فقط) وبلا آثار جانبية ولا I/O. تستقبل dicts وتُرجع
  أرقاماً/نصوصاً. لا تستورد personas_engine ولا app (تفادي الدوران) — الطبقات
  الأعلى هي من يستوردها.
- كل شيء يفشل بأمان: مفاتيح ناقصة → افتراضات محايدة؛ عائلة مجهولة → بروفايل
  متوسط؛ filter_pool لا يُرجع قائمة فارغة أبداً (لا يكسر السلة/التوليد).

المدخلات المتوقّعة:
  arch/persona: {'id'|'archId', 'g': 'male'/'female', 'age': (min,max) أو int,
                 'prefers': [...]}
  product     : عنصر من catalog.json {'g','price','scent_family','ingredients',
                 'category','product_type','name'}
"""
import random

# ═══════════════════════════════════════════════════════════════════════
#  الفئات العمرية — سُلّم مرتّب للمسافة بين جمهور العطر وعمر الشخصية
# ═══════════════════════════════════════════════════════════════════════
AGE_BANDS = ['young', 'young_adult', 'adult', 'mature', 'senior']  # 0..4
_BAND_IDX = {b: i for i, b in enumerate(AGE_BANDS)}


def age_band(age):
    """عمر (int أو tuple نطاق) → فئة عمرية. يعتمد منتصف النطاق."""
    if isinstance(age, (tuple, list)) and age:
        a = (age[0] + age[-1]) / 2.0
    elif isinstance(age, (int, float)):
        a = float(age)
    else:
        a = 30.0
    if a <= 24:
        return 'young'
    if a <= 32:
        return 'young_adult'
    if a <= 45:
        return 'adult'
    if a <= 57:
        return 'mature'
    return 'senior'


# ═══════════════════════════════════════════════════════════════════════
#  بروفايل عائلة العطر — الإشارة الأساسية الموثوقة (scent_family)
#  age     : الفئة العمرية النموذجية لجمهور العائلة
#  occ     : مجموعة المناسبات (daily/work/formal/occasion/evening/romantic/
#            sport/casual)
#  intensity: الثقل 0..1   ·  sweet: الحلاوة 0..1  ·  fresh: الانتعاش 0..1
# ═══════════════════════════════════════════════════════════════════════
FAMILY_PROFILE = {
    'oud':      {'age': 'mature',      'occ': {'formal', 'occasion', 'evening'},        'intensity': 0.90, 'sweet': 0.20, 'fresh': 0.00},
    'oriental': {'age': 'adult',       'occ': {'formal', 'occasion', 'evening'},        'intensity': 0.80, 'sweet': 0.45, 'fresh': 0.05},
    'leather':  {'age': 'mature',      'occ': {'formal', 'evening'},                    'intensity': 0.85, 'sweet': 0.10, 'fresh': 0.10},
    'woody':    {'age': 'adult',       'occ': {'work', 'daily', 'formal'},              'intensity': 0.60, 'sweet': 0.20, 'fresh': 0.25},
    'musk':     {'age': 'young_adult', 'occ': {'daily', 'work', 'romantic'},            'intensity': 0.45, 'sweet': 0.40, 'fresh': 0.35},
    'floral':   {'age': 'young_adult', 'occ': {'daily', 'romantic', 'occasion'},        'intensity': 0.50, 'sweet': 0.40, 'fresh': 0.45},
    'citrus':   {'age': 'young',       'occ': {'daily', 'sport', 'work'},               'intensity': 0.30, 'sweet': 0.10, 'fresh': 0.90},
    'fresh':    {'age': 'young',       'occ': {'daily', 'sport', 'work'},               'intensity': 0.35, 'sweet': 0.10, 'fresh': 0.85},
    'sweet':    {'age': 'young',       'occ': {'casual', 'romantic', 'evening'},        'intensity': 0.55, 'sweet': 0.90, 'fresh': 0.10},
    'gourmand': {'age': 'young',       'occ': {'casual', 'romantic', 'evening'},        'intensity': 0.60, 'sweet': 0.95, 'fresh': 0.10},
}
_NEUTRAL_FAMILY = {'age': 'adult', 'occ': {'daily', 'work'}, 'intensity': 0.5, 'sweet': 0.4, 'fresh': 0.4}

# نوتات دالّة (النوتات صاخبة — «عود/ورد» في كل مكان — فنستخدمها للتحسين فقط)
_SWEET_NOTES = ('شوكولاتة', 'شوكولا', 'كراميل', 'فانيلا', 'توفي', 'سكر', 'حلوى',
                'عسل', 'مارشميلو', 'آيس كريم', 'كيك', 'بسكويت', 'قهوة')
_FRESH_NOTES = ('برغموت', 'ليمون', 'ليم', 'نعناع', 'بحري', 'منعش', 'خيار', 'جريب فروت',
                'زنجبيل', 'أوزون', 'مائي', 'حامض', 'حمضيات', 'لافندر')
_HEAVY_NOTES = ('عنبر', 'زعفران', 'دخون', 'بخور', 'جلود', 'جلد', 'باتشولي', 'مُر', 'دهن')
_FLORAL_NOTES = ('ورد', 'ياسمين', 'زهر', 'توبيروز', 'فل', 'أوركيد', 'بنفسج', 'زنبق')

# إشارات المناسبة/الرسمية من نص التصنيف (category) — دقيقة ومباشرة
_CAT_SIGNALS = [
    ('رسمي', {'formal', 'work'}),
    ('للمناسبات', {'occasion', 'formal'}),
    ('مناسبات', {'occasion', 'formal'}),
    ('جذاب', {'evening', 'romantic'}),
    ('نواعم', {'daily', 'romantic'}),
    ('رياضي', {'sport', 'daily'}),
    ('يومي', {'daily'}),
    ('قديم', {'formal', 'occasion'}),   # «عطور قديمة» = كلاسيكي/وقور
    ('كلاسيك', {'formal', 'occasion'}),
]
# التصنيفات النيش تدل على متذوّق (تتسامح مع كل الأعمار)
_NICHE_CAT = ('نيش', 'niche')


def profile_product(product):
    """يستنتج «جمهور العطر» من عائلته + نوتاته + تصنيفه + سعره + نوعه.

    يُرجع dict:
      age(band), occ(set), intensity, sweet, fresh, gender('رجالي'/'نسائي'/'مشترك'),
      luxury(bool), niche(bool), ptype, family
    """
    fam = (product.get('scent_family') or '').strip().lower()
    base = FAMILY_PROFILE.get(fam, _NEUTRAL_FAMILY)
    occ = set(base['occ'])
    intensity, sweet, fresh = base['intensity'], base['sweet'], base['fresh']
    band = base['age']

    ing = product.get('ingredients') or ''
    cat = product.get('category') or ''
    ptype = product.get('product_type', 'عطر')

    # تحسين بالنوتات (نطاقات صغيرة كي لا تطغى على العائلة)
    if any(n in ing for n in _SWEET_NOTES):
        sweet = min(1.0, sweet + 0.25)
    if any(n in ing for n in _FRESH_NOTES):
        fresh = min(1.0, fresh + 0.20)
    if any(n in ing for n in _HEAVY_NOTES):
        intensity = min(1.0, intensity + 0.15)

    # البخور/العود دائماً تقليدي ثقيل للمجالس والمناسبات (يصحّح مثال «دهن عود معتّق»)
    if ptype in ('بخور',):
        occ = {'formal', 'occasion', 'evening'}
        intensity = max(intensity, 0.9)
        band = 'mature'

    # إشارات التصنيف صريحة → تُضاف للمناسبات
    for kw, sig in _CAT_SIGNALS:
        if kw in cat:
            occ = occ | sig

    niche = any(k in cat for k in _NICHE_CAT)

    # الفخامة من السعر (p75≈675، p90≈1025 في الكتالوج الحي)
    try:
        price = float(product.get('price') or 0)
    except (TypeError, ValueError):
        price = 0.0
    luxury = price >= 675
    if luxury:
        # الفاخر يميل للمناسبات والحضور لا للاستهلاك اليومي العابر
        occ = occ | {'occasion'}

    # عطر حلو جداً وخفيف = شبابي حتى لو صنّفته العائلة أكبر
    if sweet >= 0.8 and intensity <= 0.6:
        band = 'young'

    return {
        'family': fam or 'unknown',
        'age': band,
        'occ': occ,
        'intensity': round(intensity, 3),
        'sweet': round(sweet, 3),
        'fresh': round(fresh, 3),
        'gender': product.get('g', 'مشترك'),
        'luxury': luxury,
        'niche': niche,
        'ptype': ptype,
    }


# ═══════════════════════════════════════════════════════════════════════
#  بروفايل الشخصية — نمط حياتها (المناسبات التي تعيشها) + سماحيتها
#  ANY = تتسامح مع كل المناسبات (خبراء/متذوّقون/مؤثرون/مقارنون/مشترو هدايا)
# ═══════════════════════════════════════════════════════════════════════
_ANY_OCC = {'daily', 'work', 'formal', 'occasion', 'evening', 'romantic', 'sport', 'casual'}

ARCH_OCCASIONS = {
    # رياضيون/ميدان → انتعاش وخفة
    'شاب_رياضي': {'sport', 'daily', 'casual'},
    'عسكري': {'sport', 'work', 'daily'},
    # رسمي/مهني → حضور وثبات
    'رجل_أعمال': {'formal', 'work', 'occasion'},
    'طبيب': {'work', 'formal'}, 'طبيبة': {'work', 'formal'},
    'مهندس': {'work', 'daily'}, 'موظف': {'work', 'daily'}, 'موظفة': {'work', 'daily'},
    'معلم': {'work', 'daily'}, 'معلمة': {'work', 'daily'},
    # رومانسي/مناسبة
    'عروس': {'romantic', 'occasion', 'evening'},
    # تقليدي/عود/مجالس
    'وجيه': {'formal', 'occasion', 'evening'}, 'بدوي': {'formal', 'occasion'},
    'كبير_سن': {'formal', 'occasion', 'daily'}, 'جدة': {'occasion', 'daily', 'formal'},
    'أم_سعودية': {'occasion', 'formal', 'daily'}, 'ست_بيت': {'occasion', 'daily'},
    'أب_عائلة': {'daily', 'occasion', 'work'},
    # شبابي/كاجوال
    'شاب_جامعي': {'casual', 'daily'}, 'طالبة': {'casual', 'daily'},
    'بنت_عصرية': {'casual', 'romantic', 'daily', 'evening'},
    'عامل_مقتصد': {'daily', 'work'},
}
# شخصيات متسامحة (تكتب عن أي عطر بمنطق: يجرّب/يقارن/يحلّل/يهدي)
_TOLERANT = {
    'خبير_عطور', 'متذوق_نيش', 'خبيرة_تجميل', 'متذوقة_نيش', 'مؤثرة',
    'محب_تسوق', 'محبة_تسوق', 'مقارن', 'مبتعث',
    'هدايا_رجل', 'هدايا_أنثى',              # يشترون لغيرهم → لا مطابقة عمر
    'كسول', 'كسولة', 'ناقد_صريح', 'ناقدة_صريحة',
    'متردد', 'مترددة', 'متذمر_لطيف', 'متذمرة_لطيفة',
}
_GIFT = {'هدايا_رجل', 'هدايا_أنثى'}


def _arch_id(arch):
    return arch.get('id', arch.get('archId', ''))


def persona_profile(arch):
    """بروفايل الشخصية: id · جنس · فئة عمرية · مناسبات الحياة · تسامح."""
    aid = _arch_id(arch)
    tolerant = aid in _TOLERANT
    occ = _ANY_OCC if tolerant else ARCH_OCCASIONS.get(aid)
    if occ is None:
        # افتراض ذكي من العمر عند غياب الشخصية من الجدول
        band = age_band(arch.get('age'))
        occ = {'daily', 'work'} if band in ('adult', 'mature') else {'daily', 'casual'}
    return {
        'id': aid,
        'gender': arch.get('g', 'male'),
        'age': age_band(arch.get('age')),
        'occ': set(occ),
        'tolerant': tolerant,
        'gift': aid in _GIFT,
    }


# ═══════════════════════════════════════════════════════════════════════
#  خوارزمية الـ Scoring
# ═══════════════════════════════════════════════════════════════════════
DEFAULT_THRESHOLD = 0.45

# أوزان المكوّنات (تُجمع إلى 1.0)
_W_AGE = 0.45
_W_OCC = 0.45
_W_LUX = 0.10


def _age_fit(pp, fp):
    """ملاءمة عمرية 0..1 مع عقوبات لا متماثلة للحالات السخيفة."""
    dist = abs(_BAND_IDX[pp['age']] - _BAND_IDX[fp['age']])
    fit = max(0.0, 1.0 - dist / 3.0)          # مسافة 0→1، 3→0
    # سخافة صارخة: عطر حلويات (sweet عالٍ) على ناضج/كبير
    if fp['sweet'] >= 0.75 and _BAND_IDX[pp['age']] >= 3:
        fit *= 0.15
    # عطر عود/جلود ثقيل جداً على شاب صغير غير تقليدي: تخفيف طفيف فقط
    # (العود مقبول ثقافياً لكل الأعمار في الخليج) — لا عقوبة قاسية
    return fit


def _occ_fit(pp, fp):
    """ملاءمة المناسبة 0..1 مع قواعد تعارض صريحة."""
    if pp['tolerant']:
        return 1.0
    p_occ, f_occ = pp['occ'], fp['occ']
    overlap = p_occ & f_occ

    # تعارض صريح 1: رياضي × عطر ثقيل للمناسبات فقط (لا انتعاش)
    if 'sport' in p_occ and fp['fresh'] < 0.3 and fp['intensity'] >= 0.75 \
            and not (f_occ & {'sport', 'daily'}):
        return 0.12
    # تعارض صريح 2: مهني/دوام × عطر حلو كاجوال صرف (سهرات مراهقين)
    if p_occ <= {'work', 'formal', 'daily'} and fp['sweet'] >= 0.8 \
            and f_occ <= {'casual', 'romantic', 'evening'}:
        return 0.25

    if overlap:
        # تداخل جزئي→كامل
        return min(1.0, 0.6 + 0.4 * len(overlap) / max(1, len(f_occ)))
    return 0.3   # لا تداخل: ملاءمة ضعيفة لكن ليست صفراً


def _lux_fit(pp, fp):
    """ملاءمة الفخامة: مكافأة خفيفة عند تطابق الفخامة مع المناسبة/التقليد."""
    if not fp['luxury']:
        return 0.6
    # الفاخر يلبق بالرسمي/المناسبات والمتذوّقين؛ أقل للرياضي/الكاجوال الصرف
    if pp['tolerant'] or (pp['occ'] & {'formal', 'occasion', 'evening', 'romantic'}):
        return 1.0
    return 0.4


def score(arch, product):
    """درجة ملاءمة (شخصية/منتج) في [0,1]. أعلى = أنسب."""
    pp = persona_profile(arch)
    fp = profile_product(product)
    # مشترو الهدايا يشترون لغيرهم → لا مطابقة عمر/مناسبة (تفويض للمنطق الأعلى)
    if pp['gift']:
        return 0.85
    s = _W_AGE * _age_fit(pp, fp) + _W_OCC * _occ_fit(pp, fp) + _W_LUX * _lux_fit(pp, fp)
    # مكافأة المتذوّق للعطور النيش (تعزّز الواقعية)
    if pp['tolerant'] and fp['niche']:
        s = min(1.0, s + 0.05)
    return round(s, 4)


def is_valid_pair(arch, product, threshold=DEFAULT_THRESHOLD):
    """هل الثنائي منطقي؟ يُرجع (bool, درجة, سبب-عند-الرفض)."""
    s = score(arch, product)
    if s >= threshold:
        return True, s, ''
    pp = persona_profile(arch)
    fp = profile_product(product)
    reason = _reject_reason(pp, fp)
    return False, s, reason


def _reject_reason(pp, fp):
    """سبب مقروء للرفض (للتشخيص/التقارير)."""
    if fp['sweet'] >= 0.75 and _BAND_IDX[pp['age']] >= 3:
        return 'عطر حلويات شبابي على شخصية ناضجة/كبيرة'
    if 'sport' in pp['occ'] and fp['intensity'] >= 0.75 and fp['fresh'] < 0.3:
        return 'عطر ثقيل للمجالس على شخصية رياضية تريد انتعاشاً'
    if not (pp['occ'] & fp['occ']):
        return 'تعارض في المناسبة بين نمط حياة الشخصية وطبيعة العطر'
    return 'ملاءمة ديموغرافية ضعيفة'


def filter_pool(arch, pool, min_keep=8, threshold=DEFAULT_THRESHOLD):
    """يهذّب مجمّع المنتجات ديموغرافياً ثم يُعيد الترشيح.

    - يُبقي كل ما درجته ≥ العتبة.
    - إن قلّ الباقي عن min_keep يُكمل بالأعلى درجةً (تنازلياً) — تدرّج آمن.
    - **لا يُرجع قائمة فارغة أبداً** كي لا يكسر التوليد/السلة.
    يحافظ على تنوّع كافٍ كي تعمل خلطة blend_selection (كربتك/ترند) فوقه.
    """
    if not pool:
        return pool
    scored = [(score(arch, p), p) for p in pool]
    good = [p for sc, p in scored if sc >= threshold]
    if len(good) >= min_keep:
        return good
    scored.sort(key=lambda x: x[0], reverse=True)
    topped = [p for _, p in scored[:max(min_keep, 3)]]
    return topped or list(pool)


# ═══════════════════════════════════════════════════════════════════════
#  توجيه واعٍ بالمنتج — يعكس الاستخدام الحقيقي المطابق لطبيعة العطر
#  (يُمرَّر مع تنبيهات البرومبت — قصير وحاسم كي لا يُهمَل)
# ═══════════════════════════════════════════════════════════════════════
# كل فئة توجيه لها عدة صياغات مكافئة — تُختار عشوائياً بدل جملة ثابتة واحدة.
#
# الخلل الذي تُصلحه هذه القائمة: كانت كل فئة جملة واحدة حرفية ثابتة، والدفعة
# الواحدة لشخصية ما تحتوي غالباً عدة منتجات من نفس النوع (هذا المتجر يبيع
# سلاسل عطور متشابهة مثل «كربتك إكستري أودي» بعدة أسماء) — فكانت كل منتجات
# الدفعة تصل بنفس السطر الحرفي، فيتقارب النموذج لنفس الفكرة والمفردات عبر
# منتجات مختلفة لنفس العميل (رُصد فعلياً: «ترتيب المكتب الصباح» تكرر 3 مرات
# عبر 3 منتجات مختلفة لشخصية واحدة). التنويع هنا لا يغيّر التوجيه الدلالي،
# فقط يمنع الصياغة الحرفية المتماثلة.
_DIRECTIVE_VARIANTS = {
    'oud_heavy': [
        "هذا عود/بخور تقليدي ثقيل للمجالس والعزايم والمناسبات المسائية — "
        "عبّر عن فخامته وثباته على الثوب/العباية وحضوره في المجلس، "
        "ولا تصفه كعطر يومي خفيف عابر.",
        "عود ثقيل تقليدي مكانه المجالس والعزايم لا الاستخدام العابر — "
        "ركّز على تشبّعه بالثوب وبقاء ريحته في المكان بعد ما تطلع منه.",
        "هذا من عطور المجالس والعزايم المسائية الثقيلة (عود/بخور) — تكلّم عن "
        "حضوره القوي وثباته الطويل، لا عن خفّته أو مناسبته لجو الدوام العادي.",
    ],
    'heavy_luxury': [
        "عطر فخم ثقيل للمناسبات والعزايم والسهرات — عبّر عن ثباته وفخامته "
        "وحضوره، لا تصفه كعطر رياضي خفيف.",
        "هذا عطر مناسبات وسهرات لا عطر يومي — ركّز على قوته وبقائه لساعات "
        "طويلة وأثره على من حولك.",
        "ثقيل وفخم، أقرب لعطر عزومة أو سهرة منه لعطر رياضة أو خفة — "
        "تكلّم عن هذا الحضور القوي بالذات.",
    ],
    'fresh_light': [
        "عطر منعش خفيف مناسب للنهار والدوام والرياضة — عبّر عن الانتعاش "
        "والخفة والراحة، لا تصفه كعطر مناسبات ثقيل.",
        "خفيف ومنعش، عطر استخدام يومي عادي لا عطر مناسبات — ركّز على "
        "الراحة وخفّته على الجسم طول اليوم.",
        "من العطور النهارية الخفيفة (دوام/رياضة/يومي) — تكلّم عن انتعاشه "
        "لا عن ثقله أو فخامته.",
    ],
    'sweet_youthful': [
        "عطر حلو شبابي جذّاب — عبّر عن حلاوته وجاذبيته والكومبلمنتات، "
        "مناسب للخروج والكافيهات والأجواء الشبابية.",
        "حلو وشبابي، من عطور الخروجات والكافيهات لا الدوام الرسمي — "
        "تكلّم عن جاذبيته وردود فعل الناس حوله.",
    ],
    'floral_feminine': [
        "عطر زهري ناعم أنثوي — عبّر عن نعومته ورقّته وأنوثته، مناسب للنهار "
        "والمناسبات النسائية.",
        "زهري ناعم بطابع أنثوي — ركّز على رقّته ونعومته لا على قوته أو ثقله.",
    ],
}


def review_directive(arch, product):
    """سطر توجيه عربي قصير يربط نص التقييم بطبيعة العطر ومناسبته.

    يمنع سخافات مثل «دهن عود معتّق … بنات يجنن للدوام» (يوجّهه للمجالس لا للدوام).
    يُرجع '' للمنتجات غير العطرية (لها تنبيهات خاصة أصلاً).

    يختار عشوائياً بين عدة صياغات مكافئة لكل فئة (انظر _DIRECTIVE_VARIANTS)
    بدل جملة ثابتة واحدة — يمنع تكرار نفس الصياغة الحرفية عبر منتجات دفعة
    واحدة لنفس العميل.
    """
    fp = profile_product(product)
    if fp['ptype'] in ('مكياج', 'معطر_شعر', 'عناية_جسم', 'معطر_جسم'):
        return ''   # لهذه تنبيهاتها الخاصة في الطبقة الأعلى

    # بخور/عود تقليدي ثقيل
    if fp['ptype'] == 'بخور' or (fp['family'] == 'oud' and fp['intensity'] >= 0.85):
        return random.choice(_DIRECTIVE_VARIANTS['oud_heavy'])
    # ثقيل فاخر للمناسبات
    if fp['intensity'] >= 0.75 and fp['fresh'] < 0.3:
        return random.choice(_DIRECTIVE_VARIANTS['heavy_luxury'])
    # منعش خفيف للنهار/الرياضة
    if fp['fresh'] >= 0.7 and fp['intensity'] <= 0.4:
        return random.choice(_DIRECTIVE_VARIANTS['fresh_light'])
    # حلو شبابي
    if fp['sweet'] >= 0.75:
        return random.choice(_DIRECTIVE_VARIANTS['sweet_youthful'])
    # زهري ناعم أنثوي
    if fp['family'] == 'floral' and fp['gender'] == 'نسائي':
        return random.choice(_DIRECTIVE_VARIANTS['floral_feminine'])
    return ''


def audience_summary(product):
    """وصف مقروء لجمهور العطر (للتشخيص والتقارير)."""
    fp = profile_product(product)
    band_ar = {'young': 'شبابي', 'young_adult': 'شباب ناضج', 'adult': 'ناضج',
               'mature': 'كبار', 'senior': 'كبار السن'}
    occ_ar = {'daily': 'يومي', 'work': 'دوام', 'formal': 'رسمي', 'occasion': 'مناسبات',
              'evening': 'مسائي', 'romantic': 'رومانسي', 'sport': 'رياضي', 'casual': 'كاجوال'}
    occ_list = ' · '.join(occ_ar.get(o, o) for o in sorted(fp['occ']))
    weight = 'خفيف' if fp['intensity'] < 0.45 else ('متوسط' if fp['intensity'] < 0.72 else 'ثقيل')
    return f"جمهوره: {band_ar.get(fp['age'], fp['age'])} · مناسبته: {occ_list} · ثقله: {weight}"


if __name__ == '__main__':
    import sys
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
    # عرض ذاتي سريع
    demo_products = [
        {'name': 'دهن عود هندي معتّق', 'g': 'مشترك', 'price': 900, 'scent_family': 'oud',
         'ingredients': 'عود، عنبر، مسك، زعفران', 'category': 'عطور النيش', 'product_type': 'بخور'},
        {'name': 'عطر حلو فانيلا', 'g': 'نسائي', 'price': 180, 'scent_family': 'gourmand',
         'ingredients': 'فانيلا، شوكولاتة، كراميل', 'category': 'عطور نسائية جذابة', 'product_type': 'عطر'},
        {'name': 'عطر حمضيات منعش', 'g': 'رجالي', 'price': 150, 'scent_family': 'citrus',
         'ingredients': 'برغموت، ليمون، نعناع', 'category': 'عطور رجالية', 'product_type': 'عطر'},
    ]
    personas = [
        {'id': 'كبير_سن', 'g': 'male', 'age': (55, 75), 'prefers': ['رجالي', 'مشترك']},
        {'id': 'شاب_رياضي', 'g': 'male', 'age': (20, 30), 'prefers': ['رجالي', 'مشترك']},
        {'id': 'بنت_عصرية', 'g': 'female', 'age': (20, 28), 'prefers': ['نسائي', 'مشترك']},
    ]
    for prod in demo_products:
        print('\n■', prod['name'], '—', audience_summary(prod))
        for per in personas:
            ok, sc, why = is_valid_pair(per, prod)
            mark = '✅' if ok else '❌'
            print(f'   {mark} {per["id"]:12} score={sc:.2f}  {why}')
