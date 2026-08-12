# -*- coding: utf-8 -*-
"""نظام العطور الترند — TOP 100 عطر في السوق السعودي"""
import random
import re
from datetime import datetime

# === TOP 50 رجالي ===
TOP_MALE = [
    # (الاسم, البراند, المستوى: 'trend'=5x / 'popular'=3x / 'normal'=1x, الموسم: 'summer'/'winter'/'all')
    ('سوفاج', 'ديور', 'trend', 'all'),
    ('بلو دي شانيل', 'شانيل', 'trend', 'all'),
    ('إيروس', 'فرساتشي', 'trend', 'summer'),
    ('أكوا دي جيو', 'أرماني', 'trend', 'summer'),
    ('لا نوي دي لوم', 'إيف سان لوران', 'trend', 'winter'),
    ('افينتوس', 'كريد', 'trend', 'all'),
    ('توباكو فانيلا', 'توم فورد', 'trend', 'winter'),
    ('ذا ون', 'دولتشي آند غابانا', 'popular', 'winter'),
    ('عود وود', 'توم فورد', 'popular', 'winter'),
    ('بلو سيدوشن', 'أنطونيو بانديراس', 'popular', 'summer'),
    ('إكسبلورر', 'مونت بلانك', 'popular', 'all'),
    ('ليجند', 'مونت بلانك', 'popular', 'all'),
    ('بنتلي فور مين', 'بنتلي', 'popular', 'all'),
    ('لانفكتوس', 'باكو رابان', 'trend', 'summer'),
    ('ون مليون', 'باكو رابان', 'popular', 'winter'),
    ('بربري تاتش', 'بربري', 'normal', 'summer'),
    ('بربري هيرو', 'بربري', 'popular', 'all'),
    ('أرماني كود', 'أرماني', 'popular', 'winter'),
    ('بلاك أوبيوم هوم', 'إيف سان لوران', 'normal', 'winter'),
    ('هوجو بوس بوتلد', 'هوجو بوس', 'popular', 'all'),
    ('بلو فور مين', 'بولغاري', 'normal', 'summer'),
    ('الأمين', 'لطافة', 'popular', 'all'),
    ('رغد', 'لطافة', 'popular', 'winter'),
    ('كمال الأجسام', 'لطافة', 'normal', 'all'),
    ('مسك أهل الخير', 'عبدالصمد القرشي', 'popular', 'all'),
    ('دنهل ديزاير', 'دنهل', 'normal', 'summer'),
    ('جيفنشي جنتلمان', 'جيفنشي', 'popular', 'all'),
    ('فيرساتشي بور هوم', 'فرساتشي', 'normal', 'summer'),
    ('ديلان بلو', 'فرساتشي', 'popular', 'all'),
    ('كارتييه ديكلاريشن', 'كارتييه', 'normal', 'all'),
    ('بيليونير', 'بيليونير', 'popular', 'all'),
    ('خمرة', 'عساف', 'popular', 'winter'),
    ('ريحة عود', 'عساف', 'normal', 'winter'),
    ('انتنس', 'ديور', 'popular', 'winter'),
    ('هوم سبورت', 'ديور', 'normal', 'summer'),
    ('أمواج ريفلكشن', 'أمواج', 'popular', 'all'),
    ('أمواج جبل', 'أمواج', 'normal', 'all'),
    ('جيرلان لوم ايديال', 'جيرلان', 'normal', 'all'),
    ('بوشرون كواتر', 'بوشرون', 'normal', 'all'),
    ('كالفن كلاين إتيرنتي', 'كالفن كلاين', 'normal', 'summer'),
    ('كلايف كريستيان', 'كلايف كريستيان', 'normal', 'winter'),
    ('نيشاني هاكيفات', 'نيشاني', 'popular', 'all'),
    ('جان بول لو ميل', 'جان بول غوتييه', 'popular', 'winter'),
    ('جان بول الترا', 'جان بول غوتييه', 'normal', 'summer'),
    ('فالنتينو أومو', 'فالنتينو', 'normal', 'all'),
    ('بلوغاري مان', 'بولغاري', 'normal', 'summer'),
    ('كارولينا هيريرا 212', 'كارولينا هيريرا', 'popular', 'summer'),
    ('بوس ذا سينت', 'هوجو بوس', 'normal', 'all'),
    ('إسكادا', 'إسكادا', 'normal', 'summer'),
    ('مافي', 'أرماني', 'normal', 'winter'),
]

# === TOP 50 نسائي ===
TOP_FEMALE = [
    ('مس ديور', 'ديور', 'trend', 'all'),
    ('كوكو مادموزيل', 'شانيل', 'trend', 'winter'),
    ('لا في إيه بيل', 'لانكوم', 'trend', 'all'),
    ('بومب شيل', 'فكتوريا سيكريت', 'trend', 'summer'),
    ('بلاك أوبيوم', 'إيف سان لوران', 'trend', 'winter'),
    ('جادور', 'ديور', 'trend', 'all'),
    ('شانيل نمبر 5', 'شانيل', 'popular', 'all'),
    ('غوتشي بلوم', 'غوتشي', 'popular', 'summer'),
    ('نارسيسو رودريغز', 'نارسيسو', 'popular', 'all'),
    ('فلوربومب', 'فيكتور آند رولف', 'popular', 'all'),
    ('مون غوتشي', 'غوتشي', 'normal', 'summer'),
    ('كلوي', 'كلوي', 'popular', 'summer'),
    ('غود غيرل', 'كارولينا هيريرا', 'trend', 'winter'),
    ('دولتشي', 'دولتشي آند غابانا', 'normal', 'summer'),
    ('فري', 'إيف سان لوران', 'popular', 'summer'),
    ('أليان', 'تيري موغلر', 'normal', 'winter'),
    ('لانكوم إيدول', 'لانكوم', 'popular', 'all'),
    ('بربري هير', 'بربري', 'normal', 'all'),
    ('فيرساتشي برايت كريستال', 'فرساتشي', 'popular', 'summer'),
    ('أرماني سي', 'أرماني', 'popular', 'all'),
    ('فاليه', 'لطافة', 'popular', 'all'),
    ('سيدتي', 'لطافة', 'popular', 'winter'),
    ('مسك الختام', 'عبدالصمد القرشي', 'normal', 'winter'),
    ('جيفنشي ايريزيستبل', 'جيفنشي', 'popular', 'all'),
    ('تيفاني آند كو', 'تيفاني', 'normal', 'all'),
    ('باكارا روج 540', 'ميزون فرانسيس', 'trend', 'winter'),
    ('توم فورد بلاك اوركيد', 'توم فورد', 'popular', 'winter'),
    ('مارك جيكوبس ديزي', 'مارك جيكوبس', 'popular', 'summer'),
    ('بولغاري اومنيا', 'بولغاري', 'normal', 'all'),
    ('كارتييه بانتير', 'كارتييه', 'normal', 'all'),
    ('أمواج ايبك وومان', 'أمواج', 'normal', 'all'),
    ('إليزابيث آردن', 'إليزابيث آردن', 'normal', 'summer'),
    ('كالفن كلاين يوفوريا', 'كالفن كلاين', 'normal', 'all'),
    ('بنتلي فور هير', 'بنتلي', 'normal', 'all'),
    ('برادا كاندي', 'برادا', 'popular', 'winter'),
    ('ديور جوي', 'ديور', 'popular', 'summer'),
    ('أرماني مال فيم', 'أرماني', 'normal', 'all'),
    ('شانيل تشانس', 'شانيل', 'popular', 'all'),
    ('غوتشي غيلتي', 'غوتشي', 'normal', 'all'),
    ('رالف لورين', 'رالف لورين', 'normal', 'summer'),
    ('كلينيك هابي', 'كلينيك', 'normal', 'summer'),
    ('إسكادا بورن إن', 'إسكادا', 'normal', 'summer'),
    ('نيشاني أنا', 'نيشاني', 'normal', 'all'),
    ('هيرمس تويلي', 'هيرمس', 'normal', 'all'),
    ('فندي فانتاسيا', 'فندي', 'normal', 'all'),
    ('روبيرتو كافالي', 'روبيرتو كافالي', 'normal', 'all'),
    ('فرساتشي ايروس فيم', 'فرساتشي', 'popular', 'summer'),
    ('جنتل فلويدتي', 'ميزون فرانسيس', 'normal', 'all'),
    ('ميسوني', 'ميسوني', 'normal', 'all'),
    ('بوشرون كواتر فيم', 'بوشرون', 'normal', 'all'),
]

# Weight multipliers
LEVEL_WEIGHTS = {'trend': 5.0, 'popular': 3.0, 'normal': 1.0}

# Season multipliers (applied on top of level weight)
SEASON_BONUS = 1.5  # bonus for in-season perfumes

def _current_season():
    """تحديد الموسم الحالي"""
    month = datetime.now().month
    if month in (6, 7, 8, 9):  # Jun-Sep
        return 'summer'
    elif month in (11, 12, 1, 2):  # Nov-Feb
        return 'winter'
    return 'all'  # Spring/Autumn = neutral

def get_trending(gender='all', limit=20):
    """جلب العطور الترند مع الأوزان"""
    season = _current_season()
    
    if gender == 'male' or gender == 'رجالي':
        pool = TOP_MALE
    elif gender == 'female' or gender == 'نسائي':
        pool = TOP_FEMALE
    else:
        pool = TOP_MALE + TOP_FEMALE
    
    weighted = []
    for name, brand, level, perfume_season in pool:
        weight = LEVEL_WEIGHTS.get(level, 1.0)
        # Season bonus
        if perfume_season == season or perfume_season == 'all':
            weight *= SEASON_BONUS
        weighted.append((name, brand, level, perfume_season, weight))
    
    # Sort by weight descending
    weighted.sort(key=lambda x: -x[4])
    
    return [{'name': w[0], 'brand': w[1], 'level': w[2], 'season': w[3], 'weight': w[4]} 
            for w in weighted[:limit]]

def get_trending_brands():
    """جلب البراندات الترند (للاستخدام في product picker)"""
    brands = set()
    for name, brand, level, season in TOP_MALE + TOP_FEMALE:
        if level in ('trend', 'popular'):
            brands.add(brand)
    return list(brands)

def get_trending_names():
    """جلب أسماء العطور الترند (للبحث في الكتالوج)"""
    names = []
    for name, brand, level, season in TOP_MALE + TOP_FEMALE:
        if level in ('trend', 'popular'):
            names.append(name.lower())
            names.append(brand.lower())
    return list(set(names))

def calculate_product_weight(product_name, product_brand):
    """حساب وزن منتج معين بناءً على الترند"""
    name_lower = product_name.lower()
    brand_lower = product_brand.lower()
    season = _current_season()
    
    for pname, pbrand, level, pseason in TOP_MALE + TOP_FEMALE:
        if pname.lower() in name_lower or pbrand.lower() in brand_lower:
            weight = LEVEL_WEIGHTS.get(level, 1.0)
            if pseason == season or pseason == 'all':
                weight *= SEASON_BONUS
            return weight
    return 1.0  # default

def get_weight_for_product(product):
    """حساب وزن منتج من dict أو (اسم, براند).

    يقبل dict فيه 'name'/'brand' أو tuple/list (name, brand) أو اسم نصي فقط.
    موجود كواجهة متوافقة مع streamlit_app.py.
    """
    if isinstance(product, dict):
        name = product.get('name') or product.get('name_ar') or ''
        brand = product.get('brand') or product.get('brand_ar') or ''
    elif isinstance(product, (tuple, list)):
        name = product[0] if len(product) > 0 else ''
        brand = product[1] if len(product) > 1 else ''
    else:
        name, brand = str(product), ''
    return calculate_product_weight(name, brand)

# ═══════════════════════════════════════════════════════════
#  خلطة "محلي" الذكية التلقائية — تُدمج داخل اختيار المنتجات لكل سلة
#  الهدف: تصدّر كربتك (أولوية قصوى) + أفضل 100 ترند في تقييمات منصة محلي
#  مختلفة في كل مرة (عشوائية مضبوطة) ولا تكشف نمطاً ثابتاً
# ═══════════════════════════════════════════════════════════

# ماركة كربتك (Cryptic) — أولوية قصوى في الحملة
CRYPTIC_KEYS = ('كربتك', 'cryptic', 'كربتيك')


def is_cryptic(product):
    """هل المنتج من ماركة كربتك؟"""
    blob = ((product.get('brand', '') or '') + ' ' + (product.get('name', '') or '')).lower()
    return any(k in blob for k in CRYPTIC_KEYS)


def is_top_trending(product):
    """هل المنتج ضمن أفضل 100 ترند (الأكثر مبيعاً) حسب TOP_MALE/TOP_FEMALE؟"""
    return calculate_product_weight(product.get('name', ''), product.get('brand', '')) > 1.0


def smart_blend(pool, count):
    """خلطة إبداعية مختلفة كل مرة: 1-3 كربتك + غالبية ترند + قليل تمويه.

    تعمل على pool مُفلتر مسبقاً (جنس/سعر/نوع) فلا تكسر تماسك الشخصية.
    ترجع قائمة منتجات مختلطة ومخلوطة الترتيب.
    """
    if not pool:
        return []
    count = max(1, min(count, len(pool)))

    cryptic = [p for p in pool if is_cryptic(p)]
    trending = [p for p in pool if is_top_trending(p) and not is_cryptic(p)]
    c_ids = {id(p) for p in cryptic}
    t_ids = {id(p) for p in trending}
    others = [p for p in pool if id(p) not in c_ids and id(p) not in t_ids]

    selected, used, used_names = [], set(), set()

    def _display_key(p):
        """اسم العرض بعد تطبيع المسافات.

        id(p) وحده يميّز الكائنات لا ما يراه المستخدم: الكتالوج يحوي فعلاً
        منتجات لا تختلف إلا بمسافة مضاعفة (تحقّق مباشر: 3 أزواج بأسماء
        متطابقة بعد التطبيع، 0 متطابقة حرفياً)، والمتصفح يطوي المسافات
        فيعرضها بصورة واحدة — فتظهر «نسختان» من العطر نفسه في سلة واحدة.
        """
        return re.sub(r'\s+', ' ', (p.get('name') or '')).strip()

    def _take(items, k):
        avail = [p for p in items
                 if id(p) not in used and _display_key(p) not in used_names]
        if k <= 0 or not avail:
            return
        for p in random.sample(avail, min(k, len(avail))):
            key = _display_key(p)
            if key in used_names:   # نسخة أخرى بنفس الاسم ظهرت داخل العيّنة نفسها
                continue
            selected.append(p)
            used.add(id(p))
            used_names.add(key)

    # (1) كربتك — أولوية قصوى: 1-3 منتجات، مع ترك خانة واحدة على الأقل لغيرها
    if cryptic:
        _take(cryptic, min(random.randint(1, 3), max(1, count - 1)))

    # نحجز خانة تمويه واحدة (أو اثنتين للسلال الكبيرة) كي لا تبدو السلة إعلاناً صرفاً
    rem = count - len(selected)
    camo_k = 0
    if others and rem >= 2:
        camo_k = 1 if rem <= 3 else random.randint(1, 2)

    # (2) غالبية من أفضل 100 ترند (تأخذ كل المتبقي ما عدا خانات التمويه)
    trend_budget = rem - camo_k
    if trend_budget > 0 and trending:
        _take(trending, trend_budget)

    # (3) تمويه: عدد قليل عشوائي من الباقي
    rem = count - len(selected)
    if rem > 0 and others:
        _take(others, rem)

    # (4) إكمال من أي متبقٍ لو لم تكتمل السعة (مجمع صغير)
    rem = count - len(selected)
    if rem > 0:
        _take(pool, rem)

    random.shuffle(selected)  # ترتيب طبيعي لا يكشف النمط
    return selected


def blend_selection(all_products, pool, count, gender_prefs):
    """الواجهة الموحّدة لاختيار المنتجات: تحقن كربتك المتوافق جنسياً (ولو خارج نطاق
    السعر — لضمان الأولوية القصوى) ثم تطبّق smart_blend على pool المُفلتر.
    """
    merged = list(pool)
    have = {id(p) for p in merged}
    for p in all_products:
        if is_cryptic(p) and id(p) not in have and p.get('g') in gender_prefs:
            merged.append(p)
            have.add(id(p))
    return smart_blend(merged, count)


# Standalone test
if __name__ == '__main__':
    print('✅ Trending loaded')
    print(f'   Season: {_current_season()}')
    print(f'   Male trending: {len(TOP_MALE)}')
    print(f'   Female trending: {len(TOP_FEMALE)}')
    print(f'   Trending brands: {len(get_trending_brands())}')
    top5 = get_trending(limit=5)
    for t in top5:
        print(f'   🔥 {t["name"]} ({t["brand"]}) — weight: {t["weight"]}')
