# -*- coding: utf-8 -*-
"""نظام استخبارات محلي — Algolia Competitive Intelligence"""
import os, json, math, time, requests
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).parent
# مسار ثابت للبيانات — Railway يضبطه على /data ليبقى بين عمليات النشر
DATA_DIR = Path(os.environ.get('DATA_DIR', str(BASE_DIR)))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# === Algolia Config ===
ALGOLIA_APP_ID = 'L41Y35UONW'
ALGOLIA_API_KEY = 'f60e98a284e4b402af626d0dd1fc6cbd'
ALGOLIA_INDEX = 'products_v2_store_view'
ALGOLIA_URL = f'https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query'
ALGOLIA_HEADERS = {
    'X-Algolia-Application-Id': ALGOLIA_APP_ID,
    'X-Algolia-API-Key': ALGOLIA_API_KEY,
    'Content-Type': 'application/json',
}
OUR_STORE_ID = 986119567

# === Cache Config ===
CACHE_FILE = DATA_DIR / 'mahalli_cache.json'
HISTORY_FILE = DATA_DIR / 'mahalli_history.json'
CACHE_TTL_HOURS = 6

# === Top Search Keywords ===
TOP_SEARCHES = [
    'عطر رجالي', 'عطر نسائي', 'ديور سوفاج', 'شانيل',
    'توم فورد', 'مونت بلانك', 'بنتلي', 'كريد افينتوس',
    'نيشاني', 'أرماني', 'بربري', 'عطر عود',
    'عطر مسك', 'عطر هدية', 'عطر فاخر',
]

# ═══════════════════════════════════════════
# Cache Management
# ═══════════════════════════════════════════

def _load_cache():
    """تحميل الكاش المحلي"""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {'last_updated': None, 'our_products': [], 'competitors': {}, 'rankings': {}}

def _save_cache(cache):
    """حفظ الكاش"""
    cache['last_updated'] = datetime.now().isoformat()
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=1)

def is_cache_stale():
    """هل الكاش قديم (أكثر من 6 ساعات)؟"""
    cache = _load_cache()
    if not cache.get('last_updated'):
        return True
    try:
        last = datetime.fromisoformat(cache['last_updated'])
        return datetime.now() - last > timedelta(hours=CACHE_TTL_HOURS)
    except:
        return True

def get_cache_age():
    """عمر الكاش بالدقائق"""
    cache = _load_cache()
    if not cache.get('last_updated'):
        return -1
    try:
        last = datetime.fromisoformat(cache['last_updated'])
        delta = datetime.now() - last
        return int(delta.total_seconds() / 60)
    except:
        return -1

# ═══════════════════════════════════════════
# Algolia API Functions
# ═══════════════════════════════════════════

def _algolia_query(query='', filters='', facet_filters=None, hits_per_page=100, page=0, get_ranking=False):
    """استعلام Algolia عام"""
    body = {
        'query': query,
        'hitsPerPage': hits_per_page,
        'page': page,
        'attributesToRetrieve': [
            'name_ar', 'all_rating', 'purchase', 'price',
            'has_special_price_ranking', 'discount_percentage',
            'objectID', 'store_id', 'store_name', 'store_rating_count_ranking',
            'store_rating_ranking', 'status_ranking'
        ],
    }
    if filters:
        body['filters'] = filters
    if facet_filters:
        body['facetFilters'] = facet_filters
    if get_ranking:
        body['getRankingInfo'] = True
    
    try:
        r = requests.post(ALGOLIA_URL, headers=ALGOLIA_HEADERS, json=body, timeout=15)
        if r.status_code == 200:
            return r.json()
        else:
            print(f'[Algolia Error] HTTP {r.status_code}')
            return None  # None = فشل (نميّزه عن نتيجة فارغة مشروعة)
    except Exception as e:
        print(f'[Algolia Error] {e}')
        return None

def get_our_products(use_cache=True, allow_network=False):
    """جلب منتجاتنا من الكاش أولاً.

    لوحة Streamlit تستدعي هذه الدالة مع كل إعادة رسم؛ لذلك لا يجوز أن يؤدي
    كاش قديم إلى عشرات طلبات الشبكة. التحديث الخارجي محصور في زر التحديث
    الذي يمرر ``allow_network=True`` صراحة.
    """
    cache = _load_cache()
    cached_products = cache.get('our_products', [])
    if use_cache and cached_products:
        return cached_products
    if not allow_network:
        return cached_products
    
    products, page, failed = [], 0, False
    while True:
        result = _algolia_query(
            facet_filters=['store_id:986119567'],
            hits_per_page=100,
            page=page
        )
        if result is None:  # فشل شبكة/مفتاح
            failed = True
            break
        hits = result.get('hits', [])
        if not hits:
            break
        products.extend(hits)
        page += 1
        if page > 50:  # Safety limit
            break

    # graceful degradation: عند الفشل (أو لا نتائج) لا نكتب فوق الكاش الجيد
    if failed or not products:
        cache = _load_cache()
        old = cache.get('our_products', [])
        if old:
            print('[Algolia] using cached our_products (fetch failed/empty)')
            return old
        return products

    # Update cache
    cache = _load_cache()
    cache['our_products'] = products
    _save_cache(cache)

    return products

def get_competitors(product_name, limit=5, use_cache=True, _cache=None, _stale=None,
                    allow_network=False):
    """جلب المنافسين لنفس المنتج.

    _cache / _stale: عند تمريرهما من حلقة (مثل get_priorities) نستخدم كاشاً
    مشتركاً في الذاكرة ولا نقرأ/نكتب الملف لكل منتج — يتجنب آلاف عمليات I/O.
    """
    cache_key = product_name[:30]  # truncate for cache key
    shared = _cache is not None
    cache = _cache if shared else _load_cache()
    stale = _stale if _stale is not None else is_cache_stale()

    if use_cache:
        cached = cache.get('competitors', {}).get(cache_key)
        if cached and (not stale or not allow_network):
            return cached
    if not allow_network:
        return cache.get('competitors', {}).get(cache_key, [])

    result = _algolia_query(
        query=product_name,
        filters='store_id != 986119567',
        hits_per_page=limit,
        get_ranking=True
    )
    if result is None:  # فشل: أعد آخر كاش متاح بدل الكتابة فوقه
        return cache.get('competitors', {}).get(cache_key, [])
    competitors = result.get('hits', [])

    # تحديث الكاش (الملف يُحفظ مرة واحدة من المستدعي عند الكاش المشترك)
    cache.setdefault('competitors', {})[cache_key] = competitors
    if not shared:
        _save_cache(cache)

    return competitors

def get_our_rank(search_query, use_cache=True, allow_network=False):
    """ترتيبنا لكلمة بحث معينة"""
    if use_cache:
        cache = _load_cache()
        cached = cache.get('rankings', {}).get(search_query)
        if cached and (not is_cache_stale() or not allow_network):
            return cached.get('rank'), cached.get('product')
    if not allow_network:
        cached = _load_cache().get('rankings', {}).get(search_query, {})
        return cached.get('rank'), cached.get('product')
    
    result = _algolia_query(query=search_query, hits_per_page=50)
    if result is None:  # فشل: أعد آخر كاش متاح بدل الكتابة فوقه
        cached = _load_cache().get('rankings', {}).get(search_query, {})
        return cached.get('rank'), cached.get('product')

    rank, product = None, None
    for i, h in enumerate(result.get('hits', []), 1):
        if h.get('store_id') == OUR_STORE_ID:
            rank = i
            product = h
            break

    # Update cache
    cache = _load_cache()
    if 'rankings' not in cache:
        cache['rankings'] = {}
    cache['rankings'][search_query] = {'rank': rank, 'product': product}
    _save_cache(cache)
    
    return rank, product

# ═══════════════════════════════════════════
# Priority Engine
# ═══════════════════════════════════════════

def _num(value, default=0.0):
    """تحويل آمن إلى رقم — Algolia والكاش يخزنان قيم all_rating نصوصاً ('0'، '4.7')."""
    if isinstance(value, bool):
        return float(default)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _extract_price(product):
    """استخراج السعر الرقمي من بنية Algolia المتداخلة {'SA': {'SAR': 165.0}} أو رقم مباشر."""
    price = product.get('price', 0)
    if isinstance(price, (int, float)):
        return float(price)
    if isinstance(price, dict):
        # غُص داخل القواميس المتداخلة حتى تجد أول قيمة رقمية
        for v in price.values():
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, dict):
                for vv in v.values():
                    if isinstance(vv, (int, float)):
                        return float(vv)
    return 0.0


def calculate_priority(product, competitors):
    """حساب أولوية المنتج بناءً على الفجوة مع المتصدر"""
    our_rating = product.get('all_rating', {}) or {}
    our_weight = _num(our_rating.get('weight', 0))
    our_count = int(_num(our_rating.get('count', 0)))
    our_avg = _num(our_rating.get('average', 0))

    top_weight = 0
    top_competitor = None
    for c in competitors:
        c_rating = c.get('all_rating', {}) or {}
        c_weight = _num(c_rating.get('weight', 0))
        if c_weight > top_weight:
            top_weight = c_weight
            top_competitor = c
    
    gap = top_weight - our_weight
    
    price = _extract_price(product)
    price_tier = 3 if price > 500 else 2 if price > 200 else 1
    
    priority = (gap * 3) + (price_tier * 1)
    
    return {
        'priority_score': round(priority, 1),
        'our_weight': our_weight,
        'our_count': our_count,
        'our_avg': round(our_avg, 1) if our_avg else 0,
        'top_weight': top_weight,
        'top_competitor': top_competitor,
        'gap': round(gap, 1),
        'price_tier': price_tier,
    }

# أقصى عدد منتجات تُجلب لها بيانات المنافسين (حد لطلبات الشبكة) — الباقي يُتجاهل.
# 40 منتج ≈ 45 ثانية على الكاش البارد (مرة كل 6 ساعات)، ثم ~2 ثانية من الكاش.
MAX_PRIORITY_SCAN = 40

def get_priorities(limit=20, allow_network=False):
    """جلب قائمة الأولويات مرتبة.

    لتفادي آلاف طلبات الشبكة، نقيّم فقط أهم MAX_PRIORITY_SCAN منتج
    (الأكثر تقييمات لدينا) لأنها الأجدر بالمتابعة التنافسية.
    """
    products = [p for p in get_our_products(allow_network=allow_network) if p.get('name_ar')]

    # رتّب حسب عدد تقييماتنا تنازلياً (الأهم أولاً) ثم خذ شريحة محدودة
    def _our_count(p):
        return _num((p.get('all_rating') or {}).get('count', 0))
    products.sort(key=_our_count, reverse=True)
    products = products[:MAX_PRIORITY_SCAN]

    # كاش مشترك: قراءة وحفظ مرة واحدة بدل مرة لكل منتج
    cache = _load_cache()
    stale = is_cache_stale() if allow_network else False
    priorities = []
    for p in products:
        name = p['name_ar']
        competitors = get_competitors(
            name, limit=3, _cache=cache, _stale=stale, allow_network=allow_network
        )
        pri = calculate_priority(p, competitors)
        pri['product'] = p
        pri['name'] = name
        priorities.append(pri)
    _save_cache(cache)  # حفظ واحد

    # Sort by priority score descending
    priorities.sort(key=lambda x: -x['priority_score'])
    return priorities[:limit]

# ═══════════════════════════════════════════
# Daily Quota Calculator
# ═══════════════════════════════════════════

def daily_quota(product, target_weight, days=60, avg_rating=4.7):
    """حساب عدد التقييمات المطلوبة يومياً"""
    current_rating = product.get('all_rating', {}) or {}
    current_weight = _num(current_rating.get('weight', 0))
    needed = target_weight - current_weight
    if needed <= 0:
        return 0
    daily = math.ceil(needed / (days * avg_rating))
    return min(daily, 3)  # max 3/product/day

def generate_daily_plan(allow_network=False):
    """توليد خطة اليوم"""
    priorities = get_priorities(limit=10, allow_network=allow_network)
    plan = []
    total_reviews = 0
    
    for pri in priorities:
        if pri['gap'] <= 0:
            continue
        target = pri['top_weight'] * 1.1  # Target 10% above leader
        quota = daily_quota(pri['product'], target)
        if quota > 0:
            # Determine star distribution (30% = 4 stars for credibility)
            stars_5 = max(1, round(quota * 0.7))
            stars_4 = quota - stars_5
            plan.append({
                'name': pri['name'],
                'quota': quota,
                'stars_5': stars_5,
                'stars_4': stars_4,
                'our_count': pri['our_count'],
                'our_weight': pri['our_weight'],
                'target_weight': round(target, 1),
                'gap': pri['gap'],
                'top_competitor': pri['top_competitor'],
            })
            total_reviews += quota
        if total_reviews >= 15:  # Daily safety limit
            break
    
    return {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'total_reviews': total_reviews,
        'products': plan,
    }

# ═══════════════════════════════════════════
# Safety Rules
# ═══════════════════════════════════════════

SAFETY_RULES = {
    'max_reviews_per_product_per_day': 3,
    'max_products_per_account_per_day': 5,
    'min_days_after_purchase': 2,
    'max_days_after_purchase': 5,
    'four_star_percentage': 30,
    'num_accounts': 5,
}

def get_safety_rules():
    """جلب قواعد السلامة"""
    return SAFETY_RULES

# ═══════════════════════════════════════════
# Ranking Tracker
# ═══════════════════════════════════════════

def track_rankings(allow_network=True):
    """تتبع ترتيبنا لكل كلمات البحث الأهم"""
    results = []
    for query in TOP_SEARCHES:
        rank, product = get_our_rank(query, use_cache=False, allow_network=allow_network)
        results.append({
            'query': query,
            'rank': rank,
            'product_name': product.get('name_ar', '') if product else None,
        })
    
    # Save to history
    _save_history(results)
    return results

def _save_history(rankings):
    """حفظ التاريخ"""
    history = _load_history()
    entry = {
        'timestamp': datetime.now().isoformat(),
        'rankings': rankings
    }
    history.append(entry)
    # Keep last 100 entries
    if len(history) > 100:
        history = history[-100:]
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=1)

def _load_history():
    """تحميل التاريخ"""
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

def get_history():
    """جلب تاريخ الترتيب"""
    return _load_history()

# ═══════════════════════════════════════════
# Summary for Dashboard
# ═══════════════════════════════════════════

def get_dashboard_summary(allow_network=False):
    """ملخص لوحة التحكم من الكاش، من دون طلبات شبكة افتراضية."""
    products = get_our_products(allow_network=allow_network)
    plan = generate_daily_plan(allow_network=allow_network)
    
    active_products = len([p for p in products if _num((p.get('all_rating', {}) or {}).get('count', 0)) > 0])
    
    return {
        'total_products': len(products),
        'active_products': active_products,
        'daily_reviews_needed': plan['total_reviews'],
        'plan': plan,
        'cache_age_minutes': get_cache_age(),
        'safety_rules': SAFETY_RULES,
    }

def refresh_all_data():
    """تحديث كل البيانات من Algolia"""
    # لا نمسح الكاش قبل نجاح الشبكة، حتى تبقى اللوحة قابلة للاستخدام عند فشل التحديث.
    products = get_our_products(use_cache=False, allow_network=True)
    rankings = track_rankings(allow_network=True)
    
    return {
        'products_count': len(products),
        'rankings_count': len(rankings),
        'timestamp': datetime.now().isoformat(),
    }

# Standalone test
if __name__ == '__main__':
    print('✅ Mahalli Intel loaded')
    print(f'   Cache stale: {is_cache_stale()}')
    print(f'   Cache age: {get_cache_age()} minutes')
    print(f'   Top searches: {len(TOP_SEARCHES)}')
    print(f'   Safety rules: {SAFETY_RULES}')
