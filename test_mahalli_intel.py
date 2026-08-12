# -*- coding: utf-8 -*-
"""اختبارات تطبيع الأنواع في mahalli_intel.

تحرس ضد انحدار تبويب «محلي»: كاش mahalli_cache.json (وAlgolia نفسها)
يخزنان قيم all_rating نصوصاً ('12'، '4.7')، وكانت المقارنات الحسابية
تنفجر بـ «'>' not supported between instances of 'str' and 'int'».
"""
import mahalli_intel as mi


def test_num_safe_coercion():
    """_num يحوّل النصوص والأرقام ويتدرّج إلى الافتراضي عند العجز."""
    assert mi._num('12') == 12.0
    assert mi._num('4.7') == 4.7
    assert mi._num(3) == 3.0
    assert mi._num(2.5) == 2.5
    assert mi._num(None) == 0.0
    assert mi._num('') == 0.0
    assert mi._num('غير رقم') == 0.0
    assert mi._num(True) == 0.0
    assert mi._num('abc', default=7) == 7.0


def test_calculate_priority_with_str_rating():
    """calculate_priority يعمل مع all_rating نصّي كما يأتي من الكاش."""
    product = {
        'name_ar': 'عطر تجريبي',
        'all_rating': {'count': '12', 'average': '4.7', 'weight': '56.4'},
        'price': {'SA': {'SAR': 165.0}},
    }
    competitors = [
        {'all_rating': {'count': '30', 'average': '4.8', 'weight': '80.5'}},
        {'all_rating': {'count': '5', 'average': '4.0', 'weight': '20'}},
    ]
    pri = mi.calculate_priority(product, competitors)
    assert pri['our_count'] == 12
    assert pri['our_weight'] == 56.4
    assert pri['top_weight'] == 80.5
    assert pri['gap'] == 24.1


def _fake_cache():
    """كاش وهمي طازج بقيم نصّية يحاكي mahalli_cache.json الحقيقي."""
    from datetime import datetime
    active = {
        'name_ar': 'عطر تجريبي',
        'all_rating': {'count': '12', 'average': '4.7', 'weight': '56.4'},
        'price': {'SA': {'SAR': 165.0}},
    }
    idle = {
        'name_ar': 'عطر خامل',
        'all_rating': {'count': '0', 'average': '0', 'weight': '0'},
        'price': {'SA': {'SAR': 90.0}},
    }
    competitor = {
        'name_ar': 'عطر منافس',
        'all_rating': {'count': '30', 'average': '4.8', 'weight': '80.5'},
        'store_id': 111,
    }
    weak_competitor = {
        'name_ar': 'منافس صفري',
        'all_rating': {'count': '0', 'average': '0', 'weight': '0'},
        'store_id': 222,
    }
    # ملاحظة: قائمة فارغة [] لا تُعد إصابة كاش في get_competitors (تذهب للشبكة)،
    # لذا يحمل كل منتج منافساً واحداً على الأقل ليبقى الاختبار دون شبكة.
    return {
        'last_updated': datetime.now().isoformat(),
        'our_products': [active, idle],
        'competitors': {'عطر تجريبي': [competitor], 'عطر خامل': [weak_competitor]},
        'rankings': {},
    }


def _patch_cache(monkeypatch):
    cache = _fake_cache()
    monkeypatch.setattr(mi, '_load_cache', lambda: cache)
    monkeypatch.setattr(mi, '_save_cache', lambda c: None)

    def _no_network(*a, **kw):
        raise AssertionError('يجب ألا يلمس الاختبار الشبكة — الكاش طازج')
    monkeypatch.setattr(mi, '_algolia_query', _no_network)


def test_get_priorities_with_str_cache(monkeypatch):
    """الانحدار الأصلي: get_priorities كانت تنفجر على كاش نصّي."""
    _patch_cache(monkeypatch)
    priorities = mi.get_priorities(limit=5)
    assert len(priorities) == 2
    top = priorities[0]
    assert top['name'] == 'عطر تجريبي'
    assert top['our_count'] == 12
    assert top['gap'] == 24.1


def test_get_dashboard_summary_with_str_cache(monkeypatch):
    """الانحدار الأصلي: get_dashboard_summary كانت تنفجر على كاش نصّي."""
    _patch_cache(monkeypatch)
    summary = mi.get_dashboard_summary()
    assert summary['total_products'] == 2
    assert summary['active_products'] == 1
    assert summary['daily_reviews_needed'] >= 1
    assert summary['plan']['products'][0]['name'] == 'عطر تجريبي'


def test_stale_cache_does_not_trigger_network_on_dashboard_load(monkeypatch):
    """الكاش القديم يعرض البيانات المتاحة؛ الشبكة محصورة في التحديث الصريح."""
    from datetime import datetime, timedelta

    cache = _fake_cache()
    cache['last_updated'] = (datetime.now() - timedelta(days=2)).isoformat()
    monkeypatch.setattr(mi, '_load_cache', lambda: cache)
    monkeypatch.setattr(mi, '_save_cache', lambda c: None)

    def _no_network(*args, **kwargs):
        raise AssertionError('تحميل اللوحة لا يجب أن يتصل بالشبكة')

    monkeypatch.setattr(mi, '_algolia_query', _no_network)
    summary = mi.get_dashboard_summary()
    assert summary['total_products'] == 2
    assert summary['cache_age_minutes'] > 60
