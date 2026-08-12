# -*- coding: utf-8 -*-
"""اختبارات محرك المطابقة الديموغرافي — حتمية، بلا شبكة، بلا I/O."""
import demographic_matcher as dm


# ── عيّنات ثابتة ──────────────────────────────────────────────────────
OUD_HEAVY = {'name': 'دهن عود معتّق', 'g': 'مشترك', 'price': 900, 'scent_family': 'oud',
             'ingredients': 'عود، عنبر، مسك، زعفران، دخون', 'category': 'عطور النيش',
             'product_type': 'بخور'}
GOURMAND_SWEET = {'name': 'حلو فانيلا', 'g': 'نسائي', 'price': 180, 'scent_family': 'gourmand',
                  'ingredients': 'فانيلا، شوكولاتة، كراميل', 'category': 'عطور نسائية جذابة',
                  'product_type': 'عطر'}
CITRUS_FRESH = {'name': 'حمضيات منعش', 'g': 'رجالي', 'price': 150, 'scent_family': 'citrus',
                'ingredients': 'برغموت، ليمون، نعناع', 'category': 'عطور رجالية',
                'product_type': 'عطر'}
FLORAL_SOFT = {'name': 'ورد ناعم', 'g': 'نسائي', 'price': 250, 'scent_family': 'floral',
               'ingredients': 'ورد، ياسمين، زهر البرتقال', 'category': 'عطور نسائية نواعم',
               'product_type': 'عطر'}

SENIOR_M = {'id': 'كبير_سن', 'g': 'male', 'age': (55, 75), 'prefers': ['رجالي', 'مشترك']}
SPORT_M = {'id': 'شاب_رياضي', 'g': 'male', 'age': (20, 30), 'prefers': ['رجالي', 'مشترك']}
MODERN_F = {'id': 'بنت_عصرية', 'g': 'female', 'age': (20, 28), 'prefers': ['نسائي', 'مشترك']}
BRIDE_F = {'id': 'عروس', 'g': 'female', 'age': (21, 30), 'prefers': ['نسائي', 'مشترك']}
BIZ_M = {'id': 'رجل_أعمال', 'g': 'male', 'age': (30, 50), 'prefers': ['رجالي', 'مشترك']}
GIFT_M = {'id': 'هدايا_رجل', 'g': 'male', 'age': (25, 45), 'prefers': ['نسائي', 'مشترك']}
CONNAISSEUR = {'id': 'خبير_عطور', 'g': 'male', 'age': (25, 40), 'prefers': ['رجالي', 'مشترك']}


def test_age_band_boundaries():
    assert dm.age_band(20) == 'young'
    assert dm.age_band((18, 23)) == 'young'
    assert dm.age_band(30) == 'young_adult'
    assert dm.age_band((30, 50)) == 'adult'
    assert dm.age_band((55, 75)) == 'senior'
    assert dm.age_band(None) == 'young_adult'   # افتراض آمن (30)


def test_profile_product_families():
    oud = dm.profile_product(OUD_HEAVY)
    assert oud['intensity'] >= 0.85 and oud['age'] == 'mature'
    assert oud['occ'] == {'formal', 'occasion', 'evening'}   # البخور يُثبَّت تقليدياً
    g = dm.profile_product(GOURMAND_SWEET)
    assert g['sweet'] >= 0.8 and g['age'] == 'young'
    c = dm.profile_product(CITRUS_FRESH)
    assert c['fresh'] >= 0.7 and c['intensity'] <= 0.4 and c['age'] == 'young'


def test_profile_unknown_family_is_neutral():
    p = dm.profile_product({'scent_family': 'zzz', 'price': 100})
    assert 0.0 <= p['intensity'] <= 1.0 and p['age'] in dm.AGE_BANDS


def test_score_ordering_senior():
    # الكبير: العود ≫ الحلويات
    assert dm.score(SENIOR_M, OUD_HEAVY) > dm.score(SENIOR_M, GOURMAND_SWEET)


def test_score_ordering_sport():
    # الرياضي: الحمضيات المنعشة ≫ العود الثقيل
    assert dm.score(SPORT_M, CITRUS_FRESH) > dm.score(SPORT_M, OUD_HEAVY)


def test_absurd_pairs_rejected():
    ok, _, why = dm.is_valid_pair(SENIOR_M, GOURMAND_SWEET)
    assert ok is False and why
    ok2, _, why2 = dm.is_valid_pair(SPORT_M, OUD_HEAVY)
    assert ok2 is False and why2


def test_sensible_pairs_accepted():
    assert dm.is_valid_pair(SENIOR_M, OUD_HEAVY)[0] is True
    assert dm.is_valid_pair(SPORT_M, CITRUS_FRESH)[0] is True
    assert dm.is_valid_pair(MODERN_F, GOURMAND_SWEET)[0] is True
    assert dm.is_valid_pair(BRIDE_F, FLORAL_SOFT)[0] is True


def test_gift_persona_is_tolerant():
    # مشتري الهدايا يشتري لغيره → لا يُرفض بالعمر/المناسبة
    s = dm.score(GIFT_M, GOURMAND_SWEET)
    assert s >= 0.7


def test_connoisseur_tolerant():
    # الخبير يكتب عن أي عطر بمنطق التحليل → لا يُرفض
    assert dm.is_valid_pair(CONNAISSEUR, GOURMAND_SWEET)[0] is True
    assert dm.is_valid_pair(CONNAISSEUR, CITRUS_FRESH)[0] is True


def test_filter_pool_never_empty():
    pool = [OUD_HEAVY, GOURMAND_SWEET, CITRUS_FRESH, FLORAL_SOFT]
    out = dm.filter_pool(SENIOR_M, pool, min_keep=2)
    assert isinstance(out, list) and len(out) >= 2
    # حتى مجمّع كله غير مناسب لا يُفرَّغ
    out2 = dm.filter_pool(SENIOR_M, [GOURMAND_SWEET], min_keep=5)
    assert len(out2) >= 1


def test_filter_pool_prefers_appropriate():
    pool = [GOURMAND_SWEET, OUD_HEAVY]      # للكبير: العود أنسب
    out = dm.filter_pool(SENIOR_M, pool, min_keep=1)
    assert out[0] is OUD_HEAVY


def test_filter_pool_empty_input():
    assert dm.filter_pool(SENIOR_M, []) == []


def test_review_directive_matches_nature():
    d_oud = dm.review_directive(SENIOR_M, OUD_HEAVY)
    assert 'مجالس' in d_oud or 'عزايم' in d_oud
    assert 'للدوام' not in d_oud                       # يمنع سخافة «للدوام»
    d_citrus = dm.review_directive(SPORT_M, CITRUS_FRESH)
    assert 'منعش' in d_citrus or 'انتعاش' in d_citrus
    d_sweet = dm.review_directive(MODERN_F, GOURMAND_SWEET)
    assert 'حلو' in d_sweet or 'شبابي' in d_sweet


def test_review_directive_skips_non_perfume():
    makeup = {'name': 'أحمر شفاه', 'product_type': 'مكياج', 'scent_family': '', 'price': 90}
    assert dm.review_directive(MODERN_F, makeup) == ''


def test_review_directive_varies_within_a_batch():
    """انحدار: نفس فئة المنتج كانت تُنتج جملة توجيه ثابتة حرفياً في كل مرة.

    هذا المتجر يبيع سلاسل عطور متشابهة (مثل «كربتك إكستري أودي» بعدة أسماء)،
    فدفعة منتجات شخصية واحدة غالباً تحوي عدة منتجات من نفس الفئة. جملة توجيه
    ثابتة تصل النموذج حرفياً لكل منها فيتقارب لنفس الفكرة والمفردات عبر
    منتجات مختلفة (رُصد فعلياً: عبارة «ترتيب المكتب الصباح» تكررت 3 مرات
    عبر 3 منتجات مختلفة لشخصية واحدة). التنويع العشوائي هو الإصلاح.
    """
    outputs = {dm.review_directive(SENIOR_M, OUD_HEAVY) for _ in range(60)}
    assert len(outputs) > 1, 'جملة التوجيه ثابتة رغم تعدد المحاولات — لا تنويع'
    assert len(outputs) == len(dm._DIRECTIVE_VARIANTS['oud_heavy'])


def test_all_directive_variants_stay_semantically_consistent():
    """كل صياغات فئة واحدة يجب أن تحمل نفس القيد الدلالي (لا سخافة «للدوام» لعود ثقيل)."""
    for variant in dm._DIRECTIVE_VARIANTS['oud_heavy']:
        assert 'مجالس' in variant or 'عزايم' in variant
        assert 'للدوام' not in variant
    for variant in dm._DIRECTIVE_VARIANTS['fresh_light']:
        assert 'منعش' in variant or 'انتعاش' in variant
    for variant in dm._DIRECTIVE_VARIANTS['sweet_youthful']:
        assert 'حلو' in variant or 'شبابي' in variant


def test_audience_summary_readable():
    s = dm.audience_summary(OUD_HEAVY)
    assert 'ثقيل' in s and 'جمهوره' in s
