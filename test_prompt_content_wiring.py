# -*- coding: utf-8 -*-
"""حارس وصول الشخصية والنمط وبيانات المنتج للبرومبت فعلاً.

الخلل الذي يقفله هذا الملف: MASTER_PROMPT كان يستقبل سبعة حقول فقط
(اسم المنتج/التقييم/الطول/الأمثلة/الأخطاء/السابق) — بلا **أي** حقل شخصية،
وبلا **أي** بيان منتج غير الاسم، وبلا النمط. فكانت كل الشخصيات تكتب بصوت واحد
عن منتج بلا ملامح، وهذا المولّد المحتوائي للتكرار: بلا ما يميّز، لا يملك
النموذج ما يقوله غير «ريحته حلوة وثابتة».

وتعارضان: قاعدة «لا تذكر اسم المنتج» الثابتة مقابل MENTION_STYLES الميتة
وحقل persona['mention_product']، وعدد الكلمات داخل pattern_desc مقابل
length_rule المعاير.
"""
import personas_engine as pe
from personas_engine import (
    build_master_prompt, build_persona_block, build_product_block,
    generate_persona, generate_review_params, MENTION_STYLES,
)

_PRODUCT = {
    'name': 'عطر لطافة عود مود 100 مل',
    'brand': 'لطافة',
    'price': 69.0,
    'g': 'نسائي',
    'scent_family': 'oriental',
    'ingredients': 'عود، خشب الصندل، زعفران، قرفة، هيل',
    'product_type': 'عطر',
}


def _persona():
    p = generate_persona()
    p.update({'city': 'الرياض', 'dialect_name': 'نجدية (الرياض)',
              'occupation': 'معلم', 'age': 33, 'gender': 'male'})
    return p


def test_persona_identity_reaches_prompt():
    """انحدار: المدينة واللهجة والمهنة والعمر كانت تُولَّد ولا تصل البرومبت."""
    p = _persona()
    params = generate_review_params(p)
    prompt, _ = build_master_prompt(p, _PRODUCT['name'], params, product=_PRODUCT)
    assert 'الرياض' in prompt
    assert 'نجدية' in prompt
    assert 'معلم' in prompt
    assert '33' in prompt


def test_product_catalog_data_reaches_prompt():
    """انحدار: الماركة والعائلة العطرية والمكوّنات كانت في الكتالوج ولا تصل."""
    p = _persona()
    params = generate_review_params(p)
    prompt, _ = build_master_prompt(p, _PRODUCT['name'], params, product=_PRODUCT)
    assert 'لطافة' in prompt
    assert 'شرقية' in prompt          # oriental مُعرَّبة
    assert 'عود' in prompt
    assert 'زعفران' in prompt


def test_pattern_angle_reaches_prompt_without_its_own_word_count():
    """النمط يصل كزاوية، وعدده الخاص يُحذف كي لا يناقض length_rule."""
    p = _persona()
    params = generate_review_params(p)
    params['pattern'] = 'longevity'
    params['pattern_desc'] = 'عن الثبات بكلمتين لأربع'
    params['len_target'] = 12
    prompt, _ = build_master_prompt(p, _PRODUCT['name'], params, product=_PRODUCT)
    assert 'عن الثبات' in prompt
    assert 'بكلمتين لأربع' not in prompt, 'عدد النمط يناقض الطول المعاير'


def test_word_count_stripping_keeps_non_length_numbers():
    """الحذف مقصور على الصيغ العدّية — «5 نجوم» ليست عدد كلمات."""
    assert pe._strip_word_counts('عن الثبات بكلمتين لأربع') == 'عن الثبات'
    assert pe._strip_word_counts('كلمة إلى 3 كلمات') == ''
    keep = '5 نجوم مع شكوى تافهة لزيادة المصداقية'
    assert pe._strip_word_counts(keep) == keep


def test_mention_rule_follows_persona_not_a_fixed_ban():
    """تعارض محلول: القاعدة كانت «لا تذكر اسم المنتج» دائماً رغم mention_product."""
    p = _persona()
    params = generate_review_params(p)
    params['len_target'] = 12

    p['mention_product'] = False
    prompt_off, _ = build_master_prompt(p, _PRODUCT['name'], params, product=_PRODUCT)
    assert 'لا تذكر اسم المنتج' in prompt_off

    p['mention_product'] = True
    prompt_on, _ = build_master_prompt(p, _PRODUCT['name'], params, product=_PRODUCT)
    assert 'لا تذكر اسم المنتج' not in prompt_on
    assert any(style in prompt_on for style in MENTION_STYLES)


def test_short_reviews_never_ask_to_mention_the_name():
    """المساحة الضيقة تمنع ذكر الاسم مهما كان إعداد الشخصية (لا حشو)."""
    p = _persona()
    p['mention_product'] = True
    params = generate_review_params(p)
    params['len_target'] = 3
    prompt, _ = build_master_prompt(p, _PRODUCT['name'], params, product=_PRODUCT)
    assert 'لا تذكر اسم المنتج' in prompt
    assert not any(style in prompt for style in MENTION_STYLES)


def test_prompt_has_no_mention_contradiction():
    """لا يجتمع المنع والطلب في برومبت واحد — عبر مدى واسع من الحالات."""
    for _ in range(40):
        p = generate_persona()
        params = generate_review_params(p)
        prompt, _ = build_master_prompt(p, _PRODUCT['name'], params, product=_PRODUCT)
        bans = 'لا تذكر اسم المنتج' in prompt
        asks = any(style in prompt for style in MENTION_STYLES)
        assert bans != asks, 'تعارض: المنع والطلب معاً (أو غيابهما معاً)'


def test_blocks_degrade_safely_without_data():
    """غياب المنتج أو نقص الشخصية لا يكسر البناء (تدرّج آمن)."""
    assert build_product_block(None) == ''
    assert build_product_block({}) == ''
    assert build_persona_block(None)
    p = generate_persona()
    params = generate_review_params(p)
    prompt, _ = build_master_prompt(p, 'عطر تجريبي', params)  # بلا product
    assert isinstance(prompt, str) and len(prompt) > 0
