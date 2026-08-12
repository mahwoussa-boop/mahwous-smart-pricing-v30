# -*- coding: utf-8 -*-
"""حارس وصل الكتل الثلاث + extra_block بمسار التوليد الحيّ (commit 4d80c4e).

خلفية: pick_human_context / build_seo_block / build_temporal_block كانت
معرَّفة ومُختبرة منفردةً لكن **غير موصولة** بـ build_master_prompt، و extra_block
كان «معاملاً ميتاً» يُمرَّر ولا يُستخدم. وصلها 4d80c4e بلا أي اختبار حارس، فأي
إعادة هيكلة قد تعيدها لموت صامت. هذه الاختبارات تقفل الوصل حتمياً.

كلها لا تلمس الشبكة ولا الـ AI — تفحص نص البرومبت المبنيّ فقط.
"""
import personas_engine as pe
from personas_engine import (
    generate_persona,
    generate_review_params,
    build_master_prompt,
    SOCIO_PROFILES,
    _SEO_DUPE_PATTERNS,
)


def test_extra_block_reaches_prompt():
    """extra_block (تنبيهات الهدية/نوع المنتج/cross-sell) يصل فعلاً للبرومبت.

    انحدار: كان معاملاً ميتاً — تمريره ثم إهماله يُفقد محتواه صامتاً.
    """
    p = generate_persona()
    params = generate_review_params(p)
    sentinel = 'SENTINEL_EXTRA_BLOCK_9f3a2b'
    prompt, _ = build_master_prompt(p, 'عطر تجريبي', params, extra_block=sentinel)
    assert sentinel in prompt


def test_human_context_block_wired(monkeypatch):
    """pick_human_context موصولة: عند تفعيل بوابة الـ0.35 تظهر «زاوية شخصية»."""
    arch_with_ctx = next(
        (k for k, v in SOCIO_PROFILES.items() if v.get('life_contexts')), None
    )
    assert arch_with_ctx is not None, 'لا ملف اجتماعي فيه life_contexts — تحقّق يدوي'

    p = generate_persona()
    p['archId'] = arch_with_ctx
    params = generate_review_params(p)
    # تثبيت العشوائية: 0.0 يفتح بوابة 0.35 حتمياً (ويُبقي random.choice سليماً)
    monkeypatch.setattr(pe.random, 'random', lambda: 0.0)

    prompt, _ = build_master_prompt(p, 'عطر تجريبي', params)
    assert 'زاوية شخصية' in prompt


def test_seo_block_wired(monkeypatch):
    """build_seo_block موصولة: نمط مقارنة + عشوائية منخفضة ⇒ «لمسة تسويقية»."""
    dupe_pattern = next(iter(_SEO_DUPE_PATTERNS))  # dupe_compare/comparison/value_focus
    p = generate_persona()
    params = generate_review_params(p)
    params['pattern'] = dupe_pattern
    params['len_target'] = 12  # مساحة كافية، وتتجنّب فرع max_words القديم
    monkeypatch.setattr(pe.random, 'random', lambda: 0.0)  # < dupe_prob=0.55

    prompt, _ = build_master_prompt(p, 'عطر تجريبي', params)
    assert 'لمسة تسويقية' in prompt


def test_wiring_never_crashes_build():
    """الكتلة الموحّدة (تشمل build_temporal_block غير الحتمي) لا تُفجّر البناء أبداً."""
    p = generate_persona()
    for _ in range(30):
        params = generate_review_params(p)
        prompt, _ = build_master_prompt(p, 'عطر تجريبي', params, extra_block='x')
        assert isinstance(prompt, str) and len(prompt) > 0


def test_prompt_never_claims_an_unverified_purchase():
    p = generate_persona()
    params = generate_review_params(p)
    prompt, _ = build_master_prompt(p, 'عطر تجريبي', params)
    assert 'is_verified_purchase' not in prompt
    assert 'شراء موثق' not in prompt
