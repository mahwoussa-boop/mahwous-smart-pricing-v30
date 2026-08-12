# -*- coding: utf-8 -*-
"""اختبارات معاير الواقعية وخيط الطول المعاين عبر المنظومة.

تحرس: قراءة الكشط الشامل، سقف المسار الحي، توجيه الطول في البرومبت،
وحقن len_target في generate_review_params — قلب «التوحيد الشامل».
"""
import random

import anti_repeat as ar
import realism_calibrator as rc
from personas_engine import generate_persona, generate_review_params, build_master_prompt


def test_pool_loads_full_corpus():
    """البركة تُقرأ من competitor_reviews_full.json — 452 نصًّا لا 263."""
    pool = rc.load_length_pool()
    assert len(pool) == 452


def test_sample_respects_live_cap():
    """سقف المسار الحي يقصّ الذيل النادر (63-89 كلمة)."""
    pool = rc.load_length_pool()
    random.seed(1)
    for _ in range(2000):
        assert 1 <= rc.sample_target_length(pool, cap=rc.LIVE_MAX_TARGET) <= rc.LIVE_MAX_TARGET


def test_length_directive_shapes():
    assert 'كلمة واحدة' in rc.length_directive(1)
    assert '4' in rc.length_directive(4)
    assert 'جملة عفوية واحدة' in rc.length_directive(7)
    assert 'جملتان' in rc.length_directive(20)


def test_review_params_carry_len_target():
    random.seed(2)
    p = generate_persona()
    for _ in range(50):
        params = generate_review_params(p)
        t = params.get('len_target')
        assert t is not None and 1 <= t <= rc.LIVE_MAX_TARGET


def test_prompt_follows_len_target():
    """البرومبت يتبع الطول المعاين: القصير يُلزم بالقصر والطويل يتحرر من سقف الـ4."""
    p = generate_persona()
    params = generate_review_params(p)

    params['len_target'] = 2
    prompt, _ = build_master_prompt(p, 'عطر تجريبي', params)
    assert 'من 1 إلى 2' in prompt and 'التزم بنفس القصر' in prompt

    params['len_target'] = 12
    prompt, _ = build_master_prompt(p, 'عطر تجريبي', params)
    assert 'حوالي 12' in prompt
    assert 'لا أكثر أبداً' not in prompt
    assert 'للأسلوب واللهجة فقط' in prompt


def test_prompt_fallback_without_target():
    """بلا len_target: السلوك القديم (سقف 4 كلمات) — طريق عودة سليم."""
    p = generate_persona()
    params = generate_review_params(p)
    params.pop('len_target', None)
    prompt, _ = build_master_prompt(p, 'عطر تجريبي', params)
    assert 'لا أكثر أبداً' in prompt


def test_target_distribution_matches_real():
    """توزيع 3000 عيّنة يقارب شرائح المنافسين الحقيقية (تفاوت ±5%)."""
    pool = rc.load_length_pool()
    random.seed(3)
    samples = [rc.sample_target_length(pool, cap=rc.LIVE_MAX_TARGET) for _ in range(3000)]
    share_1 = sum(1 for s in samples if s == 1) / len(samples)
    share_long = sum(1 for s in samples if s >= 5) / len(samples)
    assert abs(share_1 - 0.27) < 0.05      # 27% كلمة واحدة عند المنافسين
    assert abs(share_long - 0.36) < 0.05   # 36% خمس كلمات فأكثر


# ── بنك الأمثلة الحقيقية (الكشط الشامل كمرجعية مفردات) ──

def test_exemplar_pool_loads_real_texts():
    """بركة الأمثلة تُقرأ من الكشط الشامل: نصوص عربية نظيفة بلا تكرار."""
    import re
    pool = rc.load_exemplar_pool()
    assert len(pool) >= 300                     # 351 نصًّا عربيًّا وقت الكتابة
    texts = [t for _, t in pool]
    assert len(texts) == len(set(texts))        # لا تكرار
    assert all(re.search('[؀-ۿ]', t) for _, t in pool[:50])  # عربي فعلي


def test_exemplar_pool_dedupes_after_normalization_not_just_literal():
    """انحدار حرج: بلاغ مراجعة كودية — التطابق الحرفي وحده كان يُبقي
    «اصلي 100%»/«اصلى 100‎%‎» و«جميل جدا»/«جميل جداً»/«جميل جدًا» كأمثلة
    «فريدة» منفصلة، فيصل بعضها معاً لنفس البرومبت كمثالين مختلفين شكلاً
    فقط لا معنى (رُصد فعلياً عبر بذور عشوائية عدّة)."""
    pool = rc.load_exemplar_pool()
    texts = [t for _, t in pool]
    normalized = [ar._normalize(t) for t in texts]
    dup_groups = {}
    for t, n in zip(texts, normalized):
        dup_groups.setdefault(n, []).append(t)
    dups = {n: v for n, v in dup_groups.items() if len(v) > 1}
    assert not dups, f'بقيت مجموعات مكرّرة بعد التطبيع: {list(dups.items())[:3]}'


def test_sampled_exemplars_never_collide_after_normalization():
    """أوسع من الاختبار أعلاه: حتى داخل نداء sample_exemplars واحد، لا
    نصّين مطابقين بعد التطبيع (لا فقط مصدر البركة)."""
    for seed in range(50):
        random.seed(seed)
        ex = rc.sample_exemplars(target_len=2, n=8)
        norms = [ar._normalize(t) for t in ex]
        assert len(set(norms)) == len(norms), (
            f'seed={seed}: أمثلة مكررة بعد التطبيع وصلت لنفس البرومبت: {ex}')


def test_sample_exemplars_biased_to_length():
    """المعاينة تنحاز لشريحة الطول المستهدف: قصير يجاور قصير، طويل يجاور طويل."""
    random.seed(7)
    short = rc.sample_exemplars(target_len=2, n=6)
    long = rc.sample_exemplars(target_len=12, n=6)
    assert short and long
    assert max(len(t.split()) for t in short) <= 5     # قرب 2 كلمة
    assert min(len(t.split()) for t in long) >= 8      # قرب 12 كلمة
    assert len(set(short)) == len(short)               # فريدة داخل النداء


def test_sample_exemplars_fallback_without_corpus():
    """بلا كشط (بركة فارغة): يتدرّج للقائمة الثابتة القديمة — لا يفقد أمثلته."""
    picks = rc.sample_exemplars(target_len=3, n=8, pool=[])
    assert 1 <= len(picks) <= 8
    assert set(picks) <= set(rc._FALLBACK_EXEMPLARS)


def test_master_prompt_injects_real_exemplars():
    """البرومبت المشترك يحقن أمثلة من نصوص المنافسين الحقيقية (لا القائمة الثابتة)."""
    pool_texts = {t for _, t in rc.load_exemplar_pool()}
    p = generate_persona()
    params = generate_review_params(p)
    params['len_target'] = 12
    random.seed(5)
    prompt, _ = build_master_prompt(p, 'عطر تجريبي', params)
    example_lines = {l[2:] for l in prompt.splitlines() if l.startswith('- ')}
    # على الأقل بعض أسطر الأمثلة نصوص فعلية من الكشط الشامل
    assert len(example_lines & pool_texts) >= 3
