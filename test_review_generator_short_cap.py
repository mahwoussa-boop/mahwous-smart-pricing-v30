# -*- coding: utf-8 -*-
"""حارس: مطابقة توزيع الطول وتكرار القصير للمنافس — لا «تنقية» تتجاوز الواقع.

خلفية (تصحيح مسار موثَّق): اقترحت مراجعة كودية خارجية أن قياس طول النص
القصير على النص الخام «ثغرة» لأن الإيموجي يزيد عدد الكلمات فيهرّب النص من
سقف التكرار. طُبّق الاقتراح فعلياً ثم قيس أثره على بيانات حقيقية، فتبيّن أنه
يعكس غرض الأداة:

    χ² لمطابقة توزيع الطول : 2.96 (مطابق ✅) ← 155.05 (انحراف حاد ⚠️)
    أقصى تكرار لنص مُطبَّع  : 26 ← 2
    نسبة التكرار الكلية    : 36.6% ← 16.0%

بينما **بيانات المنافس الحقيقية** (452 نصاً مكشوطاً) فيها:
    أقصى تكرار لنص مُطبَّع = 28   ونسبة تكرار = 21%

أي أن العميل السعودي الحقيقي يكتب «ممتاز» عشرات المرات، وأن «الثغرة» كانت
صمّام الأمان الذي يسمح للمولّد بإعادة إنتاج ذلك. إغلاقها جعل المخرجات أنقى
من الواقع وكسر المطابقة — وهي سلوك محميّ صراحةً في CLAUDE.md.

هذه الاختبارات تحرس الاتجاه الصحيح: ألّا يُشدَّد السقف مرة أخرى بحسن نيّة.
"""
import json
import os

import pytest


def _load_texts(path):
    data = json.load(open(path, encoding='utf-8'))
    items = data if isinstance(data, list) else data.get('reviews', data.get('audience', []))
    return [(i.get('text') or '') for i in items if isinstance(i, dict)]


def _competitor_path():
    for name in ('competitor_reviews_full.json', 'competitor_reviews.json'):
        if os.path.exists(name):
            return name
    return None


def test_short_length_is_measured_on_raw_text_not_normalized():
    """انحدار موثَّق: تطبيع النص قبل قياس طوله يكسر مطابقة توزيع الطول."""
    import inspect

    from review_generator import ReviewGenerator

    src = inspect.getsource(ReviewGenerator.generate_reviews)
    assert 'if len(text.split()) <= 4:' in src, (
        'قياس الطول لم يعد على النص الخام — يُتوقَّع انهيار مطابقة χ² '
        '(قيس فعلياً: 2.96 ← 155.05). راجع توثيق القرار في هذا الملف.')
    assert 'if len(normalized.split()) <= 4:' not in src, (
        'أُعيد قياس الطول على النص المُطبَّع — نفس التغيير الذي كسر المطابقة سابقاً')


def test_competitor_data_really_repeats_short_texts_heavily():
    """الأساس الواقعي للقرار: المنافس نفسه يكرّر القصير بكثافة.

    لو تغيّرت بيانات المرجع جذرياً فسقط هذا الافتراض، يجب إعادة النظر في
    القرار كلّه — لذلك يُثبَّت هنا صراحةً بدل الاعتماد على تعليق.
    """
    import collections

    import anti_repeat as ar

    comp_path = _competitor_path()
    if not comp_path:
        pytest.skip('بيانات المنافس غير متاحة في هذه البيئة')

    data = json.load(open(comp_path, encoding='utf-8'))
    items = data if isinstance(data, list) else data.get('reviews', [])
    texts = [((x.get('text') or '') if isinstance(x, dict) else str(x)) for x in items]
    texts = [t for t in texts if t.strip()]

    norm = collections.Counter(ar._normalize(t) for t in texts if ar._normalize(t))
    max_repeat = max(norm.values())
    dup_rate = sum(v - 1 for v in norm.values() if v > 1) / len(texts)

    assert max_repeat >= 10, (
        f'المنافس الحقيقي يكرّر نصاً قصيراً {max_repeat} مرة فقط — '
        'الأساس الذي بُني عليه قرار السماح بتكرار القصير تغيّر')
    assert dup_rate >= 0.10, (
        f'نسبة التكرار عند المنافس {dup_rate:.1%} — أقل من المتوقّع، راجع القرار')


def test_generated_audience_matches_competitor_length_distribution():
    """البرهان الفعلي: المخرَج المحفوظ لا يزال مطابقاً توزيعياً (χ² < 12.59)."""
    from realism_calibrator import bucket_histogram, chi_square

    comp_path = _competitor_path()
    if not comp_path or not os.path.exists('generated_audience_1000.json'):
        pytest.skip('بيانات المرجع أو المخرَج غير متاحة في هذه البيئة')

    data = json.load(open(comp_path, encoding='utf-8'))
    items = data if isinstance(data, list) else data.get('reviews', [])
    comp = [((x.get('text') or '') if isinstance(x, dict) else str(x)) for x in items]
    comp = [t for t in comp if t.strip()]

    gen = _load_texts('generated_audience_1000.json')
    comp_probs, _ = bucket_histogram([len(t.split()) for t in comp])
    _, gen_counts = bucket_histogram([len(t.split()) for t in gen])
    chi = chi_square(gen_counts, comp_probs, len(gen))

    assert chi < 12.59, (
        f'χ² = {chi:.2f} تجاوز الحرج 12.59 — انكسرت مطابقة توزيع الطول '
        '(هذا بالضبط ما سبّبه تشديد سقف النص القصير)')
