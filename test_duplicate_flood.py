# -*- coding: utf-8 -*-
"""حارس انحداري شامل: مسار التوليد الحيّ لا يغرق الأرشيف بنص واحد مكرر.

يعيد إنتاج الشرط الذي أنتج «ريحته حلوة وثابتة» 151 مرة من 500 مدخلة: نموذج
يميل لبادئة واحدة في نصوص خام مختلفة، مع len_target قصير يقصّها كلها لنفس
الثلاث كلمات. الفحص القديم (على الخام) كان يمرّرها جميعاً.

النموذج هنا مموّه بالكامل (بلا شبكة ولا مفتاح) ويستجيب لتلميح إعادة المحاولة
كما يفعل نموذج حقيقي: يغيّر البادئة عند الطلب.
"""
import json
import unittest.mock as mock

import anti_repeat as ar


# نصوص خام مختلفة تماماً لكن أول ثلاث كلمات فيها واحدة — تنهار للنص نفسه بعد القصّ
# تتشارك أول ثلاث كلمات فقط = حدّ القصّ الفعلي هنا (len_target=2 ⇒ _allow=3).
# ما بعدها متباعد عمداً كي تبقى النصوص الخام **غير متشابهة** بمقاييس
# anti_repeat (jaccard منخفض، تداخل bigram = 2 فقط) — فلا يمرّ الاختبار إلا
# إذا كان الفحص على النص النهائي المقصوص فعلاً.
_COLLAPSING = [
    'ريحته حلوة وثابتة ومرة تجنن والكل يسأل عنها',
    'ريحته حلوة وثابتة بشكل خيالي ما توقعته ابدا',
    'ريحته حلوة وثابتة من الصبح لين الليل صدق',
    'ريحته حلوة وثابتة وجربتها كثير ولا خذلتني يوم',
]
# بدائل ببدايات مختلفة — يقدّمها النموذج حين يُطلب منه تغيير البداية
_ALTERNATIVES = [
    'العلبة وصلت مرتبة والتغليف ممتاز مره',
    'جربته بالدوام وعجبني مره وناسبني',
    'العود فيه واضح وقوي يناسب السهرات',
    'الزعفران باين من اول رشة وحلو',
    'خفيف على الجو الحار ومرتاح له',
    'اشتريته هدية لأخوي وعجبه مره',
    'السعر مناسب مقابل الجودة صراحة',
    'يذكرني بعطر قديم كنت استخدمه',
]


class _FakeModel:
    """نموذج مموّه: يميل للبادئة المحروقة، ويغيّرها فقط عند تلميح إعادة المحاولة."""

    def __init__(self):
        self.calls = 0
        self._alt = iter(_ALTERNATIVES)

    def __call__(self, prompt, max_tokens=None, temperature=None, **kw):
        self.calls += 1
        asked_to_change = 'مرفوضة' in prompt or 'مكرر' in prompt
        if asked_to_change:
            text = next(self._alt, 'نص احتياطي مختلف تماما عن السابق')
        else:
            text = _COLLAPSING[self.calls % len(_COLLAPSING)]
        return json.dumps({'rating': 5, 'text': text,
                           'synthetic': True, 'publishable': False},
                          ensure_ascii=False)


def test_live_flask_path_does_not_archive_the_same_text_twice(monkeypatch):
    """المسار الحيّ في app.py: عشرة تقييمات ⇒ عشرة نصوص نهائية متمايزة.

    على السلوك القديم كانت العشرة تُقصّ إلى «ريحته حلوة وثابتة» وتُؤرشف كلها.
    """
    import app as flask_app

    ar.reset_session_texts()
    fake = _FakeModel()
    monkeypatch.setattr(flask_app, '_ai_call', fake)

    persona = flask_app._gen_persona() if hasattr(flask_app, '_gen_persona') else None
    if persona is None:
        from personas_engine import generate_persona
        persona = generate_persona()
    persona['has_typo'] = False

    products = [dict(p) for p in flask_app.PRODUCTS[:10]]
    assert len(products) == 10, 'الكتالوج أقصر من المتوقع'

    finals = []
    for pf in products:
        params = {'pattern': 'longevity', 'pattern_desc': 'عن الثبات',
                  'rating': 5, 'len_target': 2}
        prompt, params = flask_app._make_master_prompt(
            persona, pf['name'], flask_app._used_texts_block(limit=10), product=pf)
        params['len_target'] = 2
        rv = flask_app._write_review(persona, pf, prompt, params)
        finals.append(rv['text'])

    assert len(finals) == 10
    dupes = len(finals) - len(set(finals))
    assert dupes == 0, f'تكرار في المسار الحيّ: {dupes} من 10 — {finals}'
    # والنص المحروق لم يُقبل أكثر من مرة واحدة
    assert finals.count('ريحته حلوة وثابتة') <= 1


def test_archive_write_path_stores_the_checked_text(tmp_path, monkeypatch):
    """ما يُخزَّن في الأرشيف هو النص الذي مرّ بالبوابة حرفياً."""
    import app as flask_app

    ar.reset_session_texts()
    monkeypatch.setattr(flask_app, '_ai_call', _FakeModel())

    from personas_engine import generate_persona
    persona = generate_persona()
    persona['has_typo'] = False
    pf = dict(flask_app.PRODUCTS[0])

    prompt, params = flask_app._make_master_prompt(persona, pf['name'], '', product=pf)
    params['len_target'] = 2
    rv = flask_app._write_review(persona, pf, prompt, params)

    flask_app._archive_review(rv['text'], pf['name'], persona.get('name', ''))
    stored = [r.get('text') for r in ar._load_archive().get('reviews', [])]
    assert rv['text'] in stored
    # النص المخزَّن مطابق لناتج finalize بنفس الحدّ المستخدم في المسار الحيّ
    # (_allow = len_target + سماحية صغيرة — لا len_target نفسه)
    _allow = 2 + 1
    fin = flask_app._make_review_finalizer(persona, _allow)
    assert fin(rv['text']) == rv['text'], 'النص المخزَّن يتغيّر لو أُعيد تمريره — تحويل بعد الفحص'
    assert len(rv['text'].split()) <= _allow
