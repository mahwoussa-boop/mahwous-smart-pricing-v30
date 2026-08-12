# -*- coding: utf-8 -*-
"""حارس القاعدة الأساسية: يُفحص ما يُخزَّن، لا مخرج الـAI الخام.

الخلل الذي يقفله هذا الملف: كان app.py و streamlit_app.py يفحصان التفرّد على
نص الـAI الخام ثم يقصّانه إلى len_target (وسيطه 1–4 كلمات) ويخزّنان الناتج.
فمخرجات خام مختلفة تماماً تنهار إلى نفس النص بعد القصّ، وكلها تجتاز البوابة.
الأثر الموثّق: «ريحته حلوة وثابتة» 151 مرة في أرشيف من 500 مدخلة.

هذه الاختبارات تفشل على السلوك القديم وتنجح على الجديد.
"""
import anti_repeat as ar
from anti_repeat import is_duplicate, register_text, reset_session_texts
from review_text import finalize_review_text, humanize, write_unique


def _collapsing_call(texts):
    """مولّد وهمي: يرجع نصاً خاماً مختلفاً كل مرة (كلها تنهار لنفس البادئة)."""
    seq = iter(texts)

    def _call(prompt, max_tokens, temperature):
        return next(seq, None)

    return _call


def test_check_runs_on_truncated_text_not_raw(monkeypatch):
    """انحدار جوهري: نصّان خامّان مختلفان يقصّان لنفس الثلاث كلمات ⇒ الثاني مكرر.

    على السلوك القديم كان الفحص يقع على الخام (مختلف ⇒ يمرّ) ثم يُخزَّن المقصوص
    (متطابق) — وهذا بالضبط ما ولّد 151 نسخة من النص نفسه.
    """
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    reset_session_texts()

    finalize = lambda t: finalize_review_text(t, allow_words=3)
    raw_a = 'ريحته حلوة وثابتة ومرة تجنن والكل يسأل عنها'
    raw_b = 'ريحته حلوة وثابتة بشكل خيالي ما توقعته ابدا'

    # الخامّان مختلفان فعلاً، لكن نهايتهما واحدة
    assert raw_a != raw_b
    assert finalize(raw_a) == finalize(raw_b) == 'ريحته حلوة وثابتة'

    register_text(finalize(raw_a))
    assert is_duplicate(finalize(raw_b)) is True, 'النص النهائي المتطابق يجب أن يُرصَد'


def test_write_unique_retries_until_final_text_is_new(monkeypatch):
    """الحلقة تُعيد المحاولة ما دام **النهائي** مكرراً، وتقبل أول نهائي جديد."""
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    reset_session_texts()
    register_text('ريحته حلوة وثابتة')

    seen = []
    finalize = lambda t: finalize_review_text(t, allow_words=3)

    def _is_dup(final_text):
        seen.append(final_text)
        return is_duplicate(final_text)

    rv, final = write_unique(
        call=_collapsing_call([
            'ريحته حلوة وثابتة جدا وتدوم طويلا',   # ⇒ مكرر بعد القصّ
            'ريحته حلوة وثابتة مثل ما قالوا لي',   # ⇒ مكرر بعد القصّ
            'العلبة وصلت مرتبة والتغليف ممتاز جدا',  # ⇒ جديد
        ]),
        parse=lambda raw: {'text': raw} if raw else None,
        finalize=finalize,
        is_dup=_is_dup,
        prompt='p', max_tokens=50, attempts=5,
    )

    assert final == 'العلبة وصلت مرتبة'
    assert rv is not None
    # البوابة رأت النص النهائي في كل محاولة — لا الخام
    assert seen == ['ريحته حلوة وثابتة', 'ريحته حلوة وثابتة', 'العلبة وصلت مرتبة']


def test_write_unique_accepts_duplicate_after_attempts_exhausted(monkeypatch):
    """سلوك مقصود: بعد استنفاد المحاولات يُقبل المكرر (لا فبركة ولا توقف)."""
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    reset_session_texts()
    register_text('ريحته حلوة وثابتة')

    rv, final = write_unique(
        call=_collapsing_call(['ريحته حلوة وثابتة كثيرا'] * 4),
        parse=lambda raw: {'text': raw} if raw else None,
        finalize=lambda t: finalize_review_text(t, allow_words=3),
        is_dup=is_duplicate,
        prompt='p', max_tokens=50, attempts=3,
    )
    assert final == 'ريحته حلوة وثابتة'  # أفضل جهد متاح
    assert rv is not None


def test_write_unique_returns_empty_when_ai_unavailable():
    """تعذّر الـAI كلياً ⇒ (None, '') فيتوقّف المستدعي بدل الفبركة (قانون 4)."""
    rv, final = write_unique(
        call=lambda p, mt, temp: None,
        parse=lambda raw: None,
        finalize=lambda t: t,
        is_dup=lambda t: False,
        prompt='p', max_tokens=50, attempts=3,
    )
    assert rv is None and final == ''


def test_retry_hint_names_rejected_text_and_bans_padding():
    """التلميح يذكر النص المرفوض نفسه ويمنع الحشو والأخطاء المفتعلة."""
    from review_text import _retry_hint
    hint = _retry_hint(1, ['ريحته حلوة وثابتة'])
    assert 'ريحته حلوة وثابتة' in hint
    assert 'لا تحشُ' in hint
    assert 'أخطاء إملائية' in hint


def test_flask_gate_requires_a_finalizer():
    """بنيوي: بوابات app.py لا تقبل الاستدعاء بلا finalize.

    جعله إلزامياً هو ما يمنع العودة الصامتة لفحص الخام — لو صار اختيارياً
    بقيمة افتراضية، عاد الخلل بلا أن يفشل أي اختبار.
    """
    import inspect
    import app as flask_app

    for fn_name in ('_ai_unique_json', '_ai_unique_text', '_ai_write_json'):
        sig = inspect.signature(getattr(flask_app, fn_name))
        assert 'finalize' in sig.parameters, f'{fn_name} فقد معامل finalize'
        assert sig.parameters['finalize'].default is inspect.Parameter.empty, \
            f'{fn_name}.finalize صار اختيارياً — يعيد فتح باب فحص النص الخام'


def test_flask_finalizer_truncates_to_allowed_words():
    """finalizer المسار الحيّ يقصّ فعلاً — فالقصّ داخل ما تفحصه البوابة."""
    import app as flask_app

    fin = flask_app._make_review_finalizer({'has_typo': False}, 3)
    assert fin('ريحته حلوة وثابتة ومرة تجنن والكل يسأل') == 'ريحته حلوة وثابتة'


def test_streamlit_gate_requires_a_finalizer():
    """نفس الإلزام في المدخل الثاني — لا يتباعد المساران."""
    import ast

    src = open('streamlit_app.py', encoding='utf-8').read()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == 'ai_write_unique')
    args = [a.arg for a in fn.args.args]
    assert 'finalize' in args, 'ai_write_unique فقد معامل finalize'
    # بلا قيمة افتراضية: عدد الافتراضيات أقل من موضع finalize
    assert args.index('finalize') < len(args) - len(fn.args.defaults), \
        'finalize صار اختيارياً في streamlit_app'


def test_finalize_is_pure_and_matches_stored_shape():
    """finalize لا يعتمد على AI ويُنتج النص المخزَّن حرفياً (ترقيم/إيموجي/قصّ)."""
    out = finalize_review_text('ريحته حلوة، وثابتة! 🔥 ومرة تجنن', allow_words=3)
    assert out == 'ريحته حلوة وثابتة'
    # idempotent: إعادة التمرير لا تغيّر شيئاً
    assert finalize_review_text(out, allow_words=3) == out
    assert humanize('نص… مع—رموز!') == 'نص مع رموز'
