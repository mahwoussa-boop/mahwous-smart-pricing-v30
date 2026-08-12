# -*- coding: utf-8 -*-
"""حارس: رفض الأرشيف للنص المكرر يمنع وصوله للمستخدم، لا يُهمَل بصمت.

بلاغ مراجعة كودية خارجية مُتحقَّق منه مباشرة: anti_repeat.archive_review/
archive_batch يرجعان False عند اكتشاف تكرار في الفحص الأخير تحت القفل (إصلاح
سابق) — لكن أغلفة app.py/streamlit_app.py كانت تُهمل القيمة المُرجَعة
دائماً، فيستمر الاستدعاء وكأن الحفظ نجح ويصل النص المكرر للمستخدم بصمت.
البرهان المُعاد إنتاجه: أرشيف يحتوي النص مسبقاً (يحاكي عاملاً آخر حفظه للتو)،
_archive_review يرجع None (أُهمِلت True/False)، والعدد يبقى 1 (الحفظ رُفض
بصحة) بينما المُستدعي (قبل الإصلاح) كان يُعيد النص المكرر دون علم.
"""
import anti_repeat as ar


def test_archive_review_wrapper_now_propagates_the_result(monkeypatch):
    """انحدار مباشر: _archive_review (app.py) كانت تُهمل القيمة المُرجَعة
    من anti_repeat.archive_review (True/False) وتُرجع None دائماً."""
    import app as flask_app

    ar.reset_session_texts()
    text = 'نص أرشفة مختبر للتحقق من انتشار النتيجة'
    ar.archive_review(text, 'منتج', 'شخص_سابق')  # يحاكي عاملاً آخر حفظه للتو
    ar.reset_session_texts()  # الجلسة الحالية لا تعرف عنه

    result = flask_app._archive_review(text, 'منتج', 'شخص_جديد')
    assert result is False, (
        '_archive_review يجب أن يُبلّغ برفض التكرار (False) لا يبتلعه صامتاً')


def test_single_review_retries_instead_of_serving_a_known_duplicate(monkeypatch):
    """تكامل: _ai_single_review تعيد المحاولة عند رفض الأرشفة، فلا تُعيد
    النص المرفوض للمستخدم إن نجحت محاولة بديلة."""
    import app as flask_app

    ar.reset_session_texts()
    collided_text = 'تجربة المتجر مرتبة وسريعة'
    fresh_text = 'العلبة وصلت أنيقة والتغليف محكم جداً'
    ar.archive_review(collided_text, 'منتج', 'شخص_سابق')  # عامل آخر حفظه للتو
    ar.reset_session_texts()

    calls = {'n': 0}

    def _fake_write_review(persona, pf, prompt, params):
        calls['n'] += 1
        # المحاولة الأولى تصطدم بما حفظه "العامل الآخر"؛ الثانية نص جديد فعلاً
        text = collided_text if calls['n'] == 1 else fresh_text
        return {'text': text, 'rating': 5, 'product': pf['name']}

    monkeypatch.setattr(flask_app, '_write_review', _fake_write_review)
    monkeypatch.setattr(flask_app, '_make_master_prompt', lambda *a, **kw: ('p', {}))

    persona = {'name': 'شخص_جديد'}
    product = {'name': 'عطر تجريبي'}
    rv = flask_app._ai_single_review(persona, product)

    assert calls['n'] == 2, 'يفترض محاولة ثانية بعد رفض الأولى كمكرر'
    assert rv['text'] == fresh_text, (
        f'المستخدم استلم نصاً مكرراً ({rv["text"]!r}) رغم نجاح محاولة بديلة')

    arc = ar._load_archive()
    matching = [r for r in arc['reviews'] if r['text'] == collided_text]
    assert len(matching) == 1, 'يفترض بقاء نسخة واحدة فقط من النص المتصادم في الأرشيف'


def test_single_review_falls_back_to_best_effort_if_retry_also_collides(monkeypatch):
    """إن فشلت المحاولة البديلة أيضاً: نُبقي أفضل جهد (لا حظر كامل، لا فبركة) —
    نفس فلسفة استنفاد المحاولات من مراحل سابقة."""
    import app as flask_app

    ar.reset_session_texts()
    collided_text = 'نص يتصادم دائماً مهما أُعيدت المحاولة'
    ar.archive_review(collided_text, 'منتج', 'شخص_سابق')
    ar.reset_session_texts()

    monkeypatch.setattr(flask_app, '_write_review',
                        lambda persona, pf, prompt, params: {'text': collided_text, 'rating': 5})
    monkeypatch.setattr(flask_app, '_make_master_prompt', lambda *a, **kw: ('p', {}))

    persona = {'name': 'شخص_جديد'}
    product = {'name': 'عطر تجريبي'}
    rv = flask_app._ai_single_review(persona, product)

    # لا يُحظر التوليد كاملاً — يُسلَّم أفضل جهد حتى لو مكرراً
    assert rv['text'] == collided_text
