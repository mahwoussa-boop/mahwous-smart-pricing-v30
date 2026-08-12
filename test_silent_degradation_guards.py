# -*- coding: utf-8 -*-
"""حارس: لا تدهور صامت في المسارات المتبقية بعد جولات إصلاح التفرّد.

جرد مستقل (لا من بلاغ خارجي) لبقايا فئة الخلل نفسها التي طاردتها كل
المراحل: فشل يُبتلَع بلا أثر فتستمر المنظومة بحماية أقل مما تظنّ.

ثلاث فجوات مرصودة:
1. _register كانت `except Exception: pass` — فشل تسجيل النص يعني أن الكلمات
   والبدايات لن تُحرَق (فيتكرر الأسلوب) بلا أي أثر يكشف السبب.
2. _load_archive (المسار الاحتياطي) كانت `except:` عارية — تبتلع حتى
   KeyboardInterrupt/SystemExit، وتعامل الفساد كملف غائب بصمت.
3. أرشفة تقييم المتجر كانت تُسجَّل عند الفشل لكن بلا إعادة محاولة إطلاقاً،
   رغم أن أغلب حالات الفشل عابرة (تزاحم قفل/قفل ملف على وندوز).
"""
import app as flask_app


def test_register_failure_is_reported_not_swallowed(monkeypatch, capsys):
    """انحدار: فشل التسجيل يجب أن يترك أثراً في السجلّ."""
    def _boom(text, persona_name=None):
        raise RuntimeError('فشل مصطنع')

    monkeypatch.setattr(flask_app, 'USE_ANTI_REPEAT', True)
    monkeypatch.setattr(flask_app, 'ar_register_text', _boom)

    flask_app._register('نص تجريبي صالح')   # يجب ألا يرفع — التوليد لا يتوقف
    out = capsys.readouterr().out
    assert '⚠️' in out and 'الحرق' in out, (
        f'فشل التسجيل ابتُلع بلا أثر — لا يمكن تشخيص تكرار الأسلوب لاحقاً: {out!r}')


def test_fallback_archive_loader_does_not_use_a_bare_except():
    """except العارية تبتلع KeyboardInterrupt/SystemExit/MemoryError — لا يجوز
    أن تعود في مسار قراءة بيانات.

    الفحص على شجرة AST لا على النص: البحث النصي يلتقط ذكر النمط داخل
    التوثيق نفسه فيُعطي إيجابية كاذبة.
    """
    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(flask_app._load_archive)))
    handlers = [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)]
    assert handlers, 'التهيئة: يفترض وجود معالج استثناء واحد على الأقل'
    assert all(h.type is not None for h in handlers), (
        'عادت except العارية في قارئ الأرشيف الاحتياطي')


def test_fallback_archive_loader_reports_corruption(tmp_path, monkeypatch, capsys):
    """الفساد لا يُعامَل كملف غائب بصمت في المسار الاحتياطي أيضاً."""
    bad = tmp_path / 'archive.json'
    bad.write_text('{"reviews": [ مقطوع', encoding='utf-8')
    monkeypatch.setattr(flask_app, 'ARCHIVE_FILE', bad, raising=False)

    arc = flask_app._load_archive()

    assert arc == {'reviews': [], 'store_reviews': [], 'personas': []}
    out = capsys.readouterr().out
    assert '⚠️' in out, f'فساد الأرشيف مرّ بصمت في المسار الاحتياطي: {out!r}'


def test_store_review_archiving_retries_a_transient_failure(monkeypatch, capsys):
    """الفشل العابر في أرشفة تقييم المتجر يُعاد محاولته بدل التسليم بلا أرشفة."""
    calls = {'n': 0}

    def _flaky(text, product, persona_name):
        calls['n'] += 1
        if calls['n'] == 1:
            raise OSError('تزاحم عابر على القفل')
        return True

    monkeypatch.setattr(flask_app, 'USE_ANTI_REPEAT', True)
    monkeypatch.setattr(flask_app, 'ar_archive_review', _flaky)

    # نحاكي كتلة الأرشفة كما في _ai_store_review (بنفس منطق إعادة المحاولة)
    saved = None
    import time as _t
    for attempt in range(2):
        try:
            saved = flask_app.ar_archive_review('نص تقييم متجر', 'متجر مهووس', 'شخص')
            break
        except Exception:
            if attempt == 0:
                _t.sleep(0)
                continue

    assert calls['n'] == 2, 'لم تقع محاولة ثانية بعد فشل عابر'
    assert saved is True, 'المحاولة الثانية نجحت لكن النتيجة لم تُعتمَد'


def test_store_review_retry_is_wired_in_the_real_function():
    """تأكيد أن إعادة المحاولة موجودة فعلاً في _ai_store_review لا في الاختبار وحده."""
    import inspect

    src = inspect.getsource(flask_app._ai_store_review)
    assert 'for _attempt in range(2)' in src, (
        'أرشفة تقييم المتجر بلا إعادة محاولة — الفشل العابر يترك النص بلا أرشفة')
