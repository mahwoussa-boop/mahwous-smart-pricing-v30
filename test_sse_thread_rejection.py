# -*- coding: utf-8 -*-
"""حارس: مسارا البثّ (SSE) والمحادثات يتفاعلان مع رفض الأرشيف.

بلاغ مراجعة كودية خارجية مُتحقَّق منه مباشرة — مساران بقيا يُهملان القيمة
المُرجَعة من الأرشفة بعد إصلاحها في المسارات الأخرى:

1. مسار SSE: كان يبثّ نص كل تقييم للعميل فور كتابته، ثم يؤرشف الدفعة كاملة
   في نهاية الحلقة (_archive_batch) ويهمل نتيجتها. رفضُ عنصر كمكرر تحت
   القفل لا يمنع وصوله — **لا يمكن سحب ما بُثّ أصلاً**. الإصلاح: الأرشفة
   قبل البثّ لكل عنصر، مع إعادة محاولة عند الرفض.
2. مسار المحادثات: _archive_review(text, ...) كانت تُستدعى وتُهمَل نتيجتها،
   فيصل الرد المكرر للمستخدم بصمت.
"""
import anti_repeat as ar
import app as flask_app


def test_thread_reply_retries_instead_of_returning_a_known_duplicate(monkeypatch):
    """تكامل عبر نقطة النهاية الحقيقية /api/generate-thread: عند رفض الأرشفة
    تُعاد المحاولة، فلا يصل الرد المرفوض للمستخدم."""
    if not getattr(flask_app, 'USE_THREADS', False):
        import pytest
        pytest.skip('thread_generator غير متاح في هذه البيئة')

    ar.reset_session_texts()
    collided = 'وانا جربته وطلع ممتاز فعلا وما قصر'
    fresh = 'عندي منه قاروره من زمان وما مليت منها ابدا'
    ar.archive_review(collided, 'عطر تجريبي', 'شخص_سابق')  # عامل آخر حفظه للتوّ
    ar.reset_session_texts()

    # نصوص بديلة متمايزة فعلياً لكل محاولة — الأرقام لا تصلح للتمييز لأن
    # _normalize يُسقطها تماماً (ليست عربية) فتنهار كلها لنص واحد مُطبَّع.
    _WORDS = ['كتاب', 'سيارة', 'طاولة', 'حاسوب', 'شجرة', 'بحر', 'جبل', 'نافذة',
              'مصباح', 'كرسي', 'هاتف', 'ساعة', 'مفتاح', 'باب', 'حائط', 'سقف']
    calls = {'n': 0}

    def _fake_unique_text(prompt, max_tokens, finalize, attempts=4, parser=None):
        calls['n'] += 1
        if calls['n'] % 2 == 1:
            return collided          # المحاولة الأولى لكل رد تصطدم بما حُفظ للتوّ
        i = calls['n'] // 2
        return f'{_WORDS[i % len(_WORDS)]} {_WORDS[(i * 3 + 1) % len(_WORDS)]} فريد'

    monkeypatch.setattr(flask_app, '_ai_unique_text', _fake_unique_text)

    client = flask_app.app.test_client()
    resp = client.post('/api/generate-thread', json={
        'product_name': 'عطر تجريبي',
        'main_review': 'ريحته حلوة والثبات ممتاز',
    })

    assert resp.status_code == 200, f'استجابة غير متوقعة: {resp.status_code}'
    replies = resp.get_json().get('replies', [])
    assert replies, 'لم تُولَّد أي ردود'
    assert all(r['text'] != collided for r in replies), (
        'وصل رد مكرر (رفضه الأرشيف فعلاً) للمستخدم رغم نجاح محاولة بديلة')


def test_sse_path_archives_before_streaming_each_review():
    """انحدار بنيوي: الكود يجب أن يؤرشف قبل بثّ نص التقييم لا بعده.

    فحص بنيوي متعمَّد (لا سلوكي): تشغيل مسار SSE كاملاً يتطلب استدعاء AI
    حقيقياً؛ والخلل هنا **ترتيبي** بطبيعته — أي نص يُبَثّ قبل حسم أرشفته لا
    يمكن سحبه مهما كانت نتيجة الأرشفة لاحقاً.
    """
    import inspect

    src = inspect.getsource(flask_app.api_generate_stream)
    archive_pos = src.find('_archive_review(rv.get')
    stream_pos = src.find("'step': 'review'")

    assert archive_pos != -1, 'مسار SSE لا يؤرشف التقييم المفرد إطلاقاً'
    assert stream_pos != -1, 'تعذّر إيجاد بثّ التقييم في مسار SSE'
    assert archive_pos < stream_pos, (
        'يُبَثّ نص التقييم قبل حسم أرشفته — رفض الأرشيف لن يمنع وصول المكرر')


def test_sse_path_no_longer_defers_archiving_to_a_batch_at_the_end():
    """الدفعة المؤجَّلة في نهاية الحلقة كانت أصل الخلل — يجب ألا تعود."""
    import inspect

    src = inspect.getsource(flask_app.api_generate_stream)
    assert '_archive_batch(' not in src, (
        'عادت أرشفة الدفعة المؤجَّلة بعد البثّ — يستحيل معها منع بثّ المكرر')
