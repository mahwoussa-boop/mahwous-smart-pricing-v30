# -*- coding: utf-8 -*-
"""حارس: ردود المحادثات (/api/generate-thread) تُحفظ في الأرشيف المشترك.

بلاغ مراجعة كودية خارجية مُتحقَّق منه مباشرة: كانت الردود تُسجَّل عبر
_register فقط (anti_repeat._session_norm — ذاكرة العملية الحالية)، بلا أي
استدعاء لـ_archive_review/_archive_batch. برهان المراجعة: is_duplicate
للنص نفسه True داخل نفس العملية، لكن False بعد محاكاة عملية Gunicorn أخرى
بذاكرة جديدة (الإنتاج الفعلي: --workers 2 في Procfile/render.yaml) — أي
عامل آخر أو إعادة تشغيل يمكن أن يقبل الرد نفسه مرة أخرى دون أن يعرف أنه
استُخدم، لأن archive.json (المصدر المشترك بين العمليات) لم يكن يتحدّث.
"""
import anti_repeat as ar


def test_thread_reply_persists_to_shared_archive(monkeypatch):
    """رد محادثة واحد يصل الأرشيف المشترك، لا ذاكرة العملية وحدها."""
    import app as flask_app

    ar.reset_session_texts()
    monkeypatch.setattr(flask_app, '_ai_call', lambda *a, **kw: 'رد تجريبي فريد للمحادثة')

    client = flask_app.app.test_client()
    resp = client.post('/api/generate-thread', json={
        'product_name': 'عطر تجريبي',
        'main_review': 'ريحته حلوة جداً وثابتة طول اليوم',
        'reply_count': 1,
    })
    assert resp.status_code == 200, resp.get_json()

    stored = [r.get('text') for r in ar._load_archive().get('reviews', [])]
    assert 'رد تجريبي فريد للمحادثة' in stored, (
        'رد المحادثة لم يصل archive.json — لا يزال محبوساً في ذاكرة العملية')


def test_thread_reply_visible_across_a_fresh_process_simulation(monkeypatch):
    """يعيد إنتاج برهان المراجعة بالضبط: True داخل العملية، ثم يبقى True
    حتى بعد محاكاة عملية Gunicorn ثانية بذاكرة جديدة (الأرشيف هو المصدر
    المشترك الوحيد بين عمليات منفصلة — لا تشترك في _session_norm)."""
    import app as flask_app

    ar.reset_session_texts()
    monkeypatch.setattr(flask_app, '_ai_call', lambda *a, **kw: 'رد آخر مميز جداً للمحادثة')

    client = flask_app.app.test_client()
    resp = client.post('/api/generate-thread', json={
        'product_name': 'عطر آخر',
        'main_review': 'تقييم رئيسي مختلف تماماً عن غيره',
        'reply_count': 1,
    })
    assert resp.status_code == 200, resp.get_json()

    # محاكاة عامل Gunicorn آخر: ذاكرة جلسة جديدة تماماً (لا _session_norm
    # مشتركة بين عمليات منفصلة) — المصدر الوحيد المشترك هو archive.json
    ar.reset_session_texts()
    assert ar.is_duplicate('رد آخر مميز جداً للمحادثة') is True, (
        'العامل الآخر لا يرى الرد كمكرر — قد يُعاد توليده وقبوله من جديد')
