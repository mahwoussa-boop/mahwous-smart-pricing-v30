# -*- coding: utf-8 -*-
"""حارس: ذاكرة الكلمات/البدايات المحروقة تتزامن بين عمال Gunicorn.

بلاغ مراجعة كودية خارجية مُتحقَّق منه مباشرة: الإنتاج يعمل بعاملَين
(render.yaml: --workers 2) بذاكرتين منفصلتين، وكانت إعادة بناء ذاكرة الحرق
تقع **مرة واحدة عند استيراد الوحدة فقط**. فعامل بدأ قبل أن يكتب الآخر نصوصاً
يبقى إلى الأبد بـburned_words=[] رغم أن الأرشيف المشترك يحمل الدليل، فيكرّر
الكلمات والافتتاحيات التي حرقها زميله.

البرهان المرصود:
    visible_archive_rows=3 / burned_words=[] / burned_openings=[]
    وبعد المزامنة يدوياً: burned_words=["فخم"] / burned_openings=["ريحته", ...]

فحص النص الكامل (is_duplicate) لم يكن متأثراً — يقرأ الملف في كل استدعاء.
"""
import json

import anti_repeat as ar


def _write_archive_directly(reviews):
    """يكتب في الأرشيف كما لو أن **عملية أخرى** فعلت — بلا لمس ذاكرة هذه العملية."""
    payload = {'reviews': [{'text': t, 'product': 'م', 'persona': 'شخص_آخر', 'ts': i}
                           for i, t in enumerate(reviews)],
               'store_reviews': [], 'personas': []}
    ar.ARCHIVE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
    ar._archive_cache['key'] = None      # يحاكي عملية لم تقرأ الملف بعد
    ar._archive_cache['data'] = None


def test_burn_memory_picks_up_another_workers_writes():
    """انحدار: ما يحرقه عامل آخر يجب أن يظهر في هذا العامل عند التوليد التالي."""
    ar.reset_session_texts()
    w = next(iter(ar.TRACKED_WORDS))
    _write_archive_directly([
        f'هذا عطر {w} جدا ومميز',
        f'العطر ده {w} بجد وعجيب',
        f'صراحة عطر {w} ما توقعته',
    ])

    assert w not in ar.get_burned_words(), 'التهيئة: ذاكرة هذه العملية يجب أن تكون فارغة'

    # نقطة المزامنة الحقيقية: بناء كتلة البرومبت في بداية أي توليد
    ar.format_used_texts_block(limit=30)

    assert w in ar.get_burned_words(), (
        'كلمة حرقها عامل آخر لم تظهر هنا — يتكرر نفس الأسلوب بين العمال')


def test_sync_is_skipped_when_the_archive_did_not_change():
    """ضابط أداء: لا إعادة بناء على القراءات المتكررة بلا كتابة بينها."""
    ar.reset_session_texts()
    _write_archive_directly(['نص أول للمزامنة', 'نص ثان للمزامنة'])

    assert ar.sync_burn_memory_if_archive_changed() is True, 'أول مزامنة بعد تغيّر يجب أن تقع'
    assert ar.sync_burn_memory_if_archive_changed() is False, (
        'أُعيد البناء رغم عدم تغيّر الأرشيف — كلفة بلا فائدة على كل توليد')


def test_resync_does_not_double_count_burn_after_repeat_calls():
    """register_text صارت idempotent — إعادة المزامنة لا تُضاعف عدّادات الحرق
    فتحرق كلمة استُخدمت مرة واحدة فقط."""
    ar.reset_session_texts()
    w = next(iter(ar.TRACKED_WORDS))
    _write_archive_directly([f'عطر {w} وحيد لا يتكرر'])   # استخدام واحد فقط

    for _ in range(5):                    # مزامنات متكررة
        ar._archive_cache['key'] = None
        ar._last_sync['key'] = None
        ar.sync_burn_memory_if_archive_changed()

    assert w not in ar.get_burned_words(), (
        'استخدام واحد صار «محروقاً» بفعل إعادة المزامنة — تضخّم زائف للعدّادات')
