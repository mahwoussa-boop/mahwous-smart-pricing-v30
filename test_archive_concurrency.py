# -*- coding: utf-8 -*-
"""حارس تزامن: كتابات متزامنة على الأرشيف لا تفقد أي تقييم.

بلاغ مراجعة كودية خارجية مُتحقَّق منه: الإنتاج الفعلي يعمل بعاملَي Gunicorn
(Procfile/render.yaml: --workers 2) — عمليتان منفصلتان بذاكرة session منفصلة
تكتبان على archive.json المشترك. قبل _ArchiveLock كانت archive_review/
archive_batch تقرآن الملف، تعدّلانه في الذاكرة، ثم تكتبانه كاملاً — بلا قفل
ولا كتابة ذرّية. محاكاة معزولة (خارج هذا الملف) أثبتت: كتابتان متزامنتان
تنتجان أرشيفاً نهائياً بمدخلة واحدة بدل اثنتين (كتابة مفقودة كلاسيكية).

الخيوط لا العمليات المنفصلة: قفل الملف (O_CREAT|O_EXCL) كائن نظام ملفات
حقيقي يعمل بين أي كيانَي تنفيذ يتشاركان القرص — خيطان أو عمليتان سيّان.
عمليات I/O على الملفات تُحرّر GIL فتُنتج تزامناً حقيقياً بين الخيوط، وهو
اختبار حتمي وقابل للتكرار للقفل الفعلي بلا حاجة لعمليات منفصلة (multiprocessing
على وندوز يحتاج spawn وimport معقّد، والخيوط تكفي لاختبار قفل نظام الملفات).
"""
import itertools
import json
import os
import threading
import time

import anti_repeat as ar

# كلمات مختلفة فعلياً لبناء نصوص فريدة — لا الاعتماد على رقم لاحق وحده:
# _normalize يُسقط الأرقام تماماً (ليست عربية)، فـ«نص متزامن 1» و«نص متزامن 2»
# ينهاران لنفس النص المُطبَّع «نص متزامن» ويُعامَلان كمكرَّرين فعلياً — الآن
# بعد أن صارت archive_review/archive_batch تفحصان التكرار وقت الحفظ (إصلاح
# سباق الفحص-ثم-الكتابة)، هذا التطابق حقيقي لا خطأ في القفل. التباديل (لا
# صيغة حسابية يدوية) تضمن عدم تصادم أي فهرسَين ضمن المدى المستخدم.
_WORD_BANK = ['كتاب', 'سيارة', 'طاولة', 'حاسوب', 'شجرة', 'بحر', 'جبل', 'نافذة',
             'مصباح', 'كرسي', 'هاتف', 'ساعة', 'مفتاح', 'باب', 'حائط', 'سقف',
             'أرض', 'سماء', 'شمس', 'قمر', 'نجمة', 'غيمة', 'مطر', 'ريح',
             'وردة', 'حديقة', 'شارع', 'مدرسة', 'مستشفى', 'مكتبة']
_PAIRS = list(itertools.permutations(_WORD_BANK, 2))


def _distinct_text(idx):
    a, b = _PAIRS[idx % len(_PAIRS)]
    return f'{a} {b}'


def test_concurrent_archive_review_writes_lose_nothing():
    """عدة كتابات متزامنة عبر archive_review — الأرشيف النهائي يجب أن يحوي
    كل كتابة بلا فقدان، بغض النظر عن توقيت جدولة النظام."""
    ar.reset_session_texts()
    n = 12
    errors = []
    texts = [_distinct_text(i) for i in range(n)]

    def worker(idx):
        try:
            ar.archive_review(texts[idx], 'منتج تجريبي', f'شخص{idx}')
        except Exception as e:
            errors.append((idx, e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    assert not errors, f'أخطاء أثناء الكتابة المتزامنة: {errors}'
    arc = json.loads(ar.ARCHIVE_FILE.read_text(encoding='utf-8'))
    saved = {r['text'] for r in arc['reviews']}
    missing = set(texts) - saved
    assert not missing, f'كتابات مفقودة تحت التزامن: {missing} (السباق القديم يفقد كتابات)'
    assert len(arc['reviews']) == n, f"توقّعت {n} مدخلة، وُجد {len(arc['reviews'])} — مدخلات مضاعَفة أو مفقودة"


def test_concurrent_archive_batch_writes_lose_nothing():
    """نفس الضمان لمسار الدفعات (archive_batch) — يُستدعى مرة لكل شخصية."""
    ar.reset_session_texts()
    n = 8
    errors = []
    batches = [[_distinct_text(idx * 3 + j) for j in range(3)] for idx in range(n)]

    def worker(idx):
        try:
            ar.archive_batch(
                [{'text': t, 'product': 'م'} for t in batches[idx]],
                f'شخص{idx}',
            )
        except Exception as e:
            errors.append((idx, e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    assert not errors, f'أخطاء أثناء الكتابة المتزامنة: {errors}'
    arc = json.loads(ar.ARCHIVE_FILE.read_text(encoding='utf-8'))
    saved = {r['text'] for r in arc['reviews']}
    expected = {t for batch in batches for t in batch}
    missing = expected - saved
    assert not missing, f'كتابات مفقودة تحت التزامن: {missing}'


def test_archive_write_is_atomic_no_partial_file(tmp_path, monkeypatch):
    """الكتابة عبر ملف مؤقت + os.replace — لا نصف ملف حتى لو قرأ قارئ أثناء الكتابة.

    لا نحاكي انهياراً فعلياً (صعب الحتمية)؛ نتحقق أن آلية os.replace مُستخدمة
    فعلاً بدل الكتابة المباشرة فوق الملف (ما كان يعني نافذة زمنية لملف تالف
    لو قرأه قارئ آخر أثناءها، أو انهارت العملية في المنتصف)."""
    import inspect
    src = inspect.getsource(ar._save_archive)
    assert 'os.replace' in src, '_save_archive يجب أن يكتب ذرّياً عبر os.replace'
    assert '.tmp' in src, '_save_archive يجب أن يكتب لملف مؤقت أولاً'


def test_archive_lock_serializes_the_critical_section():
    """حارس بنيوي: archive_review/archive_batch يستخدمان _ArchiveLock فعلياً
    حول دورة القراءة-التعديل-الكتابة، لا حول الكتابة وحدها."""
    import inspect
    for fn in (ar.archive_review, ar.archive_batch):
        src = inspect.getsource(fn)
        assert '_ArchiveLock' in src, f'{fn.__name__} لا يستخدم القفل'
        lock_pos = src.index('_ArchiveLock')
        load_pos = src.index('_load_archive()')
        save_pos = src.index('_save_archive(')
        assert lock_pos < load_pos < save_pos, (
            f'{fn.__name__}: القفل يجب أن يسبق القراءة والكتابة كلتيهما (دورة كاملة محمية)')


def test_streamlit_archive_batch_delegates_to_locked_implementation():
    """بلاغ مراجعة كودية مُتحقَّق منه: streamlit_app كانت تملك archive_batch
    محلية مستقلة (تقرأ archive.json وتكتبه مباشرة، بلا قفل ولا كتابة ذرّية)
    تتجاوز anti_repeat.archive_batch وقفلها كلياً. محاكاة معزولة أثبتت:
    كتابتان متزامنتان عبرها تفقدان إحداهما (مدخل واحد بدل اثنين).

    الإصلاح: صارت تفوّض إلى anti_repeat.archive_batch (مستوردة كـ
    ar_archive_batch) بدل الكتابة المحلية المباشرة."""
    import streamlit_app as st_app
    import inspect
    src = inspect.getsource(st_app.archive_batch)
    assert 'ar_archive_batch' in src, (
        'streamlit_app.archive_batch لا تزال لا تفوّض لتطبيق anti_repeat المحمي بقفل')


def test_streamlit_archive_batch_loses_nothing_under_concurrency():
    """نفس ضمان test_concurrent_archive_batch_writes_lose_nothing لكن عبر
    الدالة التي يستدعيها مسار Streamlit الحيّ فعلياً (gen_reviews)، لا
    anti_repeat.archive_batch مباشرة — يثبت أن الطبقة الوسيطة لا تُعيد فتح
    الثغرة."""
    import streamlit_app as st_app
    ar.reset_session_texts()
    n = 6
    errors = []
    batches = [[_distinct_text(idx * 3 + j) for j in range(3)] for idx in range(n)]

    def worker(idx):
        try:
            st_app.archive_batch(
                [{'text': t, 'product': 'م'} for t in batches[idx]],
                f'شخص{idx}',
            )
        except Exception as e:
            errors.append((idx, e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    assert not errors, f'أخطاء أثناء الكتابة المتزامنة: {errors}'
    arc = json.loads(ar.ARCHIVE_FILE.read_text(encoding='utf-8'))
    saved = {r['text'] for r in arc['reviews']}
    expected = {t for batch in batches for t in batch}
    missing = expected - saved
    assert not missing, f'كتابات مفقودة تحت التزامن عبر مسار Streamlit: {missing}'


def test_check_then_write_race_cannot_insert_two_duplicates():
    """بلاغ مراجعة كودية مُتحقَّق منه: القفل كان يحمي الكتابة فقط، بينما
    is_duplicate يقع قبله وخارجه تماماً. عاملان يفحصان النص نفسه معاً
    (كلاهما «غير مكرر» لأن لا أحد كتب بعد)، ثم يحفظانه بالتتابع تحت القفل:
    القفل يمنع فقدان الكتابة لا تكرار المحتوى.

    الإصلاح: الفحص الأخير صار *داخل* القفل نفسه (is_duplicate(..,
    against_archive_only=True) قبل الكتابة مباشرة) — فحصان متزامنان قد
    يريان كلاهما «غير مكرر» قبل أي كتابة (كالسابق تماماً، هذا لا يتغيّر
    ولا يمكن منعه بلا تنسيق مُسبَق)، لكن عند الكتابة الفعلية تحت القفل لن
    ينجح إلا أحدهما — الآخر يكتشف الأرشيف تغيّر ويرفض الحفظ.
    """
    ar.reset_session_texts()
    text = 'نص التسابق بين الفحص والحفظ يجب ألا يتكرر أبداً مهما حدث'
    barrier = threading.Barrier(2)
    outcomes = {}

    def worker(idx):
        outcomes[f'dup_before_{idx}'] = ar.is_duplicate(text)
        barrier.wait(timeout=5)
        outcomes[f'saved_{idx}'] = ar.archive_review(text, 'منتج', f'شخص{idx}')

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    t1.start(); t2.start()
    t1.join(timeout=10); t2.join(timeout=10)

    arc = json.loads(ar.ARCHIVE_FILE.read_text(encoding='utf-8'))
    matches = [r for r in arc['reviews'] if r['text'] == text]
    assert len(matches) == 1, (
        f'يفترض دخول نسخة واحدة فقط رغم أن كلا الفحصين المسبقين رأى "غير مكرر" — '
        f'وُجد {len(matches)}. outcomes={outcomes}')
    # بالضبط أحد الطلبين قُبل والآخر رُفض عند الكتابة الفعلية
    saved_flags = [outcomes.get('saved_1'), outcomes.get('saved_2')]
    assert sorted(saved_flags) == [False, True], f'outcomes={outcomes}'


def test_archive_batch_catches_duplicates_within_the_same_batch():
    """بلاغ مراجعة كودية مُتحقَّق منه مباشرة: نص متطابق حرفياً يظهر مرتين
    في نفس الدفعة كان يُقبَل مرتين معاً — أوضح ما يظهر على أرشيف غير موجود
    بعد (لا مفتاح كاش لإبطاله، فكل قراءة داخل الحلقة تُرجع قاموساً فارغاً
    جديداً، غير مطّلعة على ما أُضيف للتوّ في نفس الحلقة).

    الإصلاح: الفحص صار مقابل نصوص الدفعة المتراكمة محلياً (بعد الدمج مع
    ما كان محفوظاً مسبقاً) لا مقابل قراءة ملف ثابتة طوال الحلقة.
    """
    ar.reset_session_texts()
    text = 'نص متطابق حرفياً يظهر مرتين في الدفعة نفسها بلا أي فارق'
    assert not ar.ARCHIVE_FILE.exists(), 'الاختبار يفترض أرشيفاً غير موجود بعد (tmp_path نظيف)'

    results = ar.archive_batch(
        [{'text': text, 'product': 'م'}, {'text': text, 'product': 'م'}],
        'شخص1',
    )
    assert results == [True, False], f'يفترض قبول الأول ورفض الثاني كمكرر داخل الدفعة نفسها، وُجد {results}'

    arc = json.loads(ar.ARCHIVE_FILE.read_text(encoding='utf-8'))
    matches = [r for r in arc['reviews'] if r['text'] == text]
    assert len(matches) == 1, f'يفترض نسخة واحدة فقط محفوظة، وُجد {len(matches)}'


def test_abandoned_lock_is_reclaimed_not_permanently_disabled(tmp_path):
    """بلاغ مراجعة كودية مُتحقَّق منه مباشرة: عملية سابقة انهارت وهي تملك
    القفل تترك ملف .lock بلا حذف أبداً. كان السلوك القديم يستسلم بعد
    استنفاد المهلة *بلا حذف الملف العالق* — فكل استدعاء لاحق، للأبد، ينتظر
    المهلة ثم يمضي بلا حماية، معطّلاً إصلاح التزامن (Phase 4) نهائياً.

    الإصلاح: بعد استنفاد المهلة نستعيد القفل بالقوة بدل الاستسلام الدائم.
    مهلة قصيرة (0.3s) هنا فقط لسرعة الاختبار — المنطق مطابق للمهلة الحقيقية.
    """
    archive_file = tmp_path / 'archive.json'
    lock_path = f'{archive_file}.lock'
    open(lock_path, 'w').close()  # يحاكي قفلاً عالقاً من عملية منهارة

    with ar._ArchiveLock(archive_file, timeout=0.3, poll=0.02) as lock:
        assert lock._held is True, (
            'القفل العالق لم يُستعَد — السلوك القديم كان يمضي بلا حماية للأبد')
        assert os.path.exists(lock_path), 'يفترض امتلاك القفل الآن فعلياً'

    assert not os.path.exists(lock_path), (
        'الملف يجب أن يُحذَف بعد الخروج طالما امتُلك القفل فعلياً')


def test_lock_reclaim_actually_restores_protection_for_next_caller(tmp_path):
    """بعد استعادة قفل عالق مرة، الاستدعاء التالي يُحمى بشكل طبيعي — لا يبقى
    النظام «معطَّلاً بشكل متقطّع» بل يُصلح نفسه بالكامل."""
    archive_file = tmp_path / 'archive.json'
    open(f'{archive_file}.lock', 'w').close()

    with ar._ArchiveLock(archive_file, timeout=0.3, poll=0.02):
        pass  # يستعيد وتُحذَف عند الخروج

    # استدعاء ثانٍ فوري: لا قفل عالق بعد الآن، يُمتلَك فوراً بلا انتظار
    t0 = time.monotonic()
    with ar._ArchiveLock(archive_file, timeout=0.3, poll=0.02) as lock2:
        elapsed = time.monotonic() - t0
        assert lock2._held is True
    assert elapsed < 0.2, f'يفترض امتلاكاً فورياً بلا قفل عالق متبقٍّ، استغرق {elapsed:.2f}s'
