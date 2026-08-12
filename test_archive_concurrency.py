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
import json
import threading

import anti_repeat as ar


def test_concurrent_archive_review_writes_lose_nothing():
    """عدة كتابات متزامنة عبر archive_review — الأرشيف النهائي يجب أن يحوي
    كل كتابة بلا فقدان، بغض النظر عن توقيت جدولة النظام."""
    ar.reset_session_texts()
    n = 12
    errors = []

    def worker(idx):
        try:
            ar.archive_review(f'نص متزامن {idx}', 'منتج تجريبي', f'شخص{idx}')
        except Exception as e:
            errors.append((idx, e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    assert not errors, f'أخطاء أثناء الكتابة المتزامنة: {errors}'
    arc = json.loads(ar.ARCHIVE_FILE.read_text(encoding='utf-8'))
    texts = {r['text'] for r in arc['reviews']}
    expected = {f'نص متزامن {i}' for i in range(n)}
    missing = expected - texts
    assert not missing, f'كتابات مفقودة تحت التزامن: {missing} (السباق القديم يفقد كتابات)'
    assert len(arc['reviews']) == n, f"توقّعت {n} مدخلة، وُجد {len(arc['reviews'])} — مدخلات مضاعَفة أو مفقودة"


def test_concurrent_archive_batch_writes_lose_nothing():
    """نفس الضمان لمسار الدفعات (archive_batch) — يُستدعى مرة لكل شخصية."""
    ar.reset_session_texts()
    n = 8
    errors = []

    def worker(idx):
        try:
            ar.archive_batch(
                [{'text': f'دفعة {idx}-{j}', 'product': 'م'} for j in range(3)],
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
    texts = {r['text'] for r in arc['reviews']}
    expected = {f'دفعة {i}-{j}' for i in range(n) for j in range(3)}
    missing = expected - texts
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
