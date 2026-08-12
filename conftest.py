# -*- coding: utf-8 -*-
"""عزل بيانات الاختبارات عن بيانات المالك الحقيقية.

الفجوة التي يسدّها هذا الملف: `anti_repeat` و`app` و`streamlit_app` تقرأ جميعها
`DATA_DIR` من البيئة **وقت الاستيراد**، وتشتق منها `ARCHIVE_FILE = DATA_DIR/'archive.json'`.
بلا عزل، أي اختبار يمرّ بمسار التوليد (خصوصاً `test_generation_flow` الذي يضغط زر
التوليد فعلياً بردّ AI مموّه) يكتب في أرشيف المالك الحيّ. أثر ذلك موثّق: 151 من أصل
500 مدخلة في `archive.json` كانت حرفياً نص الـmock (`ريحته حلوة وثابتة`) — أي أن
«التكرار الشديد» في الأرشيف كان جزئياً تلوّثاً اختبارياً لا فشل محرّك.

الطبقتان معاً ضروريتان:
1. ضبط `DATA_DIR` في البيئة عند تحميل conftest — أبكر من استيراد أي وحدة اختبار،
   فيلتقطه الحساب وقت الاستيراد.
2. تثبيت `ARCHIVE_FILE` داخل `tmp_path` لكل اختبار — يغطّي الوحدات المستوردة
   مسبقاً ويمنع التسريب بين الاختبارات.
"""
import os
import sys
import tempfile
from pathlib import Path

import pytest

# ── الطبقة 0: جذر مؤقّت صالح للكتابة لـ tmp_path ─────────────────────────
# على هذا الجهاز مجلد pytest الافتراضي (%TEMP%\pytest-of-<user>) موجود لكنه غير
# قابل للكتابة (WinError 5)، فيفشل fixture الـtmp_path في **كل** اختبار. نفحص
# الصلاحية فعلياً ونحوّل الجذر لمجلد بديل ثابت الاسم عند المنع فقط — لا نغيّر
# سلوك الأجهزة السليمة ولا نولّد مجلداً جديداً كل تشغيل (pytest يرقّم وينظّف داخله).
def _ensure_writable_temproot():
    if os.environ.get('PYTEST_DEBUG_TEMPROOT'):
        return
    probe = Path(tempfile.gettempdir()) / f'pytest-of-{os.environ.get("USER") or os.environ.get("USERNAME") or "user"}'
    try:
        probe.mkdir(parents=True, exist_ok=True)
        canary = probe / '.write_probe'
        canary.touch()
        canary.unlink()
    except OSError:
        fallback = Path(tempfile.gettempdir()) / 'mahwous-pytest-root'
        fallback.mkdir(parents=True, exist_ok=True)
        os.environ['PYTEST_DEBUG_TEMPROOT'] = str(fallback)


_ensure_writable_temproot()

# ── الطبقة 1: قبل استيراد أي وحدة مشروع ──────────────────────────────────
# لا نلمس DATA_DIR إن ضبطها المشغّل صراحةً لغرض آخر.
_SESSION_DIR = Path(tempfile.mkdtemp(prefix='mahwous_test_data_'))
os.environ['DATA_DIR'] = str(_SESSION_DIR)

BASE_DIR = Path(__file__).parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# الوحدات التي تشتقّ ARCHIVE_FILE وقت الاستيراد
_ARCHIVE_MODULES = ('anti_repeat', 'app', 'streamlit_app')


@pytest.fixture(autouse=True)
def _isolate_archive(tmp_path, monkeypatch):
    """يوجّه كل كتابة أرشيف لـ tmp_path ويصفّر حالة الجلسة بين الاختبارات.

    autouse: العزل ليس اختيارياً — أي اختبار يكتب في أرشيف المالك يفسد بياناته.
    """
    data_dir = tmp_path / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    archive = data_dir / 'archive.json'

    monkeypatch.setenv('DATA_DIR', str(data_dir))

    for mod_name in _ARCHIVE_MODULES:
        mod = sys.modules.get(mod_name)
        if mod is None:
            continue
        if hasattr(mod, 'DATA_DIR'):
            monkeypatch.setattr(mod, 'DATA_DIR', data_dir, raising=False)
        if hasattr(mod, 'ARCHIVE_FILE'):
            monkeypatch.setattr(mod, 'ARCHIVE_FILE', archive, raising=False)

    # حالة anti_repeat داخل الذاكرة تتسرّب بين الاختبارات (deque/OrderedDict عامّة)
    ar = sys.modules.get('anti_repeat')
    if ar is not None:
        for reset in ('reset_session_texts', 'reset_pattern_counts', 'reset_adjective_counts'):
            fn = getattr(ar, reset, None)
            if callable(fn):
                fn()

    yield


@pytest.fixture
def archive_path(tmp_path):
    """مسار الأرشيف المعزول للاختبارات التي تريد فحص المكتوب فعلياً."""
    return tmp_path / 'data' / 'archive.json'
