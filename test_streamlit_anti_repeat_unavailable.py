# -*- coding: utf-8 -*-
"""حارس (نسخة Streamlit من test_anti_repeat_unavailable.py): تعذّر anti_repeat
يوقف التوليد بخطأ واضح بدل الاستمرار بلا حماية من التكرار.

streamlit_app.py سكربت — لا يُستورَد مباشرة في اختبارات المشروع الأخرى (تُفضَّل
AppTest.from_file لتشغيله كاملاً)، لكن استيراده هنا مباشرة آمن وأسرع بكثير
لاختبار دالة واحدة معزولة: conftest.py يُدرجه أصلاً ضمن _ARCHIVE_MODULES
المعزولة، واستدعاءات st.* خارج ScriptRunContext حقيقي (bare mode) تُحذّر ولا
تفشل — تحقّقنا من هذا مباشرة قبل كتابة الاختبار.
"""
import pytest

import streamlit_app as sl


class _Stopped(Exception):
    pass


def _raise_stopped():
    raise _Stopped()


def test_is_dup_guarded_stops_when_anti_repeat_unavailable(monkeypatch):
    """الإصلاح المباشر: _is_dup_guarded (المُمرَّرة كـis_dup لـwrite_unique عبر
    ai_write_unique) يجب أن تتوقف بخطأ واضح (st.error + st.stop) لا تعيد
    False دائماً — نفس خلل app.py._is_dup قبل إصلاحه."""
    monkeypatch.setattr(sl, 'USE_ANTI_REPEAT', False)
    monkeypatch.setattr(sl.st, 'error', lambda *a, **kw: None)
    monkeypatch.setattr(sl.st, 'stop', _raise_stopped)

    with pytest.raises(_Stopped):
        sl._is_dup_guarded('أي نص عشوائي')


def test_is_dup_guarded_unaffected_when_anti_repeat_available(monkeypatch):
    """ضابط سلبي: لا انحدار في السلوك الطبيعي — التفويض لـar_is_duplicate كما هو."""
    monkeypatch.setattr(sl, 'USE_ANTI_REPEAT', True)
    monkeypatch.setattr(sl, 'ar_is_duplicate',
                        lambda text, is_store_review=False: text == 'مكرر بالفعل')
    assert sl._is_dup_guarded('مكرر بالفعل') is True
    assert sl._is_dup_guarded('نص جديد كلياً') is False
