# -*- coding: utf-8 -*-
"""اختبار انحداري: توليد فعلي (ضغط الزر) عبر Streamlit بلا انهيار.

الفجوة التي كشفها هذا الاختبار: AppTest.run() في test_realism/test_mahalli_intel
يتحقق فقط من تحميل السكربت (بلا أخطاء عند الإقلاع)، وهذا لا يغطي ضغط زر
التوليد الذي يستدعي gen_reviews→ai_write_unique→ai_call. الانهيار الحقيقي
("can only concatenate tuple (not str) to tuple") كان يحدث فقط عند التوليد
الفعلي — موروث منذ commit a443d02 (build_master_prompt يرجع tuple منذ حينها)
ولم يُكتشف لأن streamlit_app.py:458 لم يكن يفكّ الـ tuple.

نموّه شبكة AI (unittest.mock.patch('requests.post')) كي يعمل الاختبار بلا
مفتاح API حقيقي ودون اتصال — يتحقق من مسار الكود لا من جودة نص الذكاء.
"""
import json
import os
import unittest.mock as mock

from streamlit.testing.v1 import AppTest


def _fake_post(url, headers=None, json=None, timeout=None, **kw):
    class _FakeResponse:
        status_code = 200

        def json(self):
            return {'choices': [{'message': {
                'content': '{"rating": 5, "text": "ريحته حلوة وثابتة", "is_verified_purchase": true}'
            }}]}
    return _FakeResponse()


def test_generate_button_no_tuple_crash():
    """انحدار: build_master_prompt يرجع (prompt, params) — عدم فكّها كان يفجّر
    TypeError عند أي توليد فعلي عبر زر «✨ ولّد شخصيات جديدة»."""
    with mock.patch.dict(os.environ, {'AI_KEY': 'test-only-key'}), \
            mock.patch('requests.post', side_effect=_fake_post):
        at = AppTest.from_file('streamlit_app.py', default_timeout=120)
        at.run()
        assert not at.exception

        gen_buttons = [b for b in at.button if 'ولّد شخصيات جديدة' in b.label]
        assert gen_buttons, 'زر التوليد غير موجود في الواجهة'
        gen_buttons[0].click().run()

        assert not at.exception, f'انهيار عند التوليد الفعلي: {[str(e.value) for e in at.exception]}'
