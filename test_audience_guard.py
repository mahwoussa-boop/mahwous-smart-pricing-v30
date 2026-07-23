# -*- coding: utf-8 -*-
"""حارس حقيقي فوق فحوص test_audience.py البنيوية الخفيفة الراجعة لقوائم issues.

المشكلة: دوال test_audience.test_* تُرجع قائمة issues بلا assert — pytest
يجمعها ويُحذّر «returned list» لكن **يمرّرها دائماً** مهما امتلأت (حارس لا
يفشل). هذا الملف يستدعي الفحوص **العدّية الخفيفة** (أنماط/بنوك/لهجات — بلا
قاعدة بيانات ولا توليد) ويؤكّد خلوّ issues، فيحوّلها لبوابة تفشل فعلاً عند
انحدار الأعداد المرجعية — **بلا لمس test_audience.py** (نمطها المزدوج سليم).

عمداً تُستثنى الفحوص التوليدية/DB الثقيلة (engine/database/personas/
vocabulary): إعادة تشغيلها هنا تُثلّث زمن البوابة (18s→57s قِيس فعليًّا)؛
تحويلها لحارس حقيقي يحتاج تعديلاً موضعياً في test_audience خلف بوابة المالك.
"""
import pytest

import test_audience as ta

# فحوص بنيوية عدّية خفيفة فقط (بلا DB/توليد) — كلفة زهيدة
_LIGHT_CHECKS = [
    ta.test_review_patterns,     # أعداد بنوك الأنماط/العائلات
    ta.test_real_reviews_bank,   # TRACKED_WORDS/PATTERNS/CONTEXTS + تتبّع السياق
    ta.test_anti_repeat,         # أعداد NEGATIVE/NEUTRAL_SHORTS
    ta.test_short_texts,         # تعبيرات/أنماط شكوى لكل لهجة
    ta.test_dialects,            # أعداد الأنماط + fingerprint
]


@pytest.mark.parametrize('check', _LIGHT_CHECKS, ids=lambda f: f.__name__)
def test_audience_light_check_has_no_issues(check):
    """يؤكّد أن الفحص البنيوي لا يُبلّغ عن أي issue (حارس حقيقي بدل return مُهمَل)."""
    issues = check()
    assert isinstance(issues, list), f'{check.__name__} لم يُرجع قائمة issues'
    assert not issues, f'{check.__name__}:\n' + '\n'.join(str(i) for i in issues)
