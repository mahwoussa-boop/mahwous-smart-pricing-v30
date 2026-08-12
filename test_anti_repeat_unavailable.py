# -*- coding: utf-8 -*-
"""حارس: تعذّر استيراد anti_repeat يوقف التوليد بخطأ واضح لا يُهمَل بصمت.

بلاغ مراجعة كودية خارجية مُتحقَّق منه مباشرة: app.py يضبط USE_ANTI_REPEAT=False
عند فشل `import anti_repeat` ويستمر — كل مسارات التوليد كانت تتحقّق من
USE_ANTI_REPEAT قبل استدعاء أي دالة تفرّد، فتستمر الحلقة كأن كل نص مولَّد
"فريد" ضمانة لم تُفحص إطلاقاً، للأبد، بلا أي رصد أو تحذير. هذا مطابق لفلسفة
personas_engine (_make_master_prompt: محرك مفقود = AIUnavailable فوراً، لا
برومبت بديل متدنٍّ يعمل بصمت) — anti_repeat لم تكن تتبع نفس القاعدة.
"""
import pytest

import app as flask_app


def test_is_dup_raises_when_anti_repeat_unavailable(monkeypatch):
    """الإصلاح المباشر: _is_dup — البوابة الوحيدة التي يمرّ عبرها كل نص
    مولَّد (is_dup=_is_dup في rt_write_unique) — يجب أن تتوقف بخطأ واضح لا
    تعيد False دائماً (أي «ليس مكرراً» زائفة)."""
    monkeypatch.setattr(flask_app, 'USE_ANTI_REPEAT', False)
    with pytest.raises(flask_app.AIUnavailable):
        flask_app._is_dup('أي نص عشوائي')


def test_is_dup_unaffected_when_anti_repeat_available(monkeypatch):
    """ضابط سلبي: لا انحدار في السلوك الطبيعي — التفويض لـis_duplicate كما هو."""
    monkeypatch.setattr(flask_app, 'USE_ANTI_REPEAT', True)
    monkeypatch.setattr(flask_app, 'is_duplicate', lambda text: text == 'مكرر بالفعل')
    assert flask_app._is_dup('مكرر بالفعل') is True
    assert flask_app._is_dup('نص جديد كلياً') is False
    assert flask_app._is_dup('') is False  # فارغ لا يستدعي is_duplicate أصلاً


def test_generation_loop_stops_instead_of_silently_skipping_dup_protection(monkeypatch):
    """تكامل: تعذّر anti_repeat يوقف rt_write_unique فوراً عند أول فحص تفرّد
    بدل أن يُعيد نصاً «ناجحاً» بلا أي حماية من التكرار — يتحقّق أن الاستثناء
    يخترق كامل السلسلة (write_unique → _ai_unique_text) دون أن يُبتلَع."""
    monkeypatch.setattr(flask_app, 'USE_ANTI_REPEAT', False)
    monkeypatch.setattr(flask_app, '_ai_call',
                        lambda p, max_tokens, temperature: 'نص أي مولّد من الذكاء الاصطناعي')

    with pytest.raises(flask_app.AIUnavailable):
        flask_app._ai_unique_text('برومبت تجريبي', max_tokens=50,
                                  finalize=lambda t: t, parser=lambda out: out)
