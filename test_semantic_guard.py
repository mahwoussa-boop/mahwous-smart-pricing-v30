# -*- coding: utf-8 -*-
"""حارس وحدة semantic_guard — كان صفر تغطية رغم كونه حارساً حيّاً حرجاً.

semantic_guard مربوط في app._write_review و streamlit (إعادة توليد محروسة +
strip_broken_tail) ويمنع «مصنع الشظايا»: تقييمات مبتورة/نازفة تصل للعميل.
كسره الصامت (إعادة هيكلة تُعطّل guard_violations/strip_broken_tail) لم تكن
البوابة تمسكه — هذه الاختبارات تسدّ الفجوة. كلها نصّية بحتة بلا شبكة/AI.
"""
import semantic_guard as sg


# ── guard_violations ──────────────────────────────────────────────
def test_empty_text_is_flagged_empty():
    assert sg.guard_violations('') == ['empty']
    assert sg.guard_violations('   ') == ['empty']


def test_clean_text_has_no_violations():
    assert sg.guard_violations('عطر فخم وثابت وراقي') == []


def test_semantic_bleed_detected():
    v = sg.guard_violations('السيارة تفوح برائحة العطر')
    assert 'bleed:السيارة' in v


def test_truncated_dangling_ending_detected():
    # ينتهي بأداة معلّقة «بس» → بتر نحوي
    assert 'truncated' in sg.guard_violations('عطر حلو بس')


def test_overlength_detected():
    v = sg.guard_violations('عطر جميل جدا رائع فخم', max_words=3)
    assert 'overlength' in v
    # وبلا سقف لا يُبلَّغ عن الطول
    assert 'overlength' not in sg.guard_violations('عطر جميل جدا رائع فخم')


def test_multiple_violations_combine():
    v = sg.guard_violations('السيارة حلوة بس', max_words=1)
    assert 'bleed:السيارة' in v and 'truncated' in v and 'overlength' in v


# ── strip_broken_tail ─────────────────────────────────────────────
def test_strip_removes_trailing_dangling():
    assert sg.strip_broken_tail(['عطر', 'فخم', 'بس']) == ['عطر', 'فخم']


def test_strip_removes_chained_dangling_and_bleed():
    # «السيارة» (نزيف) ثم «في» (معلّقة) يُقشَّطان معاً حتى تكتمل الجملة
    assert sg.strip_broken_tail(['عطر', 'حلو', 'في', 'السيارة']) == ['عطر', 'حلو']


def test_strip_never_empties():
    # كلمة واحدة معلّقة تبقى (شرط الحلقة len>1) — لا يُرجَع فارغ أبداً
    assert sg.strip_broken_tail(['بس']) == ['بس']


def test_strip_keeps_clean_tail():
    assert sg.strip_broken_tail(['عطر', 'فخم', 'وراقي']) == ['عطر', 'فخم', 'وراقي']


# ── has_context / has_reservation (المرشّح العاطفي) ────────────────
def test_has_context_true_and_false():
    assert sg.has_context('أستخدمه في الدوام') is True
    assert sg.has_context('عطر فخم وثابت') is False


def test_has_reservation_true_and_false():
    assert sg.has_reservation('حلو بس غالي شوي') is True
    assert sg.has_reservation('عطر ممتاز ورائع') is False
