# -*- coding: utf-8 -*-
"""حارس حقيقي لـ anti_repeat.is_duplicate — الحارس الأساسي لمنع التكرار.

is_duplicate مربوط حياً في app.py:462 و streamlit_app.py:246؛ كسره الصامت =
تقييمات مكررة تصل للعميل. الاختبار الوحيد السابق (test_audience.test_anti_repeat)
**يُرجع قائمة issues ولا يؤكّد** (pytest يتجاهل القيمة المُرجَعة) و**لا يستدعي
is_duplicate إطلاقاً** — حارس زائف لا يفشل. هذه الاختبارات تؤكّد فعلاً.

عزل: is_duplicate يقرأ get_used_texts (من archive.json المتغيّر) — تأكيدات
«ليس مكرراً»/التشابه تُعزَل بـmonkeypatch لأرشيف فارغ؛ الباقي حتمي عبر reset.
"""
import anti_repeat as ar
from anti_repeat import (
    is_duplicate, register_text, reset_session_texts, is_registered,
    get_persona_fingerprint, get_burned_words, TRACKED_WORDS,
)

_BASE = 'العطر فخم جدا وثابت ورائع ويدوم'


def test_empty_or_too_short_is_duplicate():
    """فارغ أو أقل من 3 كلمات = غير صالح (يُرفَض كمكرر) — قبل أي قراءة أرشيف."""
    reset_session_texts()
    assert is_duplicate('') is True
    assert is_duplicate('كلمة') is True
    assert is_duplicate('كلمتان اثنتان') is True


def test_exact_registered_is_duplicate():
    """نص مُسجَّل حرفياً يُكشَف مكرراً عبر الجلسة (قبل حلقة الأرشيف)."""
    reset_session_texts()
    register_text(_BASE)
    assert is_duplicate(_BASE) is True


def test_novel_text_is_not_duplicate(monkeypatch):
    """نص فريد بجلسة نظيفة وأرشيف فارغ = ليس مكرراً."""
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    reset_session_texts()
    assert is_duplicate('منتج مختلف تماما بمفردات فريدة ونادرة جدا') is False


def test_similar_text_is_duplicate(monkeypatch):
    """تشابه عالٍ (jaccard/bigram) مع نص سابق في الجلسة = مكرر."""
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    reset_session_texts()
    register_text(_BASE)
    # يشارك 5 من 6 كلمات مع _BASE → jaccard ≈ 0.71 > العتبة 0.35
    assert is_duplicate('العطر فخم جدا وثابت ورائع وحلو') is True


def test_reset_clears_session():
    reset_session_texts()
    register_text(_BASE)
    assert is_registered(_BASE) is True
    reset_session_texts()
    assert is_registered(_BASE) is False


def test_persona_fingerprint_tracks_used_words():
    """بصمة الشخصية تسجّل الكلمات المتتبَّعة التي استخدمها العميل (منع تكرارها له)."""
    reset_session_texts()
    w = next(iter(TRACKED_WORDS))
    register_text(f'عطر جميل {w} وثابت جدا', persona_name='سعود')
    assert w in get_persona_fingerprint('سعود')
    # شخص آخر لم يستخدمها
    assert get_persona_fingerprint('عبدالله') == []


def test_burned_words_after_three_uses():
    """كلمة متتبَّعة تظهر ≥3 مرات في السجل تُصبح «محروقة»."""
    reset_session_texts()
    w = next(iter(TRACKED_WORDS))
    for _ in range(3):
        register_text(f'تقييم فيه {w} وكلمات أخرى مختلفة')
    assert w in get_burned_words()
