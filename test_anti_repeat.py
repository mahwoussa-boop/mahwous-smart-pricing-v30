# -*- coding: utf-8 -*-
"""حارس حقيقي لـ anti_repeat.is_duplicate — الحارس الأساسي لمنع التكرار.

is_duplicate مربوط حياً في app.py:462 و streamlit_app.py:246؛ كسره الصامت =
تقييمات مكررة تصل للعميل. الاختبار الوحيد السابق (test_audience.test_anti_repeat)
**يُرجع قائمة issues ولا يؤكّد** (pytest يتجاهل القيمة المُرجَعة) و**لا يستدعي
is_duplicate إطلاقاً** — حارس زائف لا يفشل. هذه الاختبارات تؤكّد فعلاً.

عزل: is_duplicate يقرأ get_used_texts (من archive.json المتغيّر) — تأكيدات
«ليس مكرراً»/التشابه تُعزَل بـmonkeypatch لأرشيف فارغ؛ الباقي حتمي عبر reset.
"""
import json

import anti_repeat as ar
from anti_repeat import (
    is_duplicate, register_text, reset_session_texts, is_registered,
    get_persona_fingerprint, get_burned_words, TRACKED_WORDS,
)

_BASE = 'العطر فخم جدا وثابت ورائع ويدوم'


def test_empty_is_duplicate():
    """الفارغ فقط يُرفَض بلا قراءة أرشيف."""
    reset_session_texts()
    assert is_duplicate('') is True
    assert is_duplicate('   ') is True
    assert is_duplicate('!!!') is True  # بلا عربي بعد التطبيع


def test_short_text_is_not_duplicate_by_length_alone(monkeypatch):
    """انحدار: النص القصير الفريد ليس مكرراً.

    السلوك القديم كان «أقل من 3 كلمات ⇒ مكرر» بينما 43% من len_target المعاير
    هو 1–2 كلمة، فكانت كل محاولات التوليد القصير تفشل ويُقبل المكرر حتماً —
    وهو المولّد المباشر لتكرار «ريحته حلوة وثابتة» في الأرشيف.
    """
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    reset_session_texts()
    assert is_duplicate('حلو') is False
    assert is_duplicate('ريحته تجنن') is False


def test_short_text_exact_repeat_is_duplicate(monkeypatch):
    """القصير المكرر حرفياً يُكشَف (المعيار الصالح الوحيد على هذا الطول)."""
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    reset_session_texts()
    register_text('ريحته حلوة وثابتة')
    assert is_duplicate('ريحته حلوة وثابتة') is True


def test_short_texts_sharing_a_word_are_not_duplicates(monkeypatch):
    """القصيران المختلفان معنى لا يُخلطان لمجرد كلمة مشتركة (jaccard 0.5)."""
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    reset_session_texts()
    register_text('ريحته حلوة')
    assert is_duplicate('ريحته تفتح النفس') is False


def test_normalize_folds_arabic_letter_variants(monkeypatch):
    """انحدار: ة/ه و أ/ا و ى/ي صور للحرف نفسه.

    بدون التوحيد احتفظ الأرشيف بـ«ريحتة حلوة وثابتة» و«ريحته حلوة وثابتة»
    كمدخلتين منفصلتين رغم أنهما النص نفسه إملائياً.
    """
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    reset_session_texts()
    register_text('ريحته حلوة وثابتة')
    assert is_duplicate('ريحتة حلوه وثابته') is True
    assert ar._normalize('أحلى') == ar._normalize('احلي')


def test_burned_openings_flagged_and_surfaced():
    """تكرار البداية نفسها يُرصَد ويظهر في كتلة البرومبت (منع تكرار البدايات)."""
    reset_session_texts()
    # ثلاث بدايات تشترك في الكلمة الأولى — بصمة آلية وإن اختلفت الكلمة الثانية
    for t in ('ريحته حلوة وثابتة جدا', 'ريحته تملى المكان كله', 'ريحته تفتح النفس مرة'):
        register_text(t)
    assert ar.is_opening_burned('ريحته تدوخ الراس') is True
    assert 'ريحته' in ar.get_burned_openings()
    assert 'بدايات محروقة' in ar.format_used_texts_block(limit=5)
    # بداية مختلفة تماماً لا تُحرَق
    assert ar.is_opening_burned('العلبة وصلت مرتبة') is False


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
    """كلمة متتبَّعة تظهر ≥3 مرات في السجل تُصبح «محروقة».

    ثلاث جمل مختلفة فعلياً (لا نفس النص الحرفي مكرراً) — منذ أن صار
    register_text idempotent (لا يُعيد عدّ نفس النص المُطبَّع مرتين، إصلاح
    التسجيل المزدوج) لم يعد تكرار النص الحرفي نفسه يُحتسَب كثلاث مراجعات."""
    reset_session_texts()
    w = next(iter(TRACKED_WORDS))
    sentences = [
        f'تقييم فيه {w} وكلمات أخرى مختلفة',
        f'هذا العطر {w} بصراحة وأنصح فيه',
        f'صراحة {w} جدا ويستاهل التجربة',
    ]
    for s in sentences:
        register_text(s)
    assert w in get_burned_words()


def test_live_register_tracks_contexts_and_ideas():
    """انحدار: تتبّع السياق والفكرة كان محبوساً في register_review_full.

    المسار الحيّ (app/streamlit) لا يستدعي إلا register_text، فبقي
    _context_usage فارغاً في الإنتاج وكانت get_available_contexts ترجع كل
    السياقات دائماً — أي أن توجيه «سياقات متاحة» في البرومبت كان بلا معنى.
    """
    reset_session_texts()
    register_text('كل ما ألبسه في المسجد سألني الجماعة عن العطر')
    # السياق المستخدم لم يعد ضمن «المتاح»
    assert 'مسجد' not in ar.get_available_contexts()
    assert ar.is_context_burned('مسجد') is True
    # والفكرة (بنية النمط) سُجّلت
    assert ar.is_pattern_structure_burned('compliment_question') is True


def test_unused_contexts_stay_available():
    """السياق غير المستخدم يبقى متاحاً (لا حرق أعمى)."""
    reset_session_texts()
    register_text('العلبة وصلت مرتبة والتغليف ممتاز جدا')
    assert 'مسجد' in ar.get_available_contexts()


def test_duplicate_check_covers_full_archive_not_just_last_100(monkeypatch):
    """انحدار حرج: is_duplicate كان يفحص آخر 100 فقط بينما الأرشيف يحتفظ
    حتى MAX_ARCHIVE=500 — نص في المدخلة رقم 101 وما قبلها (الأقدم) كان
    يمرّ كـ«غير مكرر» رغم تطابقه الحرفي مع نص محفوظ فعلاً في الأرشيف.

    نصوص الحشو مختلفة معجمياً بعمد (لا تشترك مفردات) لعزل نافذة الطول عن
    تشابه jaccard/bigram العرضي — الفحص هنا لتطابق حرفي بعد التطبيع فقط.
    """
    reset_session_texts()
    target = 'ريحته حلوة وثابتة طول اليوم بشكل رهيب'
    word_bank = ['كتاب', 'سيارة', 'طاولة', 'حاسوب', 'شجرة', 'بحر', 'جبل',
                'نافذة', 'مصباح', 'كرسي', 'هاتف', 'ساعة', 'مفتاح', 'باب',
                'حائط', 'سقف', 'أرض', 'سماء', 'شمس', 'قمر']
    import random
    rnd = random.Random(3)
    fillers = [' '.join(rnd.sample(word_bank, 5)) + f' رقم{i}' for i in range(100)]
    archive = [target] + fillers  # target هو الأقدم (المدخلة 101 من النهاية)

    def _fake_get_used_texts(limit=100):
        return archive[-limit:] if len(archive) > limit else archive

    monkeypatch.setattr(ar, 'get_used_texts', _fake_get_used_texts)
    assert is_duplicate(target) is True, (
        'النص الأقدم (خارج نافذة الـ100 القديمة) لم يُكتشف كمكرر رغم '
        'تطابقه الحرفي مع مدخلة أرشيف فعلية')


def test_session_memory_rebuilds_from_archive_at_startup(tmp_path, monkeypatch):
    """يعيد إنتاج برهان المراجعة بالضبط: كلمة محروقة قبل «إعادة التشغيل»
    (تصفير الذاكرة) تعود محروقة بعده — بشرط أن الأرشيف لا يزال يحمل الدليل.

    قبل الإصلاح: reset_session_texts (يحاكي عملية Gunicorn جديدة أو إعادة
    تشغيل) يمحو الكلمات/البدايات المحروقة كلياً بلا رجعة، رغم بقاء الدليل
    في archive.json — فيكرر عامل أو تشغيل جديد صياغة حرقها عامل آخر بالفعل.
    """
    archive_file = tmp_path / 'archive.json'
    w = next(iter(ar.TRACKED_WORDS))
    # ثلاث جمل مختلفة فعلياً — لا الاعتماد على رقم لاحق (_normalize يُسقط
    # الأرقام تماماً، فتنهار «...رقم 0/1/2» لنفس النص المُطبَّع فيُبطل
    # الغرض من الاختبار بعد أن صار register_text idempotent)
    texts = [
        f'هذا عطر {w} جدا ومميز',
        f'العطر ده {w} بجد وعجيب',
        f'صراحة عطر {w} ما توقعته',
    ]
    reviews = [
        {'text': t, 'product': 'م', 'persona': 'شخص1', 'ts': i}
        for i, t in enumerate(texts)
    ]
    archive_file.write_text(
        json.dumps({'reviews': reviews, 'store_reviews': [], 'personas': []}, ensure_ascii=False),
        encoding='utf-8')
    monkeypatch.setattr(ar, 'ARCHIVE_FILE', archive_file, raising=False)

    # محاكاة عملية Gunicorn جديدة / إعادة تشغيل: ذاكرة فارغة تماماً
    reset_session_texts()
    assert w not in get_burned_words(), 'الحالة الابتدائية يجب أن تكون فارغة قبل إعادة البناء'

    ar._rebuild_session_from_archive()

    assert w in get_burned_words(), (
        'الكلمة المحروقة في أرشيف سابق لم تُستعَد بعد محاكاة بدء عملية جديدة')


def test_register_text_is_idempotent_for_the_same_normalized_text():
    """بلاغ مراجعة كودية مُتحقَّق منه: تقييم حقيقي واحد كان يُسجَّل مرتين —
    مرة عند التوليد (_register/ar_register_text) ومرة أخرى داخل
    archive_review/archive_batch عند الحفظ — فتُحرَق الكلمات والبدايات
    بضعف السرعة الحقيقية (بداية واحدة بعد تقييم واحد فقط تصبح «محروقة»
    رغم أن عتبة الحرق تفترض استخدامَين حقيقيَّين مختلفَين).

    الإصلاح: register_text صارت idempotent — تسجيل النص نفسه (بعد التطبيع)
    مرتين في نفس الجلسة لا يُضاعف عدّادات الحرق.
    """
    reset_session_texts()
    text = 'ريحته عود فخم يذكرني بالمجلس دائما'

    register_text(text, 'شخص1')
    assert len(ar._session_recent) == 1
    assert ar.is_opening_burned(text) is False  # استخدام واحد فقط حتى الآن

    register_text(text, 'شخص1')  # التسجيل «الزائد» الذي كان يحدث عند الأرشفة
    assert len(ar._session_recent) == 1, 'تسجيل نفس النص مرة ثانية يجب ألا يُضاعف السجل'
    assert ar.is_opening_burned(text) is False, (
        'تقييم واحد حقيقي (مسجَّل مرتين بالخطأ) لا يجب أن يحرق البداية '
        'وكأنه استخدامان حقيقيان مختلفان')


def test_archive_review_no_longer_double_registers(monkeypatch):
    """تكامل: استدعاء register_text قبل archive_review (كما يفعل المسار
    الحيّ فعلياً) ثم أرشفة النص نفسه لا يُضاعف تتبّع الحرق."""
    reset_session_texts()
    monkeypatch.setattr(ar, 'get_used_texts', lambda limit=100: [])
    text = 'هذا عطر رهيب جدا ويستاهل كل ريال فيه'

    ar.register_text(text, 'شخص1')  # التسجيل أثناء التوليد (كما في _write_review)
    ar.archive_review(text, 'منتج', 'شخص1')  # الأرشفة بعده (تُسجِّل أيضاً لو لزم)

    assert len(ar._session_recent) == 1, 'الأرشفة بعد تسجيل مسبق للنص نفسه ضاعفت العدّاد'
