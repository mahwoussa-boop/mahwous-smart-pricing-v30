# -*- coding: utf-8 -*-
"""حارس تكرار المولّد القالبي الدفعي (ReviewGenerator) — أداة معايرة إحصائية
offline حصراً (generate_audience.py)، غير موصولة بمسار التوليد الحيّ
(app.py/streamlit_app.py يستبعدانها عمداً — راجع التعليق في app.py حول
«fallback_gen المَيت»؛ لا AI هنا، توليد قالبي صرف).

بلاغ مراجعة كودية خارجية مُتحقَّق منه بفحص generated_audience_1000.json
الفعلي: 102 مجموعة تكرار حرفي (214 نسخة زائدة)، 131 مجموعة بعد التطبيع
العربي (310 نسخة زائدة)، وتكرار حتى 9 مرات لكلمة واحدة («أسطوري»).
"""
import review_generator as rg


def test_is_duplicate_catches_emoji_decorated_variant():
    """برهان المراجعة الأول: نفس الجملة بإيموجي زائد تُكتشف كمكرر الآن.

    قبل الإصلاح: _is_duplicate كان يقارن أول 50 حرفاً حرفياً بعد إزالة
    المسافات فقط — لا يزيل الإيموجي، فيراهما نصّين مختلفين رغم تطابق المعنى.
    """
    gen = rg.ReviewGenerator()
    a = 'لبسته في عزيمة عشاء يعطيك حضور'
    b = 'لبسته في عزيمة عشاء يعطيك حضور🙌'
    assert gen._is_duplicate(a) is False
    assert gen._is_duplicate(b) is True, 'الإيموجي الزائد لم يعد يُخفي التكرار'


def test_is_duplicate_catches_elongated_variant():
    """برهان المراجعة الثاني: تمطيط حرف واحد (أدور → أدوررر) يُكتشف كمكرر.

    maybe_elongate (realism_calibrator.py) يستبدل حرفاً بنسخته المكرّرة
    كنسيج بشري متعمّد *قبل* فحص التكرار مباشرة (_post_process) — التطبيع
    القديم لم يطوِ هذا التكرار الحرفي فمرّ كنص «جديد».
    """
    gen = rg.ReviewGenerator()
    c = 'كنت أدور عطر يومي ولقيته'
    d = 'كنت أدوررر عطر يومي ولقيته'
    assert gen._is_duplicate(c) is False
    assert gen._is_duplicate(d) is True, 'التمطيط لم يعد يُخفي التكرار'


def test_is_duplicate_catches_letter_variant_and_punctuation():
    """ة/ه، أ/ا، والترقيم لا تُنتج نصوصاً «مختلفة» زوراً."""
    gen = rg.ReviewGenerator()
    assert gen._is_duplicate('ريحته حلوة وثابته جدا') is False
    assert gen._is_duplicate('ريحتة حلوه وثابته جدا!') is True
    gen2 = rg.ReviewGenerator()
    assert gen2._is_duplicate('احسن عطر جربته') is False
    assert gen2._is_duplicate('أحسن عطر جربته') is True


def test_is_duplicate_still_allows_genuinely_different_short_text():
    """التطبيع لا يُفرط: نصّان مختلفان فعلياً يبقيان غير مكررين."""
    gen = rg.ReviewGenerator()
    assert gen._is_duplicate('ريحته حلوة') is False
    assert gen._is_duplicate('التغليف ممتاز') is False


def test_short_text_cap_is_tight_not_fifty():
    """انحدار: السقف القديم max(4, count*5%) يسمح بـ50 تكراراً لدفعة 1000 —
    أعلى بكثير من أي تكرار واقعي رُصد فعلياً (أقصاه 9). السقف الجديد يُبقي
    التكرار الواقعي (2-5) بلا فتح الباب لعشرات النسخ من نفس الجملة."""
    gen = rg.ReviewGenerator()
    text = 'رهيب'
    accepted = 0
    for _ in range(60):
        normalized = rg._fold_elongation(rg._ar._normalize(text))
        cap = max(2, min(5, round(1000 * 0.01)))
        if gen._short_freq[normalized] < cap:
            gen._short_freq[normalized] += 1
            accepted += 1
    assert accepted <= 5, f'السقف سمح بـ{accepted} تكراراً لنص واحد — أعلى من المتوقع'
    assert accepted >= 2, 'السقف لا يجب أن يمنع كل تكرار (هدف الأداة: مطابقة واقع لا صفر)'


def test_real_batch_respects_the_tightened_cap():
    """سلوكي على generate_reviews الفعلية لا نسخة مستقلة من الصيغة — دفعة
    صغيرة (200) بسقف max(2, min(5, round(200*0.01)))=2 لكل نص قصير مُطبَّع."""
    import random
    random.seed(11)
    gen = rg.ReviewGenerator()
    reviews = gen.generate_reviews(
        product_name='عطر تجريبي للاختبار', price=200, category='oud',
        gender='unisex', count=200,
    )
    import collections
    short = [r['text'] for r in reviews if len(r['text'].split()) <= 4]
    counts = collections.Counter(rg._fold_elongation(rg._ar._normalize(t)) for t in short)
    worst = max(counts.values()) if counts else 0
    assert worst <= 2, f'نص قصير تكرر {worst} مرة رغم سقف=2 المتوقع لدفعة 200'


def test_short_freq_key_is_normalized_not_raw():
    """انحدار: _short_freq كان يُفهرَس بالنص الخام — فتُفلت الزخارف
    (إيموجي/تمطيط) من عدّاد السقف نفسه، لا فحص _is_duplicate فقط."""
    gen = rg.ReviewGenerator()
    n1 = rg._fold_elongation(rg._ar._normalize('رهيب'))
    n2 = rg._fold_elongation(rg._ar._normalize('رهيب🔥'))
    n3 = rg._fold_elongation(rg._ar._normalize('رهييييب'))
    assert n1 == n2 == n3, 'الزخارف يجب أن تنهار لنفس المفتاح المُطبَّع'
