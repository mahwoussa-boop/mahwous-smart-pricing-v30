# -*- coding: utf-8 -*-
"""حارس: الإيموجي لا يُهرّب النص القصير من سقف التكرار (المولّد غير المتصل).

بلاغ مراجعة كودية خارجية مُتحقَّق منه مباشرة: قرار «قصير أم لا» في
ReviewGenerator.generate_reviews كان يُحسَب على عدد كلمات النص **الخام**،
بينما _post_process يضيف إيموجي كتوكن مستقل. فنص من 4 كلمات فعلية يصير 5
كلمات خام، فيهرب من فرع النص القصير (≤4) ولا يدخل عدّاد _short_freq
إطلاقاً — أي يتجاوز سقف التكرار كلياً. البرهان المرصود: 6 نسخ من نص مُطبَّع
واحد رغم سقف 5، بعدد كلمات خام [4,5,4,4,4,4].

مفتاح العدّاد كان مُطبَّعاً منذ إصلاح سابق — وبقي **الطول** خاماً، وهو ما
أبقى الثغرة مفتوحة رغم ذلك الإصلاح.

هذا المسار خاص بالأداة الإحصائية غير المتصلة (generate_audience.py) لا
بتوليد Flask الحيّ.
"""
import anti_repeat as _ar


_BASE = 'ريحته حلوة وثابتة جدا'   # أربع كلمات فعلية


def test_emoji_does_not_change_the_short_text_classification():
    """جوهر الخلل: الإيموجي يزيد عدد الكلمات الخام دون تغيير النص فعلياً."""
    raw_words = len(_BASE.split())
    raw_with_emoji = len((_BASE + ' 🙌').split())
    norm_with_emoji = len(_ar._normalize(_BASE + ' 🙌').split())

    assert raw_words == 4
    assert raw_with_emoji == 5, 'التهيئة: الإيموجي يجب أن يُحتسب توكناً خاماً'
    assert norm_with_emoji == 4, (
        'التطبيع يجب أن يُسقط الإيموجي فيعود الطول الحقيقي 4')
    assert _ar._normalize(_BASE) == _ar._normalize(_BASE + ' 🙌'), (
        'النصّان متطابقان فعلياً بعد التطبيع — فلا يجوز أن يسلكا فرعين مختلفين')


def test_generate_reviews_decides_shortness_on_normalized_text():
    """انحدار بنيوي: قرار الفرع يجب أن يُبنى على النص المُطبَّع لا الخام.

    فحص بنيوي متعمَّد: الفرع يقع داخل حلقة توليد عشوائية طويلة يصعب دفعها
    لإنتاج نص بعينه بشكل حتمي؛ والخلل هنا **في المقياس المستخدم** لا في
    نتيجة عشوائية.
    """
    import inspect

    from review_generator import ReviewGenerator

    src = inspect.getsource(ReviewGenerator.generate_reviews)
    assert 'len(normalized.split()) <= 4' in src, (
        'قرار طول النص القصير لا يُحسب على النص المُطبَّع — الإيموجي يهرّبه من السقف')
    assert 'len(text.split()) <= 4' not in src, (
        'عاد قياس الطول على النص الخام — تعود ثغرة تجاوز سقف التكرار القصير')
