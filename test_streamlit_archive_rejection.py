# -*- coding: utf-8 -*-
"""حارس: غلاف archive_batch في Streamlit ينقل نتيجة الرفض لا يبتلعها.

بلاغ مراجعة كودية خارجية مُتحقَّق منه مباشرة: غلاف streamlit_app.archive_batch
كان يستدعي anti_repeat.archive_batch ثم `return` مجرّداً — فتُهمَل قائمة
True/False دائماً ويصير `results` عند المُستدعي None حتماً. أثر ذلك أن كتلة
«إعادة التوليد عند رفض الأرشفة» في gen_reviews (المضافة لإصلاح سابق) كانت
**كوداً ميتاً لا يُنفَّذ إطلاقاً**، فيصل النص المكرر للمستخدم بصمت رغم أن
الطبقة الدنيا رصدته ورفضته فعلاً.
"""
import anti_repeat as ar
import streamlit_app as sl


def test_archive_batch_wrapper_propagates_per_item_results():
    """انحدار مباشر: الغلاف يجب أن يُرجع قائمة النتائج لا None."""
    ar.reset_session_texts()
    collided = 'نص حفظه عامل آخر للتوّ فعلاً'
    ar.archive_review(collided, 'منتج', 'شخص_سابق')
    ar.reset_session_texts()

    reviews = [{'text': collided, 'product': 'منتج'},
               {'text': 'نص جديد كلياً بمفردات نادرة ومختلفة', 'product': 'منتج'}]
    results = sl.archive_batch(reviews, 'شخص_جديد')

    assert results is not None, (
        'الغلاف ابتلع نتيجة archive_batch — كتلة إعادة التوليد عند الرفض تصبح كوداً ميتاً')
    assert results == [False, True], (
        f'يفترض رفض المكرر وقبول الجديد، النتيجة: {results}')


def test_wrapper_result_actually_enables_the_retry_branch():
    """تكامل: الشرط الفعلي المستخدم في gen_reviews (`if results is not None`)
    يجب أن يصير صحيحاً، وأن يحمل False للعنصر المرفوض."""
    ar.reset_session_texts()
    collided = 'تجربة الشراء كانت مرتبة وسريعة جدا'
    ar.archive_review(collided, 'منتج', 'شخص_سابق')
    ar.reset_session_texts()

    results = sl.archive_batch([{'text': collided, 'product': 'منتج'}], 'شخص_جديد')

    assert results is not None and any(saved is False for saved in results), (
        'فرع إعادة المحاولة لن ينفَّذ: لا نتيجة أو لا رفض مرصود')
