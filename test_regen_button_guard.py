# -*- coding: utf-8 -*-
"""حارس: زر «استبدال» التقييم لا يسمح بنقرات متزامنة بلا حماية.

بلاغ مراجعة كودية خارجية: reRev() لم تكن تعطّل الزر ولا تضع علامة «طلب
جارٍ» — نقرة مزدوجة تُطلق طلبَي توليد وأرشفة متزامنَين لـ/api/regen-review
بينما الواجهة تعرض نتيجة واحدة فقط. مقترناً بسباق الفحص-ثم-الكتابة (مُصلَح
في anti_repeat.py) كان يمكن أن يُحفظ نص متطابق مرتين.

هذا الملف حارس بنيوي على قالب templates/index.html (لا محرّك JS في بايثون)
— يتحقّق أن حماية النقر المزدوج موجودة في المصدر فعلياً، لا أنها تعمل
داخل متصفح حقيقي (ذاك يحتاج تحققاً حيّاً في المتصفح، منفّذ يدوياً هنا).
"""


def _read_template():
    with open('templates/index.html', encoding='utf-8') as f:
        return f.read()


def test_regen_button_passes_element_reference_to_disable_it():
    """الزر يمرّر مرجعه لنفسه (this) — شرط أساسي لتعطيله أثناء الطلب."""
    src = _read_template()
    assert "onclick=\"reRev(" in src
    assert ",this)" in src, 'الزر لا يمرّر مرجعه (this) — لا يمكن تعطيله من reRev'


def test_rerev_disables_button_before_request_and_reenables_after():
    """reRev تعطّل الزر قبل fetch وتعيد تفعيله دائماً (نجاح أو فشل)،
    وترفض التنفيذ إن كان الزر معطَّلاً بالفعل (نقرة ثانية أثناء طلب جارٍ)."""
    src = _read_template()
    start = src.index('async function reRev(')
    end = src.index('\n}', start)
    body = src[start:end]

    assert 'btn.disabled=true' in body, 'reRev لا تعطّل الزر عند بدء الطلب'
    assert 'if(btn.disabled) return' in body, (
        'reRev لا ترفض نقرة ثانية والزر معطَّل بالفعل — نقرتان سريعتان قد '
        'تمرّان كلتاهما قبل أن يُعطَّل الزر فعلياً في DOM')
    # إعادة التفعيل يجب أن تقع في finally — تعمل سواء نجح الطلب أو فشل
    finally_pos = body.find('finally')
    reenable_pos = body.find('btn.disabled=false')
    assert finally_pos != -1 and reenable_pos != -1 and reenable_pos > finally_pos, (
        'إعادة تفعيل الزر يجب أن تقع في finally — وإلا يبقى الزر معطَّلاً للأبد عند فشل الطلب')
