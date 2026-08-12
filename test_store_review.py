# -*- coding: utf-8 -*-
"""اختبارات الوحدة المشتركة لتقييم المتجر (store_review).

تحرس السلوك المرجعي الموروث من app.py قبل ربط التطبيقين به: برومبت متكيّف
مع النطاق، حذف النداء، حظر الفخامة، توزيع الطول، وسقف الموضوع 20%.
"""
import random

import store_review as sr

PERSONA = {'label': 'خبير عطور', 'age': 33, 'city': 'الرياض', 'name': 'محمد القحطاني'}


def test_length_bands_sum_to_one():
    assert abs(sum(p for p, _, _ in sr.LENGTH_BANDS) - 1.0) < 1e-9


def test_sample_length_distribution():
    """التوزيع يقارب 35/40/25 (تفاوت ±5%)."""
    random.seed(1)
    bands = [sr.band_for(sr.sample_length_target()[1]) for _ in range(3000)]
    v = bands.count('vshort') / len(bands)
    s = bands.count('short') / len(bands)
    st = bands.count('story') / len(bands)
    assert abs(v - 0.35) < 0.05 and abs(s - 0.40) < 0.05 and abs(st - 0.25) < 0.05


def test_band_for_thresholds():
    assert sr.band_for(5) == 'vshort'
    assert sr.band_for(10) == 'short'
    assert sr.band_for(18) == 'story'


def test_build_store_prompt_vshort_drops_story():
    """القصير جدًا: لا افتتاحية ولا جانب ثانٍ، ويطلب انطباعًا خاطفًا."""
    p = sr.build_store_prompt(PERSONA, 'vshort', sr.STORE_ASPECTS[:2],
                              sr.STORE_OPENERS[0], '')
    assert 'انطباعًا خاطفًا' in p
    assert sr.STORE_OPENERS[0] not in p            # لا افتتاحية في القصير جدًا
    assert 'JSON' in p and 'مهووس للعطور' in p


def test_build_store_prompt_story_has_two_aspects_and_opener():
    p = sr.build_store_prompt(PERSONA, 'story', sr.STORE_ASPECTS[:2],
                              sr.STORE_OPENERS[0], '')
    assert sr.STORE_ASPECTS[0] in p and sr.STORE_ASPECTS[1] in p
    assert sr.STORE_OPENERS[0] in p


def test_build_store_prompt_ban_line_default():
    p = sr.build_store_prompt(PERSONA, 'short', sr.STORE_ASPECTS[:2], sr.STORE_OPENERS[0], '')
    assert 'ممنوع تشبيه' in p                       # حظر الفخامة حاضر افتراضيًّا


def test_strip_vocatives_removes_name_call_keeps_idioms():
    """يحذف «يا <اسم/نداء>» ويُبقي «يا سلام/يا رب»."""
    names = ['محمد', 'نورة', 'القحطاني']
    out = sr.strip_store_vocatives('يا محمد المتجر ممتاز يا صاحبي بس يا سلام عليه', 'محمد القحطاني', names)
    assert 'يا محمد' not in out and 'يا صاحبي' not in out
    assert 'يا سلام' in out                          # تعبير أصيل لا يُمسّ


def test_luxury_metaphor_detect_and_scrub():
    txt = 'الطلب وصل كأنه صندوق كنز فخم'
    assert sr.has_luxury_metaphor(txt)
    scrubbed = sr.scrub_luxury_metaphor(txt)
    assert not sr.has_luxury_metaphor(scrubbed)
    assert 'كأنه' not in scrubbed                    # أداة التشبيه تُحذف مع الاستعارة


def test_topic_tracker_cap_and_classify():
    """السقف يستبعد الموضوع المشبع بعد تجاوز الأرضية."""
    tr = sr.StoreTopicTracker()
    assert tr.blocked() == set()                     # قبل الأرضية: لا حظر
    for _ in range(10):
        tr.record('التوصيل سريع وصل بسرعة قبل الموعد')
    assert 'سرعة' in tr.blocked()                    # موضوع مهيمن → محظور
    assert tr.classify('التغليف فخم والكرتون مزدوج') == 'تغليف'


def test_topic_tracker_reset():
    tr = sr.StoreTopicTracker()
    tr.record('توصيل سريع')
    tr.reset()
    assert tr.total == 0 and all(v == 0 for v in tr.counts.values())


def test_aspect_not_reused_within_recency_window():
    """انحدار: نفس الجانب الحرفي لا يُعرَض على النموذج مرتين في جلسة قصيرة.

    سقف الموضوع الإحصائي (٪) لا يعمل قبل تجميع 5 تقييمات متجر (_TOPIC_FLOOR)،
    فكانت جلسة من 2-3 تقييمات (الشائعة فعليًّا) تُعيد نفس الجانب الحرفي
    (مثل التقسيط) بلا أي مانع، فينتج نصّان بصياغة شبه واحدة رغم اختلاف
    الشخصية. هذا يُثبت أن الاستبعاد يعمل *قبل* بلوغ تلك العتبة.
    """
    tr = sr.StoreTopicTracker()
    seen_pairs = []
    for _ in range(4):
        recent = tr.recently_used_aspects()
        pool = [a for a in sr.STORE_ASPECTS if a not in recent]
        assert len(pool) >= 2, 'استُنفد كامل جوانب المتجر قبل انتهاء النافذة'
        aspects = pool[:2]
        assert not (set(aspects) & recent), 'أُعيد عرض جانب مستخدم مؤخراً'
        tr.record_aspects(aspects)
        seen_pairs.append(tuple(aspects))
    # لا يزال بإمكان الجانب أن يتكرر بعد خروجه من نافذة الحداثة (ليس حظراً دائماً)
    assert tr.recently_used_aspects()


def test_installment_aspect_is_not_a_ready_made_sentence():
    """انحدار: جانب التقسيط لم يعد يحمل عبارة إعلانية جاهزة يكررها النموذج حرفياً.

    الصياغة القديمة «التقسيط — تابي وتمارا بدون فوائد» كانت تُمرَّر حرفياً في
    البرومبت، فكرّرها النموذج شبه حرفياً عبر شخصيات مختلفة (رُصد فعلياً: نفس
    عبارة «تابي وتمارا» في تقييمي متجر منفصلين). التصنيف بالكلمات الدالّة
    يجب أن يبقى يعمل مهما اختار النموذج الصياغة.
    """
    installment_aspect = next(a for a, t in sr.ASPECT_TOPIC.items() if t == 'تقسيط')
    assert 'بدون فوائد' not in installment_aspect
    assert not ('تابي' in installment_aspect and 'تمارا' in installment_aspect)
    tr = sr.StoreTopicTracker()
    assert tr.classify('استخدمت تابي وسهل علي التقسيط') == 'تقسيط'


def test_scrub_before_check_catches_the_collision(monkeypatch):
    """انحدار حرج: مراجعة كودية خارجية أثبتت هذا — scrub_luxury_metaphor كان
    يُستدعى *بعد* آخر فحص تفرّد في app.py/streamlit_app.py (كخطوة أخيرة منفصلة)
    ثم يُخزَّن الناتج المُعدَّل بلا إعادة فحص.

    برهان المراجعة: «التغليف مثل الذهب مرتب» و«التغليف مثل الالماس مرتب»
    نصّان مختلفان تماماً قبل scrub، وكلاهما ينهار لنفس النص «التغليف مرتب»
    بعده. الإصلاح: scrub صار جزءاً من finalize، فتفحصه بوابة التفرّد على
    النتيجة النهائية المُنظَّفة لا الخام — فيُكتشف الثاني كمكرر فعلياً.
    """
    import anti_repeat as ar
    ar.reset_session_texts()

    def _finalize(raw):
        # نفس ترتيب _finalize المُصلَح في app.py/streamlit_app.py: تنظيف بشري
        # ← حذف الاستعارة ← قصّ الطول (الترتيب هو ما يضمن فحص النتيجة النهائية)
        text = sr.scrub_luxury_metaphor(raw)
        return text.strip()

    raw_a = 'التغليف مثل الذهب مرتب'
    raw_b = 'التغليف مثل الالماس مرتب'
    assert raw_a != raw_b, 'النصّان الخامّان مختلفان فعلاً'

    final_a = _finalize(raw_a)
    final_b = _finalize(raw_b)
    assert final_a == final_b == 'التغليف مرتب', 'يفترض أن يتطابقا بعد الحذف'

    # الأول: يُسجَّل كنص جديد (لا سابق له)
    assert ar.is_duplicate(final_a) is False
    ar.register_text(final_a)

    # الثاني: بعد الإصلاح، الفحص يقع على النتيجة النهائية المتطابقة فعلياً
    assert ar.is_duplicate(final_b) is True, (
        'الثاني كان يمرّ كـ"غير مكرر" لأن الفحص القديم كان يقع على الخام '
        'المختلف قبل scrub، لا على "التغليف مرتب" المُخزَّن فعلياً')


def test_scrub_runs_inside_finalize_not_after_the_check():
    """حارس بنيوي: يمنع عودة scrub_luxury_metaphor كخطوة منفصلة بعد آخر فحص تفرّد.

    كل من app.py وstreamlit_app.py كانا يستدعيان scrub_luxury_metaphor مرتين:
    مرة كشرط لإعادة التوليد (has_luxury_metaphor)، ومرة كـ«ضمان حتمي أخير»
    بعد خروج النص من بوابة التفرّد — تلك الثانية هي مصدر الخلل (تعديل بعد
    الفحص). الإصلاح استدعاء واحد فقط، داخل _finalize، قبل استدعاء بوابة
    التوليد (_ai_write_json / ai_write_unique) في نص المصدر.
    """
    for path in ('app.py', 'streamlit_app.py'):
        src = open(path, encoding='utf-8').read()
        calls = src.count('scrub_luxury_metaphor(')
        assert calls == 1, f'{path}: توقّعت استدعاءً واحداً لـscrub_luxury_metaphor، وُجد {calls}'

        finalize_pos = src.index('def _finalize(text):')
        scrub_pos = src.index('scrub_luxury_metaphor(')
        gate_pos = (src.find('_ai_write_json(prompt, max_tokens=200, finalize=_finalize')
                    if path == 'app.py' else
                    src.find('ai_write_unique(prompt, max_tokens=200, finalize=_finalize'))
        assert finalize_pos < scrub_pos < gate_pos, (
            f'{path}: scrub_luxury_metaphor يجب أن يقع داخل _finalize وقبل استدعاء بوابة التوليد')


def test_app_binds_shared_store_module():
    """حارس التناغم: app.py يستورد منطق المتجر من store_review (لا نسخة inline).

    يمنع عودة الازدواج الذي كان يُبعد مسار Flask عن Streamlit.
    """
    import app
    assert app.STORE_ASPECTS is sr.STORE_ASPECTS
    assert app.build_store_prompt is sr.build_store_prompt
    assert app.strip_store_vocatives is sr.strip_store_vocatives
    assert isinstance(app._store_topics, sr.StoreTopicTracker)
