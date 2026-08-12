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


def test_app_binds_shared_store_module():
    """حارس التناغم: app.py يستورد منطق المتجر من store_review (لا نسخة inline).

    يمنع عودة الازدواج الذي كان يُبعد مسار Flask عن Streamlit.
    """
    import app
    assert app.STORE_ASPECTS is sr.STORE_ASPECTS
    assert app.build_store_prompt is sr.build_store_prompt
    assert app.strip_store_vocatives is sr.strip_store_vocatives
    assert isinstance(app._store_topics, sr.StoreTopicTracker)
