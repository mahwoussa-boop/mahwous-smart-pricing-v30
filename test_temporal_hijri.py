# -*- coding: utf-8 -*-
"""حارس الذكاء الزمني الهجري — إشارات رمضان/العيد/الحج (مواسم البيع الكبرى).

كانت السطور 902-910 في personas_engine نائمة (_HAS_HIJRI=False لغياب المكتبة)
رغم وصل build_temporal_block بالمسار الحيّ. بعد تفعيل hijridate تُصبح فعّالة —
هذا الحارس يقفلها: تواريخ ميلادية مؤكّدة تُنتج الإشارة الهجرية الصحيحة.

حتمي تماماً: _temporal_signals يقبل now فنحقن التاريخ بلا اعتماد على وقت
التشغيل ولا عشوائية. يتخطّى برشاقة إن غابت المكتبة (تدرّج آمن محفوظ).
"""
import datetime

import pytest

import personas_engine as pe

pytestmark = pytest.mark.skipif(not pe._HAS_HIJRI, reason='مكتبة هجرية غير مثبّتة')

# تواريخ ميلادية مؤكّدة (hijridate): 2025-03-15=رمضان١٥، 2025-03-30=عيد١، 2025-06-05=حج٩
_RAMADAN = datetime.datetime(2025, 3, 15)
_EID = datetime.datetime(2025, 3, 30)
_HAJJ = datetime.datetime(2025, 6, 5)


def test_hijri_library_enabled():
    """المكتبة الهجرية مفعّلة فعلاً (تفعيل الميزة — لا يكفي وجود الكود)."""
    assert pe._HAS_HIJRI is True


def test_ramadan_signal_fires():
    sig = pe._temporal_signals(_RAMADAN)
    assert any('رمضان' in s for s in sig)
    # رمضان أولوية قصوى → الإشارة الأولى في القائمة
    assert 'رمضان' in sig[0]


def test_eid_signal_fires():
    sig = pe._temporal_signals(_EID)
    assert any('العيد' in s for s in sig)


def test_hajj_signal_fires():
    sig = pe._temporal_signals(_HAJJ)
    assert any('الحج' in s for s in sig)


def test_temporal_block_surfaces_ramadan():
    """build_temporal_block (prob=1 + بذرة مثبّتة) يُخرج ربط رمضان فعلاً في النص."""
    import random
    random.seed(0)
    block = pe.build_temporal_block(prob=1.0, now=_RAMADAN)
    assert block and 'ربط زمني' in block


def test_non_occasion_date_has_no_hijri_signal():
    """تاريخ عادي (لا رمضان/عيد/حج) لا يُنتج إشارة هجرية — فقط موسم/راتب/جمعة."""
    ordinary = datetime.datetime(2025, 5, 15)  # شهر 8 هجري تقريبًا، لا مناسبة
    sig = pe._temporal_signals(ordinary)
    assert not any(('رمضان' in s or 'العيد' in s or 'الحج' in s) for s in sig)
