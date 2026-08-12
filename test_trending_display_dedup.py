# -*- coding: utf-8 -*-
"""حارس: لا تُختار نسختان من العطر نفسه داخل سلة شخصية واحدة.

بلاغ مراجعة كودية خارجية مُتحقَّق منه مباشرة: catalog.json يحوي منتجات لا
تختلف إلا بمسافة مضاعفة داخل الاسم (تحقّق: 3 أزواج متطابقة بعد تطبيع
المسافات، و0 متطابقة حرفياً). المتصفح يطوي المسافات فيعرضها بصورة واحدة،
بينما smart_blend كانت تمنع تكرار **كائن Python نفسه** فقط عبر id(p) — وهما
كائنان مختلفان — فتظهر «نسختان» من العطر ذاته في السلة الواحدة.
"""
import re

import trending


def _norm(name):
    return re.sub(r'\s+', ' ', name).strip()


def _pool():
    return [
        {'name': 'عطر  فخم  100مل', 'brand': 'ب', 'price': 100, 'g': 'رجالي'},
        {'name': 'عطر فخم 100مل', 'brand': 'ب', 'price': 100, 'g': 'رجالي'},
        {'name': 'عطر ثاني مختلف تماما', 'brand': 'ب', 'price': 120, 'g': 'رجالي'},
    ]


def test_visually_identical_products_are_never_both_selected():
    """انحدار: اسمان متطابقان بعد تطبيع المسافات = منتج واحد للمستخدم."""
    for _ in range(200):   # smart_blend عشوائية — نكرّر لتغطية المسارات
        selected = trending.smart_blend(_pool(), 3)
        names = [_norm(p['name']) for p in selected]
        assert len(names) == len(set(names)), (
            f'اختيرت نسختان بنفس الاسم المعروض: {names}')


def test_dedup_does_not_break_normal_selection():
    """ضابط سلبي: الفلترة تُسقط المكرر بصرياً فقط — لا تُفرغ الاختيار ولا
    تُسقط منتجات مختلفة فعلاً."""
    for _ in range(50):
        selected = trending.smart_blend(_pool(), 3)
        assert 1 <= len(selected) <= 2, (
            f'يفترض منتجين فريدين بصرياً من مجمع فيه 3 (نسختان منها واحدة): {len(selected)}')

    distinct = [
        {'name': 'عطر ألف', 'brand': 'ب', 'price': 100, 'g': 'رجالي'},
        {'name': 'عطر باء', 'brand': 'ب', 'price': 110, 'g': 'رجالي'},
        {'name': 'عطر جيم', 'brand': 'ب', 'price': 120, 'g': 'رجالي'},
    ]
    assert len(trending.smart_blend(distinct, 3)) == 3, (
        'منتجات مختلفة فعلاً يجب أن تُختار كلها — الفلترة تجاوزت هدفها')
