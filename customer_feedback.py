# -*- coding: utf-8 -*-
"""أدوات آمنة لطلب وتجهيز آراء العملاء الحقيقيين.

لا تنشئ هذه الوحدة مراجعات ولا تنتحل شخصية عميل. مهمتها تنظيم طلب الرأي
والتحقق من موافقة العميل قبل تجهيز نصه الأصلي للنشر.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple


class FeedbackValidationError(ValueError):
    """يرفع عند غياب أدلة الموافقة أو الشراء الحقيقي."""


@dataclass(frozen=True)
class VerifiedFeedback:
    """رأي أصلي يمكن عرضه بعد تحقق متطلبات النشر."""

    product_name: str
    text: str
    order_reference: str
    publish_consent: bool


def build_feedback_request(product_name: str, customer_name: str = "") -> str:
    """رسالة سعودية محايدة لطلب رأي صريح من العميل بعد تجربته الفعلية."""
    greeting = f"أهلًا {customer_name.strip()}، " if customer_name.strip() else "أهلًا، "
    return (
        f"{greeting}نتمنى أن تكون تجربتك مع {product_name.strip() or 'طلبك'} جميلة. "
        "يسعدنا رأيك الصريح عن المنتج أو التغليف أو التوصيل، سواء كان إيجابيًا أو فيه ملاحظة. "
        "لن ننشر كلامك إلا بموافقتك، وباسمك الأول فقط إن رغبت."
    )


def build_experience_questions(product_name: str) -> Tuple[str, ...]:
    """أسئلة محايدة تستخرج التفاصيل الحقيقية من العميل من دون تلقينه مدحاً."""
    product = product_name.strip() or "المنتج"
    return (
        f"ما أول شيء لاحظته عند تجربة {product}؟",
        "كيف تغيّرت الرائحة أو التجربة بعد مرور الوقت؟ اذكر المدة إن كنت متأكداً.",
        "كيف وصل التغليف؟ وما التفصيلة التي أعجبتك أو أزعجتك؟",
        "هل استخدمته في موقف محدد؟ صف الموقف من دون ذكر بيانات أشخاص آخرين.",
        "ما الشيء الذي تتمنى أن نحسّنه بصراحة؟",
        "هل تنصح به لشخص يشبه ذوقك؟ ولماذا أو لماذا لا؟",
    )


def prepare_verified_feedback(feedback: VerifiedFeedback) -> str:
    """نظّف تنسيق رأي العميل من دون تغيير معناه أو اختلاق تفاصيل جديدة."""
    errors: List[str] = []
    text = re.sub(r"\s+", " ", feedback.text or "").strip()
    if len(text) < 3:
        errors.append("نص الرأي قصير جدًا أو فارغ")
    if not feedback.product_name.strip():
        errors.append("اسم المنتج مطلوب")
    if len(feedback.order_reference.strip()) < 4:
        errors.append("مرجع طلب صالح مطلوب")
    if not feedback.publish_consent:
        errors.append("موافقة العميل على النشر مطلوبة")
    if errors:
        raise FeedbackValidationError("؛ ".join(errors))
    return text


def build_publication_record(feedback: VerifiedFeedback) -> dict:
    """سجل نشر قابل للتدقيق؛ يحتفظ بالمصدر والموافقة ولا يغيّر صوت العميل."""
    return {
        "product_name": feedback.product_name.strip(),
        "text": prepare_verified_feedback(feedback),
        "source_type": "verified_order_feedback",
        "source_reference": feedback.order_reference.strip(),
        "publish_consent": True,
        "synthetic": False,
        "publishable": True,
        "editing_policy": "formatting_only_no_new_claims",
    }
