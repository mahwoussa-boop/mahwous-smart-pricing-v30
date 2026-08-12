# -*- coding: utf-8 -*-
"""محرك تعاطف لشخصيات بحثية اصطناعية.

يبني فرضيات إنسانية لاختبار الرسائل وتجارب التسوق. لا ينتحل تجربة عميل،
ولا يسمح بتحويل مخرجاته إلى مراجعة عامة من دون مصدر حقيقي وموافقة موثقة.
"""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


SYNTHETIC_DISCLOSURE_AR = (
    "شخصية ومحتوى اصطناعيان لأغراض البحث الداخلي واختبار تجربة العميل؛ "
    "ليسا شهادة عميل ولا يجوز نشرهما كمراجعة حقيقية."
)


@dataclass(frozen=True)
class EvidenceBundle:
    """حقائق يجوز الاستناد إليها عند إعداد مسودة تسويقية قابلة للمراجعة."""

    source_type: str
    source_reference: str
    product_facts: Sequence[str] = field(default_factory=tuple)
    verified_observations: Sequence[str] = field(default_factory=tuple)
    customer_consent: bool = False

    @property
    def is_publishable_source(self) -> bool:
        return bool(
            self.customer_consent
            and self.source_reference.strip()
            and self.verified_observations
        )


_MEMORY_HYPOTHESES = {
    "young": [
        ("حمضيات خفيفة", "بداية يوم جامعة جديد", "حماس مع شيء بسيط من التوتر", "بداية واثقة"),
        ("مسك نظيف", "تجهيز سريع قبل لقاء الأصدقاء", "راحة وعفوية", "قبول اجتماعي بلا تكلّف"),
    ],
    "father": [
        ("عود دافئ", "مجلس العائلة مساء الجمعة", "طمأنينة وانتماء", "استمرار طقوس البيت"),
        ("عنبر هادئ", "لحظة خروج الأبناء للمدرسة", "مسؤولية وحنان", "حضور يبعث الأمان"),
    ],
    "working_woman": [
        ("مسك أبيض", "الدقائق الهادئة قبل اجتماع مهم", "تركيز وثبات", "ثقة مهنية هادئة"),
        ("ورد ناعم", "الانتقال من الدوام إلى مناسبة عائلية", "توازن ومرونة", "هوية لا تضيع مع ضغط اليوم"),
    ],
    "professional": [
        ("أخشاب نظيفة", "ترتيب المكتب في أول الصباح", "وضوح وسيطرة", "استعداد ليوم عملي طويل"),
        ("برغموت", "الطريق إلى موعد مهم", "يقظة وتفاؤل", "انطباع أول متزن"),
    ],
    "family": [
        ("بخور هادئ", "تجهيز البيت قبل وصول الضيوف", "كرم ودفء", "بيت له ذاكرة ورائحة"),
        ("ورد وعنبر", "مناسبة عائلية قديمة", "حنين وامتنان", "صلة بين الأجيال"),
    ],
}


def _stable_rng(persona: Mapping[str, Any]) -> random.Random:
    identity = "|".join(
        str(persona.get(key, "")) for key in ("name", "archId", "age", "city")
    )
    seed = int(hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16], 16)
    return random.Random(seed)


def _segment(persona: Mapping[str, Any]) -> str:
    arch = str(persona.get("archId", ""))
    age = int(persona.get("age", 30) or 30)
    if arch == "أب_عائلة":
        return "father"
    if arch in {"موظفة", "معلمة", "طبيبة", "خبيرة_تجميل"}:
        return "working_woman"
    if arch in {"أم_سعودية", "ست_بيت", "جدة"}:
        return "family"
    if age <= 24:
        return "young"
    return "professional"


def build_empathy_profile(persona: Mapping[str, Any]) -> dict[str, Any]:
    """أضف عمقاً إنسانياً ثابتاً مع إبقاء كل التفاصيل في نطاق الفرضية."""
    rng = _stable_rng(persona)
    segment = _segment(persona)
    trigger, scene, emotion, meaning = rng.choice(_MEMORY_HYPOTHESES[segment])
    dialect = persona.get("dialect_name") or persona.get("dialect") or "سعودية محلية"
    occupation = persona.get("occupation") or persona.get("label") or "عميل محتمل"

    return {
        "profile_type": "synthetic_research_persona",
        "synthetic": True,
        "publishable": False,
        "disclosure_ar": SYNTHETIC_DISCLOSURE_AR,
        "background": {
            "life_stage": segment,
            "occupation_context": occupation,
            "daily_pressure": rng.choice(
                ["ضيق الوقت", "الحيرة بين الخيارات", "الخوف من شراء رائحة لا تناسبه"]
            ),
        },
        "scent_memory_hypothesis": {
            "trigger": trigger,
            "scene": scene,
            "emotion": emotion,
            "meaning": meaning,
            "evidence_status": "hypothesis_to_validate",
        },
        "empathy": {
            "emotional_job": meaning,
            "functional_job": "اختيار عطر مناسب للسياق من دون مخاطرة شراء عمياء",
            "trust_needs": [
                "وصف واضح للرائحة بلا مبالغة",
                "توقع واقعي للثبات والفوحان حسب الاستخدام",
                "سياسة استبدال وخدمة مفهومة",
            ],
            "loyalty_condition": (
                "تتكوّن المناصرة فقط بعد تجربة حقيقية متسقة وخدمة ما بعد شراء جيدة"
            ),
        },
        "voice": {
            "dialect": dialect,
            "register": "عامية سعودية محترمة",
            "typo_budget": 1 if persona.get("has_typo") else 0,
            "variation_rule": "الاختلاف في الإيقاع والمفردات لا في اختلاق حقائق المنتج",
        },
    }


def mark_synthetic_output(output: Mapping[str, Any]) -> dict[str, Any]:
    """وسم غير قابل للتجاوز لمخرجات المحاكاة قبل الأرشفة أو التصدير."""
    marked = dict(output)
    marked.update(
        {
            "synthetic": True,
            "publishable": False,
            "content_purpose": "internal_research",
            "disclosure_ar": SYNTHETIC_DISCLOSURE_AR,
        }
    )
    marked.pop("is_verified_purchase", None)
    return marked


def build_evidence_grounded_brief(
    persona: Mapping[str, Any], product_name: str, evidence: EvidenceBundle | None = None
) -> str:
    """أنشئ موجز تعاطف؛ يمنع ادعاءات الأداء عند غياب دليل قابل للتتبع."""
    profile = build_empathy_profile(persona)
    memory = profile["scent_memory_hypothesis"]
    evidence_lines = list(evidence.product_facts) if evidence else []
    observation_lines = list(evidence.verified_observations) if evidence else []
    facts = "\n".join(f"- {item}" for item in evidence_lines) or "- لا توجد حقائق أداء موثقة"
    observations = (
        "\n".join(f"- {item}" for item in observation_lines)
        or "- لا توجد ملاحظة عميل موثقة؛ حوّل الادعاءات إلى أسئلة بحثية"
    )
    return f"""موجز تعاطف لبحث داخلي — غير قابل للنشر كشهادة عميل
المنتج: {product_name.strip() or 'غير محدد'}
السياق العاطفي المفترض: {memory['scene']} → {memory['meaning']}
احتياج الثقة: {profile['empathy']['trust_needs'][0]}

حقائق المنتج المسموح بها:
{facts}

ملاحظات موثقة:
{observations}

القواعد:
- لا تنسب شراءً أو استخداماً أو ثباتاً أو تغليفاً لشخص افتراضي على أنه حقيقة
- عند غياب الدليل اكتب سؤال مقابلة أو فرضية اختبار، لا مراجعة
- أي اقتباس عام يتطلب مرجع مصدر وموافقة صريحة
"""
