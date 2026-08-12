# -*- coding: utf-8 -*-
"""فحص يدوي اختياري لاتصال OpenRouter.

لا يُخزَّن أي مفتاح في هذا الملف. ضع ``AI_KEY`` في ملف ``.env`` المحلي أو
في متغيرات النظام قبل تشغيله. هذا الملف ليس ضمن اختبارات pytest لأنه ينفذ
طلباً خارجياً وقد يستهلك من الرصيد.
"""
from __future__ import annotations

import os
from pathlib import Path

import requests


def load_local_key() -> str:
    """اقرأ AI_KEY من البيئة، ثم من .env المحلي من دون طباعة قيمته."""
    key = os.getenv("AI_KEY", "").strip()
    if key:
        return key

    env_file = Path(__file__).with_name(".env")
    if not env_file.exists():
        return ""

    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith("AI_KEY="):
            return line.partition("=")[2].strip().strip('"').strip("'")
    return ""


def check_connection() -> str:
    """نفّذ فحصاً صغيراً وأعد رسالة آمنة لا تعرض المفتاح."""
    key = load_local_key()
    if not key:
        return "لم يُعثر على AI_KEY في البيئة أو ملف .env."

    model = os.getenv("AI_MODEL", "google/gemini-2.5-flash").strip()
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": "Respond with OK."}],
                "max_tokens": 5,
            },
            timeout=20,
        )
    except requests.RequestException:
        return "تعذّر الوصول إلى OpenRouter. تحقّق من اتصال الشبكة ثم أعد المحاولة."

    if response.ok:
        return "اتصال OpenRouter يعمل."
    return f"فشل اتصال OpenRouter برمز HTTP {response.status_code}."


if __name__ == "__main__":
    print(check_connection())
