# -*- coding: utf-8 -*-
"""ضمان: لا طلب خارجي من Streamlit عند غياب مفتاح الذكاء الاصطناعي."""
import importlib


def test_streamlit_skips_provider_call_without_key(monkeypatch):
    monkeypatch.delenv('AI_KEY', raising=False)
    import streamlit_app

    module = importlib.reload(streamlit_app)
    monkeypatch.setattr(module, 'AI_KEY', '')

    def no_provider_call(*args, **kwargs):
        raise AssertionError('لا يجوز الاتصال بمزود الذكاء الاصطناعي بلا مفتاح')

    monkeypatch.setattr(module.http_req, 'post', no_provider_call)
    assert module.ai_call('اختبار') is None
