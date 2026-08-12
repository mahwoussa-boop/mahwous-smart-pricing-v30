# -*- coding: utf-8 -*-
from unittest.mock import Mock, patch

import test_api


@patch("test_api.requests.post")
def test_connection_check_uses_the_app_default_model(post, monkeypatch):
    monkeypatch.setattr(test_api, "load_local_key", lambda: "test-key")
    response = Mock()
    response.ok = True
    post.return_value = response

    assert test_api.check_connection() == "اتصال OpenRouter يعمل."
    assert post.call_args.kwargs["json"]["model"] == "google/gemini-2.5-flash"


@patch("test_api.requests.post")
def test_connection_check_handles_network_errors_without_traceback(post, monkeypatch):
    monkeypatch.setattr(test_api, "load_local_key", lambda: "test-key")
    post.side_effect = test_api.requests.RequestException()

    assert "تعذّر الوصول" in test_api.check_connection()
