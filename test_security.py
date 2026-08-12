# -*- coding: utf-8 -*-
"""اختبارات حماية المسارات العامة ومفاتيح الذكاء الاصطناعي."""
import app as flask_app


def test_health_does_not_expose_server_paths_or_key_source():
    response = flask_app.app.test_client().get('/health')
    data = response.get_json()
    assert response.status_code == 200
    assert 'data_dir' not in data
    assert 'ai_key_source' not in data


def test_ai_check_is_not_public_and_does_not_ping_provider(monkeypatch):
    monkeypatch.setattr(flask_app, 'ADMIN_TOKEN', '')
    response = flask_app.app.test_client().get('/api/ai-check')
    assert response.status_code == 404


def test_authorized_ai_check_is_local_only(monkeypatch):
    monkeypatch.setattr(flask_app, 'ADMIN_TOKEN', 'admin-test-token')

    def no_provider_call(*args, **kwargs):
        raise AssertionError('يجب ألا ينفذ مسار التشخيص طلباً خارجياً')

    monkeypatch.setattr(flask_app._ai_session, 'post', no_provider_call)
    response = flask_app.app.test_client().get(
        '/api/ai-check', headers={'X-Admin-Token': 'admin-test-token'}
    )
    assert response.status_code == 200
    data = response.get_json()
    assert 'key_masked' not in data
    assert 'key_source' not in data


def test_sensitive_admin_routes_are_hidden_without_token(monkeypatch):
    monkeypatch.setattr(flask_app, 'ADMIN_TOKEN', '')
    client = flask_app.app.test_client()
    for route in ('/api/git-push', '/api/archive/clear', '/api/intel/refresh'):
        response = client.post(route)
        assert response.status_code == 404, route


def test_git_push_stays_disabled_even_for_admin_by_default(monkeypatch):
    monkeypatch.setattr(flask_app, 'ADMIN_TOKEN', 'admin-test-token')
    monkeypatch.delenv('ENABLE_GIT_PUSH', raising=False)
    response = flask_app.app.test_client().post(
        '/api/git-push', headers={'X-Admin-Token': 'admin-test-token'}
    )
    assert response.status_code == 403
    assert response.get_json()['error'] == 'git_push_disabled'
