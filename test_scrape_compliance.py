# -*- coding: utf-8 -*-
from unittest.mock import Mock, patch

from customer_feedback import FeedbackValidationError, VerifiedFeedback, prepare_verified_feedback
from scrape_compliance import ScrapePolicy, should_stop_after_response


def _robots_response(status_code=200, text="User-agent: *\nAllow: /\n"):
    response = Mock()
    response.status_code = status_code
    response.text = text
    return response


@patch("scrape_compliance.requests.get")
def test_scrape_policy_honors_robots_rules(get):
    get.return_value = _robots_response(text="User-agent: *\nDisallow: /private/\n")
    policy = ScrapePolicy()
    assert policy.check("https://example.test/public/page").allowed
    assert not policy.check("https://example.test/private/page").allowed
    assert get.call_count == 1


@patch("scrape_compliance.requests.get")
def test_scrape_policy_stops_when_robots_cannot_be_checked(get):
    get.return_value = _robots_response(status_code=503)
    assert not ScrapePolicy().check("https://example.test/catalog").allowed


def test_policy_does_not_impersonate_a_consumer_browser():
    assert "Mozilla/" not in ScrapePolicy().headers()["User-Agent"]


def test_blocking_statuses_stop_the_scraper():
    assert should_stop_after_response(403)
    assert should_stop_after_response(429)
    assert not should_stop_after_response(500)


def test_verified_feedback_requires_real_purchase_and_consent():
    feedback = VerifiedFeedback("عطر ورد", "ناسبني وثباته جيد", "ORD-1234", True)
    assert prepare_verified_feedback(feedback) == "ناسبني وثباته جيد"

    missing_consent = VerifiedFeedback("عطر ورد", "ناسبني", "ORD-1234", False)
    try:
        prepare_verified_feedback(missing_consent)
    except FeedbackValidationError as exc:
        assert "موافقة العميل" in str(exc)
    else:
        raise AssertionError("يجب رفض رأي بلا موافقة نشر")
