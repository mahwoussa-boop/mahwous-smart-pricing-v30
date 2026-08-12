# -*- coding: utf-8 -*-
import pytest

from customer_feedback import (
    FeedbackValidationError,
    VerifiedFeedback,
    build_experience_questions,
    build_publication_record,
)


def test_questions_are_neutral_and_include_improvement_prompt():
    questions = build_experience_questions('عطر مهووس')
    assert len(questions) >= 5
    assert any('نحسّنه' in question for question in questions)
    assert not any('امدح' in question for question in questions)


def test_publication_record_preserves_verified_customer_words():
    feedback = VerifiedFeedback(
        product_name='عطر مهووس',
        text='  الرائحة هادئة ومناسبة للدوام  ',
        order_reference='SALLA-1234',
        publish_consent=True,
    )
    record = build_publication_record(feedback)
    assert record['text'] == 'الرائحة هادئة ومناسبة للدوام'
    assert record['synthetic'] is False
    assert record['publishable'] is True


def test_publication_fails_without_explicit_consent():
    feedback = VerifiedFeedback('عطر', 'رأي واضح', 'ORDER-1', False)
    with pytest.raises(FeedbackValidationError):
        build_publication_record(feedback)
