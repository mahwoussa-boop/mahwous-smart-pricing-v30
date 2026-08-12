# -*- coding: utf-8 -*-
from empathy_engine import (
    EvidenceBundle,
    build_empathy_profile,
    build_evidence_grounded_brief,
    mark_synthetic_output,
)
from personas_engine import build_master_prompt, generate_persona, generate_review_params


def test_persona_has_deep_synthetic_empathy_profile():
    persona = generate_persona()
    profile = persona['empathy_profile']
    assert persona['synthetic'] is True
    assert persona['publishable'] is False
    assert profile['scent_memory_hypothesis']['evidence_status'] == 'hypothesis_to_validate'
    assert profile['empathy']['trust_needs']
    assert profile['voice']['typo_budget'] in (0, 1)


def test_profile_is_stable_for_same_persona():
    persona = {'name': 'اختبار', 'archId': 'أب_عائلة', 'age': 42, 'city': 'الرياض'}
    assert build_empathy_profile(persona) == build_empathy_profile(persona)


def test_synthetic_output_cannot_claim_verified_purchase():
    marked = mark_synthetic_output({'text': 'مثال', 'is_verified_purchase': True})
    assert marked['synthetic'] is True
    assert marked['publishable'] is False
    assert 'is_verified_purchase' not in marked


def test_prompt_carries_non_publishable_disclosure():
    persona = generate_persona()
    prompt, _ = build_master_prompt(
        persona, 'عطر تجريبي', generate_review_params(persona)
    )
    assert 'شخصية افتراضية' in prompt
    assert '"synthetic": true' in prompt
    assert '"publishable": false' in prompt


def test_brief_without_evidence_requests_research_not_claims():
    brief = build_evidence_grounded_brief({'name': 'بحث'}, 'عطر تجريبي')
    assert 'حوّل الادعاءات إلى أسئلة بحثية' in brief
    assert 'لا تنسب شراءً أو استخداماً' in brief


def test_evidence_bundle_requires_consent_and_observation():
    bundle = EvidenceBundle(
        source_type='salla_order',
        source_reference='ORDER-1234',
        verified_observations=('ذكر العميل أن التغليف وصل سليماً',),
        customer_consent=True,
    )
    assert bundle.is_publishable_source is True
