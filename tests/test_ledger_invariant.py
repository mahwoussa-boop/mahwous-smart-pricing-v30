"""
Phase 0 — invariant tests for observability.CompetitorIntakeLedger.

Rules enforced:
  ingested == confirmed + missing + rejected_structural
              + rejected_low_confidence + retry_pending + errors
  no row may remain INGESTED at run-end.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# Make repo root importable regardless of how pytest is invoked.
_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from observability.ledger import (  # noqa: E402
    CompetitorIntakeLedger,
    TERMINAL_STATES,
    CONFIRMED_MATCH,
    REJECTED_STRUCTURAL,
    RETRY_PENDING,
    make_comp_id,
)


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture()
def tmp_db(tmp_path):
    return str(tmp_path / "ledger_test.db")


@pytest.fixture()
def ledger(tmp_db):
    led = CompetitorIntakeLedger(tmp_db)
    try:
        yield led
    finally:
        led.close()


# ── Unit-level tests for the ledger itself ────────────────────────────────

def test_make_comp_id_is_stable():
    a = make_comp_id("shop.x", "Dior Sauvage 100ml", "https://shop.x/p/1")
    b = make_comp_id("shop.x", "Dior Sauvage 100ml", "https://shop.x/p/1")
    assert a == b
    assert len(a) == 16


def test_make_comp_id_differs_on_name_or_url():
    a = make_comp_id("shop.x", "Dior Sauvage 100ml", "https://shop.x/p/1")
    b = make_comp_id("shop.x", "Dior Sauvage 200ml", "https://shop.x/p/1")
    c = make_comp_id("shop.x", "Dior Sauvage 100ml", "https://shop.x/p/2")
    assert a != b and a != c and b != c


def test_mark_ingested_is_idempotent(ledger):
    cid = ledger.mark_ingested("shop.x", "A", url="u", raw={"price": 1})
    cid2 = ledger.mark_ingested("shop.x", "A", url="u", raw={"price": 2})
    assert cid == cid2
    counters = ledger.counters()
    assert counters["ingested"] == 1


def test_transition_and_counters(ledger):
    a = ledger.mark_ingested("x", "A", url="u1")
    b = ledger.mark_ingested("x", "B", url="u2")
    c = ledger.mark_ingested("x", "C", url="u3")
    ledger.mark_state(a, CONFIRMED_MATCH, last_score=94.0)
    ledger.mark_state(b, REJECTED_STRUCTURAL, reason_code="no_candidates")
    ledger.mark_state(c, RETRY_PENDING, reason_code="ai_unavailable")
    counters = ledger.counters()
    assert counters == {
        "ingested": 3,
        "confirmed": 1,
        "missing": 0,
        "rejected_structural": 1,
        "rejected_low_confidence": 0,
        "retry_pending": 1,
        "errors": 0,
        "inflight_ingested": 0,
        "degradation_events": 0,
    }
    ok, report = ledger.check_invariant()
    assert ok, report


def test_sweep_flips_ingested_to_terminal(ledger):
    ledger.mark_ingested("x", "A", url="u1")
    ledger.mark_ingested("x", "B", url="u2")
    # Only one transitions explicitly.
    cid_a = make_comp_id("x", "A", "u1")
    ledger.mark_state(cid_a, CONFIRMED_MATCH)
    ok_before, _ = ledger.check_invariant()
    assert not ok_before
    swept = ledger.sweep_untransitioned()
    assert swept == 1
    ok, report = ledger.check_invariant()
    assert ok, report
    assert report["counters"]["rejected_low_confidence"] == 1


def test_mark_error_counts_without_dropping_row(ledger):
    a = ledger.mark_ingested("x", "A", url="u1")
    ledger.mark_error(a, "test_error", "traceback excerpt")
    counters = ledger.counters()
    # Error rows are terminal and counted under errors.
    assert counters["errors"] == 1
    assert counters["ingested"] == 1
    ok, report = ledger.check_invariant()
    assert ok, report


def test_stateless_degradation_does_not_break_invariant(ledger):
    """
    ``counters_inc_error`` records telemetry that is NOT a row loss (e.g. an
    inner-loop scoring failure where the row is still alive). It must not
    break the ingested==terminal invariant.
    """
    ledger.mark_ingested("x", "A", url="u1")
    ledger.counters_inc_error("stateless_scoring_degradation")
    c_before_sweep = ledger.counters()
    assert c_before_sweep["degradation_events"] == 1
    assert c_before_sweep["errors"] == 0  # no row is in ERROR state
    ledger.sweep_untransitioned()
    ok, report = ledger.check_invariant()
    assert ok, report
    c = report["counters"]
    # Degradation events are reported but orthogonal to the invariant.
    assert c["degradation_events"] == 1
    assert c["ingested"] == 1
    assert c["rejected_low_confidence"] == 1


# ── Integration: missing_products_engine.detect_missing respects the ledger

def _mini_catalog():
    df = pd.DataFrame({
        "أسم المنتج": [
            "Dior Sauvage EDP 100ml",
            "Chanel Bleu EDT 100ml",
            "Creed Aventus 100ml",
        ],
        "سعر المنتج": [550, 600, 1200],
    })
    from engines.missing_products_engine import _normalize
    df["_norm"] = df["أسم المنتج"].apply(_normalize)
    return df[df["_norm"].str.len() > 0].reset_index(drop=True)


def _mini_comp_df():
    from engines.missing_products_engine import _normalize
    rows = [
        # Exact catalog match (score ≥ 85)
        {"اسم_المنتج": "Dior Sauvage EDP 100ml", "السعر": 540,
         "الرابط": "https://c1/p/1", "الصورة": "", "المنافس": "c1"},
        # Missing (no match)
        {"اسم_المنتج": "Tom Ford Oud Wood 50ml", "السعر": 900,
         "الرابط": "https://c2/p/2", "الصورة": "", "المنافس": "c2"},
        # Empty-normalized row — hits REJECTED_STRUCTURAL branch.
        {"اسم_المنتج": "...",
         "السعر": 0, "الرابط": "", "الصورة": "", "المنافس": "c3"},
    ]
    df = pd.DataFrame(rows)
    df["_norm"] = df["اسم_المنتج"].apply(_normalize)
    return df


def test_detect_missing_invariant(tmp_db):
    from engines.missing_products_engine import detect_missing

    led = CompetitorIntakeLedger(tmp_db)
    catalog = _mini_catalog()
    comp = _mini_comp_df()
    try:
        missing = detect_missing(comp, catalog, use_ai=False, ledger=led)
        # The one true missing row must be detected.
        names = {m.name for m in missing}
        assert "Tom Ford Oud Wood 50ml" in names

        led.sweep_untransitioned()
        ok, report = led.check_invariant()
        assert ok, report
        c = report["counters"]
        # 3 rows ingested (the dotted row still goes through mark_ingested
        # only when non-empty normalized name survives — but the explicit
        # REJECTED_STRUCTURAL path ingests+transitions it too).
        assert c["ingested"] >= 2
        assert c["confirmed"] >= 1              # Dior matched
        assert c["missing"] >= 1                # Tom Ford missing
    finally:
        led.close()


# ── Integration: run_full_analysis exposes ledger counters in audit ───────

def test_run_full_analysis_emits_ledger_audit(tmp_db, monkeypatch):
    """
    A tiny end-to-end: one our_df row, one comp_df row, no AI.
    The returned audit_stats must carry a "ledger" report with invariant_ok.
    """
    from engines.engine import run_full_analysis

    our_df = pd.DataFrame({
        "اسم المنتج": ["Dior Sauvage EDP 100ml"],
        "سعر المنتج": [550.0],
        "رقم المنتج": ["P001"],
    })
    comp_df = pd.DataFrame({
        "اسم المنتج": [
            "Dior Sauvage EDP 100ml",
            "Totally Unrelated Product XYZ",
        ],
        "سعر المنتج": [520.0, 99.0],
        "رابط المنتج": [
            "https://shop-x/p/sauvage",
            "https://shop-x/p/xyz",
        ],
    })
    led = CompetitorIntakeLedger(tmp_db)
    try:
        df_out, audit = run_full_analysis(
            our_df, {"shop-x": comp_df},
            progress_callback=None, use_ai=False, ledger=led,
        )
    finally:
        led.close()

    assert "ledger" in audit, audit
    ledger_report = audit["ledger"]
    assert ledger_report["invariant_ok"] is True, ledger_report
    c = ledger_report["counters"]
    # 2 competitor rows ingested, all terminal by end-of-run.
    assert c["ingested"] == 2
    assert c["inflight_ingested"] == 0
    terminal_sum = (
        c["confirmed"] + c["missing"]
        + c["rejected_structural"] + c["rejected_low_confidence"]
        + c["retry_pending"] + c["errors"]
    )
    assert terminal_sum == c["ingested"]
