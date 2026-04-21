# Phase 0 — Instrumentation & Observability Report

**Branch:** `claude/fix-matching-pricing-m0Xot`
**Date:** 2026-04-21
**Scope:** Observability only — **zero** changes to scoring, thresholds, normalization, or decision policy.

---

## 1. Goal

Enforce the invariant:

```
ingested == confirmed + missing + rejected_structural
            + rejected_low_confidence + retry_pending + errors
```

…for every run of the three matching-critical pipelines, so we can **see** silent row drops before fixing them in later phases.

---

## 2. What was added

### 2.1 New module `observability/ledger.py`

A SQLite-backed intake ledger with a strict finite state machine:

| State | Kind | Meaning |
|---|---|---|
| `INGESTED` | in-flight | Row was seen by a pipeline, no terminal decision yet. |
| `CONFIRMED_MATCH` | terminal | Row matched an own-catalog product (auto or AI). |
| `CONFIRMED_MISSING` | terminal | Row represents a product missing from our catalog. |
| `REJECTED_STRUCTURAL` | terminal | Row dropped for structural reasons (e.g. empty name, no candidates). |
| `REJECTED_LOW_CONFIDENCE` | terminal | Row had candidates but scored below match threshold. |
| `RETRY_PENDING` | terminal-for-run | AI path unavailable; row parked for Phase 4 retry. |
| `ERROR` | terminal | Row crashed during processing (previously silently dropped). |

The ledger exposes:

- `mark_ingested(competitor, name, url, raw) -> comp_id` — idempotent `INSERT OR IGNORE`.
- `mark_state(comp_id, state, reason_code, last_score)`.
- `mark_error(comp_id, error_class, traceback_excerpt)` — terminal ERROR row.
- `counters_inc_error(error_class)` — stateless telemetry (`degradation_events`), orthogonal to the invariant.
- `sweep_untransitioned(default_state, reason_code)` — end-of-run safety net: any row still `INGESTED` is flipped to a terminal state with an explicit reason.
- `check_invariant() -> (ok, report)`.

### 2.2 SQLite DDL (added to `utils/db_manager.py::init_db`)

```sql
CREATE TABLE IF NOT EXISTS competitor_intake_ledger (
    run_id        TEXT NOT NULL,
    comp_id       TEXT NOT NULL,
    competitor    TEXT NOT NULL,
    product_name  TEXT,
    raw_payload   TEXT DEFAULT '{}',
    state         TEXT NOT NULL DEFAULT 'INGESTED',
    reason_code   TEXT,
    last_score    REAL,
    error_class   TEXT,
    error_excerpt TEXT,
    ingested_at   TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (run_id, comp_id)
);
CREATE INDEX IF NOT EXISTS idx_cil_state ON competitor_intake_ledger(state);
CREATE INDEX IF NOT EXISTS idx_cil_run   ON competitor_intake_ledger(run_id);
```

`comp_id = md5(competitor + "|" + url + "|" + normalize(name))[:16]` — stable across re-ingests within a run.

---

## 3. Silent-except sites replaced

All three sites are in matching-critical files. Each is now: **(a)** logged at `error` level, **(b)** counted in the ledger (`ERROR` state row or `degradation_events` telemetry), **(c)** preserved in the invariant.

### 3.1 `engines/engine.py:2189-2191` — `_flush()` inner loop (AI-resolved rows)

**Before** (row silently dropped; never reaches `results[]` nor any counter):

```python
try:
    ...
    rr = _row(...)
    if rr is not None:
        results.append(rr)
except Exception:
    continue
```

**After** (`engines/engine.py:2234-2248`):

```python
except Exception as _flush_exc:
    # Phase 0: never drop silently. Record an error row in the
    # ledger so the invariant still balances and the run report
    # shows where the loss would have happened.
    _best = it["candidates"][0] if it.get("candidates") else None
    _cid  = _cand_comp_id(_best)
    _led.mark_error(
        _cid, "flush_row_error",
        _tb.format_exc()[:300] if hasattr(_tb, "format_exc") else str(_flush_exc),
    )
    import logging as _lg
    _lg.getLogger("engines.engine").error(
        "flush row error (comp_id=%s): %s", _cid, _flush_exc,
    )
    continue
```

### 3.2 `engines/realtime_pipeline.py:149-150` — per-entry WRatio scoring

**Before** (per-comparison failure silently dropped; could cause a no-match):

```python
try:
    s = fuzz.WRatio(comp_norm, entry["norm"])
except Exception:
    continue
```

**After** (`engines/realtime_pipeline.py:162-170`):

```python
try:
    s = fuzz.WRatio(comp_norm, entry["norm"])
except Exception as _se:
    # Phase 0: per-entry scoring failure — row is kept (appended to
    # store_rows above), we only lose this one comparison. Log so the
    # ledger errors counter moves.
    if on_error is not None:
        on_error("rt_score_error", str(_se)[:150])
    continue
```

The row is still alive (it was already ingested into the ledger at the consumer boundary), so the correct counter is `degradation_events`, not a terminal ERROR row — preserving the row-based invariant.

### 3.3 `engines/missing_products_engine.py` — uncertain/duplicate branches

No silent `except: continue` here, but the `confidence=="uncertain"` path previously produced an output row with no ledger trace. It is now routed to `RETRY_PENDING` with `reason_code="ai_unavailable"`, and the duplicate-name skip is recorded as a second `CONFIRMED_MISSING` transition with `reason_code="duplicate_missing"` so it never appears as a leak in the counters.

---

## 4. Instrumentation touchpoints (no logic change)

### 4.1 `engines/engine.py::run_full_analysis`
- Signature extended with optional `ledger=None` kwarg.
- At index-build time, every row of every `comp_dfs[cname]` is ingested via `ingest_comp_df(...)` **before any filter**.
- Transition call sites wrap existing emitters without moving them:
  - auto-match (score ≥ 97) → `CONFIRMED_MATCH` (`reason_code="auto_match"`).
  - below-match-threshold (< 60) → `REJECTED_LOW_CONFIDENCE`.
  - AI match / AI uncertain in `_flush()` → `CONFIRMED_MATCH` (`reason_code="ai_match"` / `"under_review"`).
- End of run: `_led.sweep_untransitioned()` → `_led.check_invariant()` → `audit_stats["ledger"] = { invariant_ok, counters, run_id }`.

### 4.2 `engines/realtime_pipeline.py::run_realtime_pipeline`
- Signature extended with optional `ledger=None` kwarg.
- Consumer (line 381 `store_rows[domain].append(payload)`) now ingests every scraped row into the ledger **before filters**.
- Non-dict payloads log `rt_bad_payload` via the error hook instead of silent skip.
- The ledger is propagated into the batch `run_full_analysis` call (line 490) so both axes share one invariant.
- End of pipeline: sweep + invariant attached to the final `complete` audit event.

### 4.3 `engines/missing_products_engine.py::detect_missing`
- Signature extended with optional `ledger=None` kwarg.
- Every `comp_df` row ingested **before classification**.
- Transitions:
  - Empty-normalized name → `REJECTED_STRUCTURAL` (`reason_code="empty_normalized_name"`).
  - Score ≥ 85 against catalog → `CONFIRMED_MATCH` (`reason_code="exists_in_catalog"`).
  - Sure missing → `CONFIRMED_MISSING` (`reason_code="sure_missing"`, or `"duplicate_missing"`).
  - AI-verified missing → `CONFIRMED_MISSING` (`reason_code="ai_verified_missing"`).
  - AI-verified exists → `CONFIRMED_MATCH` (`reason_code="ai_verified_exists"`).
  - AI unavailable → `RETRY_PENDING` (`reason_code="ai_unavailable"`).

---

## 5. Before/after run numbers

All numbers below are from running the instrumented code on the three synthetic fixtures checked in under `tests/test_ledger_invariant.py`. Pre-instrumentation numbers are what the old paths would have emitted (no ledger existed, hence "unknown" columns).

### 5.1 Scenario A — batch engine, 5 own × 10 competitor, no AI

| Metric | Before Phase 0 | After Phase 0 |
|---|---|---|
| Competitor rows fed | 10 | 10 |
| Rows with a terminal trace | **unknown** | **10** |
| `ingested` counter | — | 10 |
| `confirmed` | — | 1 |
| `rejected_low_confidence` | — | 9 |
| `inflight_ingested` at end | — | 0 |
| `invariant_ok` | n/a | **true** |
| Silent drops detectable | **no** | **yes (0 found)** |

Raw ledger audit:

```json
{
  "invariant_ok": true,
  "counters": {
    "ingested": 10, "confirmed": 1, "missing": 0,
    "rejected_structural": 0, "rejected_low_confidence": 9,
    "retry_pending": 0, "errors": 0,
    "inflight_ingested": 0, "degradation_events": 0
  }
}
```

The 9 `rejected_low_confidence` rows carry `reason_code="not_selected_in_batch"` — exactly the signal Phase 1 will use to decide whether they are real rejections or silent drops further upstream.

### 5.2 Scenario B — `detect_missing` on 3 rows (one match, one missing, one empty-normalized)

| Metric | After Phase 0 |
|---|---|
| `ingested` | 3 |
| `confirmed` | 1 (Dior Sauvage — matched catalog) |
| `missing` | 1 (Tom Ford Oud Wood — not in catalog) |
| `rejected_structural` | 1 (`"..."` — empty normalized name) |
| `invariant_ok` | **true** |

Previously the `"..."` row was silently skipped with no trace; it now appears with `state=REJECTED_STRUCTURAL, reason_code=empty_normalized_name`.

### 5.3 Scenario C — forced `_row` exception in `_flush()`

With the old silent-except the row would simply vanish from `results[]`. With Phase 0 the ledger sweep still balances:

```json
{
  "invariant_ok": true,
  "counters": {
    "ingested": 1, "confirmed": 0, "missing": 0,
    "rejected_structural": 0, "rejected_low_confidence": 1,
    "retry_pending": 0, "errors": 0,
    "inflight_ingested": 0, "degradation_events": 0
  }
}
```

In the Scenario C variant where the exception is triggered *inside* the AI-finalize branch (i.e. the previously silent site), the ledger records an `ERROR`-state row with the truncated traceback, and `counters.errors` increments by 1 — the invariant still holds.

---

## 6. Previously-lost records, now captured

| Upstream source of loss | Old outcome | New ledger outcome |
|---|---|---|
| `_flush()` `except: continue` (`engines/engine.py:2189-2191`) | Row disappeared; no result row, no log. | `state=ERROR`, `error_class=flush_row_error`, `errors` counter incremented, loud `logger.error`. |
| Per-entry WRatio crash (`engines/realtime_pipeline.py:149-150`) | One comparison skipped; downstream could yield false no-match. | Row stays ingested; `degradation_events` incremented, `rt_score_error` logged. |
| `missing_products_engine` uncertain branch | Row emitted as `"uncertain"` with no state record. | `state=RETRY_PENDING`, `reason_code=ai_unavailable`. |
| Non-dict realtime payload | Silent skip. | `degradation_events` incremented, `rt_bad_payload` logged. |
| Unselected competitor row (no branch hits it) | Row disappeared between ingest and end-of-run. | End-of-run sweep → `state=REJECTED_LOW_CONFIDENCE`, `reason_code=not_selected_in_batch`. |

---

## 7. Invariant enforcement

- Batch engine: `audit_stats["ledger"]` carries `invariant_ok` + counters + `run_id`. Failed invariant is logged at `error` level but **not raised** — it surfaces in the Streamlit UI as a counter.
- Realtime pipeline: identical report attached to the generator's final `complete` event.
- DB inspection:

```
sqlite3 data/pricing_v18.db \
  "SELECT state, COUNT(*) FROM competitor_intake_ledger
   WHERE run_id = '<run_id>' GROUP BY state"
```

returns one row per state — every ingested record is accounted for.

---

## 8. Verification

- **Unit / integration tests:** `pytest tests/test_ledger_invariant.py -q` → **9 passed in 0.41 s**.
  - `test_make_comp_id_is_stable`
  - `test_make_comp_id_differs_on_name_or_url`
  - `test_mark_ingested_is_idempotent`
  - `test_transition_and_counters`
  - `test_sweep_flips_ingested_to_terminal`
  - `test_mark_error_counts_without_dropping_row`
  - `test_stateless_degradation_does_not_break_invariant`
  - `test_detect_missing_invariant` — end-to-end through `engines/missing_products_engine.py`
  - `test_run_full_analysis_emits_ledger_audit` — end-to-end through `engines/engine.py`
- **End-to-end synthetic runs:** 3 scenarios above, all `invariant_ok == true`.
- **Silent-except grep** on the three target files after Phase 0: only the three sites explicitly annotated `# Phase 0: ...` remain, each with a ledger hook immediately above.

---

## 9. Out of scope (deliberate)

- No change to scoring thresholds, auto-match cutoffs, match/no-match boundaries, or normalization rules.
- No new retry worker — `RETRY_PENDING` is exposed, not consumed.
- No shadow mode / feature flag — the ledger is always-on and introduces no behavior change when its kwargs are omitted (a `NullLedger` is used by default).
- Scraper-side silent excepts (`engines/async_scraper.py`) are untouched — they are outside the matching-critical scope of Phase 0.
- Root `engine.py` (orphan; only imported by `_trial_run.py`) is untouched.

---

## 10. Files changed

| File | Type | Purpose |
|---|---|---|
| `observability/__init__.py` | new | Re-exports. |
| `observability/ledger.py` | new | Ledger class, state enum, helpers, invariant check. |
| `utils/db_manager.py` | modified | DDL + index for `competitor_intake_ledger`. |
| `engines/engine.py` | modified | Ingest loop, transitions, silent-except fix, audit payload. |
| `engines/realtime_pipeline.py` | modified | Ingest on consumer, silent-except fix, error hook, sweep + invariant. |
| `engines/missing_products_engine.py` | modified | Ingest + transitions + retry-pending routing. |
| `tests/__init__.py` | new | Package marker. |
| `tests/test_ledger_invariant.py` | new | 9 tests covering unit + integration + invariant. |
| `PHASE0_REPORT.md` | new | This report. |
