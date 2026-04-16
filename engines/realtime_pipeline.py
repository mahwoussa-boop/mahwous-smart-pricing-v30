"""
engines/realtime_pipeline.py — Real-Time Scraping + Matching Pipeline v1.0
===========================================================================
Streams competitor products as they are scraped and yields structured progress
events so the Streamlit UI (Task 2.4) can update in near-real-time.

Architecture (Producer / Consumer):
  ┌──────────────────────────────────────┐
  │  scrape_one_store_streaming(url)     │  ←── one asyncio.Task per store
  │  (async generator, yields dicts)    │
  └──────────────┬───────────────────────┘
                 │  put(event)
                 ▼
          asyncio.Queue  (shared, maxsize=500)
                 │
                 ▼
     run_realtime_pipeline()  ←── async generator consumed by Streamlit
         yields (event_type, data)

Event types emitted (in chronological order):
  "scraping_progress"  dict(store=str, count=int)
      → a new product row was scraped from <store>
  "scraping_done"      dict(store=str, total=int)
      → one store finished scraping
  "matching_start"     dict(total_rows=int, stores=list[str])
      → all stores done; matching begins now
  "complete"           dict(df=pd.DataFrame, audit=dict)
      → matching finished; full results available

Backward-compatibility guarantee:
  The old batch system (scrape_one_store / run_scraper) is untouched.
  This module only imports from engines.async_scraper and engines.engine —
  no monkey-patching.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger("RealtimePipeline")

# Unique sentinel — signals that a producer task has finished.
# Defined at module level so it survives import caching across calls.
_STORE_DONE = object()


# ══════════════════════════════════════════════════════════════════════════════
#  Public API
# ══════════════════════════════════════════════════════════════════════════════

async def run_realtime_pipeline(
    our_df: pd.DataFrame,
    store_urls: List[str],
    concurrency: int = 10,
    max_products_per_store: int = 0,
    use_ai: bool = False,
    result_callback: Optional[Callable[[str, Any], None]] = None,
) -> AsyncGenerator[Tuple[str, Any], None]:
    """
    Async generator that drives the full scrape-then-match pipeline and
    yields structured progress events for the Streamlit UI.

    Args:
        our_df                : our product catalogue (DataFrame)
        store_urls            : list of competitor store root URLs
        concurrency           : max parallel URL fetches *per store*
        max_products_per_store: cap per store; 0 means no cap (process until each
            store stream ends). The consumer has no row batch limit — it runs until
            every producer sends its finished sentinel (payload is None).
        use_ai                : pass to run_full_analysis(); False = fast fuzzy only
        result_callback       : optional sync callable(event_type, data) on each event
                                (same tuples as yielded); must be thread-safe if used
                                from mixed contexts.

    Yields:
        ("scraping_progress", {"store": str, "count": int})
        ("scraping_done",     {"store": str, "total": int})
        ("matching_start",    {"total_rows": int, "stores": list})
        ("complete",          {"df": pd.DataFrame, "audit": dict})

    Example (Streamlit Task 2.4):
        async for event_type, data in run_realtime_pipeline(our_df, urls):
            if event_type == "scraping_progress":
                st.session_state.live_count[data["store"]] = data["count"]
                st.rerun()
            elif event_type == "complete":
                st.session_state.results_df = data["df"]
                st.rerun()

    Fallback behaviour:
        Any per-store scraping error is caught and logged; that store
        contributes 0 rows but does not abort the pipeline.
        If no competitor data is scraped at all, ("complete", {"df": empty})
        is still yielded so callers do not hang.
    """
    # ── Guard: reject obviously bad inputs early ──────────────────────────────
    if our_df is None or our_df.empty:
        logger.warning("run_realtime_pipeline: our_df is empty — aborting")
        yield ("complete", {"df": pd.DataFrame(), "audit": {"error": "our_df_empty"}})
        return

    if not store_urls:
        logger.warning("run_realtime_pipeline: no store URLs provided — aborting")
        yield ("complete", {"df": pd.DataFrame(), "audit": {"error": "no_store_urls"}})
        return

    # Lazy import — avoids circular imports at module load time
    from engines.async_scraper import scrape_one_store_streaming, _domain

    # ── Phase 1: Producer / raw-queue / Consumer ─────────────────────────────
    # Producers: one task per store → raw_queue (domain, row|None sentinel).
    # Consumer: drains raw_queue, appends rows with per-row try/except, forwards
    #            structured events to event_queue. One bad row never kills the pipe.

    raw_queue: asyncio.Queue[Tuple[str, Any]] = asyncio.Queue(maxsize=500)
    event_queue: asyncio.Queue[Tuple[str, Any]] = asyncio.Queue(maxsize=500)

    store_rows: Dict[str, List[dict]] = {_domain(u): [] for u in store_urls}

    async def _producer(url: str) -> None:
        """Scrape one store; push (domain, row) or (domain, None) when finished."""
        domain = _domain(url)
        try:
            async for row in scrape_one_store_streaming(
                url,
                concurrency=concurrency,
                max_products=max_products_per_store,
            ):
                await raw_queue.put((domain, row))
        except Exception:
            logger.error(
                "Pipeline producer error for %s: %s",
                domain, traceback.format_exc()[:300],
            )
        finally:
            await raw_queue.put((domain, None))

    async def _consumer() -> None:
        """Drain raw_queue; isolate per-row failures; emit UI / callback events."""
        finished_stores = 0
        total = len(store_urls)
        while finished_stores < total:
            domain, payload = await raw_queue.get()
            if payload is None:
                finished_stores += 1
                try:
                    await event_queue.put(
                        (
                            "scraping_done",
                            {"store": domain, "total": len(store_rows[domain])},
                        )
                    )
                except Exception:
                    logger.error(
                        "Pipeline consumer (scraping_done): %s",
                        traceback.format_exc()[:200],
                    )
                continue
            try:
                if not isinstance(payload, dict):
                    continue
                store_rows[domain].append(payload)
                await event_queue.put(
                    (
                        "scraping_progress",
                        {
                            "store": domain,
                            "count": len(store_rows[domain]),
                        },
                    )
                )
            except Exception:
                logger.error(
                    "Pipeline consumer skipped bad row for %s: %s",
                    domain,
                    traceback.format_exc()[:300],
                )

    consumer_task = asyncio.create_task(_consumer())
    producer_tasks = [
        asyncio.create_task(_producer(url)) for url in store_urls
    ]

    stores_finished = 0
    total_stores = len(store_urls)

    try:
        while stores_finished < total_stores:
            event_type, data = await event_queue.get()
            if result_callback is not None:
                try:
                    result_callback(event_type, data)
                except Exception:
                    logger.error(
                        "result_callback error: %s",
                        traceback.format_exc()[:200],
                    )
            yield (event_type, data)
            if event_type == "scraping_done":
                stores_finished += 1
                logger.info(
                    "Pipeline: %s finished — %d rows  (%d/%d stores done)",
                    data["store"],
                    data["total"],
                    stores_finished,
                    total_stores,
                )
    finally:
        for t in producer_tasks:
            if not t.done():
                t.cancel()
        if not consumer_task.done():
            consumer_task.cancel()
        await asyncio.gather(*producer_tasks, consumer_task, return_exceptions=True)

    # ── Phase 2: Build competitor DataFrames from accumulated rows ────────────
    comp_dfs: Dict[str, pd.DataFrame] = {}
    total_rows = 0
    finished_stores: List[str] = []

    for domain, rows in store_rows.items():
        if rows:
            comp_dfs[domain] = pd.DataFrame(rows)
            total_rows       += len(rows)
            finished_stores.append(domain)
            logger.info("Pipeline: built comp_df for %s (%d rows)", domain, len(rows))

    if not comp_dfs:
        logger.warning(
            "run_realtime_pipeline: no competitor data scraped from any store"
        )
        yield (
            "complete",
            {"df": pd.DataFrame(), "audit": {"error": "no_competitor_data", "total_input": 0}},
        )
        return

    yield ("matching_start", {"total_rows": total_rows, "stores": finished_stores})
    logger.info("Pipeline: starting matching — %d competitor rows", total_rows)

    # ── Phase 3: Match against our catalogue (reuses existing engine) ─────────
    try:
        from engines.engine import run_full_analysis
        results_df, audit = run_full_analysis(
            our_df,
            comp_dfs,
            progress_callback=None,  # no per-row callback in pipeline mode
            use_ai=use_ai,
        )
        logger.info(
            "Pipeline: matching complete — %d result rows  (audit: %s)",
            len(results_df), audit,
        )
    except Exception:
        logger.error(
            "run_realtime_pipeline matching failed: %s",
            traceback.format_exc()[:400],
        )
        results_df = pd.DataFrame()
        audit      = {"error": "matching_failed", "traceback": traceback.format_exc()[:200]}

    yield ("complete", {"df": results_df, "audit": audit})


# ══════════════════════════════════════════════════════════════════════════════
#  Sync convenience wrapper (for non-async callers / testing)
# ══════════════════════════════════════════════════════════════════════════════

def run_realtime_pipeline_sync(
    our_df: pd.DataFrame,
    store_urls: List[str],
    concurrency: int = 10,
    max_products_per_store: int = 0,
    use_ai: bool = False,
    on_event: Optional[Any] = None,
    result_callback: Optional[Callable[[str, Any], None]] = None,
) -> pd.DataFrame:
    """
    Synchronous wrapper around run_realtime_pipeline().

    Runs a fresh event loop (safe from Streamlit threads or CLI scripts).
    Returns the final results DataFrame.

    Args:
        on_event: optional legacy alias for result_callback (if both given,
                  result_callback wins).
        result_callback: optional callable(event_type: str, data: Any) per event.

    Returns:
        pd.DataFrame with match results (empty if scraping failed).
    """
    _cb = result_callback if result_callback is not None else on_event

    async def _run() -> pd.DataFrame:
        result_df = pd.DataFrame()
        async for event_type, data in run_realtime_pipeline(
            our_df,
            store_urls,
            concurrency=concurrency,
            max_products_per_store=max_products_per_store,
            use_ai=use_ai,
            result_callback=_cb,
        ):
            if event_type == "complete":
                result_df = data.get("df", pd.DataFrame())
        return result_df

    return asyncio.run(_run())
