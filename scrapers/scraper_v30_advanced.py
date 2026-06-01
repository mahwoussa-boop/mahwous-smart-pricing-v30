"""
scrapers/scraper_v30_advanced.py — Shim (Phase 2 Item 6)
════════════════════════════════════════════════════════
The real implementation lives in `engines.scraper_v30_advanced` (v30.3).

Before Phase 2 this file held a stale **duplicate** of the extractor
(v30.2): PriceExtractor, AdvancedScraper, and run_advanced_price_scraping
were defined twice with subtle drift (missing Salla selectors, different
error handling, silent USD/SAR inconsistency). Nothing actually imported
from this path (verified via grep), so the duplicate was dead code that
kept divergence invisible.

Rolling up to a single source of truth means every price extraction now
goes through the same SAR-first selectors, the same USD exclusion logic,
and the same JSON-LD priceCurrency check.
"""
from engines.scraper_v30_advanced import (  # noqa: F401
    PriceExtractor,
    AdvancedScraper,
    run_advanced_price_scraping,
    _line_has_foreign_currency,
    _ai_extract_price,
    _ai_clean_product_name,
)
from engines.scraper_v30_advanced import *  # noqa: F401, F403
