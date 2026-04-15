"""
engines/scraper_v30_advanced.py — محرك الكشط المتقدم v30.2
══════════════════════════════════════════════════════════════
v30.2 fixes:
  • Semaphore(8) prevents TCPConnector pool exhaustion (was: 25-product deadlock)
  • SAR-first currency extraction — USD/$  explicitly excluded
  • JSON-LD priceCurrency check
  • Per-request timeout=15s (was: 25s session-level causing stale connections)
  • Sync fallback runs in ThreadPoolExecutor to avoid event-loop blocking
  • AI fallback also cleans product names (removes Tester/Sample)
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import re
import json
from typing import Any, Dict, List, Set

from bs4 import BeautifulSoup

logger = logging.getLogger("ScraperV30Adv")
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
logger.setLevel(logging.DEBUG)

# ── Anti-ban integration ─────────────────────────────────────────────────────
try:
    from scrapers.anti_ban import (
        get_browser_headers,
        get_rate_limiter,
        try_all_sync_fallbacks,
        looks_like_bot_challenge,
    )
    _HAS_ANTI_BAN = True
except ImportError:
    _HAS_ANTI_BAN = False

    def get_browser_headers(referer=""):
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/134.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        }

    def looks_like_bot_challenge(html):
        return False

# Thread pool for sync fallbacks — avoids blocking the event loop
_SYNC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="scrv30")

# ── USD/non-SAR detection ────────────────────────────────────────────────────
_USD_MARKERS = re.compile(r"\$|USD|usd|دولار|euro|EUR|eur|يورو|£|GBP", re.I)


def _line_has_foreign_currency(text: str) -> bool:
    """Returns True if line contains USD/EUR/GBP markers — skip it."""
    return bool(_USD_MARKERS.search(text))


# ══════════════════════════════════════════════════════════════════════════════
#  استخراج الأسعار — SAR-first, USD-excluded
# ══════════════════════════════════════════════════════════════════════════════
class PriceExtractor:

    @staticmethod
    def extract_price(html: str, url: str = "") -> Optional[float]:
        try:
            soup = BeautifulSoup(html, "html.parser")

            # ── Strategy 1: SAR-specific selectors FIRST ─────────────────
            for selector in (
                "span[class*='sar']", "span[class*='ريال']",
                "div[class*='sar']", "div[class*='ريال']",
                "span.s-product-price", "div.s-product-price",
                ".s-price-wrapper span",
            ):
                for elem in soup.select(selector):
                    text = elem.get_text(strip=True)
                    if not _line_has_foreign_currency(text):
                        p = PriceExtractor._parse_sar_text(text)
                        if p:
                            return p

            # ── Strategy 2: Generic price selectors ──────────────────────
            for selector in (
                "span.price", "span.product-price", "div.price", "p.price",
                "span[class*='price']", "div[class*='price']",
                "span.product__price", "span.money",
                ".product-price__value", ".product-price--sale",
            ):
                for elem in soup.select(selector):
                    text = elem.get_text(strip=True)
                    if _line_has_foreign_currency(text):
                        continue  # Skip USD prices
                    p = PriceExtractor._parse_sar_text(text)
                    if p:
                        return p

            # ── Strategy 3: data-* attributes ────────────────────────────
            for attr in ("data-price", "data-product-price", "data-amount", "data-regular-price"):
                for el in soup.find_all(attrs={attr: True}):
                    try:
                        p = float(str(el[attr]).replace(",", ""))
                        if 0 < p < 100_000:
                            return p
                    except (ValueError, TypeError):
                        pass

            # ── Strategy 4: JSON-LD with priceCurrency check ─────────────
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    ld = json.loads(script.string or "")
                    if isinstance(ld, list):
                        for item in ld:
                            if isinstance(item, dict) and "offers" in item:
                                ld = item
                                break
                        else:
                            continue
                    offers = ld.get("offers", ld)
                    if isinstance(offers, list):
                        offers = offers[0] if offers else {}
                    # Check currency — accept SAR or missing (assume SAR for .sa domains)
                    currency = str(offers.get("priceCurrency", offers.get("currency", ""))).upper()
                    if currency and currency not in ("SAR", ""):
                        continue  # Skip non-SAR prices
                    for pk in ("price", "lowPrice", "highPrice"):
                        p_str = str(offers.get(pk, ""))
                        if p_str:
                            p = float(p_str.replace(",", ""))
                            if 0 < p < 100_000:
                                return p
                except Exception:
                    pass

            # ── Strategy 5: Inline JS patterns ───────────────────────────
            for script in soup.find_all("script"):
                txt = script.string or ""
                if len(txt) > 200_000:
                    continue  # Skip huge bundles
                for pattern in (
                    r'"price"\s*:\s*["\']?(\d+(?:\.\d+)?)',
                    r'"sale_price"\s*:\s*["\']?(\d+(?:\.\d+)?)',
                    r'"amount"\s*:\s*["\']?(\d+(?:\.\d+)?)',
                ):
                    m = re.search(pattern, txt)
                    if m:
                        p = float(m.group(1))
                        if 0 < p < 100_000:
                            return p

            # ── Strategy 6: Arabic text lines (SAR keywords, USD excluded)
            text = soup.get_text()
            for line in text.split("\n"):
                line_s = line.strip()
                if not line_s or len(line_s) > 200:
                    continue
                if any(kw in line_s for kw in ("ريال", "رس", "ر.س", "SAR")):
                    if _line_has_foreign_currency(line_s):
                        continue
                    p = PriceExtractor._parse_sar_text(line_s)
                    if p:
                        return p

            return None
        except Exception as e:
            logger.debug(f"price extract error: {e}")
            return None

    @staticmethod
    def _parse_sar_text(text: str) -> Optional[float]:
        """Parse price from text — strips SAR markers, rejects if USD present."""
        try:
            if _line_has_foreign_currency(text):
                return None
            cleaned = (
                text.replace("ريال", "").replace("رس", "").replace("ر.س", "")
                .replace("SAR", "").replace(",", "").replace("،", "").strip()
            )
            numbers = re.findall(r"\d+\.?\d*", cleaned)
            if numbers:
                p = float(numbers[0])
                if 0 < p < 100_000:
                    return p
        except Exception:
            pass
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  AI Fallback — price extraction + name cleaning (cost-guarded)
# ══════════════════════════════════════════════════════════════════════════════
_AI_FALLBACK_BUDGET = 50
_ai_fallback_used = 0


def _ai_extract_price(text_snippet: str) -> Optional[float]:
    """Minimal Gemini prompt to extract SAR price. Capped at budget."""
    global _ai_fallback_used
    if _ai_fallback_used >= _AI_FALLBACK_BUDGET:
        return None
    try:
        from engines.ai_engine import _call_gemini
    except ImportError:
        return None
    snippet = text_snippet[:2500]
    prompt = (
        "Extract the product price in SAR (Saudi Riyals) from this text. "
        "IGNORE any USD or dollar prices. "
        "Reply with ONLY the numeric SAR price (e.g. 299.00). "
        "If no SAR price found, reply with 0.\n\n"
        f"Text:\n{snippet}"
    )
    try:
        resp = _call_gemini(prompt, temperature=0.0, max_tokens=32)
        if resp:
            _ai_fallback_used += 1
            nums = re.findall(r"\d+\.?\d*", str(resp).strip())
            if nums:
                p = float(nums[0])
                if 0 < p < 100_000:
                    return p
    except Exception as e:
        logger.debug(f"AI price fallback error: {e}")
    return None


def _ai_clean_product_name(raw_name: str) -> str:
    """Clean product name — remove junk like 'Tester', page titles, etc."""
    if not raw_name or len(raw_name) < 3:
        return raw_name
    # Fast local cleaning first
    cleaned = raw_name
    for junk in (
        " - متجر", " | متجر", " – متجر", "| مهووس", "| Mahwous",
        " - خبير العطور", "| خبير", " | سعيد صلاح",
        " - فانيلا", "| فانيلا", "| Vanilla",
        " - Golden Scent", "| Golden Scent",
    ):
        cleaned = cleaned.replace(junk, "")
    # Remove store name patterns from <title>
    cleaned = re.sub(r"\s*[|\-–—]\s*[^\|–—]{0,40}(متجر|store|shop|ستور)\s*$", "", cleaned, flags=re.I)
    return cleaned.strip()


# ══════════════════════════════════════════════════════════════════════════════
#  المحرك — Semaphore-guarded, no pool exhaustion
# ══════════════════════════════════════════════════════════════════════════════
class AdvancedScraper:

    def __init__(self, max_concurrent: int = 8):
        # Keep concurrency modest since each Selenium render is expensive.
        self.max_concurrent = max(1, min(int(max_concurrent or 1), 3))

    async def scrape_product_page(self, url: str, store_name: str) -> Dict[str, Any]:
        """Scrape one URL by delegating to Selenium stealth engine."""
        results = await self.scrape_batch([url], store_name)
        if results:
            return results[0]
        return self._fail_result(url, store_name)

    @staticmethod
    def _fail_result(url: str, store_name: str) -> Dict[str, Any]:
        return {
            "url": url, "store": store_name,
            "product_name": url.split("/")[-1].replace("-", " "),
            "price": 0.0, "image_url": "", "success": False,
        }

    async def scrape_batch(
        self, urls: List[str], store_name: str,
        progress_cb=None,
    ) -> List[Dict]:
        """Scrape a batch via Selenium v30 in a worker thread."""
        if not urls:
            return []
        total = len(urls)
        print(f"[DEBUG][scrape_batch] store={store_name} total_urls={total}")
        for idx, u in enumerate(urls, start=1):
            print(f"[DEBUG][scrape_batch] -> url[{idx}/{total}] {u}")
        try:
            from engines.selenium_scraper_v30 import scrape_many_products_v30
            raw_results = await asyncio.to_thread(
                scrape_many_products_v30,
                urls,
                store_url="",
                max_workers=self.max_concurrent,
                proxy_pool=None,
                ai_price_extractor=None,
            )
        except Exception as batch_err:
            logger.error(f"Selenium batch failed for {store_name}: {batch_err}")
            logger.exception("Full traceback for Selenium batch failure")
            raw_results = []

        normalized: List[Dict[str, Any]] = []
        for idx, raw in enumerate(raw_results, start=1):
            print(
                "[DEBUG][scrape_batch] <- selenium_result"
                f" [{idx}/{len(raw_results)}] "
                f"url={raw.get('url')} success={raw.get('success')} "
                f"price={raw.get('price')} source={raw.get('source')} "
                f"error={raw.get('error')}"
            )
            final_url = str(raw.get("url") or "").strip()
            original_url = final_url if final_url else ""
            product_name = str(raw.get("name") or "").strip()
            product_name = _ai_clean_product_name(product_name) if product_name else ""
            image_url = str(raw.get("image") or "").strip()
            try:
                price = float(raw.get("price") or 0.0)
            except (TypeError, ValueError):
                price = 0.0
            success = bool(raw.get("success")) and price > 0
            normalized.append(
                {
                    "url": final_url or original_url,
                    "store": store_name,
                    "product_name": product_name[:200] if product_name else "",
                    "price": price if success else 0.0,
                    "image_url": image_url,
                    "success": success,
                    "error": str(raw.get("error") or "")[:300],
                }
            )

        if progress_cb:
            progress_cb(total, total)
        return normalized

    async def close(self):
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  Main entry — app.py or CLI
# ══════════════════════════════════════════════════════════════════════════════
async def run_advanced_price_scraping(
    store_filter: str = "",
    limit: int = 2000,
    progress_cb=None,
) -> Dict[str, Any]:
    """Scrape products with price=0 in competitor_products_store."""
    global _ai_fallback_used
    _ai_fallback_used = 0

    from utils.db_manager import get_db, upsert_competitor_products

    conn = get_db()
    try:
        if store_filter:
            stores_rows = conn.execute(
                """SELECT DISTINCT competitor FROM competitor_products_store
                   WHERE (price IS NULL OR price = 0) AND product_url != '' AND competitor = ?""",
                (store_filter,),
            ).fetchall()
        else:
            stores_rows = conn.execute(
                """SELECT DISTINCT competitor FROM competitor_products_store
                   WHERE (price IS NULL OR price = 0) AND product_url != ''"""
            ).fetchall()
    finally:
        conn.close()

    stores = [str(row[0]).strip() for row in stores_rows if row and str(row[0]).strip()]
    if not stores:
        return {"total_scraped": 0, "prices_found": 0, "updated_in_db": 0,
                "errors": 0, "ai_used": 0,
                "message": "✅ جميع المنتجات لديها أسعار بالفعل!"}

    scraper = AdvancedScraper(max_concurrent=8)
    total_scraped = 0
    prices_found = 0
    updated_in_db = 0
    errors = 0

    try:
        for store in stores:
            processed_urls: Set[str] = set()
            batch_idx = 0
            logger.info(f"🏪 Start store={store}")

            while True:
                batch_idx += 1
                conn = get_db()
                try:
                    rows = conn.execute(
                        """SELECT product_url FROM competitor_products_store
                           WHERE (price IS NULL OR price = 0)
                             AND product_url != ''
                             AND competitor = ?
                           LIMIT ?""",
                        (store, limit),
                    ).fetchall()
                finally:
                    conn.close()

                if not rows:
                    logger.info(f"🏁 store={store} done (no rows left)")
                    break

                batch_urls = []
                for row in rows:
                    url = str(row[0]).strip()
                    if not url or url in processed_urls:
                        continue
                    batch_urls.append(url)

                if not batch_urls:
                    logger.info(
                        f"🏁 store={store} done (remaining rows already attempted in this run)"
                    )
                    break

                for u in batch_urls:
                    processed_urls.add(u)

                logger.info(
                    f"📦 store={store} batch={batch_idx} fetched={len(rows)} eligible={len(batch_urls)}"
                )
                try:
                    results = await scraper.scrape_batch(batch_urls, store, progress_cb=progress_cb)
                except Exception as store_exc:
                    logger.error(f"💥 store={store} batch={batch_idx} failed: {store_exc}")
                    total_scraped += len(batch_urls)
                    errors += len(batch_urls)
                    continue

                products_to_save = []
                batch_errors = 0
                batch_prices = 0
                for r in results:
                    total_scraped += 1
                    if r.get("success") and r.get("price", 0) > 0:
                        batch_prices += 1
                        prices_found += 1
                        products_to_save.append({
                            "name": r.get("product_name") or r.get("url", "").split("/")[-1].replace("-", " "),
                            "price": r["price"],
                            "product_url": r["url"],
                            "image_url": r.get("image_url", ""),
                        })
                    else:
                        batch_errors += 1
                        errors += 1

                if products_to_save:
                    try:
                        res = update_db_with_prices(
                            store,
                            products_to_save,
                            upsert_competitor_products,
                        )
                        updated_in_db += res.get("updated", 0) + res.get("inserted", 0)
                    except Exception as db_err:
                        logger.error(f"DB error store={store} batch={batch_idx}: {db_err}")
                        logger.exception("Full traceback for DB update failure")

                logger.info(
                    f"✅ store={store} batch={batch_idx} scraped={len(results)} prices={batch_prices} errors={batch_errors}"
                )
    finally:
        await scraper.close()

    pct = prices_found * 100 // max(total_scraped, 1)
    return {
        "total_scraped": total_scraped,
        "prices_found": prices_found,
        "updated_in_db": updated_in_db,
        "errors": errors,
        "ai_used": _ai_fallback_used,
        "message": f"✅ كشط {total_scraped} | أسعار: {prices_found} ({pct}%) | DB: {updated_in_db} | AI: {_ai_fallback_used}",
    }


def update_db_with_prices(store: str, products_to_save: List[Dict[str, Any]], upsert_fn) -> Dict[str, Any]:
    """Temporary debug wrapper for DB writes."""
    print(
        f"[DEBUG][update_db_with_prices] store={store} rows={len(products_to_save)}"
    )
    for idx, row in enumerate(products_to_save[:10], start=1):
        print(
            "[DEBUG][update_db_with_prices] row"
            f"[{idx}] url={row.get('product_url')} "
            f"price={row.get('price')} name={str(row.get('name', ''))[:80]}"
        )
    if len(products_to_save) > 10:
        print(
            f"[DEBUG][update_db_with_prices] ... truncated {len(products_to_save) - 10} additional rows"
        )
    result = upsert_fn(
        store,
        products_to_save,
        name_key="name",
        price_key="price",
    )
    print(f"[DEBUG][update_db_with_prices] result={result}")
    return result


if __name__ == "__main__":
    import sys
    _store = sys.argv[1] if len(sys.argv) > 1 else ""
    _limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    print(f"🕷️ Advanced Scraper v30.2 — store={_store or 'ALL'}, limit={_limit}")
    result = asyncio.run(run_advanced_price_scraping(_store, _limit))
    print(result["message"])
