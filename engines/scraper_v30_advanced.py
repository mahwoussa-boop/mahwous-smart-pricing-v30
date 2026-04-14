"""
engines/scraper_v30_advanced.py — محرك الكشط المتقدم v30.1
══════════════════════════════════════════════════════════════
يكشط صفحات المنتجات التي لديها URLs لكن بدون أسعار (price=0).
متكامل مع:
  • scrapers/anti_ban.py — headers حقيقية + AdaptiveRateLimiter + sync fallbacks
  • engines/ai_engine.py — AI fallback لاستخراج الأسعار من صفحات معقدة
  • utils/db_manager.py  — competitor_products_store
"""
from __future__ import annotations

import asyncio
import logging
import random
import re
import json
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger("ScraperV30Adv")

# ── Anti-ban integration ─────────────────────────────────────────────────────
try:
    from scrapers.anti_ban import (
        get_browser_headers,
        get_rate_limiter,
        fetch_with_retry,
        try_all_sync_fallbacks,
        looks_like_bot_challenge,
    )
    _HAS_ANTI_BAN = True
except ImportError:
    _HAS_ANTI_BAN = False
    # Minimal fallback if anti_ban not available
    def get_browser_headers(referer=""):
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/134.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        }


# ══════════════════════════════════════════════════════════════════════════════
#  استخراج الأسعار من HTML — 7+ استراتيجيات
# ══════════════════════════════════════════════════════════════════════════════
class PriceExtractor:

    @staticmethod
    def extract_price(html: str, url: str = "") -> Optional[float]:
        try:
            soup = BeautifulSoup(html, "html.parser")

            # 1. CSS selectors شائعة
            for selector in (
                "span.price", "span.product-price", "div.price", "p.price",
                "span[data-price]", "span[class*='price']", "div[class*='price']",
                "span[class*='sar']", "span[class*='ريال']",
                "span.s-product-price", "div.s-product-price",
                "span.product__price", "span.money",
                # Salla v2 + Zid + Shopify Arabic
                ".product-price__value", ".product-price--sale",
                "[data-product-price]", ".s-price-wrapper span",
            ):
                for elem in soup.select(selector):
                    p = PriceExtractor._parse_price_text(elem.get_text(strip=True))
                    if p and p > 0:
                        return p

            # 2. data-* attributes
            for attr in ("data-price", "data-product-price", "data-amount", "data-regular-price"):
                for el in soup.find_all(attrs={attr: True}):
                    try:
                        p = float(str(el[attr]).replace(",", ""))
                        if 0 < p < 100_000:
                            return p
                    except (ValueError, TypeError):
                        pass

            # 3. JSON-LD structured data
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    ld = json.loads(script.string or "")
                    # Handle @graph arrays
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
                    for pk in ("price", "lowPrice", "highPrice"):
                        p_str = str(offers.get(pk, ""))
                        if p_str:
                            p = float(p_str.replace(",", ""))
                            if 0 < p < 100_000:
                                return p
                except Exception:
                    pass

            # 4. Inline JS: window.__INITIAL_STATE__ or similar
            for script in soup.find_all("script"):
                txt = script.string or ""
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

            # 5. Arabic text patterns
            text = soup.get_text()
            for line in text.split("\n"):
                if any(kw in line for kw in ("ريال", "رس", "SAR", "ر.س")):
                    p = PriceExtractor._parse_price_text(line)
                    if p and p > 0:
                        return p

            return None
        except Exception as e:
            logger.debug(f"price extract error: {e}")
            return None

    @staticmethod
    def _parse_price_text(text: str) -> Optional[float]:
        try:
            text = (
                text.replace("ريال", "").replace("رس", "").replace("ر.س", "")
                .replace("SAR", "").replace(",", "").replace("،", "").strip()
            )
            numbers = re.findall(r"\d+\.?\d*", text)
            if numbers:
                p = float(numbers[0])
                if 0 < p < 100_000:
                    return p
        except Exception:
            pass
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  AI Fallback — lightweight Gemini price extraction (cost-guarded)
# ══════════════════════════════════════════════════════════════════════════════
_AI_FALLBACK_BUDGET = 50  # Max AI calls per run
_ai_fallback_used = 0


def _ai_extract_price(text_snippet: str) -> Optional[float]:
    """Send a minimal prompt to Gemini to extract price. Returns float or None."""
    global _ai_fallback_used
    if _ai_fallback_used >= _AI_FALLBACK_BUDGET:
        return None
    try:
        from engines.ai_engine import _call_gemini
    except ImportError:
        return None

    # Truncate to save tokens
    snippet = text_snippet[:2500]
    prompt = (
        "Extract the product price in SAR (Saudi Riyals) from this text. "
        "Reply with ONLY the numeric price (e.g. 299.00). "
        "If no price found, reply with 0.\n\n"
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
        logger.debug(f"AI fallback error: {e}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  المحرك الأساسي — مع anti-ban + retry + sync fallback
# ══════════════════════════════════════════════════════════════════════════════
class AdvancedScraper:

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.price_extractor = PriceExtractor()
        self._rate_limiter = get_rate_limiter() if _HAS_ANTI_BAN else None

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=25, connect=10)
            connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, ssl=False)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def scrape_product_page(self, url: str, store_name: str) -> Dict[str, Any]:
        """كشط صفحة منتج واحدة — مع retry + sync fallback + AI fallback."""
        domain = urlparse(url).netloc
        html = None
        http_status = 0

        # ── Phase 1: aiohttp with anti-ban headers + rate limiter ────────
        try:
            if self._rate_limiter:
                await self._rate_limiter.wait(domain)
            else:
                await asyncio.sleep(random.uniform(1.0, 3.0))

            await self._ensure_session()
            headers = get_browser_headers(referer=f"https://{domain}/")

            async with self.session.get(
                url, headers=headers, ssl=False, allow_redirects=True,
            ) as response:
                http_status = response.status
                if response.status == 200:
                    html = await response.text(errors="ignore")
                    if self._rate_limiter:
                        self._rate_limiter.record_success(domain)
                else:
                    if self._rate_limiter:
                        self._rate_limiter.record_error(domain, response.status)
        except asyncio.TimeoutError:
            logger.debug(f"⏱️ aiohttp timeout: {url}")
        except (aiohttp.ClientError, OSError) as e:
            logger.debug(f"aiohttp error {url}: {e}")

        # ── Phase 2: Sync fallbacks (curl_cffi → cloudscraper → requests) ─
        if (not html or (html and looks_like_bot_challenge(html) if _HAS_ANTI_BAN else False)):
            if _HAS_ANTI_BAN:
                try:
                    loop = asyncio.get_running_loop()
                    html_sync = await loop.run_in_executor(
                        None, try_all_sync_fallbacks, url, 20
                    )
                    if html_sync:
                        html = html_sync
                except Exception as e:
                    logger.debug(f"sync fallback error {url}: {e}")

        # ── Phase 3: Extract data from HTML ──────────────────────────────
        if not html:
            return self._fail_result(url, store_name)

        price = self.price_extractor.extract_price(html, url)

        # ── Phase 4: AI Fallback — only if DOM extraction failed ─────────
        if not price or price <= 0:
            text_for_ai = BeautifulSoup(html, "html.parser").get_text()[:3000]
            if text_for_ai.strip():
                ai_price = _ai_extract_price(text_for_ai)
                if ai_price:
                    price = ai_price
                    logger.debug(f"🤖 AI extracted price {price} from {url}")

        # ── Extract metadata ─────────────────────────────────────────────
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find("title")
        product_name = (
            title_tag.get_text(strip=True) if title_tag
            else url.split("/")[-1].replace("-", " ")
        )

        image_url = ""
        for img_sel in ("img.product-image", "img[class*='product']", "meta[property='og:image']"):
            el = soup.select_one(img_sel)
            if el:
                src = el.get("src") or el.get("content", "")
                if src:
                    image_url = urljoin(url, src)
                    break

        return {
            "url": url,
            "store": store_name,
            "product_name": product_name[:200],
            "price": price or 0.0,
            "image_url": image_url,
            "success": price is not None and price > 0,
        }

    @staticmethod
    def _fail_result(url: str, store_name: str) -> Dict[str, Any]:
        return {
            "url": url, "store": store_name,
            "product_name": url.split("/")[-1].replace("-", " "),
            "price": 0.0, "image_url": "", "success": False,
        }

    async def scrape_batch(
        self, urls: List[str], store_name: str, batch_size: int = 6,
        progress_cb=None,
    ) -> List[Dict]:
        """كشط مجموعة روابط بشكل متوازي — مع inter-batch jitter."""
        results = []
        total = len(urls)
        for i in range(0, total, batch_size):
            batch = urls[i : i + batch_size]
            tasks = [self.scrape_product_page(u, store_name) for u in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, dict):
                    results.append(r)
                else:
                    # Exception from gather — log and record as failure
                    logger.debug(f"batch exception: {r}")
                    results.append(self._fail_result(batch[0] if batch else "", store_name))

            if progress_cb:
                progress_cb(min(i + batch_size, total), total)

            # Inter-batch jitter — avoid burst pattern detection
            if i + batch_size < total:
                await asyncio.sleep(random.uniform(1.5, 4.0))

        return results

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


# ══════════════════════════════════════════════════════════════════════════════
#  دالة التشغيل الرئيسية — تُستدعى من app.py أو CLI
# ══════════════════════════════════════════════════════════════════════════════
async def run_advanced_price_scraping(
    store_filter: str = "",
    limit: int = 2000,
    progress_cb=None,
) -> Dict[str, Any]:
    """
    يكشط المنتجات ذات price=0 في competitor_products_store.

    Returns
    -------
    dict: {total_scraped, prices_found, updated_in_db, errors, ai_used, message}
    """
    global _ai_fallback_used
    _ai_fallback_used = 0  # Reset per run

    from utils.db_manager import get_db, upsert_competitor_products

    conn = get_db()
    try:
        if store_filter:
            rows = conn.execute(
                """SELECT product_url, competitor FROM competitor_products_store
                   WHERE (price IS NULL OR price = 0) AND product_url != '' AND competitor = ?
                   LIMIT ?""",
                (store_filter, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT product_url, competitor FROM competitor_products_store
                   WHERE (price IS NULL OR price = 0) AND product_url != ''
                   LIMIT ?""",
                (limit,),
            ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"total_scraped": 0, "prices_found": 0, "updated_in_db": 0,
                "errors": 0, "ai_used": 0,
                "message": "✅ جميع المنتجات لديها أسعار بالفعل!"}

    # Group by store
    urls_by_store: Dict[str, List[str]] = {}
    for url, store in rows:
        urls_by_store.setdefault(store, []).append(url)

    scraper = AdvancedScraper()
    total_scraped = 0
    prices_found = 0
    updated_in_db = 0
    errors = 0

    try:
        for store, urls in urls_by_store.items():
            logger.info(f"🏪 كشط {store}: {len(urls)} منتج بدون سعر")

            try:
                results = await scraper.scrape_batch(
                    urls, store, batch_size=6, progress_cb=progress_cb
                )
            except Exception as store_exc:
                # Store-level isolation — log and skip to next store
                logger.error(f"💥 {store} failed entirely: {store_exc}")
                errors += len(urls)
                continue

            products_to_save = []
            for r in results:
                total_scraped += 1
                if r.get("success") and r.get("price", 0) > 0:
                    prices_found += 1
                    products_to_save.append({
                        "name": r["product_name"],
                        "price": r["price"],
                        "product_url": r["url"],
                        "image_url": r.get("image_url", ""),
                    })
                elif not r.get("success"):
                    errors += 1

            if products_to_save:
                try:
                    res = upsert_competitor_products(store, products_to_save, name_key="name", price_key="price")
                    updated_in_db += res.get("updated", 0) + res.get("inserted", 0)
                except Exception as db_err:
                    logger.error(f"DB save error for {store}: {db_err}")
    finally:
        await scraper.close()

    return {
        "total_scraped": total_scraped,
        "prices_found": prices_found,
        "updated_in_db": updated_in_db,
        "errors": errors,
        "ai_used": _ai_fallback_used,
        "message": (
            f"✅ تم كشط {total_scraped} منتج | "
            f"أسعار: {prices_found} ({prices_found*100//max(total_scraped,1)}%) | "
            f"محدّث: {updated_in_db} | AI: {_ai_fallback_used}"
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  CLI entry point
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys
    _store = sys.argv[1] if len(sys.argv) > 1 else ""
    _limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

    print(f"🕷️ Advanced Scraper v30.1 — store={_store or 'ALL'}, limit={_limit}")
    result = asyncio.run(run_advanced_price_scraping(_store, _limit))
    print(result["message"])
