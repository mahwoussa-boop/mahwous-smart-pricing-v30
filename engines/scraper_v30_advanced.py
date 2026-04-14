"""
engines/scraper_v30_advanced.py — محرك الكشط المتقدم v30
══════════════════════════════════════════════════════════
يكشط صفحات المنتجات التي لديها URLs لكن بدون أسعار (price=0).
يستخدم aiohttp + BeautifulSoup مع حماية anti-ban.
متكامل مع utils/db_manager.py و competitor_products_store.
"""
from __future__ import annotations

import asyncio
import logging
import random
import re
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger("ScraperV30Adv")

# ── User-Agents لتجاوز الحماية ──────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]


# ══════════════════════════════════════════════════════════════════════════════
#  استخراج الأسعار من HTML
# ══════════════════════════════════════════════════════════════════════════════
class PriceExtractor:
    """استخراج الأسعار من HTML بذكاء — 7 استراتيجيات."""

    @staticmethod
    def extract_price(html: str, url: str = "") -> Optional[float]:
        try:
            soup = BeautifulSoup(html, "html.parser")

            # الاستراتيجية 1: CSS selectors شائعة
            price_selectors = [
                "span.price", "span.product-price", "div.price", "p.price",
                "span[data-price]", "span[class*='price']", "div[class*='price']",
                "span[class*='sar']", "span[class*='ريال']",
                # Salla-specific
                "span.s-product-price", "div.s-product-price",
                "span.product__price", "span.money",
            ]
            for selector in price_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    price = PriceExtractor._parse_price_text(text)
                    if price and price > 0:
                        return price

            # الاستراتيجية 2: data attributes
            for attr in ("data-price", "data-product-price", "data-amount"):
                elems = soup.find_all(attrs={attr: True})
                for el in elems:
                    try:
                        p = float(str(el[attr]).replace(",", ""))
                        if 0 < p < 100_000:
                            return p
                    except (ValueError, TypeError):
                        pass

            # الاستراتيجية 3: JSON-LD structured data
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    ld = json.loads(script.string or "")
                    offers = ld.get("offers", ld)
                    if isinstance(offers, list):
                        offers = offers[0] if offers else {}
                    p_str = str(offers.get("price", ""))
                    if p_str:
                        p = float(p_str.replace(",", ""))
                        if 0 < p < 100_000:
                            return p
                except Exception:
                    pass

            # الاستراتيجية 4: البحث في النص عن أنماط عربية
            text = soup.get_text()
            for line in text.split("\n"):
                if any(kw in line for kw in ("ريال", "رس", "SAR", "ر.س")):
                    price = PriceExtractor._parse_price_text(line)
                    if price and price > 0:
                        return price

            return None
        except Exception as e:
            logger.debug(f"خطأ في استخراج السعر: {e}")
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
                price = float(numbers[0])
                if 0 < price < 100_000:
                    return price
        except Exception:
            pass
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  المحرك الأساسي
# ══════════════════════════════════════════════════════════════════════════════
class AdvancedScraper:
    """محرك الكشط المتقدم — يعمل مع DB مركزية من db_manager."""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.price_extractor = PriceExtractor()

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def scrape_product_page(self, url: str, store_name: str) -> Dict[str, Any]:
        """كشط صفحة منتج واحدة واستخراج السعر."""
        try:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            await self._ensure_session()

            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ar-SA,ar;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }

            async with self.session.get(
                url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
                ssl=False,
                allow_redirects=True,
            ) as response:
                if response.status == 200:
                    html = await response.text(errors="ignore")
                    price = self.price_extractor.extract_price(html, url)

                    soup = BeautifulSoup(html, "html.parser")
                    title = soup.find("title")
                    product_name = (
                        title.get_text(strip=True) if title
                        else url.split("/")[-1].replace("-", " ")
                    )

                    image_url = ""
                    img_tag = soup.find("img", {"class": lambda x: x and "product" in str(x).lower()})
                    if img_tag and img_tag.get("src"):
                        image_url = urljoin(url, img_tag["src"])

                    return {
                        "url": url,
                        "store": store_name,
                        "product_name": product_name[:200],
                        "price": price or 0.0,
                        "image_url": image_url,
                        "success": price is not None and price > 0,
                    }
                else:
                    logger.debug(f"HTTP {response.status}: {url}")
        except asyncio.TimeoutError:
            logger.debug(f"⏱️ timeout: {url}")
        except Exception as e:
            logger.debug(f"❌ scrape error {url}: {e}")

        return {
            "url": url, "store": store_name,
            "product_name": url.split("/")[-1].replace("-", " "),
            "price": 0.0, "image_url": "", "success": False,
        }

    async def scrape_batch(
        self, urls: List[str], store_name: str, batch_size: int = 8,
        progress_cb=None,
    ) -> List[Dict]:
        """كشط مجموعة روابط بشكل متوازي."""
        results = []
        total = len(urls)
        for i in range(0, total, batch_size):
            batch = urls[i : i + batch_size]
            tasks = [self.scrape_product_page(u, store_name) for u in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, dict):
                    results.append(r)
            if progress_cb:
                progress_cb(min(i + batch_size, total), total)
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

    Parameters
    ----------
    store_filter : str
        اسم المنافس (فارغ = كل المنافسين).
    limit : int
        الحد الأقصى لعدد المنتجات.
    progress_cb : callable(done, total) or None

    Returns
    -------
    dict: {"total_scraped": int, "prices_found": int, "updated_in_db": int, "errors": int}
    """
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
        return {"total_scraped": 0, "prices_found": 0, "updated_in_db": 0, "errors": 0,
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
            results = await scraper.scrape_batch(urls, store, batch_size=8, progress_cb=progress_cb)

            # Save via upsert (only updates price where it was 0)
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
                res = upsert_competitor_products(store, products_to_save, name_key="name", price_key="price")
                updated_in_db += res.get("updated", 0) + res.get("inserted", 0)
    finally:
        await scraper.close()

    return {
        "total_scraped": total_scraped,
        "prices_found": prices_found,
        "updated_in_db": updated_in_db,
        "errors": errors,
        "message": f"✅ تم كشط {total_scraped} منتج | أسعار: {prices_found} | محدّث: {updated_in_db}",
    }


# ══════════════════════════════════════════════════════════════════════════════
#  CLI entry point
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys
    _store = sys.argv[1] if len(sys.argv) > 1 else ""
    _limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

    print(f"🕷️ Advanced Scraper v30 — store={_store or 'ALL'}, limit={_limit}")
    result = asyncio.run(run_advanced_price_scraping(_store, _limit))
    print(result["message"])
