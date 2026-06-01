"""
🚀 نظام كشط فائق السرعة — Turbo Scraper v2
═══════════════════════════════════════════════
مصمّم لكشط 160,000+ منتج من 20+ منافس بأقصى سرعة وأقل حظر.

الاستراتيجية:
1. Sitemaps أولاً (سريع جداً — آلاف الروابط في ثوانٍ)
2. كشط متوازي عالي (20 concurrent per store)
3. تناوب ذكي بين المتاجر لتجنب الحظر
4. خطط backoff متدرجة
5. تخزين فوري في DB كل 50 منتج
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import json
import asyncio
import aiohttp
import re
import time
import warnings
import random
from collections import defaultdict
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from utils.db_manager import get_db, init_db, upsert_competitor_products
from scrapers.anti_ban import try_curl_cffi, try_httpx

# ── Config ──
MAX_CONCURRENT_TOTAL = 30      # إجمالي الطلبات المتزامنة
MAX_CONCURRENT_PER_STORE = 8   # أقصى عدد متزامن لكل متجر
MAX_PRODUCTS_PER_STORE = 8000  # أقصى عدد منتجات لكل متجر
BATCH_SIZE = 50                # حفظ كل 50 منتج
INTER_REQUEST_DELAY = 0.3      # تأخير بين الطلبات (ثانية)

# ── User-Agent Pool ──
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
]

PRODUCT_PATTERNS = [re.compile(r"/products?/", re.I), re.compile(r"/p/", re.I)]
PRICE_RE = re.compile(r'(?:price|سعر)["\s:]*["\s]*(\d[\d,.]+)', re.I)
JSON_LD_PRICE = re.compile(r'"price"\s*:\s*["\s]*(\d[\d,.]+)', re.I)


def is_product_url(url):
    return any(p.search(url) for p in PRODUCT_PATTERNS)


async def fetch_sitemap(session, sitemap_url, store_name, sem):
    """Fetch sitemap and all sub-sitemaps"""
    product_urls = []
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/xml,text/xml,*/*;q=0.8"}
    
    async with sem:
        text = None
        try:
            async with session.get(sitemap_url, headers=headers, ssl=False,
                                   timeout=aiohttp.ClientTimeout(total=25)) as resp:
                if resp.status == 200:
                    text = await resp.text(errors="ignore")
        except Exception:
            pass
        
        if not text:
            text = try_curl_cffi(sitemap_url, timeout=15)
        if not text:
            text = try_httpx(sitemap_url, timeout=15)
        if not text:
            print(f"  SKIP {store_name}: sitemap unreachable")
            return product_urls
    
    soup = BeautifulSoup(text, "html.parser")
    sub_sitemaps = soup.find_all("sitemap")
    
    if sub_sitemaps:
        # Fetch product sub-sitemaps concurrently
        sub_urls = []
        for sm in sub_sitemaps:
            loc = sm.find("loc")
            if loc and loc.text:
                sub_url = loc.text.strip()
                if "product" in sub_url.lower():
                    sub_urls.append(sub_url)
        
        # If no product-specific sitemaps, use all (limit 5)
        if not sub_urls:
            for sm in sub_sitemaps[:5]:
                loc = sm.find("loc")
                if loc and loc.text:
                    sub_urls.append(loc.text.strip())
        
        # Fetch all sub-sitemaps concurrently
        tasks = [_fetch_sub_sitemap(session, url, sem, headers) for url in sub_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for urls in results:
            if isinstance(urls, list):
                if sub_urls and any("product" in s.lower() for s in sub_urls):
                    product_urls.extend(urls)  # All URLs from product sitemaps
                else:
                    product_urls.extend(u for u in urls if is_product_url(u))
    else:
        for url_tag in soup.find_all("url"):
            loc = url_tag.find("loc")
            if loc and loc.text:
                u = loc.text.strip()
                if is_product_url(u):
                    product_urls.append(u)
    
    # Deduplicate
    product_urls = list(dict.fromkeys(product_urls))
    return product_urls[:MAX_PRODUCTS_PER_STORE]


async def _fetch_sub_sitemap(session, url, sem, headers):
    """Fetch a single sub-sitemap"""
    urls = []
    async with sem:
        try:
            async with session.get(url, headers=headers, ssl=False,
                                   timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    text = await resp.text(errors="ignore")
                    soup = BeautifulSoup(text, "html.parser")
                    for url_tag in soup.find_all("url"):
                        loc = url_tag.find("loc")
                        if loc and loc.text:
                            urls.append(loc.text.strip())
        except Exception:
            pass
    return urls


async def scrape_price_fast(session, url, sem, store_stats):
    """Ultra-fast price scraper — lightweight HTTP only"""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
    }
    
    async with sem:
        await asyncio.sleep(INTER_REQUEST_DELAY + random.uniform(0, 0.2))
        
        try:
            async with session.get(url, headers=headers, ssl=False,
                                   timeout=aiohttp.ClientTimeout(total=12),
                                   allow_redirects=True) as resp:
                if resp.status == 403:
                    store_stats["blocked"] += 1
                    return None
                if resp.status != 200:
                    store_stats["errors"] += 1
                    return None
                
                html = await resp.text(errors="ignore")
        except (asyncio.TimeoutError, aiohttp.ClientError):
            store_stats["errors"] += 1
            return None
    
    if not html or len(html) < 200:
        return None
    
    # Fast extraction — no BeautifulSoup, pure regex + string search
    result = {"url": url, "success": False}
    
    # 1) JSON-LD (most reliable)
    json_ld_match = re.search(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S)
    if json_ld_match:
        try:
            ld_data = json.loads(json_ld_match.group(1))
            if isinstance(ld_data, list):
                ld_data = ld_data[0]
            if "offers" in ld_data:
                offers = ld_data["offers"]
                if isinstance(offers, list):
                    offers = offers[0]
                price = float(str(offers.get("price", 0)).replace(",", ""))
                if price > 0:
                    result["price"] = price
                    result["product_name"] = ld_data.get("name", "")
                    result["image_url"] = ld_data.get("image", [""])[0] if isinstance(ld_data.get("image"), list) else str(ld_data.get("image", ""))
                    result["success"] = True
                    store_stats["prices"] += 1
                    return result
            elif ld_data.get("@type") == "Product":
                price_str = str(ld_data.get("price", "0")).replace(",", "")
                try:
                    price = float(price_str)
                except:
                    price = 0
                if price > 0:
                    result["price"] = price
                    result["product_name"] = ld_data.get("name", "")
                    result["success"] = True
                    store_stats["prices"] += 1
                    return result
        except Exception:
            pass
    
    # 2) Meta tags
    og_price = re.search(r'<meta[^>]*property="product:price:amount"[^>]*content="([^"]+)"', html)
    if og_price:
        try:
            price = float(og_price.group(1).replace(",", ""))
            if price > 0:
                og_title = re.search(r'<meta[^>]*property="og:title"[^>]*content="([^"]+)"', html)
                og_image = re.search(r'<meta[^>]*property="og:image"[^>]*content="([^"]+)"', html)
                result["price"] = price
                result["product_name"] = og_title.group(1) if og_title else ""
                result["image_url"] = og_image.group(1) if og_image else ""
                result["success"] = True
                store_stats["prices"] += 1
                return result
        except:
            pass
    
    # 3) Price regex in HTML
    price_matches = JSON_LD_PRICE.findall(html)
    for pm in price_matches:
        try:
            price = float(pm.replace(",", ""))
            if 1 < price < 50000:
                title_m = re.search(r'<title>([^<]+)</title>', html)
                result["price"] = price
                result["product_name"] = title_m.group(1).strip() if title_m else url.split("/")[-1].replace("-", " ")
                result["success"] = True
                store_stats["prices"] += 1
                return result
        except:
            pass
    
    store_stats["no_price"] += 1
    return None


async def scrape_store(session, store_name, urls, global_sem, store_sem):
    """Scrape a single store's products"""
    stats = {"total": len(urls), "prices": 0, "blocked": 0, "errors": 0, "no_price": 0, "stored": 0}
    results_buffer = []
    
    print(f"\n  [{store_name}] Starting {len(urls)} products...")
    
    # Process in chunks for progress reporting
    chunk_size = 100
    for chunk_start in range(0, len(urls), chunk_size):
        chunk = urls[chunk_start:chunk_start + chunk_size]
        tasks = [scrape_price_fast(session, url, store_sem, stats) for url in chunk]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for url, res in zip(chunk, results):
            if isinstance(res, dict) and res.get("success"):
                results_buffer.append({
                    "name": res["product_name"],
                    "price": res["price"],
                    "product_url": res.get("url", url),
                    "image_url": res.get("image_url", ""),
                })
        
        # Batch save every BATCH_SIZE
        if len(results_buffer) >= BATCH_SIZE:
            try:
                r = upsert_competitor_products(store_name, results_buffer, name_key="name", price_key="price")
                stats["stored"] += r.get("inserted", 0) + r.get("updated", 0)
            except Exception:
                pass
            results_buffer = []
        
        # Progress
        done = min(chunk_start + chunk_size, len(urls))
        pct = done * 100 // len(urls)
        print(f"    [{store_name}] {done}/{len(urls)} ({pct}%) | Prices: {stats['prices']} | Blocked: {stats['blocked']}")
        
        # Back off if too many blocks
        if stats["blocked"] > 10 and stats["blocked"] > stats["prices"]:
            print(f"    [{store_name}] Too many blocks — backing off 30s")
            await asyncio.sleep(30)
            stats["blocked"] = 0  # Reset
    
    # Save remaining
    if results_buffer:
        try:
            r = upsert_competitor_products(store_name, results_buffer, name_key="name", price_key="price")
            stats["stored"] += r.get("inserted", 0) + r.get("updated", 0)
        except Exception:
            pass
    
    print(f"  [{store_name}] DONE: {stats['prices']}/{stats['total']} prices ({stats['prices']*100//max(stats['total'],1)}%) | Stored: {stats['stored']}")
    return stats


async def main():
    start_time = time.time()
    
    # Load competitors
    with open("data/competitors_list_v30.json", encoding="utf-8") as f:
        comps = json.load(f)
    print(f"=== TURBO SCRAPER v2 ===")
    print(f"Competitors: {len(comps)}")
    print(f"Target: ~{len(comps) * MAX_PRODUCTS_PER_STORE:,} products max")
    
    # Register in DB
    init_db()
    conn = get_db()
    for c in comps:
        conn.execute("INSERT OR IGNORE INTO competitors (name, domain, is_active) VALUES (?, ?, 1)", (c["name"], c["store_url"]))
    conn.commit()
    conn.close()
    
    # Semaphores
    global_sem = asyncio.Semaphore(MAX_CONCURRENT_TOTAL)
    
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_TOTAL, ssl=False, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        
        # Phase 1: Fetch ALL sitemaps concurrently
        print("\n" + "="*60)
        print("  PHASE 1: Fetching ALL Sitemaps (concurrent)")
        print("="*60)
        
        sitemap_sem = asyncio.Semaphore(10)  # 10 concurrent sitemap fetches
        sitemap_tasks = []
        for comp in comps:
            if comp.get("sitemap_url"):
                sitemap_tasks.append(
                    fetch_sitemap(session, comp["sitemap_url"], comp["name"], sitemap_sem)
                )
        
        sitemap_results = await asyncio.gather(*sitemap_tasks, return_exceptions=True)
        
        all_stores = {}
        total_urls = 0
        for comp, result in zip([c for c in comps if c.get("sitemap_url")], sitemap_results):
            if isinstance(result, list) and result:
                all_stores[comp["name"]] = result
                total_urls += len(result)
                print(f"  {comp['name']}: {len(result):,} URLs")
            elif isinstance(result, Exception):
                print(f"  {comp['name']}: ERROR {type(result).__name__}")
            else:
                print(f"  {comp['name']}: 0 URLs")
        
        sitemap_time = time.time() - start_time
        print(f"\n  Total: {total_urls:,} product URLs from {len(all_stores)} stores")
        print(f"  Sitemap fetch time: {sitemap_time:.1f}s")
        
        # Phase 2: Scrape prices — stores in parallel, but each store rate-limited
        print("\n" + "="*60)
        print("  PHASE 2: Scraping Prices (parallel stores)")
        print("="*60)
        
        # Create per-store semaphores
        store_tasks = []
        for store_name, urls in all_stores.items():
            store_sem = asyncio.Semaphore(MAX_CONCURRENT_PER_STORE)
            store_tasks.append(scrape_store(session, store_name, urls, global_sem, store_sem))
        
        # Run all stores concurrently (each internally rate-limited)
        all_stats = await asyncio.gather(*store_tasks, return_exceptions=True)
    
    # Final summary
    elapsed = time.time() - start_time
    total_scraped = sum(s.get("total", 0) for s in all_stats if isinstance(s, dict))
    total_prices = sum(s.get("prices", 0) for s in all_stats if isinstance(s, dict))
    total_stored = sum(s.get("stored", 0) for s in all_stats if isinstance(s, dict))
    
    # DB verification
    conn = get_db()
    db_total = conn.execute("SELECT COUNT(*) FROM competitor_products_store").fetchone()[0]
    db_priced = conn.execute("SELECT COUNT(*) FROM competitor_products_store WHERE price > 0").fetchone()[0]
    conn.close()
    
    print("\n" + "="*60)
    print(f"  TURBO SCRAPER COMPLETE")
    print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Scraped: {total_scraped:,} pages")
    print(f"  Prices: {total_prices:,}")
    print(f"  Stored: {total_stored:,}")
    print(f"  Rate: {total_scraped/max(elapsed,1):.0f} pages/sec")
    print(f"  DB Total: {db_total:,} products ({db_priced:,} with price)")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
