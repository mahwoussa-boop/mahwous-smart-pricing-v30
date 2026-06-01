"""
كشط عالم جيفنشي وتخزين البيانات
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import asyncio
import aiohttp
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from utils.db_manager import get_db, upsert_competitor_products
from scrapers.anti_ban import try_curl_cffi, try_httpx

STORE_NAME = "عالم جيفنشي"
SITEMAP_URL = "https://worldgivenchy.com/sitemap.xml"

import re
PRODUCT_PATTERNS = [re.compile(r"/products?/", re.I), re.compile(r"/p/", re.I)]

def is_product_url(url):
    return any(p.search(url) for p in PRODUCT_PATTERNS)


async def fetch_sitemap(session, url):
    """Fetch sitemap and extract product URLs"""
    product_urls = []
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Accept": "application/xml,text/xml,*/*;q=0.8",
    }
    
    text = None
    try:
        async with session.get(url, headers=headers, ssl=False,
                               timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 200:
                text = await resp.text(errors="ignore")
    except Exception:
        pass
    
    if not text:
        text = try_curl_cffi(url, timeout=15)
    if not text:
        text = try_httpx(url, timeout=15)
    if not text:
        print(f"FAIL: Could not fetch {url}")
        return product_urls
    
    soup = BeautifulSoup(text, "html.parser")
    
    # Check for sitemap index
    sub_sitemaps = soup.find_all("sitemap")
    if sub_sitemaps:
        for sm in sub_sitemaps:
            loc = sm.find("loc")
            if loc and loc.text:
                sub_url = loc.text.strip()
                if "product" in sub_url.lower():
                    try:
                        async with session.get(sub_url, headers=headers, ssl=False,
                                               timeout=aiohttp.ClientTimeout(total=20)) as sub_resp:
                            if sub_resp.status == 200:
                                sub_text = await sub_resp.text(errors="ignore")
                                sub_soup = BeautifulSoup(sub_text, "html.parser")
                                for url_tag in sub_soup.find_all("url"):
                                    loc2 = url_tag.find("loc")
                                    if loc2 and loc2.text:
                                        product_urls.append(loc2.text.strip())
                    except Exception:
                        pass
        # If no product sitemaps, try all
        if not product_urls:
            for sm in sub_sitemaps[:5]:
                loc = sm.find("loc")
                if loc and loc.text:
                    try:
                        async with session.get(loc.text.strip(), headers=headers, ssl=False,
                                               timeout=aiohttp.ClientTimeout(total=20)) as sub_resp:
                            if sub_resp.status == 200:
                                sub_text = await sub_resp.text(errors="ignore")
                                sub_soup = BeautifulSoup(sub_text, "html.parser")
                                for url_tag in sub_soup.find_all("url"):
                                    loc2 = url_tag.find("loc")
                                    if loc2 and loc2.text:
                                        u = loc2.text.strip()
                                        if is_product_url(u):
                                            product_urls.append(u)
                    except Exception:
                        pass
    else:
        for url_tag in soup.find_all("url"):
            loc = url_tag.find("loc")
            if loc and loc.text:
                u = loc.text.strip()
                if is_product_url(u):
                    product_urls.append(u)
    
    return product_urls


async def main():
    print(f"=== Scraping {STORE_NAME} ===")
    
    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        
        # Step 1: Get product URLs
        print(f"  Fetching sitemap: {SITEMAP_URL}")
        urls = await fetch_sitemap(session, SITEMAP_URL)
        print(f"  Found {len(urls)} product URLs")
        
        if not urls:
            print("  No products found. Trying direct product page scraping...")
            return
        
        # Step 2: Scrape prices
        print(f"\n  Scraping prices (max 50)...")
        from engines.scraper_v30_advanced import AdvancedScraper
        scraper = AdvancedScraper(max_concurrent=6)
        
        results = []
        scrape_limit = min(len(urls), 50)
        prices_found = 0
        
        for i, url in enumerate(urls[:scrape_limit]):
            try:
                result = await scraper.scrape_product_page(url, STORE_NAME)
                if result.get("success") and result.get("price", 0) > 0:
                    prices_found += 1
                    results.append({
                        "name": result["product_name"],
                        "price": result["price"],
                        "product_url": result["url"],
                        "image_url": result.get("image_url", ""),
                    })
            except Exception:
                pass
            
            if (i + 1) % 10 == 0:
                print(f"    Progress: {i+1}/{scrape_limit} | Prices: {prices_found}")
        
        await scraper.close()
        
        # Step 3: Store in DB
        if results:
            res = upsert_competitor_products(
                STORE_NAME, results, name_key="name", price_key="price"
            )
            stored = res.get("inserted", 0) + res.get("updated", 0)
            print(f"\n  Stored: {stored} products in DB")
        else:
            print(f"\n  No prices found to store")
        
        # Step 4: Summary
        conn = get_db()
        total = conn.execute(
            "SELECT COUNT(*) FROM competitor_products_store WHERE competitor = ?",
            (STORE_NAME,)
        ).fetchone()[0]
        with_price = conn.execute(
            "SELECT COUNT(*) FROM competitor_products_store WHERE competitor = ? AND price > 0",
            (STORE_NAME,)
        ).fetchone()[0]
        conn.close()
        
        print(f"\n  FINAL: {STORE_NAME}")
        print(f"    Total products: {total}")
        print(f"    With price: {with_price}")
        print(f"    Scrape rate: {prices_found}/{scrape_limit} ({prices_found*100//max(scrape_limit,1)}%)")

if __name__ == "__main__":
    asyncio.run(main())
