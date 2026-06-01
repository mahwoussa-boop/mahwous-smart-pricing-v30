"""
سكربت كشط المنافسين v2 — يستخدم upsert_competitor_products الرسمية
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import json
import asyncio
import aiohttp
import re
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Load competitors
with open("data/competitors_list_v30.json", encoding="utf-8") as f:
    comps = json.load(f)
print(f"Found {len(comps)} competitors")

# Register in DB
from utils.db_manager import get_db, init_db, upsert_competitor_products
init_db()

conn = get_db()
for c in comps:
    conn.execute(
        "INSERT OR IGNORE INTO competitors (name, domain, is_active) VALUES (?, ?, 1)",
        (c["name"], c["store_url"])
    )
conn.commit()
conn.close()
print(f"Competitors registered in DB")

# ── Sitemap fetching ──
from scrapers.anti_ban import try_curl_cffi, try_httpx

PRODUCT_PATTERNS = [re.compile(r"/products?/", re.I), re.compile(r"/p/", re.I)]

def is_product_url(url):
    return any(p.search(url) for p in PRODUCT_PATTERNS)

async def fetch_sitemap(session, sitemap_url, store_name):
    product_urls = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "Accept": "application/xml,text/xml,*/*;q=0.8",
        }
        text = None
        try:
            async with session.get(sitemap_url, headers=headers, ssl=False, 
                                   timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    text = await resp.text(errors="ignore")
        except Exception:
            pass
        
        if not text:
            text = try_curl_cffi(sitemap_url, timeout=15)
        if not text:
            text = try_httpx(sitemap_url, timeout=15)
        if not text:
            print(f"    FAIL: {store_name}")
            return product_urls

        soup = BeautifulSoup(text, "html.parser")
        
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
            if not product_urls:
                for sm in sub_sitemaps[:3]:
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
    except Exception as e:
        print(f"    ERROR {store_name}: {type(e).__name__}")
    
    print(f"    {store_name}: {len(product_urls)} product URLs")
    return product_urls


async def main():
    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        
        print("\n" + "="*60)
        print("  STEP 1: Fetching Sitemaps")
        print("="*60)
        
        all_products = {}
        for comp in comps[:8]:
            name = comp["name"]
            sitemap = comp.get("sitemap_url", "")
            if not sitemap:
                continue
            urls = await fetch_sitemap(session, sitemap, name)
            if urls:
                all_products[name] = urls[:50]  # Limit 50 per store
            await asyncio.sleep(1)
        
        total_urls = sum(len(v) for v in all_products.values())
        print(f"\nTotal product URLs: {total_urls} across {len(all_products)} stores")
        
        # Step 2: Scrape prices and store using upsert
        print("\n" + "="*60)
        print("  STEP 2: Scraping Prices & Storing in DB")
        print("="*60)
        
        from engines.scraper_v30_advanced import AdvancedScraper
        scraper = AdvancedScraper(max_concurrent=6)
        
        grand_total = 0
        grand_prices = 0
        grand_stored = 0
        
        for store_name, urls in all_products.items():
            print(f"\n  Scraping {store_name} ({len(urls)} products)...")
            batch_results = []
            batch_prices = 0
            
            scrape_limit = min(len(urls), 30)
            for i, url in enumerate(urls[:scrape_limit]):
                try:
                    result = await scraper.scrape_product_page(url, store_name)
                    grand_total += 1
                    
                    if result.get("success") and result.get("price", 0) > 0:
                        batch_prices += 1
                        grand_prices += 1
                        batch_results.append({
                            "name": result["product_name"],
                            "price": result["price"],
                            "product_url": result["url"],
                            "image_url": result.get("image_url", ""),
                        })
                except Exception:
                    pass
                
                if (i + 1) % 10 == 0:
                    print(f"    Progress: {i+1}/{scrape_limit} | Prices: {batch_prices}")
            
            # Store in DB using official upsert
            if batch_results:
                try:
                    res = upsert_competitor_products(
                        store_name, batch_results, 
                        name_key="name", price_key="price"
                    )
                    stored = res.get("inserted", 0) + res.get("updated", 0)
                    grand_stored += stored
                    print(f"    OK {store_name}: {batch_prices} prices, {stored} stored in DB")
                except Exception as e:
                    print(f"    DB ERROR {store_name}: {e}")
            else:
                print(f"    {store_name}: {batch_prices} prices (nothing to store)")
        
        await scraper.close()
        
        # Verify DB
        conn = get_db()
        db_total = conn.execute("SELECT COUNT(*) FROM competitor_products_store").fetchone()[0]
        db_priced = conn.execute("SELECT COUNT(*) FROM competitor_products_store WHERE price > 0").fetchone()[0]
        conn.close()
        
        print("\n" + "="*60)
        print(f"  FINAL RESULTS")
        print(f"  Scraped: {grand_total} pages")
        print(f"  Prices found: {grand_prices}")
        print(f"  Stored in DB: {grand_stored}")
        print(f"  DB total: {db_total} products ({db_priced} with price)")
        print(f"  Success rate: {grand_prices*100//max(grand_total,1)}%")
        print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
