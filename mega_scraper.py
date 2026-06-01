"""
🚀 MEGA SCRAPER — كشط كامل بأقصى سرعة
كل متجر يعمل بشكل مستقل مع 15 طلب متزامن
"""
import sys, json, asyncio, aiohttp, re, time, random, warnings
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
from utils.db_manager import get_db, init_db, upsert_competitor_products

UA = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
]

def extract_price(html):
    if not html or len(html) < 100: return None
    # JSON-LD
    for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S):
        try:
            d = json.loads(m.group(1))
            if isinstance(d, list): d = d[0]
            off = d.get("offers", d)
            if isinstance(off, list): off = off[0]
            if isinstance(off, dict):
                p = float(str(off.get("price", off.get("lowPrice", 0))).replace(",",""))
                if p > 0:
                    img = d.get("image","")
                    return {"price": p, "name": d.get("name",""), "image": img[0] if isinstance(img,list) else str(img)}
        except: pass
    # Meta
    m = re.search(r'<meta[^>]*property="product:price:amount"[^>]*content="([^"]+)"', html, re.I)
    if m:
        try:
            p = float(m.group(1).replace(",",""))
            if p > 0:
                t = re.search(r'<meta[^>]*property="og:title"[^>]*content="([^"]+)"', html)
                i = re.search(r'<meta[^>]*property="og:image"[^>]*content="([^"]+)"', html)
                return {"price": p, "name": t.group(1) if t else "", "image": i.group(1) if i else ""}
        except: pass
    # Regex
    for pm in re.findall(r'"price"\s*:\s*(\d+(?:\.\d+)?)', html):
        try:
            p = float(pm)
            if 1 < p < 50000:
                t = re.search(r'<meta[^>]*property="og:title"[^>]*content="([^"]+)"', html)
                i = re.search(r'<meta[^>]*property="og:image"[^>]*content="([^"]+)"', html)
                return {"price": p, "name": t.group(1) if t else "", "image": i.group(1) if i else ""}
        except: pass
    return None


async def get_sitemap_urls(session, sitemap_url):
    urls = []
    try:
        h = {"User-Agent": random.choice(UA)}
        async with session.get(sitemap_url, headers=h, ssl=False, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status != 200: return urls
            text = await r.text(errors="ignore")
        soup = BeautifulSoup(text, "html.parser")
        subs = soup.find_all("sitemap")
        if subs:
            for sm in subs:
                loc = sm.find("loc")
                if not loc: continue
                try:
                    async with session.get(loc.text.strip(), headers=h, ssl=False, timeout=aiohttp.ClientTimeout(total=20)) as r2:
                        if r2.status == 200:
                            s2 = BeautifulSoup(await r2.text(errors="ignore"), "html.parser")
                            for u in s2.find_all("url"):
                                l = u.find("loc")
                                if l: urls.append(l.text.strip())
                except: pass
        else:
            for u in soup.find_all("url"):
                l = u.find("loc")
                if l: urls.append(l.text.strip())
    except: pass
    # Filter product URLs
    product_urls = [u for u in urls if re.search(r"/products?/|/p/", u, re.I)]
    return list(dict.fromkeys(product_urls))[:8000] if product_urls else list(dict.fromkeys(urls))[:8000]


async def scrape_one(session, url, sem):
    async with sem:
        await asyncio.sleep(0.15 + random.uniform(0, 0.15))
        try:
            h = {"User-Agent": random.choice(UA), "Accept": "text/html,*/*;q=0.8", "Accept-Language": "ar,en;q=0.5"}
            async with session.get(url, headers=h, ssl=False, timeout=aiohttp.ClientTimeout(total=10), allow_redirects=True) as r:
                if r.status != 200: return None
                html = await r.text(errors="ignore")
            result = extract_price(html)
            if result:
                result["url"] = url
            return result
        except: return None


async def scrape_store(session, name, sitemap_url, existing_count):
    print(f"\n  [{name}] Fetching sitemap...")
    urls = await get_sitemap_urls(session, sitemap_url)
    if not urls:
        print(f"  [{name}] No URLs found")
        return 0

    print(f"  [{name}] {len(urls)} URLs — scraping...")
    sem = asyncio.Semaphore(15)
    buffer = []
    total_prices = 0

    for chunk_start in range(0, len(urls), 200):
        chunk = urls[chunk_start:chunk_start+200]
        tasks = [scrape_one(session, u, sem) for u in chunk]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, dict) and res.get("price"):
                buffer.append({"name": res["name"], "price": res["price"],
                               "product_url": res.get("url",""), "image_url": res.get("image","")})

        if len(buffer) >= 50:
            try:
                r = upsert_competitor_products(name, buffer, name_key="name", price_key="price")
                total_prices += r.get("inserted",0) + r.get("updated",0)
            except: pass
            buffer = []

        done = min(chunk_start+200, len(urls))
        pct = done*100//len(urls)
        cur_prices = total_prices + len(buffer)
        print(f"    [{name}] {done}/{len(urls)} ({pct}%) | Prices: {cur_prices}")

    if buffer:
        try:
            r = upsert_competitor_products(name, buffer, name_key="name", price_key="price")
            total_prices += r.get("inserted",0) + r.get("updated",0)
        except: pass

    print(f"  [{name}] DONE: {total_prices} stored (was {existing_count})")
    return total_prices


async def main():
    start = time.time()
    init_db()

    with open("data/competitors_list_v30.json", encoding="utf-8") as f:
        comps = json.load(f)

    conn = get_db()
    existing = {}
    for row in conn.execute("SELECT competitor, COUNT(*) FROM competitor_products_store WHERE price > 0 GROUP BY competitor").fetchall():
        existing[row[0]] = row[1]
    conn.close()

    # Scrape ALL stores (even ones with some data, to get more)
    to_scrape = [c for c in comps if c.get("sitemap_url")]
    print(f"=== MEGA SCRAPER — {len(to_scrape)} stores ===")

    connector = aiohttp.TCPConnector(limit=100, ssl=False, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Run ALL stores concurrently
        tasks = [scrape_store(session, c["name"], c["sitemap_url"], existing.get(c["name"], 0)) for c in to_scrape]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.time() - start
    conn = get_db()
    rows = conn.execute("SELECT competitor, COUNT(*) as t FROM competitor_products_store WHERE price > 0 GROUP BY competitor ORDER BY t DESC").fetchall()
    grand = conn.execute("SELECT COUNT(*) FROM competitor_products_store WHERE price > 0").fetchone()[0]
    conn.close()

    print(f"\n{'='*60}")
    print(f"  MEGA SCRAPER COMPLETE — {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"{'='*60}")
    for store, tot in rows:
        print(f"  {store}: {tot}")
    print(f"  {'─'*40}")
    print(f"  GRAND TOTAL: {grand:,} products")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
