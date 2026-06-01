"""
🚀 Turbo Scraper v3 — كشط شامل لجميع المنافسين
═══════════════════════════════════════════════════
محسّن لاستخراج الأسعار من المتاجر العربية (Salla/Zid/Shopify/WooCommerce)
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import json, asyncio, aiohttp, re, time, random, warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
from utils.db_manager import get_db, init_db, upsert_competitor_products

MAX_CONCURRENT = 25
BATCH_SIZE = 50

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
]


def extract_price_from_html(html, url=""):
    """استخراج ذكي للسعر من HTML — يجرّب 5 طرق"""
    if not html or len(html) < 100:
        return None

    result = {"url": url, "success": False}

    # 1) JSON-LD (الأدق)
    for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S):
        try:
            data = json.loads(m.group(1))
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") == "Product" or "offers" in item:
                    offers = item.get("offers", item)
                    if isinstance(offers, list):
                        offers = offers[0]
                    if isinstance(offers, dict):
                        p = offers.get("price") or offers.get("lowPrice", 0)
                    else:
                        p = 0
                    price = float(str(p).replace(",", "").strip())
                    if price > 0:
                        result["price"] = price
                        result["product_name"] = item.get("name", "")
                        img = item.get("image", "")
                        result["image_url"] = img[0] if isinstance(img, list) else str(img)
                        result["success"] = True
                        return result
        except:
            pass

    # 2) Meta tags (og:price / product:price)
    og_price = re.search(r'<meta[^>]*(?:property|name)="product:price:amount"[^>]*content="([^"]+)"', html, re.I)
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
                return result
        except:
            pass

    # 3) Salla/Zid specific: window.__INITIAL_STATE__ or twilight.product
    salla_price = re.search(r'"price"\s*:\s*(\d+(?:\.\d+)?)\s*[,}]', html)
    salla_name = re.search(r'"name"\s*:\s*"([^"]{5,100})"', html)
    if salla_price:
        try:
            price = float(salla_price.group(1))
            if 1 < price < 50000:
                result["price"] = price
                result["product_name"] = salla_name.group(1) if salla_name else ""
                og_image = re.search(r'<meta[^>]*property="og:image"[^>]*content="([^"]+)"', html)
                result["image_url"] = og_image.group(1) if og_image else ""
                result["success"] = True
                return result
        except:
            pass

    # 4) HTML price class patterns (Arab stores)
    soup = BeautifulSoup(html, "html.parser")
    price_selectors = [
        {"class": re.compile(r"price|سعر", re.I)},
        {"itemprop": "price"},
        {"class": re.compile(r"product-price|product__price|woocommerce-Price", re.I)},
    ]
    for sel in price_selectors:
        for el in soup.find_all(["span", "div", "p", "ins", "bdi"], attrs=sel):
            text = el.get_text(strip=True)
            nums = re.findall(r"(\d[\d,.]+)", text)
            for n in nums:
                try:
                    price = float(n.replace(",", ""))
                    if 1 < price < 50000:
                        title_tag = soup.find("title")
                        og_img = soup.find("meta", {"property": "og:image"})
                        result["price"] = price
                        result["product_name"] = title_tag.get_text(strip=True) if title_tag else ""
                        result["image_url"] = og_img["content"] if og_img else ""
                        result["success"] = True
                        return result
                except:
                    pass

    # 5) Title extraction even without price
    return None


async def fetch_sitemap_urls(session, sitemap_url, store_name):
    """Fetch all product URLs from sitemap"""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    urls = []

    async def fetch_one(url):
        try:
            async with session.get(url, headers=headers, ssl=False,
                                   timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    return await resp.text(errors="ignore")
        except:
            pass
        # Fallback
        try:
            from scrapers.anti_ban import try_curl_cffi
            return try_curl_cffi(url, timeout=15)
        except:
            pass
        return None

    text = await fetch_one(sitemap_url)
    if not text:
        print(f"    [{store_name}] Sitemap unreachable")
        return urls

    soup = BeautifulSoup(text, "html.parser")
    sub_sitemaps = soup.find_all("sitemap")

    if sub_sitemaps:
        for sm in sub_sitemaps:
            loc = sm.find("loc")
            if loc and loc.text:
                sub_text = await fetch_one(loc.text.strip())
                if sub_text:
                    sub_soup = BeautifulSoup(sub_text, "html.parser")
                    for url_tag in sub_soup.find_all("url"):
                        loc2 = url_tag.find("loc")
                        if loc2 and loc2.text:
                            u = loc2.text.strip()
                            if re.search(r"/products?/|/p/", u, re.I):
                                urls.append(u)
                            elif not sub_sitemaps[0].find("loc").text.lower().__contains__("product"):
                                urls.append(u)
    else:
        for url_tag in soup.find_all("url"):
            loc = url_tag.find("loc")
            if loc and loc.text:
                u = loc.text.strip()
                if re.search(r"/products?/|/p/", u, re.I):
                    urls.append(u)

    # Also try /products.json for Salla/Shopify
    if len(urls) < 20:
        base = sitemap_url.split("/sitemap")[0]
        for endpoint in ["/products.json?limit=250", "/api/products?per_page=250"]:
            try:
                async with session.get(base + endpoint, headers=headers, ssl=False,
                                       timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        products = data.get("data", data.get("products", []))
                        for p in products:
                            if p.get("url"):
                                urls.append(p["url"])
                            elif p.get("id"):
                                urls.append(f"{base}/products/{p['id']}")
            except:
                pass

    return list(dict.fromkeys(urls))[:8000]


async def scrape_store(session, store_name, urls, sem):
    """Scrape one store"""
    stats = {"total": len(urls), "prices": 0, "errors": 0, "stored": 0}
    buffer = []
    headers_pool = [{"User-Agent": ua, "Accept": "text/html,*/*;q=0.8",
                     "Accept-Language": "ar,en;q=0.5"} for ua in USER_AGENTS]

    for i, url in enumerate(urls):
        async with sem:
            await asyncio.sleep(0.2 + random.uniform(0, 0.3))
            try:
                h = random.choice(headers_pool)
                async with session.get(url, headers=h, ssl=False,
                                       timeout=aiohttp.ClientTimeout(total=12),
                                       allow_redirects=True) as resp:
                    if resp.status != 200:
                        stats["errors"] += 1
                        continue
                    html = await resp.text(errors="ignore")
            except:
                stats["errors"] += 1
                continue

        result = extract_price_from_html(html, url)
        if result and result.get("success"):
            stats["prices"] += 1
            buffer.append({
                "name": result["product_name"],
                "price": result["price"],
                "product_url": url,
                "image_url": result.get("image_url", ""),
            })

        # Save batch
        if len(buffer) >= BATCH_SIZE:
            try:
                r = upsert_competitor_products(store_name, buffer, name_key="name", price_key="price")
                stats["stored"] += r.get("inserted", 0) + r.get("updated", 0)
            except:
                pass
            buffer = []

        if (i + 1) % 100 == 0:
            pct = (i + 1) * 100 // len(urls)
            print(f"    [{store_name}] {i+1}/{len(urls)} ({pct}%) | Prices: {stats['prices']}")

    # Save remaining
    if buffer:
        try:
            r = upsert_competitor_products(store_name, buffer, name_key="name", price_key="price")
            stats["stored"] += r.get("inserted", 0) + r.get("updated", 0)
        except:
            pass

    rate = stats["prices"] * 100 // max(stats["total"], 1)
    print(f"  [{store_name}] DONE: {stats['prices']}/{stats['total']} ({rate}%) | Stored: {stats['stored']}")
    return stats


async def main():
    start = time.time()
    init_db()

    with open("data/competitors_list_v30.json", encoding="utf-8") as f:
        comps = json.load(f)

    # Check what's already well-scraped
    conn = get_db()
    existing = {}
    for row in conn.execute("SELECT competitor, COUNT(*) FROM competitor_products_store WHERE price > 0 GROUP BY competitor").fetchall():
        existing[row[0]] = row[1]
    conn.close()

    # Only scrape stores with < 50 products
    to_scrape = []
    for c in comps:
        count = existing.get(c["name"], 0)
        if count < 50:
            to_scrape.append(c)

    print(f"=== TURBO SCRAPER v3 — Targeted ===")
    print(f"  Total competitors: {len(comps)}")
    print(f"  Already scraped (50+): {len(comps) - len(to_scrape)}")
    print(f"  To scrape: {len(to_scrape)}")
    for c in to_scrape:
        print(f"    - {c['name']} ({existing.get(c['name'], 0)} existing)")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Phase 1: Fetch all sitemaps
        print(f"\n--- Phase 1: Sitemaps ---")
        all_stores = {}
        for c in to_scrape:
            if c.get("sitemap_url"):
                urls = await fetch_sitemap_urls(session, c["sitemap_url"], c["name"])
                if urls:
                    all_stores[c["name"]] = urls
                    print(f"  {c['name']}: {len(urls)} URLs")
                else:
                    print(f"  {c['name']}: 0 URLs (trying store pages...)")
                    # Try common product listing pages
                    base = c["store_url"].rstrip("/")
                    for path in ["/collections/all", "/products", "/shop", "/store"]:
                        try:
                            async with session.get(base + path,
                                                   headers={"User-Agent": random.choice(USER_AGENTS)},
                                                   ssl=False, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                                if resp.status == 200:
                                    html = await resp.text(errors="ignore")
                                    # Extract product links
                                    found = re.findall(r'href="(/products?/[^"]+)"', html, re.I)
                                    page_urls = [base + u for u in set(found)]
                                    if page_urls:
                                        all_stores[c["name"]] = page_urls
                                        print(f"  {c['name']}: {len(page_urls)} from {path}")
                                        break
                        except:
                            pass

        total_urls = sum(len(u) for u in all_stores.values())
        print(f"\n  Total URLs to scrape: {total_urls}")

        # Phase 2: Scrape all stores concurrently
        print(f"\n--- Phase 2: Scraping ---")
        sem = asyncio.Semaphore(8)  # per-request limit
        tasks = []
        for name, urls in all_stores.items():
            tasks.append(scrape_store(session, name, urls, sem))

        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Final summary
    elapsed = time.time() - start
    conn = get_db()
    rows = conn.execute("""
        SELECT competitor, COUNT(*) as total, SUM(CASE WHEN price > 0 THEN 1 ELSE 0 END) as priced
        FROM competitor_products_store GROUP BY competitor ORDER BY total DESC
    """).fetchall()
    grand = conn.execute("SELECT COUNT(*) FROM competitor_products_store WHERE price > 0").fetchone()[0]
    conn.close()

    print(f"\n{'='*60}")
    print(f"  SCRAPE COMPLETE — {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"{'='*60}")
    for store, tot, priced in rows:
        print(f"  {store}: {priced} products")
    print(f"  {'─'*40}")
    print(f"  GRAND TOTAL: {grand} products with price")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
