import asyncio
import aiohttp
import logging
import os
import sys
import json
import re
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# إضافة جذر المشروع إلى sys.path
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from engines.sitemap_resolve import _fetch_and_parse_sitemap
from engines.ai_scraper_v27 import scrape_product_ai, clean_product_name_ai
from engines.selenium_scraper_v30 import scrape_product_v30
from utils.db_manager import upsert_competitor_products
from scrapers.anti_ban import get_browser_headers

# إعداد السجل
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[
        logging.FileHandler(os.path.join("data", "sitemap_automation.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SitemapAutomation_v27")

def _filter_product_entries(entries, store_url):
    """تصفية روابط المنتجات فقط بناءً على أنماط شائعة"""
    product_entries = []
    patterns = ["/p/", "/product/", "/products/", "/item/", "/shop/", "منتج"]
    
    for entry in entries:
        url = entry.url.lower()
        if any(x in url for x in ["/blog/", "/page/", "/category/", "/tag/", "/cart", "/checkout", "/contact"]):
            continue
        if any(p in url for p in patterns) or url.rstrip('/').split('/')[-1].startswith('p'):
            product_entries.append(entry)
            
    return product_entries

def _slug_from_url(product_url: str) -> str:
    slug = product_url.rstrip('/').split('/')[-1].replace('-', ' ').replace('_', ' ')
    if slug.startswith('p') and any(c.isdigit() for c in slug):
        slug = "منتج " + slug
    return slug


async def _fetch_and_scrape_product(session, product_url, store_name):
    """جلب صفحة المنتج مع fallback إلى v30 عند غياب السعر أو الحظر."""
    slug = _slug_from_url(product_url)
    try:
        async with session.get(
            product_url,
            headers=get_browser_headers(),
            ssl=False,
            timeout=aiohttp.ClientTimeout(total=15),
            allow_redirects=True
        ) as resp:
            if resp.status == 200:
                html = await resp.text(errors='ignore')
                product_data = scrape_product_ai(html, product_url, slug)
                if product_data and float(product_data.get("price") or 0) > 0:
                    return product_data
    except Exception as e:
        logger.debug(f"⚠️ خطأ HTTP في جلب {product_url}: {e}")

    try:
        loop = asyncio.get_running_loop()
        v30 = await loop.run_in_executor(None, lambda: scrape_product_v30(product_url, store_url=product_url))
        if isinstance(v30, dict):
            return {
                "name": v30.get("name") or clean_product_name_ai(slug),
                "price": float(v30.get("price") or 0),
                "price_source": v30.get("source") or "v30",
                "size": "",
                "brand": v30.get("brand") or "",
                "gender": "للجنسين",
                "type": "عطر",
                "url": v30.get("url") or product_url,
                "success": bool((v30.get("name") or slug) and float(v30.get("price") or 0) > 0),
                "confidence": 0.95 if float(v30.get("price") or 0) > 0 else 0.2,
                "image_url": v30.get("image") or "",
                "sku": v30.get("sku") or "",
            }
    except Exception as e:
        logger.debug(f"⚠️ خطأ v30 في {product_url}: {e}")

    return {
        "name": clean_product_name_ai(slug),
        "price": 0.0,
        "price_source": "failed",
        "size": "",
        "brand": "",
        "gender": "للجنسين",
        "type": "عطر",
        "url": product_url,
        "success": False,
        "confidence": 0.0,
        "image_url": "",
        "sku": "",
    }

async def process_store_sitemap(session, store_name, store_url, sitemap_url):
    """جلب المنتجات من sitemap وكشطها باستخدام محرك v27 الهجين"""
    logger.info(f"🚀 بدء معالجة المتجر: {store_name} ({store_url})")
    
    try:
        # 1. جلب الروابط من Sitemap
        entries = await _fetch_and_parse_sitemap(session, sitemap_url)
        if not entries:
            logger.warning(f"⚠️ لم يتم العثور على روابط في sitemap لـ {store_name}")
            return 0
        
        # 2. تصفية روابط المنتجات
        product_entries = _filter_product_entries(entries, store_url)
        if not product_entries:
            logger.warning(f"⚠️ لم يتم العثور على منتجات بعد التصفية لـ {store_name}")
            return 0
            
        logger.info(f"✅ تم العثور على {len(product_entries)} منتج في {store_name}")
        
        # 3. كشط كل المنتجات على دفعات متوازية
        db_products = []
        successful_scrapes = 0
        concurrency = max(1, min(int(os.environ.get("SITEMAP_V30_CONCURRENCY", "10")), 10))
        max_products = int(os.environ.get("SITEMAP_MAX_PRODUCTS", "0"))
        target_entries = product_entries[:max_products] if max_products > 0 else product_entries

        for start in range(0, len(target_entries), concurrency):
            batch = target_entries[start:start + concurrency]
            logger.info(f"  📍 معالجة الدفعة {start + 1}-{start + len(batch)} من {len(target_entries)}...")
            batch_results = await asyncio.gather(
                *[_fetch_and_scrape_product(session, entry.url, store_name) for entry in batch],
                return_exceptions=True,
            )

            rows_to_save = []
            for entry, product_data in zip(batch, batch_results):
                if isinstance(product_data, Exception) or not isinstance(product_data, dict):
                    product_data = {
                        "name": clean_product_name_ai(_slug_from_url(entry.url)),
                        "price": 0.0,
                        "url": entry.url,
                        "image_url": "",
                        "brand": "",
                        "size": "",
                        "gender": "للجنسين",
                        "success": False,
                    }

                row = {
                    "name": product_data.get("name") or clean_product_name_ai(_slug_from_url(entry.url)),
                    "price": float(product_data.get("price") or 0),
                    "product_url": product_data.get("url") or entry.url,
                    "image_url": product_data.get("image_url") or "",
                    "brand": product_data.get("brand", ""),
                    "size": product_data.get("size", ""),
                    "gender": product_data.get("gender", "للجنسين"),
                }
                db_products.append(row)
                rows_to_save.append(row)
                if row["price"] > 0:
                    successful_scrapes += 1

            if rows_to_save:
                res = upsert_competitor_products(store_name, rows_to_save, name_key="name", price_key="price")
                logger.info(
                    f"💾 {store_name}: حفظ دفعة {len(rows_to_save)} منتج | أسعار فعلية حتى الآن: {successful_scrapes}/{len(db_products)} | inserted={res.get('inserted', 0)} updated={res.get('updated', 0)}"
                )

        # 4. تخزين البيانات في قاعدة البيانات
        if db_products:
            logger.info(f"✅ {store_name}: اكتمل كشط {len(db_products)} منتج | أسعار فعلية: {successful_scrapes}")
            return len(db_products)
        return 0
    except Exception as e:
        logger.error(f"❌ خطأ أثناء معالجة {store_name}: {str(e)}")
        return 0

async def run_automation():
    """تشغيل الأتمتة لجميع المنافسين المسجلين"""
    competitors_file = os.path.join("data", "competitors_list.json")
    if not os.path.exists(competitors_file):
        logger.error("❌ ملف المنافسين غير موجود")
        return
        
    with open(competitors_file, "r", encoding="utf-8") as f:
        stores = json.load(f)
        
    async with aiohttp.ClientSession() as session:
        tasks = []
        for store_url in stores:
            domain = store_url.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0]
            sitemap_url = f"{store_url.rstrip('/')}/sitemap.xml"
            tasks.append(process_store_sitemap(session, domain, store_url, sitemap_url))
        
        results = await asyncio.gather(*tasks)
        total_saved = sum(r for r in results if r)
        logger.info(f"🏁 انتهت الأتمتة. إجمالي المنتجات المكتشفة: {total_saved}")
        return total_saved

if __name__ == "__main__":
    asyncio.run(run_automation())
