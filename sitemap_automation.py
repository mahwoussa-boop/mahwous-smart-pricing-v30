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
from utils.db_manager import (
    get_scraped_urls_today,
    save_job_progress,
    upsert_competitor_products,
)
from scrapers.anti_ban import get_browser_headers
from utils import sitemap_cache as _sm_cache

# مسار ملف التقدم — يُقرأ من الواجهة لعرض شريط التقدم الحي
_PROGRESS_PATH = os.path.join(os.environ.get("DATA_DIR", "data"), "sitemap_auto_progress.json")

# ثوابت الضبط — قابلة للتجاوز عبر ENV بدون نشر
_CONCURRENCY = max(1, min(int(os.environ.get("SITEMAP_CONCURRENCY", "12")), 20))
_CONNECTOR_LIMIT = max(_CONCURRENCY, int(os.environ.get("SITEMAP_CONN_LIMIT", "30")))
_COMMIT_BATCH = max(1, int(os.environ.get("SITEMAP_COMMIT_BATCH", "5")))
_COMMIT_INTERVAL_SEC = max(1.0, float(os.environ.get("SITEMAP_COMMIT_INTERVAL_SEC", "3.0")))
_JOB_ID_PREFIX = "sitemap_auto"


def _write_progress(payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(_PROGRESS_PATH), exist_ok=True)
        with open(_PROGRESS_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

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
    """جلب صفحة المنتج مع fallback إلى v30 عند غياب السعر أو الحظر.

    يُعيد None صراحةً عند الفشل (403/Cloudflare/انتهاء المهلة/سعر غير صالح)
    بدلاً من إنشاء "منتج وهمي" باسم placeholder وسعر 0 — هذه السلوكية
    السابقة كانت تُلوّث قاعدة البيانات بآلاف الصفوف الفاسدة وتُعلّق محرك
    التحليل.
    """
    slug = _slug_from_url(product_url)
    last_status: int | None = None

    try:
        async with session.get(
            product_url,
            headers=get_browser_headers(),
            ssl=False,
            timeout=aiohttp.ClientTimeout(total=15),
            allow_redirects=True,
        ) as resp:
            last_status = resp.status
            if resp.status == 200:
                html = await resp.text(errors="ignore")
                product_data = scrape_product_ai(html, product_url, slug)
                if (
                    product_data
                    and str(product_data.get("name") or "").strip()
                    and float(product_data.get("price") or 0) > 0
                ):
                    return product_data
            elif resp.status in (403, 429, 503):
                # حماية Cloudflare/معدل محدود — لا داعي لمحاولة AI، انتقل لـ v30
                logger.debug(
                    f"⚠️ {store_name}: حظر محتمل {resp.status} على {product_url}"
                )
    except Exception as e:
        logger.debug(f"⚠️ خطأ HTTP في جلب {product_url}: {e}")

    try:
        loop = asyncio.get_running_loop()
        v30 = await loop.run_in_executor(
            None, lambda: scrape_product_v30(product_url, store_url=product_url)
        )
        if isinstance(v30, dict):
            v30_name = str(v30.get("name") or "").strip()
            v30_price = float(v30.get("price") or 0)
            if v30_name and v30_price > 0:
                return {
                    "name": v30_name,
                    "price": v30_price,
                    "price_source": v30.get("source") or "v30",
                    "size": "",
                    "brand": v30.get("brand") or "",
                    "gender": "للجنسين",
                    "type": "عطر",
                    "url": v30.get("url") or product_url,
                    "success": True,
                    "confidence": 0.95,
                    "image_url": v30.get("image") or "",
                    "sku": v30.get("sku") or "",
                }
    except Exception as e:
        logger.debug(f"⚠️ خطأ v30 في {product_url}: {e}")

    # فشل كامل → لا نُرجع placeholder. الاستدعاء الأعلى سيتجاهل None.
    logger.debug(
        f"🚫 فشل كشط {product_url} (status={last_status}) — سيتم تجاهل الرابط "
        "وعدم إدراج منتج وهمي."
    )
    return None

async def _flush_rows(
    store_name: str,
    buffer: list[dict],
) -> dict:
    """يحفظ دفعة صغيرة (≤ _COMMIT_BATCH) إلى SQLite بشكل فوري.

    يُستدعى بشكل متكرر أثناء الكشط لضمان عدم فقدان البيانات عند انقطاع
    العملية (Cloud Run idle-timeout / إغلاق التبويب / إعادة تشغيل الحاوية).
    """
    if not buffer:
        return {"inserted": 0, "updated": 0}
    # نُنفّذ upsert في executor لأنه sync I/O على SQLite — تجنّب حجب حلقة
    # الأحداث أثناء الكتابة.
    loop = asyncio.get_running_loop()
    try:
        res = await loop.run_in_executor(
            None,
            lambda rows=list(buffer): upsert_competitor_products(
                store_name, rows, name_key="name", price_key="price"
            ),
        )
        buffer.clear()
        return res or {}
    except Exception as exc:
        logger.warning(f"⚠️ فشل حفظ دفعة لـ {store_name}: {exc}")
        # لا نُفرغ البافر ليُعاد المحاولة في flush اللاحق
        return {"inserted": 0, "updated": 0}


async def process_store_sitemap(
    session,
    store_name,
    store_url,
    sitemap_url,
    incremental: bool = True,
    progress_cb=None,
    job_id: str | None = None,
):
    """جلب المنتجات من sitemap وكشطها باستخدام محرك v27 الهجين.

    ميزات معمارية:
      - Resumption من DB: تخطّي روابط كُشطت بنجاح اليوم (pricing_v30.db).
      - Real-time commit: حفظ كل 5 منتجات أو كل 3 ثوانٍ — لا فقدان بيانات
        عند انقطاع الـ Session.
      - Connection pooling: Semaphore موحّد يتحكم في التوازي عبر جلسة
        aiohttp واحدة مشتركة، بدلاً من دفعات gather ثابتة.
      - Checkpointing في job_progress: الواجهة تقرأ الحالة من SQLite حتى
        لو قُتل الـ subprocess الأب.
    """
    logger.info(f"🚀 بدء معالجة المتجر: {store_name} ({store_url})  incremental={incremental}")

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

        # 2.5 — وضع التحديث التزايدي: استبعاد ما لم يتغيّر
        if incremental:
            old_cache = _sm_cache.load(store_url).get("urls", {})
            added, modified, unchanged = _sm_cache.diff(old_cache, product_entries)
            target_urls = set(added) | set(modified)
            if old_cache and target_urls:
                logger.info(
                    f"📊 {store_name}: تزايدي → جديد {len(added)} | تعديل {len(modified)} | "
                    f"بدون تغيير {len(unchanged)}"
                )
                product_entries = [e for e in product_entries if e.url in target_urls]
            elif not old_cache:
                logger.info(f"🆕 {store_name}: لا يوجد كاش سابق — سيتم كشط الكل ({len(product_entries)})")
            else:
                logger.info(f"✅ {store_name}: لا يوجد تغيير منذ آخر تشغيل")
                # حدّث الكاش بنفس البيانات (لتسجيل fetched_at الجديد)
                _sm_cache.merge_after_scrape(store_url, product_entries, [])
                return 0

        # 2.6 — Resumption من DB: استبعد الروابط التي كُشطت بنجاح اليوم
        try:
            already_done = get_scraped_urls_today(store_name)
        except Exception:
            already_done = set()
        if already_done:
            before = len(product_entries)
            product_entries = [e for e in product_entries if e.url not in already_done]
            skipped = before - len(product_entries)
            if skipped:
                logger.info(
                    f"♻️ {store_name}: استئناف من DB — تخطّي {skipped} رابط "
                    f"كُشط بنجاح اليوم. المتبقي: {len(product_entries)}"
                )
            if not product_entries:
                logger.info(f"✅ {store_name}: كل الروابط كُشطت بالفعل اليوم — لا شيء للتنفيذ.")
                return 0

        # 3. إعداد حلقة التنفيذ المتوازي مع Semaphore موحّد
        max_products = int(os.environ.get("SITEMAP_MAX_PRODUCTS", "0"))
        target_entries = product_entries[:max_products] if max_products > 0 else product_entries
        total_target = len(target_entries)

        semaphore = asyncio.Semaphore(_CONCURRENCY)
        flush_lock = asyncio.Lock()
        pending_rows: list[dict] = []
        successful_urls: list[str] = []
        counters = {
            "done": 0,
            "success": 0,
            "failed": 0,
            "saved": 0,  # رقم تراكمي للمُدخل/المحدَّث في DB
        }
        last_flush_ts = [asyncio.get_running_loop().time()]

        async def _maybe_flush(force: bool = False) -> None:
            """Commit real-time buffer when batch threshold or interval hit."""
            now = asyncio.get_running_loop().time()
            should = (
                force
                or len(pending_rows) >= _COMMIT_BATCH
                or (pending_rows and (now - last_flush_ts[0]) >= _COMMIT_INTERVAL_SEC)
            )
            if not should:
                return
            async with flush_lock:
                if not pending_rows:
                    return
                to_flush = list(pending_rows)
                pending_rows.clear()
                res = await _flush_rows(store_name, to_flush)
                counters["saved"] += (
                    int(res.get("inserted", 0)) + int(res.get("updated", 0))
                )
                last_flush_ts[0] = asyncio.get_running_loop().time()
                logger.info(
                    f"💾 {store_name}: commit {len(to_flush)} | "
                    f"تراكمي محفوظ: {counters['saved']}/{total_target}"
                )

        async def _report_progress() -> None:
            if progress_cb:
                try:
                    progress_cb(
                        store_name, counters["done"], total_target, counters["success"]
                    )
                except Exception:
                    pass
            if job_id:
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None,
                        lambda: save_job_progress(
                            job_id,
                            total_target,
                            counters["done"],
                            [],
                            "running",
                            f"sitemap:{store_name}",
                            store_name,
                        ),
                    )
                except Exception:
                    pass

        async def _process_one(entry) -> None:
            async with semaphore:
                try:
                    product_data = await _fetch_and_scrape_product(
                        session, entry.url, store_name
                    )
                except Exception as exc:
                    logger.debug(f"⚠️ خطأ غير متوقع في {entry.url}: {exc}")
                    product_data = None

                # تحقق من الصحة: اسم حقيقي + سعر > 0
                row = None
                if isinstance(product_data, dict):
                    pname = str(product_data.get("name") or "").strip()
                    try:
                        pprice = float(product_data.get("price") or 0)
                    except (TypeError, ValueError):
                        pprice = 0.0
                    if pname and pprice > 0:
                        row = {
                            "name": pname,
                            "price": pprice,
                            "product_url": product_data.get("url") or entry.url,
                            "image_url": product_data.get("image_url") or "",
                            "brand": product_data.get("brand", ""),
                            "size": product_data.get("size", ""),
                            "gender": product_data.get("gender", "للجنسين"),
                        }

                counters["done"] += 1
                if row is not None:
                    counters["success"] += 1
                    successful_urls.append(row["product_url"])
                    async with flush_lock:
                        pending_rows.append(row)
                else:
                    counters["failed"] += 1

                # Commit فوري إذا تجاوزنا حدّ الدفعة
                await _maybe_flush(force=False)

                # تقرير إلى الواجهة كل ~10 منتجات لتقليل overhead
                if counters["done"] % 10 == 0 or counters["done"] >= total_target:
                    await _report_progress()

        # 4. إطلاق كل المهام — الـ Semaphore يتحكم بالتوازي فعلياً
        tasks = [asyncio.create_task(_process_one(e)) for e in target_entries]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            # flush نهائي لأي بقايا في البافر — حتى لو حصل استثناء
            await _maybe_flush(force=True)
            await _report_progress()

        # 5. تحديث كاش Sitemap مع lastmod للمنتجات التي نجحت
        try:
            _sm_cache.merge_after_scrape(store_url, product_entries, successful_urls)
        except Exception as _ce:
            logger.warning(f"⚠️ تعذّر حفظ كاش Sitemap لـ {store_name}: {_ce}")

        if counters["failed"]:
            logger.info(
                f"⏭️ {store_name}: تم تجاهل {counters['failed']} رابط فاشل "
                "(بدون اسم أو سعر) — لن يتم إدراج منتجات وهمية."
            )
        logger.info(
            f"✅ {store_name}: اكتمل — نجح {counters['success']}/{total_target} | "
            f"محفوظ في DB: {counters['saved']}"
        )
        return counters["success"]
    except Exception as e:
        logger.error(f"❌ خطأ أثناء معالجة {store_name}: {str(e)}")
        return 0

def _load_competitors() -> list:
    """تحميل المنافسين — يدعم الشكل المُثرى (v30) والشكل القديم (URLs فقط)."""
    # أولوية: الملف المُثرى v30
    v30_file = os.path.join("data", "competitors_list_v30.json")
    legacy_file = os.path.join("data", "competitors_list.json")

    target = v30_file if os.path.exists(v30_file) else legacy_file
    if not os.path.exists(target):
        logger.error("❌ ملف المنافسين غير موجود")
        return []

    with open(target, "r", encoding="utf-8") as f:
        raw = json.load(f)

    entries = []
    for item in raw:
        if isinstance(item, dict):
            # شكل مُثرى: {"name": ..., "store_url": ..., "sitemap_url": ...}
            entries.append({
                "name": item.get("name", ""),
                "store_url": item.get("store_url", ""),
                "sitemap_url": item.get("sitemap_url", ""),
            })
        elif isinstance(item, str):
            # شكل قديم: URL فقط
            domain = item.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0]
            entries.append({
                "name": domain,
                "store_url": item,
                "sitemap_url": f"{item.rstrip('/')}/sitemap.xml",
            })
    return entries


async def run_automation(incremental: bool = True):
    """تشغيل الأتمتة لجميع المنافسين المسجلين.

    incremental=True (الافتراضي): يكشط فقط ما تغيّر منذ آخر تشغيل.

    تُشغَّل هذه الدالة إمّا في subprocess منفصل (عبر zxUI: app.py) أو في
    daemon thread — كلاهما مفصول تماماً عن الـ Main Thread لـ Streamlit،
    بحيث لا يُلغى الكشط إذا أغلق المستخدم المتصفح.
    """
    entries = _load_competitors()
    if not entries:
        _write_progress({"running": False, "phase": "error", "message": "لا يوجد منافسون"})
        return 0

    started_at = datetime.now().isoformat(timespec="seconds")
    job_id = f"{_JOB_ID_PREFIX}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    _write_progress({
        "running": True,
        "phase": "starting",
        "started_at": started_at,
        "incremental": incremental,
        "job_id": job_id,
        "total_stores": len(entries),
        "store_index": 0,
        "current_store": "",
        "products_done": 0,
        "products_total": 0,
        "successful": 0,
        "totals_per_store": {},
    })

    # Connection pooling: جلسة aiohttp واحدة مشتركة لكل المتاجر،
    # مع connector بسعة أعلى + DNS cache لتقليل handshake overhead.
    connector = aiohttp.TCPConnector(
        limit=_CONNECTOR_LIMIT,
        limit_per_host=_CONCURRENCY,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    timeout = aiohttp.ClientTimeout(total=120, connect=15)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        results = []
        totals_per_store: dict[str, int] = {}

        def _cb(store, done, total, ok):
            _write_progress({
                "running": True,
                "phase": "scraping",
                "started_at": started_at,
                "incremental": incremental,
                "job_id": job_id,
                "total_stores": len(entries),
                "store_index": len(results) + 1,
                "current_store": store,
                "products_done": done,
                "products_total": total,
                "successful": ok,
                "totals_per_store": totals_per_store,
            })

        for entry in entries:
            try:
                count = await process_store_sitemap(
                    session,
                    entry["name"],
                    entry["store_url"],
                    entry["sitemap_url"],
                    incremental=incremental,
                    progress_cb=_cb,
                    job_id=job_id,
                )
                results.append(count or 0)
                totals_per_store[entry["name"]] = count or 0
            except Exception as e:
                logger.error(f"❌ خطأ في {entry['name']}: {e}")
                results.append(0)
                totals_per_store[entry["name"]] = 0

        total_saved = sum(results)
        logger.info(f"🏁 انتهت الأتمتة. إجمالي المنتجات المكتشفة: {total_saved}")
        _write_progress({
            "running": False,
            "phase": "completed",
            "started_at": started_at,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "incremental": incremental,
            "job_id": job_id,
            "total_stores": len(entries),
            "products_done": total_saved,
            "totals_per_store": totals_per_store,
        })
        # تسجيل نهاية الوظيفة في job_progress لسهولة قراءة الحالة من الواجهة
        try:
            save_job_progress(
                job_id,
                total_saved,
                total_saved,
                [],
                "done",
                "sitemap_automation",
                ",".join(totals_per_store.keys()),
            )
        except Exception:
            logger.debug("save_job_progress (done) failed", exc_info=True)
        return total_saved


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="كشط كامل (تجاهل الكاش التزايدي)")
    args = ap.parse_args()
    asyncio.run(run_automation(incremental=not args.full))
