"""
sitemap_automation.py — كاشط ذكي مُحصَّن ضد الحظر v4.0
═══════════════════════════════════════════════════════════
يستخدم سلسلة anti-ban الكاملة من scrapers/anti_ban.py:
  1. curl_cffi (Chrome TLS fingerprint)
  2. curl_cffi (Safari iOS + XHR)
  3. cloudscraper (JS challenge bypass)
  4. httpx (HTTP/2)
  5. requests (browser headers)
  6. ZenRows / Selenium (last resort)

التغييرات عن v27:
  - استبدال aiohttp الخام بـ try_all_sync_fallbacks
  - تخفيض التوازي من 12 → 6
  - تأخير ذكي بين الطلبات (0.3-1.0s jitter)
  - تنظيف progress العالقة عند البدء
"""
import asyncio
import aiohttp
import logging
import os
import sys
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

# إضافة جذر المشروع إلى sys.path
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from engines.sitemap_resolve import _fetch_and_parse_sitemap
from engines.ai_scraper_v27 import scrape_product_ai, clean_product_name_ai
from utils.db_manager import (
    get_scraped_urls_today,
    save_job_progress,
    upsert_competitor_products,
)
from scrapers.anti_ban import (
    get_browser_headers,
    try_all_sync_fallbacks,
    try_curl_cffi,
    looks_like_bot_challenge,
    get_rate_limiter,
    _ua_rotator,
)
from utils import sitemap_cache as _sm_cache

# مسار ملف التقدم
_PROGRESS_PATH = os.path.join(os.environ.get("DATA_DIR", "data"), "sitemap_auto_progress.json")

# ── ثوابت الضبط المُحسَّنة ──────────────────────────────────────────────
# تقليل التوازي لمنع الحظر — curl_cffi sync يعمل في ThreadPoolExecutor
_CONCURRENCY = max(1, min(int(os.environ.get("SITEMAP_CONCURRENCY", "6")), 12))
_CONNECTOR_LIMIT = max(_CONCURRENCY, int(os.environ.get("SITEMAP_CONN_LIMIT", "20")))
_COMMIT_BATCH = max(1, int(os.environ.get("SITEMAP_COMMIT_BATCH", "10")))
_COMMIT_INTERVAL_SEC = max(1.0, float(os.environ.get("SITEMAP_COMMIT_INTERVAL_SEC", "3.0")))
_JOB_ID_PREFIX = "sitemap_auto"

# تأخير بين الطلبات — يمنع burst requests
_MIN_DELAY = float(os.environ.get("SITEMAP_MIN_DELAY", "0.3"))
_MAX_DELAY = float(os.environ.get("SITEMAP_MAX_DELAY", "1.0"))

# ThreadPoolExecutor مشترك لكل الطلبات sync
_SCRAPE_EXECUTOR = ThreadPoolExecutor(
    max_workers=_CONCURRENCY + 2,
    thread_name_prefix="scrape_worker",
)


def _write_progress(payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(_PROGRESS_PATH), exist_ok=True)
        with open(_PROGRESS_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# إعداد السجل بـ UTF-8 لتجنب UnicodeEncodeError على Windows
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join("data", "sitemap_automation.log"),
            encoding="utf-8",
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("SitemapAutomation_v4")


def _filter_product_entries(entries, store_url):
    """تصفية روابط المنتجات — نستبعد الصفحات غير المنتجات فقط.

    المنطق: أمرّر كل الروابط إلا اللي أكيد مش منتجات.
    المنتجات اللي ما فيها اسم/سعر سيتم رفضها تلقائياً في الكشط.
    """
    # صفحات مستبعدة بشكل قاطع
    _EXCLUDE = [
        "/blog/", "/page/", "/category/", "/categories/", "/tag/", "/tags/",
        "/cart", "/checkout", "/contact", "/account", "/wishlist", "/compare",
        "/login", "/register", "/search", "/about", "/faq", "/privacy",
        "/terms", "/policy", "/shipping", "/return", "/sitemap", "/feed",
        "/wp-content/", "/wp-admin/", "/wp-includes/",
        "/cdn-cgi/", "/.well-known/",
    ]
    # امتدادات ملفات مستبعدة (صور، ملفات)
    _EXCLUDE_EXT = (
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
        ".pdf", ".css", ".js", ".xml", ".json", ".txt", ".zip",
        ".mp4", ".mp3", ".woff", ".woff2", ".ttf", ".eot",
    )
    product_entries = []
    store_domain = store_url.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0].lower()

    for entry in entries:
        url = entry.url
        url_lower = url.lower()

        # استبعاد روابط من دومينات أخرى (CDN/صور)
        entry_domain = url_lower.replace("https://", "").replace("http://", "").split("/")[0]
        if entry_domain != store_domain:
            continue

        # استبعاد الصفحة الرئيسية
        path = url_lower.replace("https://", "").replace("http://", "")
        path = path[path.find("/"):] if "/" in path else "/"
        if path.rstrip("/") in ("", "/"):
            continue

        # استبعاد أنماط الصفحات غير المنتجات
        if any(x in url_lower for x in _EXCLUDE):
            continue

        # استبعاد ملفات (صور، CSS، JS)
        if any(url_lower.endswith(ext) for ext in _EXCLUDE_EXT):
            continue

        product_entries.append(entry)

    return product_entries



def _slug_from_url(product_url: str) -> str:
    slug = product_url.rstrip('/').split('/')[-1].replace('-', ' ').replace('_', ' ')
    if slug.startswith('p') and any(c.isdigit() for c in slug):
        slug = "منتج " + slug
    return slug


def _scrape_single_product_sync(product_url: str, store_name: str) -> dict | None:
    """
    جلب وكشط منتج واحد — يعمل في thread منفصل.

    يستخدم سلسلة anti-ban الكاملة:
      1. curl_cffi (Chrome TLS fingerprint)
      2. curl_cffi (Safari iOS + XHR)
      3. cloudscraper (JS challenge bypass)
      4. httpx (HTTP/2)
      5. requests (browser headers)
      6. ZenRows / Selenium (last resort)
    """
    slug = _slug_from_url(product_url)
    domain = product_url.split("//")[-1].split("/")[0]

    # ── adaptive rate limiting ──
    rl = get_rate_limiter()
    backoff_remaining = rl.get_backoff_remaining(domain)
    if backoff_remaining > 0:
        time.sleep(min(backoff_remaining, 30))

    # ── تأخير عشوائي لمنع burst ──
    time.sleep(random.uniform(_MIN_DELAY, _MAX_DELAY))

    html = None
    try:
        # المحرك الرئيسي: سلسلة 6 طبقات ضد الحظر
        html = try_all_sync_fallbacks(product_url, timeout=20)
    except Exception as exc:
        logger.debug(f"anti-ban chain error for {product_url}: {exc}")

    if not html:
        rl.record_error(domain, 403)
        _ua_rotator.mark_failed(domain, "all")
        return None

    # ── فحص bot challenge ──
    if looks_like_bot_challenge(html):
        logger.debug(f"bot challenge detected: {product_url}")
        rl.record_error(domain, 403)
        return None

    # ── استخراج بيانات المنتج بمحرك AI ──
    rl.record_success(domain)

    try:
        product_data = scrape_product_ai(html, product_url, slug)
    except Exception as exc:
        logger.debug(f"scrape_product_ai error {product_url}: {exc}")
        return None

    if (
        product_data
        and str(product_data.get("name") or "").strip()
        and float(product_data.get("price") or 0) > 0
    ):
        return product_data

    # فشل استخراج البيانات
    return None


async def _fetch_and_scrape_product_async(product_url: str, store_name: str) -> dict | None:
    """
    غلاف async حول _scrape_single_product_sync.
    يُشغّل الكشط في ThreadPoolExecutor لعدم حجب event loop.
    """
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            _SCRAPE_EXECUTOR,
            _scrape_single_product_sync,
            product_url,
            store_name,
        )
        return result
    except Exception as exc:
        logger.debug(f"executor error {product_url}: {exc}")
        return None


async def _flush_rows(
    store_name: str,
    buffer: list[dict],
) -> dict:
    """حفظ دفعة إلى SQLite بشكل فوري."""
    if not buffer:
        return {"inserted": 0, "updated": 0}
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
        logger.warning(f"flush error for {store_name}: {exc}")
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
    """جلب المنتجات من sitemap وكشطها بسلسلة anti-ban الكاملة."""
    logger.info(f"[START] {store_name} ({store_url}) incremental={incremental}")

    try:
        # 1. جلب الروابط من Sitemap
        entries = await _fetch_and_parse_sitemap(session, sitemap_url)
        if not entries:
            logger.warning(f"[SKIP] {store_name}: no sitemap entries found")
            return 0

        # 2. تصفية روابط المنتجات
        product_entries = _filter_product_entries(entries, store_url)
        if not product_entries:
            logger.warning(f"[SKIP] {store_name}: no products after filtering")
            return 0

        logger.info(f"[FOUND] {store_name}: {len(product_entries)} product URLs")

        # 2.5 — تحديث تزايدي
        if incremental:
            old_cache = _sm_cache.load(store_url).get("urls", {})
            added, modified, unchanged = _sm_cache.diff(old_cache, product_entries)
            target_urls = set(added) | set(modified)
            if old_cache and target_urls:
                logger.info(
                    f"[INCREMENTAL] {store_name}: new={len(added)} modified={len(modified)} "
                    f"unchanged={len(unchanged)}"
                )
                product_entries = [e for e in product_entries if e.url in target_urls]
            elif not old_cache:
                logger.info(f"[FULL] {store_name}: no cache, scraping all ({len(product_entries)})")
            else:
                logger.info(f"[SKIP] {store_name}: no changes since last run")
                _sm_cache.merge_after_scrape(store_url, product_entries, [])
                return 0

        # 2.6 — Resumption: تخطي ما كُشط اليوم
        try:
            already_done = get_scraped_urls_today(store_name)
        except Exception:
            already_done = set()
        if already_done:
            before = len(product_entries)
            product_entries = [e for e in product_entries if e.url not in already_done]
            skipped = before - len(product_entries)
            if skipped:
                logger.info(f"[RESUME] {store_name}: skipping {skipped} already scraped today")
            if not product_entries:
                logger.info(f"[DONE] {store_name}: all URLs already scraped today")
                return 0

        # 3. إعداد التنفيذ المتوازي
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
            "saved": 0,
        }
        last_flush_ts = [asyncio.get_running_loop().time()]

        async def _maybe_flush(force: bool = False) -> None:
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
                    f"[SAVE] {store_name}: committed {len(to_flush)} | "
                    f"total saved: {counters['saved']}/{total_target}"
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
                    product_data = await _fetch_and_scrape_product_async(
                        entry.url, store_name
                    )
                except Exception as exc:
                    logger.debug(f"unexpected error {entry.url}: {exc}")
                    product_data = None

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

                await _maybe_flush(force=False)

                # تقرير كل 10 منتجات
                if counters["done"] % 10 == 0 or counters["done"] >= total_target:
                    pct = (counters["done"] / total_target * 100) if total_target else 0
                    logger.info(
                        f"[PROGRESS] {store_name}: {counters['done']}/{total_target} "
                        f"({pct:.0f}%) | OK={counters['success']} FAIL={counters['failed']}"
                    )
                    await _report_progress()

        # 4. إطلاق المهام — Semaphore يتحكم بالتوازي
        tasks = [asyncio.create_task(_process_one(e)) for e in target_entries]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await _maybe_flush(force=True)
            await _report_progress()

        # 5. تحديث كاش Sitemap
        try:
            _sm_cache.merge_after_scrape(store_url, product_entries, successful_urls)
        except Exception as _ce:
            logger.warning(f"cache update error for {store_name}: {_ce}")

        if counters["failed"]:
            logger.info(
                f"[SKIP] {store_name}: {counters['failed']} URLs failed "
                "(no name/price) — no phantom rows inserted."
            )
        logger.info(
            f"[DONE] {store_name}: success={counters['success']}/{total_target} | "
            f"saved={counters['saved']}"
        )
        return counters["success"]

    except Exception as e:
        logger.error(f"[ERROR] {store_name}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def _load_competitors() -> list:
    """تحميل المنافسين — يدعم الشكل v30 والقديم."""
    v30_file = os.path.join("data", "competitors_list_v30.json")
    legacy_file = os.path.join("data", "competitors_list.json")

    target = v30_file if os.path.exists(v30_file) else legacy_file
    if not os.path.exists(target):
        logger.error("competitors file not found")
        return []

    with open(target, "r", encoding="utf-8") as f:
        raw = json.load(f)

    entries = []
    for item in raw:
        if isinstance(item, dict):
            entries.append({
                "name": item.get("name", ""),
                "store_url": item.get("store_url", ""),
                "sitemap_url": item.get("sitemap_url", ""),
            })
        elif isinstance(item, str):
            domain = item.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0]
            entries.append({
                "name": domain,
                "store_url": item,
                "sitemap_url": f"{item.rstrip('/')}/sitemap.xml",
            })
    return entries


async def run_automation(incremental: bool = True):
    """تشغيل الأتمتة لجميع المنافسين المسجلين."""
    entries = _load_competitors()
    if not entries:
        _write_progress({"running": False, "phase": "error", "message": "no competitors found"})
        return 0

    started_at = datetime.now().isoformat(timespec="seconds")
    job_id = f"{_JOB_ID_PREFIX}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    logger.info(f"=== Sitemap Automation v4.0 START === stores={len(entries)} incremental={incremental}")

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

    # جلسة aiohttp مشتركة — تُستخدم فقط لجلب Sitemaps
    connector = aiohttp.TCPConnector(
        limit=_CONNECTOR_LIMIT,
        limit_per_host=_CONCURRENCY,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    timeout = aiohttp.ClientTimeout(total=120, connect=15)

    try:
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
                    logger.error(f"[ERROR] {entry['name']}: {e}")
                    results.append(0)
                    totals_per_store[entry["name"]] = 0

            total_saved = sum(results)
            logger.info(f"=== AUTOMATION COMPLETE === total products: {total_saved}")
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

    except Exception as e:
        logger.error(f"=== AUTOMATION FAILED === {e}")
        import traceback
        logger.error(traceback.format_exc())
        _write_progress({
            "running": False,
            "phase": "error",
            "started_at": started_at,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "message": str(e)[:200],
        })
        return 0


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="Full scrape (ignore incremental cache)")
    args = ap.parse_args()

    # تنظيف progress العالقة من تشغيل سابق
    _write_progress({"running": False, "phase": "starting"})

    asyncio.run(run_automation(incremental=not args.full))
