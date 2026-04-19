"""
sitemap_cache.py
================
كاش روابط منتجات لكل متجر منافس مع تاريخ آخر تعديل (lastmod).
يُستخدم للتحديث التزايدي: في كل تشغيل نكتفي بكشط المنتجات الجديدة
أو التي تغيّر تاريخها فقط — مما يقلل وقت التحديث وضغط الكشط.

البنية على القرص: data/sitemap_cache/{store_slug}.json
{
  "store_url": "https://saeedsalah.com/",
  "fetched_at": 1776560000,
  "urls": {
    "https://saeedsalah.com/.../p123": "2026-04-15T12:00:00+03:00",
    ...
  }
}
"""
from __future__ import annotations
import os
import re
import json
import time
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse

DATA_DIR = os.environ.get("DATA_DIR", "data")
CACHE_DIR = os.path.join(DATA_DIR, "sitemap_cache")


def _slug(store_url: str) -> str:
    host = urlparse(store_url).hostname or store_url
    return re.sub(r"[^a-z0-9.-]+", "_", host.lower())


def _path(store_url: str) -> str:
    return os.path.join(CACHE_DIR, f"{_slug(store_url)}.json")


def load(store_url: str) -> Dict:
    p = _path(store_url)
    if not os.path.exists(p):
        return {"store_url": store_url, "fetched_at": 0, "urls": {}}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"store_url": store_url, "fetched_at": 0, "urls": {}}


def save(store_url: str, urls: Dict[str, str]) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    p = _path(store_url)
    payload = {
        "store_url": store_url,
        "fetched_at": int(time.time()),
        "urls": urls,
    }
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return p


def diff(old_urls: Dict[str, str], new_entries: Iterable) -> Tuple[List[str], List[str], List[str]]:
    """
    يُرجع (added, modified, unchanged).
    new_entries: iterable من SitemapEntry (.url, .lastmod) أو dict {url, lastmod}.
    """
    added: List[str] = []
    modified: List[str] = []
    unchanged: List[str] = []
    for e in new_entries:
        if hasattr(e, "url"):
            url = e.url
            lm = getattr(e, "lastmod", "") or ""
        else:
            url = e.get("url", "")
            lm = e.get("lastmod", "") or ""
        if not url:
            continue
        prev = old_urls.get(url)
        if prev is None:
            added.append(url)
        elif lm and prev != lm:
            modified.append(url)
        else:
            unchanged.append(url)
    return added, modified, unchanged


def merge_after_scrape(
    store_url: str,
    new_entries: Iterable,
    successfully_scraped_urls: Iterable[str] = (),
) -> Dict[str, str]:
    """
    يدمج لقطة Sitemap الجديدة مع الكاش القديم.
    - لكل URL تم كشطه بنجاح: نُحدّث lastmod من Sitemap.
    - URLs لم تُكشط (بسبب فشل أو لأنها unchanged): نحتفظ بـ lastmod القديم.
    """
    old = load(store_url).get("urls", {})
    success = set(successfully_scraped_urls or [])
    merged = dict(old)  # احتفظ بالقديم
    for e in new_entries:
        if hasattr(e, "url"):
            url = e.url
            lm = getattr(e, "lastmod", "") or ""
        else:
            url = e.get("url", "")
            lm = e.get("lastmod", "") or ""
        if not url:
            continue
        if url in success:
            merged[url] = lm  # حدّث
        elif url not in merged:
            merged[url] = lm  # سجّل ولو فشل (للمحاولة لاحقاً)
    save(store_url, merged)
    return merged


def status_all() -> List[Dict]:
    """يُرجع ملخص لكل ملفات الكاش الموجودة."""
    out: List[Dict] = []
    if not os.path.isdir(CACHE_DIR):
        return out
    for fn in sorted(os.listdir(CACHE_DIR)):
        if not fn.endswith(".json"):
            continue
        try:
            with open(os.path.join(CACHE_DIR, fn), "r", encoding="utf-8") as f:
                d = json.load(f)
            out.append({
                "store_url": d.get("store_url", fn),
                "urls_count": len(d.get("urls", {})),
                "fetched_at": d.get("fetched_at", 0),
                "file": fn,
            })
        except Exception:
            continue
    return out
