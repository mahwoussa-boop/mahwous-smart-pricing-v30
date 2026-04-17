"""
scrapers/sitemap_resolve.py — Shim (توافق رجعي)
المصدر الحقيقي: engines/sitemap_resolve.py
"""
from __future__ import annotations

import aiohttp

from engines.sitemap_resolve import (  # noqa: F401
    SitemapDiscoveryError,
    resolve_product_urls,
)


class SitemapResolver:
    """غلاف قديم: يفتح جلسة قصيرة ويحوّل إلى resolve_product_urls."""

    async def resolve(self, base_url: str) -> list[str]:
        connector = aiohttp.TCPConnector(ssl=False, limit=10)
        timeout = aiohttp.ClientTimeout(total=120, connect=20, sock_read=60)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            return await resolve_product_urls(base_url.rstrip("/"), session)


sitemap_resolver = SitemapResolver()
