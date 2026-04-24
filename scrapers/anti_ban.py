"""
scrapers/anti_ban.py — Anti-Ban Arsenal v3.0 (2026)  *** MASTER ***
═══════════════════════════════════════════════════════════════════
v3.0 additions over v2.1:
  + SmartUARotator  — per-domain UA success tracking, avoids repeating
                       recently-failed User-Agents for a domain.
  + ProxyRotator    — thread-safe round-robin pool loaded from env var
                       SCRAPER_PROXIES. Infrastructure ready; no proxies
                       needed at runtime (returns None silently).
  + Retry-After     — fetch_with_retry now reads the Retry-After header
                       from 429 responses and waits the server-requested
                       interval (capped at MAX_RETRY_AFTER_SECS).
  + Improved jitter — backoff formula adds full ±25 % random jitter to
                       prevent synchronised retry storms across parallel tasks.

Backward-compat guarantee:
  All existing call signatures are unchanged. New parameters are
  keyword-only with safe defaults so callers need zero changes.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import threading
import time
import warnings
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

# ── tuneable constants ──────────────────────────────────────────────────────
# Maximum seconds we will honour a Retry-After header (prevent DoS by server)
MAX_RETRY_AFTER_SECS = 120
# How many recent UA failures to remember per domain
UA_FAILURE_MEMORY = 6
# Jitter fraction applied to every computed backoff (±25 %)
BACKOFF_JITTER_FRACTION = 0.25


# ══════════════════════════════════════════════════════════════════════════
#  1. User-Agent pool — real 2026 browser strings
# ══════════════════════════════════════════════════════════════════════════
_REAL_UA_POOL: List[str] = [
    # Chrome / Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    # Chrome / macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:134.0) Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    # Mobile
    "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.99 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Samsung SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36",
    # Crawlers (bypass some bot-checks that whitelist known crawlers)
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
]

_ACCEPT_LANGUAGES = [
    "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
    "ar,en-US;q=0.9,en;q=0.8",
    "en-US,en;q=0.9,ar;q=0.8",
    "ar-SA,ar;q=0.8,en;q=0.5",
]

_ACCEPT_HEADERS = [
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
]

# Mobile Safari (iOS) + AJAX (XHR) header profile. Used as a secondary
# curl_cffi attempt: some stores gate their public HTML behind a browser
# check but return JSON via the same URL when the request looks like an
# in-page XHR from the mobile site.
_MOBILE_AJAX_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


# ══════════════════════════════════════════════════════════════════════════
#  2. SmartUARotator — per-domain failure-aware UA selection (NEW v3.0)
# ══════════════════════════════════════════════════════════════════════════
class SmartUARotator:
    """
    Chooses User-Agents with awareness of past failures per domain.

    For each domain it keeps a rolling deque of recently-failed UAs
    (length UA_FAILURE_MEMORY). When picking a UA it tries to avoid
    those that failed recently, falling back to pure random if all
    remaining candidates have also failed.
    """

    def __init__(self, pool: List[str] = None):
        self._pool = pool or _REAL_UA_POOL
        # domain → deque of recently-failed UA strings
        self._failed: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=UA_FAILURE_MEMORY)
        )
        self._lock = threading.Lock()

    def pick(self, domain: str = "") -> str:
        """Return a UA string that has not recently failed for this domain."""
        with self._lock:
            if not domain:
                return random.choice(self._pool)
            failed_set = set(self._failed[domain])
            candidates = [ua for ua in self._pool if ua not in failed_set]
            # If we have exhausted all candidates, reset and try fresh
            if not candidates:
                self._failed[domain].clear()
                candidates = self._pool
            return random.choice(candidates)

    def mark_failed(self, domain: str, ua: str) -> None:
        """Record that this UA triggered a ban signal from this domain."""
        with self._lock:
            self._failed[domain].append(ua)


# Module-level singleton shared by all fetchers
_ua_rotator = SmartUARotator()


def get_browser_headers(referer: str = "", domain: str = "") -> dict:
    """
    Generate full browser-like request headers.
    Uses SmartUARotator to avoid recently-banned UAs per domain.
    """
    ua = _ua_rotator.pick(domain=domain or urlparse(referer).netloc)
    headers = {
        "User-Agent":                ua,
        "Accept":                    random.choice(_ACCEPT_HEADERS),
        "Accept-Language":           random.choice(_ACCEPT_LANGUAGES),
        "Accept-Encoding":           "gzip, deflate",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none" if not referer else "cross-site",
        "Sec-Fetch-User":            "?1",
        "Cache-Control":             "max-age=0",
        "DNT":                       "1",
    }
    if referer:
        headers["Referer"] = referer
        headers["Sec-Fetch-Site"] = "cross-site"

    try:
        if "Chrome" in ua and "Edg" not in ua:
            major = ua.split("Chrome/")[1].split(".")[0] if "Chrome/" in ua else "134"
            headers.update({
                "sec-ch-ua":          f'"Chromium";v="{major}", "Google Chrome";v="{major}", "Not-A.Brand";v="99"',
                "sec-ch-ua-mobile":   "?0" if "Mobile" not in ua else "?1",
                "sec-ch-ua-platform": '"Windows"' if "Windows" in ua else ('"macOS"' if "Mac" in ua else '"Android"'),
            })
        elif "Edg" in ua:
            major = ua.split("Edg/")[1].split(".")[0] if "Edg/" in ua else "134"
            headers.update({
                "sec-ch-ua":          f'"Chromium";v="{major}", "Microsoft Edge";v="{major}", "Not-A.Brand";v="99"',
                "sec-ch-ua-mobile":   "?0",
                "sec-ch-ua-platform": '"Windows"',
            })
    except IndexError:
        pass  # safe: malformed UA string, skip hints

    return headers


def get_xml_headers() -> dict:
    """Request headers suitable for Sitemap XML fetches."""
    ua = random.choice([
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    ])
    return {
        "User-Agent":      ua,
        "Accept":          "application/xml,text/xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection":      "keep-alive",
        "Cache-Control":   "no-cache",
    }


# ══════════════════════════════════════════════════════════════════════════
#  3. ProxyRotator — ready for future proxy pool (NEW v3.0)
# ══════════════════════════════════════════════════════════════════════════
class ProxyRotator:
    """
    Thread-safe round-robin proxy pool.

    Proxies are loaded from the SCRAPER_PROXIES environment variable as a
    comma-separated list, e.g.:
        SCRAPER_PROXIES=http://user:pass@1.2.3.4:8080,http://5.6.7.8:3128

    If the env var is empty or unset, get_proxy() always returns None and
    the code behaves exactly as before (no proxies).

    Usage (extend fetch calls gradually):
        proxy = proxy_rotator.get_proxy(domain)
        resp  = await session.get(url, proxy=proxy, ...)
        if resp.status == 403:
            proxy_rotator.mark_failed(proxy)
    """

    def __init__(self) -> None:
        self._proxies: List[str] = self._load_from_env()
        self._failed:  set       = set()
        self._index:   int       = 0
        self._lock     = threading.Lock()

    @staticmethod
    def _load_from_env() -> List[str]:
        # Cloud Run friendly: support several env var aliases. The list form
        # wins when both are set.
        raw_list = (
            os.environ.get("SCRAPER_PROXIES", "")
            or os.environ.get("SCRAPER_PROXY_LIST", "")
        ).strip()
        proxies: List[str] = []
        if raw_list:
            proxies.extend(p.strip() for p in raw_list.split(",") if p.strip())
        # Single-proxy aliases (one OR both may be set); de-dup while preserving order.
        single = (
            os.environ.get("SCRAPER_HTTPS_PROXY", "")
            or os.environ.get("SCRAPER_HTTP_PROXY", "")
        ).strip()
        if single and single not in proxies:
            proxies.append(single)
        return proxies

    def get_proxy_for_domain(self, domain: str) -> Optional[str]:
        """
        Sticky-per-store selection. Rationale: sites that use session cookies
        or per-IP rate windows punish IP-hopping within a single store. A
        deterministic hash(domain) -> proxy keeps a full store crawl on the
        same egress while different stores still fan out across the pool.
        Falls back to healthy round-robin when the hashed proxy is failed.
        """
        with self._lock:
            healthy = [p for p in self._proxies if p not in self._failed]
            if not healthy:
                if not self._proxies:
                    return None
                # All proxies failed — reset & retry
                self._failed.clear()
                healthy = list(self._proxies)
            if not domain:
                self._index = (self._index + 1) % len(healthy)
                return healthy[self._index]
            idx = hash(domain) % len(healthy)
            return healthy[idx]

    def get_proxy(self, domain: str = "") -> Optional[str]:
        """
        Return the next healthy proxy in round-robin order, or None if
        no proxies are configured (fully backward-compatible).
        """
        with self._lock:
            healthy = [p for p in self._proxies if p not in self._failed]
            if not healthy:
                if self._proxies:
                    # All proxies failed — reset failures and retry
                    logger.warning("All proxies failed; resetting failure list.")
                    self._failed.clear()
                    healthy = list(self._proxies)
                else:
                    return None
            self._index = (self._index + 1) % len(healthy)
            return healthy[self._index % len(healthy)]

    def mark_failed(self, proxy: Optional[str]) -> None:
        """Flag a proxy as temporarily failed (e.g. after a 403 or timeout)."""
        if proxy:
            with self._lock:
                self._failed.add(proxy)
                logger.warning("Proxy marked failed: %s", proxy[:40])

    def has_proxies(self) -> bool:
        return bool(self._proxies)


# Module-level singleton
proxy_rotator = ProxyRotator()


# ══════════════════════════════════════════════════════════════════════════
#  4. AdaptiveRateLimiter — improved with Retry-After support (v3.0)
# ══════════════════════════════════════════════════════════════════════════
class AdaptiveRateLimiter:
    def __init__(self):
        self._state: dict[str, dict] = defaultdict(lambda: {
            "delay":          random.uniform(0.5, 1.5),
            "consecutive_ok": 0,
            "backing_off":    False,
            "backoff_until":  0.0,
            # Observability: per-domain HTTP block counters surfaced to the
            # dashboard via get_block_counts() so the "HTTP block dominant"
            # hint is accurate instead of falling back to "JSON-LD missing".
            "n_403": 0,
            "n_429": 0,
            "n_5xx": 0,
        })

    def get_block_counts(self, domain: str) -> dict:
        s = self._state.get(domain)
        if not s:
            return {"403": 0, "429": 0, "5xx": 0}
        return {
            "403": int(s.get("n_403", 0)),
            "429": int(s.get("n_429", 0)),
            "5xx": int(s.get("n_5xx", 0)),
        }

    async def wait(self, domain: str) -> None:
        s = self._state[domain]
        now = time.monotonic()
        if s["backing_off"] and now < s["backoff_until"]:
            wait_t = s["backoff_until"] - now
            logger.debug("domain=%s rate-limited, waiting %.1fs", domain, wait_t)
            await asyncio.sleep(wait_t)
        else:
            # Add ±25 % jitter to prevent synchronised requests
            jitter = s["delay"] * random.uniform(
                -BACKOFF_JITTER_FRACTION, BACKOFF_JITTER_FRACTION
            )
            await asyncio.sleep(max(0.1, s["delay"] + jitter))

    def record_success(self, domain: str) -> None:
        s = self._state[domain]
        s["consecutive_ok"] += 1
        s["backing_off"] = False
        # Gradually speed up after 5 consecutive successes
        if s["consecutive_ok"] >= 5 and s["delay"] > 0.25:
            s["delay"] = max(0.25, s["delay"] * 0.85)

    def record_error(
        self,
        domain: str,
        status: int,
        retry_after: Optional[float] = None,
    ) -> None:
        """
        Record an HTTP error and update backoff state.

        Args:
            domain:      target domain
            status:      HTTP status code received
            retry_after: value of Retry-After header in seconds (if present)
        """
        s = self._state[domain]
        s["consecutive_ok"] = 0

        # Observability counters
        if status == 429:
            s["n_429"] = int(s.get("n_429", 0)) + 1
        elif status == 403:
            s["n_403"] = int(s.get("n_403", 0)) + 1
        elif status in (500, 502, 503, 504):
            s["n_5xx"] = int(s.get("n_5xx", 0)) + 1

        if status == 429:
            if retry_after and 0 < retry_after <= MAX_RETRY_AFTER_SECS:
                # Honour server-requested wait, add small safety margin
                backoff = retry_after + random.uniform(1, 5)
                logger.warning(
                    "429 from %s — server Retry-After=%.0fs (honouring)",
                    domain, retry_after,
                )
            else:
                # Exponential with ±25 % jitter, capped at 90 s
                backoff = min(
                    s["delay"] * 3 + random.uniform(2, 8),
                    90.0,
                )
                logger.warning("429 from %s — backing off %.0fs", domain, backoff)
            s["delay"] = min(s["delay"] * 2, 20.0)
            s["backing_off"] = True
            s["backoff_until"] = time.monotonic() + backoff

        elif status == 403:
            backoff = random.uniform(15, 60)
            s["backing_off"] = True
            s["backoff_until"] = time.monotonic() + backoff
            logger.warning("403 from %s — backing off %.0fs", domain, backoff)

        elif status in (500, 502, 503, 504):
            s["delay"] = min(s["delay"] * 1.5, 10.0)

    def get_backoff_remaining(self, domain: str) -> float:
        """Return seconds left in current backoff window (0 if not backing off)."""
        s = self._state[domain]
        if not s["backing_off"]:
            return 0.0
        remaining = s["backoff_until"] - time.monotonic()
        return max(0.0, remaining)


_rate_limiter = AdaptiveRateLimiter()


def get_rate_limiter() -> AdaptiveRateLimiter:
    return _rate_limiter


# ── Phase 1 (2026-04-19): per-domain concurrency cap ───────────────────────
# The AdaptiveRateLimiter above already applies per-domain *delay* and backoff,
# but N workers that clear `rl.wait(domain)` inside the same event-loop tick
# can still burst-fire on the same origin — the classic Cloudflare 429 trigger
# on large sitemapindex fan-outs. We add a second, independent gate: a
# per-domain asyncio.Semaphore limiting how many requests to a single host
# may be *in flight* concurrently. Tunable via DOMAIN_CONCURRENCY env var.
_DOMAIN_CONCURRENCY_DEFAULT = int(os.environ.get("DOMAIN_CONCURRENCY", "4"))
_DOMAIN_SEMAPHORES: dict[str, asyncio.Semaphore] = {}
_DOMAIN_SEM_LOCK = threading.Lock()


def get_domain_semaphore(domain: str) -> asyncio.Semaphore:
    """Return (and lazily create) a per-domain concurrency semaphore."""
    sem = _DOMAIN_SEMAPHORES.get(domain)
    if sem is not None:
        return sem
    with _DOMAIN_SEM_LOCK:
        sem = _DOMAIN_SEMAPHORES.get(domain)
        if sem is None:
            sem = asyncio.Semaphore(_DOMAIN_CONCURRENCY_DEFAULT)
            _DOMAIN_SEMAPHORES[domain] = sem
    return sem


# ══════════════════════════════════════════════════════════════════════════
#  5. fetch_with_retry — now Retry-After aware + optional proxy (v3.0)
# ══════════════════════════════════════════════════════════════════════════
async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    *,
    max_retries: int = 3,
    base_delay:  float = 2.0,
    referer:     str = "",
    proxy:       Optional[str] = None,
) -> Optional[aiohttp.ClientResponse]:
    """
    Fetch URL with retry, exponential backoff, Retry-After support, and
    optional proxy routing.

    Args:
        session:     shared aiohttp ClientSession
        url:         target URL
        max_retries: total attempts before giving up
        base_delay:  base backoff seconds (doubles each retry)
        referer:     Referer header value
        proxy:       optional proxy URL (e.g. 'http://1.2.3.4:8080')
                     None = direct connection (default, backward-compat)

    Returns:
        Open ClientResponse on HTTP 200, None otherwise.
        Caller MUST close the response after reading.
    """
    domain = urlparse(url).netloc
    rl = get_rate_limiter()
    dom_sem = get_domain_semaphore(domain)

    for attempt in range(max_retries):
        headers = get_browser_headers(referer=referer or f"https://{domain}/", domain=domain)
        # Track the UA we are using so we can mark it failed on ban signals
        current_ua = headers.get("User-Agent", "")
        try:
            await rl.wait(domain)
            request_kwargs = dict(
                headers=headers,
                ssl=False,
                allow_redirects=True,
            )
            if proxy:
                request_kwargs["proxy"] = proxy

            # Phase 1: per-domain concurrency gate — prevents in-flight bursts
            # even when N workers all clear rl.wait() simultaneously.
            async with dom_sem:
                resp = await session.get(url, **request_kwargs)

            if resp.status == 200:
                rl.record_success(domain)
                return resp  # Caller owns this response; they must close it

            # Parse Retry-After header if present
            retry_after_raw = resp.headers.get("Retry-After", "")
            retry_after: Optional[float] = None
            if retry_after_raw:
                try:
                    retry_after = float(retry_after_raw)
                except ValueError:
                    retry_after = None

            rl.record_error(domain, resp.status, retry_after=retry_after)

            # Non-retriable: resource gone
            if resp.status in (404, 410):
                resp.close()
                return None

            # Retriable: rate-limited or server error
            if resp.status in (429, 403, 500, 502, 503):
                # Mark UA as failed for ban-type responses
                if resp.status in (429, 403):
                    _ua_rotator.mark_failed(domain, current_ua)

                resp.close()

                # Compute backoff: exponential with ±25 % jitter
                raw_backoff = base_delay * (2 ** attempt)
                jitter = raw_backoff * random.uniform(
                    -BACKOFF_JITTER_FRACTION, BACKOFF_JITTER_FRACTION
                )
                # Also respect whatever the rate limiter computed
                rl_remaining = rl.get_backoff_remaining(domain)
                delay = max(raw_backoff + jitter, rl_remaining, 0.5)

                logger.debug(
                    "attempt %d/%d status=%d — sleeping %.1fs before retry",
                    attempt + 1, max_retries, resp.status, delay,
                )
                await asyncio.sleep(delay)
                continue

            resp.close()
            return None

        except (aiohttp.ClientConnectorError, asyncio.TimeoutError) as exc:
            raw_backoff = base_delay * (2 ** attempt)
            jitter = raw_backoff * random.uniform(
                -BACKOFF_JITTER_FRACTION, BACKOFF_JITTER_FRACTION
            )
            delay = max(raw_backoff + jitter, 0.5)
            logger.debug(
                "attempt %d/%d %s — sleeping %.1fs",
                attempt + 1, max_retries, type(exc).__name__, delay,
            )
            await asyncio.sleep(delay)

        except Exception as exc:
            logger.debug("fetch_with_retry unexpected error: %s", exc)
            return None

    return None


# ══════════════════════════════════════════════════════════════════════════
#  6. Thread-safe singleton fallback sessions
# ══════════════════════════════════════════════════════════════════════════
_SESSION_LOCK  = threading.Lock()
_CFFI_SESSION  = None
_CFFI_SESSIONS: Dict[str, object] = {}  # keyed by impersonate string
_CLOUD_SCRAPER = None
_REQ_SESSION   = None

# When a caller asks for an impersonate value that the installed curl_cffi
# doesn't recognise, try these same-family fallbacks so we still get a
# working session instead of silently falling through to weaker fallbacks.
_IMPERSONATE_FALLBACKS: Dict[str, Tuple[str, ...]] = {
    "chrome131": ("chrome131", "chrome124", "chrome120", "chrome110"),
    "safari_ios": ("safari_ios", "safari17_2_ios", "safari17_0_ios", "safari15_5"),
}


def _get_cffi_session(impersonate: Optional[str] = None):
    """
    Return a curl_cffi Session.

    impersonate=None → legacy singleton (tries chrome131 → 110).
    impersonate=<str> → cached per-value session. If the exact string fails
    to initialise we try the family fallbacks in _IMPERSONATE_FALLBACKS
    so a curl_cffi version mismatch degrades gracefully.
    """
    global _CFFI_SESSION

    if impersonate:
        sess = _CFFI_SESSIONS.get(impersonate)
        if sess is not None:
            return sess
        with _SESSION_LOCK:
            sess = _CFFI_SESSIONS.get(impersonate)
            if sess is not None:
                return sess
            try:
                from curl_cffi import requests as cffi_requests
            except ImportError:
                return None
            for imp in _IMPERSONATE_FALLBACKS.get(impersonate, (impersonate,)):
                try:
                    sess = cffi_requests.Session(impersonate=imp)
                    _CFFI_SESSIONS[impersonate] = sess
                    return sess
                except Exception:
                    continue
        return None

    if _CFFI_SESSION is None:
        with _SESSION_LOCK:
            if _CFFI_SESSION is None:
                try:
                    from curl_cffi import requests as cffi_requests
                    # Try newest Chrome impersonation, fall back gracefully
                    for imp in ("chrome131", "chrome124", "chrome120", "chrome110"):
                        try:
                            _CFFI_SESSION = cffi_requests.Session(impersonate=imp)
                            break
                        except Exception:
                            continue
                except ImportError:
                    pass
    return _CFFI_SESSION


def _get_cloudscraper():
    global _CLOUD_SCRAPER
    if _CLOUD_SCRAPER is None:
        with _SESSION_LOCK:
            if _CLOUD_SCRAPER is None:
                try:
                    import cloudscraper
                    _CLOUD_SCRAPER = cloudscraper.create_scraper(
                        browser={"browser": "chrome", "platform": "windows", "mobile": False}
                    )
                except ImportError:
                    pass
    return _CLOUD_SCRAPER


def _get_req_session():
    global _REQ_SESSION
    if _REQ_SESSION is None:
        with _SESSION_LOCK:
            if _REQ_SESSION is None:
                import requests
                _REQ_SESSION = requests.Session()
    return _REQ_SESSION


# ══════════════════════════════════════════════════════════════════════════
#  7. curl_cffi — real TLS fingerprint (proxy-aware)
# ══════════════════════════════════════════════════════════════════════════
def try_curl_cffi(
    url: str,
    timeout: int = 25,
    proxy: Optional[str] = None,
    impersonate: Optional[str] = "chrome131",
    extra_headers: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """
    Attempt fetch via curl_cffi browser impersonation.

    Args:
        url:           target URL
        timeout:       request timeout in seconds
        proxy:         optional proxy URL string — passed to curl_cffi if provided
        impersonate:   curl_cffi browser fingerprint (default "chrome131").
                       Other useful values: "safari_ios" for mobile Safari.
        extra_headers: additional headers merged on top of the default browser
                       headers. Useful to force an XHR-style request via
                       _MOBILE_AJAX_HEADERS as a second-pass attempt.
    """
    session = _get_cffi_session(impersonate=impersonate)
    if session is None:
        return None
    try:
        kwargs: dict = dict(timeout=timeout, allow_redirects=True)
        if proxy:
            kwargs["proxies"] = {"http": proxy, "https": proxy}
        if extra_headers:
            domain = urlparse(url).netloc
            headers = get_browser_headers(
                referer=f"https://{domain}/", domain=domain
            )
            headers.update(extra_headers)
            kwargs["headers"] = headers
        resp = session.get(url, **kwargs)
        if resp.status_code == 200:
            return resp.text
    except Exception as exc:
        logger.debug("curl_cffi %s: %s", url, type(exc).__name__)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  8. cloudscraper — JS-challenge bypass
# ══════════════════════════════════════════════════════════════════════════
def try_cloudscraper(url: str, timeout: int = 25) -> Optional[str]:
    scraper = _get_cloudscraper()
    if scraper is None:
        return None
    try:
        resp = scraper.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
    except Exception as exc:
        logger.debug("cloudscraper %s: %s", url, type(exc).__name__)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  8b. ZenRows Web Unlocker — paid last-resort fallback
# ══════════════════════════════════════════════════════════════════════════
def try_web_unlocker(url: str, timeout: int = 30) -> Optional[str]:
    """
    Final paid-proxy fallback via ZenRows Web Unlocker.

    Activated only when the ZENROWS_API_KEY environment variable is set,
    so the call is a silent no-op in dev / self-hosted environments where
    the key isn't configured.
    """
    import os
    import requests
    api_key = os.environ.get("ZENROWS_API_KEY")
    if not api_key:
        return None

    api_url = "https://api.zenrows.com/v1/"
    params = {
        "apikey": api_key,
        "url": url,
        "js_render": "true",
        "premium_proxy": "true",
    }
    try:
        resp = requests.get(api_url, params=params, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
    except Exception as exc:
        logger.debug("zenrows unlocker %s: %s", url, type(exc).__name__)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  9. Full sync fallback chain (called from asyncio executor)
# ══════════════════════════════════════════════════════════════════════════
def try_all_sync_fallbacks(
    url: str,
    timeout: int = 25,
    proxy: Optional[str] = None,
) -> Optional[str]:
    """
    Sync fallback chain (in order):
      1. curl_cffi (Chrome 131 desktop fingerprint)
      2. curl_cffi (Safari iOS fingerprint + XHR/AJAX headers)
      3. cloudscraper (JS-challenge bypass)
      4. requests (rotated browser headers, optional proxy)
      5. ZenRows Web Unlocker (paid, only if ZENROWS_API_KEY is set)

    proxy: optional proxy URL forwarded to curl_cffi and requests. The
    ZenRows step uses its own premium proxy pool and ignores this.
    """
    domain = urlparse(url).netloc

    # Attempt 1: curl_cffi — strongest TLS impersonation (Chrome desktop)
    html = try_curl_cffi(url, timeout=timeout, proxy=proxy)
    if html and not looks_like_bot_challenge(html):
        return html

    # Attempt 2: curl_cffi — Safari iOS + XHR headers. Some stores gate
    # their desktop HTML behind bot-checks but happily return mobile JSON
    # when the request looks like an in-page AJAX call from m.* site.
    html_mobile = try_curl_cffi(
        url,
        timeout=timeout,
        proxy=proxy,
        impersonate="safari_ios",
        extra_headers=_MOBILE_AJAX_HEADERS,
    )
    if html_mobile and not looks_like_bot_challenge(html_mobile):
        return html_mobile

    # Attempt 3: cloudscraper — JS-challenge bypass
    html_cs = try_cloudscraper(url, timeout=timeout)
    if html_cs and not looks_like_bot_challenge(html_cs):
        return html_cs

    # Attempt 4: requests — rotated browser headers, optional proxy
    html_req: Optional[str] = None
    try:
        headers  = get_browser_headers(
            referer=f"https://{domain}/", domain=domain
        )
        session  = _get_req_session()
        req_kwargs: dict = dict(
            headers=headers, timeout=timeout,
            allow_redirects=True, verify=False,
        )
        if proxy:
            req_kwargs["proxies"] = {"http": proxy, "https": proxy}
        resp = session.get(url, **req_kwargs)
        if resp.status_code == 200:
            html_req = resp.text
            if html_req and not looks_like_bot_challenge(html_req):
                return html_req
        elif resp.status_code in (403, 429):
            _ua_rotator.mark_failed(domain, headers.get("User-Agent", ""))
    except Exception as exc:
        logger.debug("requests fallback %s: %s", url, type(exc).__name__)

    # Attempt 5: ZenRows Web Unlocker — paid last-resort when IP is fully
    # banned. Silent no-op if ZENROWS_API_KEY is unset.
    html_wu = try_web_unlocker(url, timeout=max(timeout, 30))
    if html_wu and not looks_like_bot_challenge(html_wu):
        return html_wu

    # Return whatever partial HTML we got rather than None so callers can
    # still try to parse a soft-blocked page.
    return html or html_mobile or html_cs or html_req or html_wu or None


def looks_like_bot_challenge(html: str) -> bool:
    """Detect Cloudflare / DDoS-Guard bot challenge pages."""
    if not html or len(html) < 500:
        return True
    snippets = [
        "just a moment", "checking your browser", "cf-browser-verification",
        "enable javascript", "ddos protection by", "attention required! | cloudflare",
    ]
    head = html[:15000].lower()
    return any(s in head for s in snippets)


# ══════════════════════════════════════════════════════════════════════════
#  10. StealthManager — compatibility shim for older engine callers
# ══════════════════════════════════════════════════════════════════════════
class _StealthManagerCompat:
    """
    Compatibility wrapper used by engines that import stealth_manager.
    All methods delegate to the v3.0 functions above.
    """

    def get_secure_headers(self, referer: str = "", domain: str = "") -> dict:
        return get_browser_headers(referer=referer, domain=domain)

    async def apply_smart_delay(
        self, min_delay: float = 0.5, max_delay: float = 1.5
    ) -> None:
        base = random.uniform(max(0.0, min_delay), max(min_delay, max_delay))
        jitter = base * random.uniform(-BACKOFF_JITTER_FRACTION, BACKOFF_JITTER_FRACTION)
        await asyncio.sleep(max(0.05, base + jitter))

    def is_shadow_banned(self, html: str, status_code: int) -> Tuple[bool, str]:
        if status_code in (403, 429):
            return True, f"http_{status_code}"
        if status_code >= 500:
            return True, f"http_{status_code}"
        text = (html or "").strip().lower()
        is_xml = text.startswith("<?xml") or "<urlset" in text or "<sitemapindex" in text
        is_html = "<html" in text or "<!doctype html" in text
        if is_html and not is_xml:
            markers = (
                "just a moment", "checking your browser",
                "cf-browser-verification", "attention required! | cloudflare",
                "ddos protection by",
            )
            if any(m in text[:20000] for m in markers):
                return True, "bot_challenge"
        return False, ""

    async def dynamic_backoff(self, attempt_number: int = 1) -> None:
        attempt = max(1, int(attempt_number))
        raw = min(30.0, (2 ** attempt))
        jitter = raw * random.uniform(-BACKOFF_JITTER_FRACTION, BACKOFF_JITTER_FRACTION)
        await asyncio.sleep(max(0.5, raw + jitter))


# Module-level singletons used by all engine callers
stealth_manager = _StealthManagerCompat()
