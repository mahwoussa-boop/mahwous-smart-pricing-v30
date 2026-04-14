"""
engines/anti_ban.py — Shim (توافق رجعي)
════════════════════════════════════════
المصدر الحقيقي: scrapers/anti_ban.py
هذا الملف مجرد جسر للتوافق مع أي import قديم يستخدم engines.anti_ban
"""
from __future__ import annotations

import os
from selenium import webdriver
from selenium_stealth import stealth

# noinspection PyUnresolvedReferences
from scrapers.anti_ban import (  # noqa: F401, F403
    get_browser_headers,
    get_xml_headers,
    get_rate_limiter,
    fetch_with_retry,
    try_curl_cffi,
    try_cloudscraper,
    try_all_sync_fallbacks,
    AdaptiveRateLimiter,
    _REAL_UA_POOL,
    _ACCEPT_LANGUAGES,
    _ACCEPT_HEADERS,
    _rate_limiter,
)


def get_stealth_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Stealth Chrome driver for anti-bot protected pages.
    Caller must always close it via try/finally -> driver.quit().
    """
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1440,2200")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    chrome_binary = os.environ.get("CHROMIUM_BINARY")
    if chrome_binary:
        options.binary_location = chrome_binary

    driver = webdriver.Chrome(options=options)
    stealth(
        driver,
        languages=["en-US", "en", "ar-SA", "ar"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    return driver

# wildcard للتوافق الكامل
from scrapers.anti_ban import *  # noqa: F401, F403
