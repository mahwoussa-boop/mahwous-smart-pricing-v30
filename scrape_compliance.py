# -*- coding: utf-8 -*-
"""حدود امتثال مشتركة لكاشطات البيانات العامة.

هذه الوحدة لا تتجاوز الحماية ولا تتنكر كمتصفح شخصي. تتحقق من ``robots.txt``
قبل الطلب، وتفرض مهلة لكل نطاق، وتتوقف عند أي تعذر في التحقق.
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Dict
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests


@dataclass(frozen=True)
class ScrapeDecision:
    """نتيجة التحقق قبل سحب عنوان ما."""

    allowed: bool
    reason: str


class ScrapePolicy:
    """سياسة محافظة للسحب من الصفحات العامة المصرح بها فقط."""

    def __init__(self, min_interval_seconds: float = 5.0) -> None:
        self.min_interval_seconds = max(5.0, float(min_interval_seconds))
        self.user_agent = self._build_user_agent()
        self._robots: Dict[str, RobotFileParser | bool] = {}
        self._last_request: Dict[str, float] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _build_user_agent() -> str:
        contact = os.getenv("SCRAPER_CONTACT_EMAIL", "").strip()
        suffix = f" (+mailto:{contact})" if contact else ""
        return f"MahwousCatalogResearch/1.0{suffix}"

    def headers(self) -> Dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "Accept-Language": "ar,en-US;q=0.8,en;q=0.6",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    @staticmethod
    def _origin(url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _robots_rules(self, url: str, timeout: int = 10) -> RobotFileParser | bool:
        origin = self._origin(url)
        with self._lock:
            cached = self._robots.get(origin)
        if cached is not None:
            return cached

        robots_url = f"{origin}/robots.txt"
        try:
            response = requests.get(robots_url, headers=self.headers(), timeout=timeout)
        except requests.RequestException:
            rules: RobotFileParser | bool = False
        else:
            if response.status_code == 404:
                rules = True
            elif response.status_code == 200:
                parser = RobotFileParser()
                parser.set_url(robots_url)
                parser.parse(response.text.splitlines())
                rules = parser
            else:
                rules = False

        with self._lock:
            self._robots[origin] = rules
        return rules

    def check(self, url: str) -> ScrapeDecision:
        """اسمح فقط بعناوين HTTP(S) التي يجيزها robots.txt صراحة."""
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return ScrapeDecision(False, "عنوان غير صالح للسحب")

        rules = self._robots_rules(url)
        if rules is False:
            return ScrapeDecision(False, "تعذّر التحقق من robots.txt أو رُفض الوصول")
        if rules is True or rules.can_fetch(self.user_agent, url):
            return ScrapeDecision(True, "مسموح وفق robots.txt")
        return ScrapeDecision(False, "robots.txt لا يجيز سحب هذا المسار")

    def wait_for_slot(self, url: str) -> None:
        """افرض فجوة دنيا بين الطلبات إلى النطاق نفسه."""
        host = urlparse(url).netloc.lower()
        with self._lock:
            previous = self._last_request.get(host)
            now = time.monotonic()
            wait_for = max(0.0, self.min_interval_seconds - (now - previous)) if previous else 0.0
        if wait_for:
            time.sleep(wait_for)
        with self._lock:
            self._last_request[host] = time.monotonic()


def should_stop_after_response(status_code: int) -> bool:
    """الأكواد التي تعني: لا تعاود المحاولة ولا تحاول الالتفاف على الحماية."""
    return status_code in {401, 403, 429}
