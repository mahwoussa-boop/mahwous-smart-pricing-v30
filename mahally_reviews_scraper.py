# -*- coding: utf-8 -*-
"""
mahally_reviews_scraper.py — لبنة 1: كاشط تقييمات منصّة «محلي»
=============================================================
التقييمات مُصيَّرة من الخادم على mahally.com/ar/stores/{id}/about/?page=N
لذا: requests + BeautifulSoup فقط — لا Selenium، لا متصفّح.

كل بطاقة تُعطي: {store_id, product, stars, date, text}.
كثير من البطاقات بلا نص (منتج+نجوم فقط) — تُحفظ كلها:
  • النصية  → بنك الأسلوب (لبنة 3)
  • بلا نص  → محرّك التصدّر (لبنة 2): أي منتج تتراكم عليه التقييمات.

قرار قانوني ثابت: كشط صفحات عامة بتأخير محترم (≥2ث)؛ النصوص تُتعلَّم كإشارة أسلوب لا تُنسخ حرفيًا.
"""
import json
import random
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from scrape_compliance import ScrapePolicy, should_stop_after_response

BASE_DIR = Path(__file__).parent
OUT_FILE = BASE_DIR / 'competitor_reviews.json'

POLICY = ScrapePolicy(min_interval_seconds=5.0)

_DATE_RE = re.compile(r'\d{2}/\d{2}/\d{4}')


def _parse_reviews(html, store_id):
    """يستخرج بطاقات التقييم من HTML مُصيَّر. مرتكز على data-testid=rating-component،
    ويصعد للبطاقة الحاوية على img[alt=customer] — فيتجاهل تلقائيًّا تقييم المتجر العام."""
    soup = BeautifulSoup(html, 'html.parser')
    out = []
    for rc in soup.select('span[data-testid="rating-component"]'):
        # اصعد للبطاقة: أول جدّ يحوي صورة العميل (يميّز بطاقة تقييم عن تقييم المتجر الكلي)
        card = rc
        for _ in range(12):
            card = card.parent
            if card is None or card.find('img', alt='customer'):
                break
        if card is None:
            continue
        product, text = '', ''
        for sp in card.find_all('span'):
            c = sp.get('class') or []
            if 'text-black-300' in c and 'font-bold' in c:
                if 'mb-1.5' in c:            # span النص يتميّز بـ mb-1.5
                    text = sp.get_text(strip=True)
                elif not product:            # span المنتج (بلا mb-1.5)
                    product = sp.get_text(strip=True)
        # ملاحظة: نجوم HTML الخام طبقة عرض ثابتة لا التقييم الحقيقي (لا JS) — لا نشحنها.
        # إشارة الرواج للبنة 2 تُبنى على عدد التقييمات × الحداثة (product+date)، لا على قيمة النجوم.
        m = _DATE_RE.search(card.get_text(' ', strip=True))
        date = m.group(0) if m else ''
        if not product and not date:         # ليست بطاقة تقييم حقيقية
            continue
        out.append({'store_id': str(store_id), 'product': product,
                    'date': date, 'text': text})
    return out


def scrape_store(store_id, max_pages=3, delay_range=(5.0, 5.0), timeout=25):
    """يكشط تقييمات متجر واحد من «محلي» عبر ترقيم الصفحات.

    - User-Agent معلن وثابت + فاصل 5 ثوانٍ على الأقل بين الطلبات.
    - فحص robots.txt والتوقف عند 401/403/429 بدلاً من الالتفاف على الحماية.
    - تدقيق تكرار عند الإدخال عبر hash(store|product|text).
    - يتوقّف عند صفحة فارغة أو max_pages أو صفحة بلا جديد.
    يرجع: (reviews, request_log) حيث request_log طوابع زمنية لإثبات التأخير.
    """
    base = f'https://mahally.com/ar/stores/{store_id}/about/'
    seen, reviews, request_log = set(), [], []
    decision = POLICY.check(base)
    if not decision.allowed:
        return reviews, [{'at': datetime.now().isoformat(), 'error': decision.reason}]

    for page in range(1, max_pages + 1):
        params = None if page == 1 else {'page': page}
        POLICY.wait_for_slot(base)
        headers = POLICY.headers()
        t0 = datetime.now()
        try:
            resp = requests.get(base, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            request_log.append({'page': page, 'at': t0.isoformat(), 'error': str(e)})
            break
        request_log.append({'page': page, 'at': t0.isoformat(), 'status': resp.status_code})
        if resp.status_code != 200:
            if should_stop_after_response(resp.status_code):
                request_log[-1]['stopped'] = 'blocked_or_rate_limited'
            break
        cards = _parse_reviews(resp.text, store_id)
        if not cards:                         # صفحة فارغة → توقّف
            break
        new = 0
        for r in cards:
            key = hashlib.md5(f"{r['store_id']}|{r['product']}|{r['text']}".encode('utf-8')).hexdigest()
            if key in seen:
                continue
            seen.add(key)
            reviews.append(r)
            new += 1
        if new == 0:                          # كل البطاقات مكرّرة → توقّف
            break
    return reviews, request_log


def save_reviews(reviews, path=OUT_FILE):
    payload = {'scraped_at': datetime.now().isoformat(),
               'count': len(reviews), 'reviews': reviews}
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return path


def scrape_all(competitors_path, max_pages=3, store_delay=(5.0, 5.0), page_delay=(5.0, 5.0)):
    """يكشط كل متجر له mahally_store_id في قائمة المنافسين، بتدقيق تكرار عالمي وتحمّل أخطاء المتجر."""
    comps = json.loads(Path(competitors_path).read_text(encoding='utf-8'))
    targets = [c for c in comps if c.get('mahally_store_id')]
    all_reviews, seen, logs = [], set(), []
    for i, c in enumerate(targets):
        sid = c['mahally_store_id']
        try:
            revs, req_log = scrape_store(sid, max_pages=max_pages, delay_range=page_delay)
        except Exception as e:
            logs.append({'store': c.get('name'), 'id': sid, 'error': str(e)})
            continue
        added = 0
        for r in revs:
            key = hashlib.md5(f"{r['store_id']}|{r['product']}|{r['text']}".encode('utf-8')).hexdigest()
            if key in seen:
                continue
            seen.add(key)
            all_reviews.append(r)
            added += 1
        logs.append({'store': c.get('name'), 'id': sid, 'scraped': len(revs),
                     'added': added, 'pages': len(req_log)})
    save_reviews(all_reviews)
    return all_reviews, logs


if __name__ == '__main__':
    revs, log = scrape_store('1891860617', max_pages=2)
    save_reviews(revs)
    print(json.dumps({'total': len(revs),
                      'with_text': sum(1 for r in revs if r['text']),
                      'request_log': log}, ensure_ascii=False, indent=2))
    for r in revs[:10]:
        print(r)
