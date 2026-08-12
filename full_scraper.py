# -*- coding: utf-8 -*-
"""
full_scraper.py — كاشط شامل لكل تقييمات العطور والتجميل من محلي
================================================================
يكشط كل المتاجر المعروفة + يكتشف متاجر جديدة من صفحة التصنيف.
يكشط كل الصفحات (بدون حد max_pages).
المخرج: competitor_reviews_full.json
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
OUT_FILE = BASE_DIR / 'competitor_reviews_full.json'
LOG_FILE = BASE_DIR / 'scrape_log.json'

POLICY = ScrapePolicy(min_interval_seconds=5.0)

_DATE_RE = re.compile(r'\d{2}/\d{2}/\d{4}')
_STAR_RE = re.compile(r'(\d+(?:\.\d+)?)\s*(?:من|out of|/)\s*5')

# === المتاجر المعروفة (من الكشط السابق + صفحة جمال وعناية) ===
KNOWN_STORE_IDS = [
    # --- من الكشط السابق (_scrape_summary.json) ---
    '216339537',   # سعيد صلاح
    '1891860617',  # فانيلا
    # --- من صفحة جمال وعناية ---
    '125548687', '1301218167', '1355012420', '1375458544',
    '1566267704', '1612775524', '1696817336', '1892036484',
    '2021340281', '2088520282', '21846792', '286429764',
    '295498147', '305127441', '534522753', '553600259',
    '555275606', '620747497', '650837859', '704523804',
    '773029340', '911239496', '91363561',
]


def _get_headers():
    """رأس معلن وثابت؛ لا ينتحل متصفحاً شخصياً."""
    return POLICY.headers()


def _parse_stars(card_text):
    """محاولة استخراج عدد النجوم من نص البطاقة"""
    m = _STAR_RE.search(card_text)
    if m:
        try:
            return round(float(m.group(1)))
        except ValueError:
            pass
    # عد أيقونات النجوم الممتلئة
    filled = card_text.count('★') + card_text.count('⭐')
    if filled > 0:
        return min(filled, 5)
    return None


def _parse_reviews(html, store_id):
    """يستخرج بطاقات التقييم من HTML"""
    soup = BeautifulSoup(html, 'html.parser')
    out = []
    for rc in soup.select('span[data-testid="rating-component"]'):
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
                if 'mb-1.5' in c:
                    text = sp.get_text(strip=True)
                elif not product:
                    product = sp.get_text(strip=True)

        card_text = card.get_text(' ', strip=True)
        m = _DATE_RE.search(card_text)
        date = m.group(0) if m else ''
        stars = _parse_stars(card_text)

        if not product and not date:
            continue
        entry = {
            'store_id': str(store_id),
            'product': product,
            'date': date,
            'text': text,
        }
        if stars is not None:
            entry['stars'] = stars
        out.append(entry)
    return out


def _get_store_name(html):
    """استخراج اسم المتجر من صفحة about"""
    soup = BeautifulSoup(html, 'html.parser')
    # محاولة 1: عنوان الصفحة
    title = soup.find('title')
    if title:
        t = title.get_text(strip=True)
        # عادة يكون "اسم المتجر | محلي" أو "تقييمات اسم المتجر"
        for sep in ['|', '-', '–']:
            if sep in t:
                return t.split(sep)[0].strip()
        return t
    # محاولة 2: h1
    h1 = soup.find('h1')
    if h1:
        return h1.get_text(strip=True)
    return 'غير معروف'


def scrape_store(store_id, max_pages=50, delay_range=(5.0, 5.0), timeout=30):
    """يكشط صفحات عامة مسموحاً بها فقط، بتوقف فوري عند الرفض أو الحظر."""
    base = f'https://mahally.com/ar/stores/{store_id}/about/'
    seen, reviews, request_log = set(), [], []
    store_name = ''
    empty_streak = 0

    decision = POLICY.check(base)
    if not decision.allowed:
        return reviews, [{'at': datetime.now().isoformat(), 'error': decision.reason}], store_name

    print(f'  ⏳ كشط متجر {store_id}...', end='', flush=True)

    for page in range(1, max_pages + 1):
        POLICY.wait_for_slot(base)

        params = None if page == 1 else {'page': page}
        t0 = datetime.now()
        try:
            resp = requests.get(base, params=params, headers=_get_headers(), timeout=timeout)
        except requests.RequestException as e:
            request_log.append({'page': page, 'at': t0.isoformat(), 'error': str(e)})
            break
        request_log.append({'page': page, 'at': t0.isoformat(), 'status': resp.status_code})

        if resp.status_code != 200:
            if should_stop_after_response(resp.status_code):
                request_log[-1]['stopped'] = 'blocked_or_rate_limited'
            break

        if page == 1:
            store_name = _get_store_name(resp.text)

        cards = _parse_reviews(resp.text, store_id)
        if not cards:
            empty_streak += 1
            if empty_streak >= 2:  # صفحتين فاضيتين متتاليتين = خلاص
                break
            continue
        empty_streak = 0

        new = 0
        for r in cards:
            key = hashlib.md5(
                f"{r['store_id']}|{r['product']}|{r['text']}|{r['date']}".encode('utf-8')
            ).hexdigest()
            if key in seen:
                continue
            seen.add(key)
            r['store_name'] = store_name
            reviews.append(r)
            new += 1

        if new == 0:  # كل البطاقات مكررة
            break

        print(f' ص{page}({new})', end='', flush=True)

    with_text = sum(1 for r in reviews if r.get('text'))
    print(f' ✅ {len(reviews)} تقييم ({with_text} بنص)')
    return reviews, request_log, store_name


def discover_stores_from_browse():
    """اكتشاف متاجر جديدة من صفحة التصفح (جمال وعناية)"""
    discovered = set()
    urls = [
        'https://mahally.com/ar/browse/%D8%AC%D9%85%D8%A7%D9%84-%D9%88%D8%B9%D9%86%D8%A7%D9%8A%D8%A9/',
    ]
    # محاولة كشط عدة صفحات من التصفح
    for page in range(1, 6):
        for url in urls:
            decision = POLICY.check(url)
            if not decision.allowed:
                continue
            try:
                params = {'page': page} if page > 1 else None
                POLICY.wait_for_slot(url)
                resp = requests.get(url, params=params, headers=_get_headers(), timeout=25)
                if resp.status_code != 200:
                    if should_stop_after_response(resp.status_code):
                        break
                    continue
                # استخراج store IDs من الروابط
                store_ids = re.findall(r'/stores/(\d+)', resp.text)
                discovered.update(store_ids)
            except Exception:
                continue
    return list(discovered)


def load_existing():
    """تحميل التقييمات الموجودة سابقاً لتجنب التكرار"""
    existing = []
    for f in [BASE_DIR / 'competitor_reviews.json', OUT_FILE]:
        if f.exists():
            try:
                data = json.loads(f.read_text(encoding='utf-8'))
                if isinstance(data, dict) and 'reviews' in data:
                    existing.extend(data['reviews'])
                elif isinstance(data, list):
                    existing.extend(data)
            except Exception:
                continue
    return existing


def main():
    print('=' * 60)
    print('  🔍 كاشط تقييمات محلي الشامل — العطور والتجميل')
    print('=' * 60)

    # 1. اكتشاف متاجر جديدة
    print('\n📡 اكتشاف المتاجر من صفحة التصفح...')
    discovered = discover_stores_from_browse()
    print(f'   وجدت {len(discovered)} متجر من التصفح')

    # 2. دمج كل المتاجر (معروفة + مكتشفة)
    all_store_ids = list(set(KNOWN_STORE_IDS + discovered))
    print(f'   إجمالي المتاجر: {len(all_store_ids)}')

    # 3. تحميل التقييمات الموجودة لتجنب التكرار
    existing = load_existing()
    existing_keys = set()
    for r in existing:
        key = hashlib.md5(
            f"{r.get('store_id','')}|{r.get('product','')}|{r.get('text','')}|{r.get('date','')}".encode('utf-8')
        ).hexdigest()
        existing_keys.add(key)
    print(f'   تقييمات موجودة سابقاً: {len(existing)}')

    # 4. كشط كل المتاجر
    all_reviews = list(existing)  # نبدأ من الموجود
    logs = []
    total_new = 0

    print(f'\n🚀 بدء الكشط ({len(all_store_ids)} متجر)...\n')

    for i, sid in enumerate(all_store_ids):
        print(f'[{i+1}/{len(all_store_ids)}]', end='')
        try:
            revs, req_log, store_name = scrape_store(sid, max_pages=50)
        except Exception as e:
            print(f'  ❌ خطأ: {e}')
            logs.append({'store_id': sid, 'error': str(e)})
            continue

        added = 0
        for r in revs:
            key = hashlib.md5(
                f"{r.get('store_id','')}|{r.get('product','')}|{r.get('text','')}|{r.get('date','')}".encode('utf-8')
            ).hexdigest()
            if key not in existing_keys:
                existing_keys.add(key)
                all_reviews.append(r)
                added += 1
                total_new += 1

        logs.append({
            'store_id': sid,
            'store_name': store_name,
            'scraped': len(revs),
            'new_added': added,
            'pages': len(req_log),
        })

    # 5. حفظ النتائج
    with_text = sum(1 for r in all_reviews if r.get('text'))
    output = {
        'scraped_at': datetime.now().isoformat(),
        'total_reviews': len(all_reviews),
        'reviews_with_text': with_text,
        'stores_processed': len(logs),
        'new_reviews_added': total_new,
        'reviews': all_reviews,
    }
    OUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')

    # 6. حفظ تقرير الكشط
    log_output = {
        'completed_at': datetime.now().isoformat(),
        'summary': {
            'total_stores': len(all_store_ids),
            'total_reviews': len(all_reviews),
            'with_text': with_text,
            'new_added': total_new,
        },
        'per_store': logs,
    }
    LOG_FILE.write_text(json.dumps(log_output, ensure_ascii=False, indent=2), encoding='utf-8')

    # 7. طباعة الملخص
    print('\n' + '=' * 60)
    print('  ✅ اكتمل الكشط!')
    print('=' * 60)
    print(f'  📊 إجمالي التقييمات: {len(all_reviews)}')
    print(f'  📝 تقييمات بنص: {with_text}')
    print(f'  🆕 تقييمات جديدة: {total_new}')
    print(f'  🏪 متاجر: {len(logs)}')
    print(f'  💾 الملف: {OUT_FILE}')
    print(f'  📋 التقرير: {LOG_FILE}')
    print('=' * 60)

    # طباعة عينة
    text_reviews = [r for r in all_reviews if r.get('text')]
    if text_reviews:
        print('\n--- عينة من التقييمات ---')
        sample = random.sample(text_reviews, min(15, len(text_reviews)))
        for r in sample:
            stars = f"{'⭐' * r.get('stars', 0)} " if r.get('stars') else ''
            print(f'  {stars}[{r.get("store_name", "?")}] {r["text"]}')


if __name__ == '__main__':
    main()
