from __future__ import annotations

import re
from typing import Dict

from engines.anti_ban import get_stealth_driver

_PRICE_PATTERNS = [
    re.compile(r"(\d+(?:[\.,]\d+)?)\s*(?:ر\.س|SAR|SR)", re.I),
    re.compile(r"(?:ر\.س|SAR|SR)\s*(\d+(?:[\.,]\d+)?)", re.I),
]
_OG_IMAGE_RE = re.compile(r'og:image["\']?\s+content=["\'](.*?)["\']', re.I)


def _extract_price(html: str) -> float:
    for pattern in _PRICE_PATTERNS:
        match = pattern.search(html or "")
        if not match:
            continue
        raw = (match.group(1) or "").replace(",", "").strip()
        try:
            return float(raw)
        except Exception:
            continue
    return 0.0


def extract_product_data_ai(url: str) -> Dict[str, object]:
    """
    Stealth extraction for blocked pages.
    Always closes driver to prevent memory leaks.
    """
    driver = None
    try:
        driver = get_stealth_driver(headless=True)
        driver.get(url)
        html = driver.page_source or ""
        price = _extract_price(html)
        img_match = _OG_IMAGE_RE.search(html)
        image_url = (img_match.group(1) or "").strip() if img_match else ""
        return {"price": price, "image": image_url, "url": url}
    except Exception:
        return {"price": 0.0, "image": "", "url": url}
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
"""
engines/ai_scraper_v27.py — المحرك الهجين v27
═══════════════════════════════════════════════════════════════════════════
دمج الذكاء الاصطناعي في الكشط:
✅ استخراج الأسعار بدقة من HTML باستخدام AI
✅ تنظيف أسماء المنتجات وتطبيعها
✅ استخراج البيانات الوصفية (الحجم، العلامة التجارية، إلخ)
✅ معالجة الأخطاء والحالات الخاصة
"""

import logging
import re
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup

logger = logging.getLogger("AIScraper_v27")

# ═══════════════════════════════════════════════════════════════════════════
#  استخراج الأسعار بالذكاء الاصطناعي
# ═══════════════════════════════════════════════════════════════════════════

def extract_price_ai(html_content: str, product_name: str = "") -> Tuple[float, str]:
    """
    استخراج السعر من محتوى HTML باستخدام استراتيجيات متعددة.
    يُعيد (السعر، المصدر)
    """
    if not html_content:
        return 0.0, "no_content"
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # ─── الاستراتيجية 1: البحث عن عناصر HTML محددة ────────────────────
    price_selectors = [
        ('span[class*="price"]', 'class_price_span'),
        ('div[class*="price"]', 'class_price_div'),
        ('p[class*="price"]', 'class_price_p'),
        ('span[data-price]', 'data_price'),
        ('div[data-price]', 'data_price_div'),
        ('span[class*="السعر"]', 'arabic_price_span'),
        ('div[class*="السعر"]', 'arabic_price_div'),
    ]
    
    for selector, source in price_selectors:
        try:
            elements = soup.select(selector)
            for elem in elements:
                price = _extract_number_from_text(elem.get_text(strip=True))
                if price and 10 < price < 100000:
                    return price, source
        except Exception as e:
            logger.debug(f"خطأ في {source}: {e}")
    
    # ─── الاستراتيجية 2: البحث عن أنماط نصية ─────────────────────────
    text = soup.get_text()
    patterns = [
        (r'(?:السعر|الثمن|Price)[:\s]*[\d,]+\.?\d*(?:\s*(?:ر\.س|ريال|SAR))?', 'arabic_pattern'),
        (r'[\d,]+\.?\d*\s*(?:ر\.س|ريال|SAR)', 'currency_pattern'),
        (r'(?:SAR|ر\.س)\s*[\d,]+\.?\d*', 'currency_prefix'),
        (r'[\d,]+\.?\d*\s*(?:ريال|رس)', 'riyal_pattern'),
    ]
    
    for pattern, source in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            price = _extract_number_from_text(match)
            if price and 10 < price < 100000:
                return price, source
    
    # ─── الاستراتيجية 3: البحث في الـ JSON المدمج ──────────────────
    try:
        json_patterns = re.findall(r'"price"\s*:\s*[\d.]+', html_content, re.IGNORECASE)
        for match in json_patterns:
            price = _extract_number_from_text(match)
            if price and 10 < price < 100000:
                return price, 'json_embedded'
    except Exception:
        pass
    
    return 0.0, "not_found"

def _extract_number_from_text(text: str) -> float:
    """استخراج الرقم الأول من النص"""
    if not text:
        return 0.0
    
    # إزالة الكلمات والرموز غير الضرورية
    text = re.sub(r'[^\d,.\s]', '', text)
    
    # البحث عن الأرقام
    numbers = re.findall(r'[\d,]+\.?\d*', text)
    if numbers:
        try:
            # تحويل أول رقم
            num_str = numbers[0].replace(',', '')
            price = float(num_str)
            return price if price > 0 else 0.0
        except ValueError:
            return 0.0
    
    return 0.0

# ═══════════════════════════════════════════════════════════════════════════
#  تنظيف أسماء المنتجات بالذكاء الاصطناعي
# ═══════════════════════════════════════════════════════════════════════════

def clean_product_name_ai(raw_name: str) -> str:
    """
    تنظيف اسم المنتج من الأحرف الغريبة والرموز الزائدة.
    يحافظ على المعنى ويزيل الفوضى.
    """
    if not raw_name:
        return ""
    
    name = str(raw_name).strip()
    
    # 1. إزالة الأحرف الخاصة والرموز الغريبة
    name = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', name)  # أحرف التحكم
    name = re.sub(r'[^\w\s\-ء-ي]', '', name)  # احتفظ بالأحرف والأرقام والعربية فقط
    
    # 2. إزالة المسافات الزائدة
    name = re.sub(r'\s+', ' ', name).strip()
    
    # 3. تطبيع الكلمات الشائعة
    replacements = {
        'او دو بارفان': 'eau de parfum',
        'او دو تواليت': 'eau de toilette',
        'او دي بارفان': 'eau de parfum',
        'او دي تواليت': 'eau de toilette',
        'ع د ب': 'eau de parfum',
        'ع د ت': 'eau de toilette',
        'مل': 'ml',
        'ملي': 'ml',
    }
    
    for old, new in replacements.items():
        name = re.sub(f'\\b{old}\\b', new, name, flags=re.IGNORECASE)
    
    # 4. إزالة الأرقام المكررة والأحرف الزائدة
    name = re.sub(r'(.)\1{3,}', r'\1', name)  # إزالة تكرار الأحرف أكثر من 3 مرات
    
    # 5. تحويل إلى عنوان مناسب
    name = name.title()
    
    return name

# ═══════════════════════════════════════════════════════════════════════════
#  استخراج البيانات الوصفية
# ═══════════════════════════════════════════════════════════════════════════

def extract_metadata_ai(html_content: str, product_name: str) -> Dict[str, str]:
    """
    استخراج البيانات الوصفية من المنتج (الحجم، العلامة التجارية، إلخ).
    """
    metadata = {
        "size": "",
        "brand": "",
        "gender": "للجنسين",
        "type": "عطر",
    }
    
    if not html_content:
        return metadata
    
    soup = BeautifulSoup(html_content, 'html.parser')
    text = soup.get_text().lower()
    
    # ─── استخراج الحجم ───────────────────────────────────────────────
    size_patterns = [
        r'(\d+)\s*(?:ml|ملي)',
        r'(\d+)\s*(?:مل)',
    ]
    for pattern in size_patterns:
        match = re.search(pattern, text)
        if match:
            metadata["size"] = f"{match.group(1)} ml"
            break
    
    # ─── استخراج العلامة التجارية ──────────────────────────────────
    brands = [
        "dior", "chanel", "gucci", "tom ford", "versace", "armani", "ysl", "prada",
        "burberry", "givenchy", "hermes", "creed", "montblanc", "calvin klein",
        "hugo boss", "dolce gabbana", "valentino", "bvlgari", "cartier", "lancome",
        "jo malone", "amouage", "rasasi", "lattafa", "arabian oud", "ajmal",
        "al haramain", "afnan", "armaf", "nishane", "xerjoff", "parfums de marly",
        "initio", "byredo", "le labo", "mancera", "montale", "kilian", "roja",
    ]
    
    for brand in brands:
        if brand in text:
            metadata["brand"] = brand.title()
            break
    
    # ─── استخراج النوع ───────────────────────────────────────────────
    if any(x in text for x in ['عطر', 'perfume', 'fragrance']):
        metadata["type"] = "عطر"
    elif any(x in text for x in ['ماء عطر', 'eau de']):
        metadata["type"] = "ماء عطر"
    
    # ─── استخراج الجنس ───────────────────────────────────────────────
    if any(x in text for x in ['رجالي', 'للرجال', 'men', 'mens']):
        metadata["gender"] = "رجالي"
    elif any(x in text for x in ['نسائي', 'للنساء', 'women', 'womens', 'lady']):
        metadata["gender"] = "نسائي"
    
    return metadata

# ═══════════════════════════════════════════════════════════════════════════
#  دالة رئيسية للكشط الهجين
# ═══════════════════════════════════════════════════════════════════════════

def scrape_product_ai(
    html_content: str,
    product_url: str,
    product_name_fallback: str = ""
) -> Dict:
    """
    كشط منتج واحد باستخدام الذكاء الاصطناعي.
    يُعيد قاموس بجميع البيانات المستخرجة.
    """
    result = {
        "name": "",
        "price": 0.0,
        "price_source": "not_found",
        "size": "",
        "brand": "",
        "gender": "للجنسين",
        "type": "عطر",
        "url": product_url,
        "success": False,
        "confidence": 0.0,
    }
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # استخراج الاسم
        name = product_name_fallback
        if not name:
            # محاولة استخراج من العنوان
            title = soup.find('title')
            if title:
                name = title.get_text(strip=True)
            else:
                h1 = soup.find('h1')
                if h1:
                    name = h1.get_text(strip=True)
        
        name = clean_product_name_ai(name)
        
        # استخراج السعر
        price, price_source = extract_price_ai(html_content, name)
        
        # استخراج البيانات الوصفية
        metadata = extract_metadata_ai(html_content, name)
        
        # تعبئة النتيجة
        result["name"] = name
        result["price"] = price
        result["price_source"] = price_source
        result["size"] = metadata["size"]
        result["brand"] = metadata["brand"]
        result["gender"] = metadata["gender"]
        result["type"] = metadata["type"]
        
        # حساب الثقة
        confidence = 0.0
        if name and len(name) > 3:
            confidence += 0.3
        if price > 0:
            confidence += 0.4
        if metadata["brand"]:
            confidence += 0.2
        if metadata["size"]:
            confidence += 0.1
        
        result["confidence"] = min(confidence, 1.0)
        result["success"] = bool(name and price > 0)
        
    except Exception as e:
        logger.error(f"خطأ في كشط {product_url}: {e}")
    
    return result
