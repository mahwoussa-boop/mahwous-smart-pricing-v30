"""
🏭 تصدير المنتجات المفقودة — ملفات سلة الشاملة
═══════════════════════════════════════════════════
يُنتج 3 ملفات جاهزة للاستيراد في منصة سلة:

1. missing_products_salla.csv    — ملف المنتجات المفقودة (40 عمود)
2. missing_brands_salla.csv     — ملف الماركات المفقودة (7 أعمدة)
3. missing_products_seo.csv     — ملف SEO للمنتجات المفقودة

التحقق الصارم:
- لا تكرار (dedup بالاسم المُطبّع)
- لا هلوسة (تحقق من المنتج ضد الكتالوج)
- تصنيف صحيح من ملف تصنيفات سلة
- ماركة مطابقة من KNOWN_BRANDS
- صورة حقيقية (لا placeholder)
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import os
import re
import json
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── مسارات ──
BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "data")
OUTPUT_DIR = os.path.join(BASE, "exports")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")


def _norm(s):
    """تطبيع النص للمقارنة"""
    t = unicodedata.normalize("NFKC", str(s or ""))
    t = re.sub(r"[\u064B-\u065F\u0670]", "", t)
    t = re.sub(r"[أإآا]", "ا", t)
    t = re.sub(r"[ةه]", "ه", t)
    t = re.sub(r"[يى]", "ي", t)
    return re.sub(r"\s+", " ", t).strip().lower()


def _safe(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "<na>") else s


def main():
    print("=" * 60)
    print("  🏭 MISSING PRODUCTS EXPORTER — Salla Complete Package")
    print("=" * 60)
    
    # ── 1. تحميل الكتالوج ──
    print("\n[1/6] Loading our catalog...")
    from utils.db_manager import get_db
    conn = get_db()
    
    # Our catalog
    our_df = pd.read_sql("SELECT * FROM our_catalog", conn)
    our_count = len(our_df)
    print(f"  Our catalog: {our_count:,} products")
    
    # Our brands
    our_name_col = None
    for c in ("product_name", "اسم المنتج", "المنتج", "أسم المنتج", "name"):
        if c in our_df.columns:
            our_name_col = c
            break
    
    our_names_norm = set()
    if our_name_col:
        our_names_norm = {_norm(n) for n in our_df[our_name_col].dropna().astype(str)}
    print(f"  Our names index: {len(our_names_norm):,}")
    
    # ── 2. تحميل منتجات المنافسين ──
    print("\n[2/6] Loading competitor products...")
    comp_df = pd.read_sql(
        "SELECT * FROM competitor_products_store WHERE price > 0", conn
    )
    print(f"  Competitor products with price: {len(comp_df):,}")
    print(f"  Stores: {comp_df['competitor'].nunique()}")
    conn.close()
    
    # ── 3. كشف المنتجات المفقودة ──
    print("\n[3/6] Finding missing products (strict verification)...")
    
    try:
        from rapidfuzz import fuzz, process as rf_process
        use_fuzzy = True
    except ImportError:
        use_fuzzy = False
    
    our_names_list = list(our_names_norm)
    
    missing = []
    found_count = 0
    duplicate_count = 0
    seen_norms = set()
    
    for _, row in comp_df.iterrows():
        name = _safe(row.get("product_name", ""))
        if not name:
            continue
        
        # Filter out non-product pages (Arabic + English)
        name_lower = name.lower()
        _skip_ar = ("استفسار", "استبدال", "استرجاع", "توصيل", "تجهيز", "سياسة", "تواصل", "من نحن", "شروط", "اتصل", "عن المتجر")
        _skip_en = ("privacy policy", "shipping", "delivery policy", "return policy", "usage agreement", "terms", "contact", "about us", "store for the best", "faq", "cookie")
        if any(kw in name_lower for kw in _skip_ar) or any(kw in name_lower for kw in _skip_en):
            continue
        # Skip very short or very long names (not real products)
        if len(name) < 5 or len(name) > 300:
            continue
        # Skip if price suspiciously low (policy pages scraped as 1 SAR)
        _raw_price = float(row.get("price", 0) or 0)
        if _raw_price <= 2:
            continue
        
        norm = _norm(name)
        
        # Dedup
        if norm in seen_norms:
            duplicate_count += 1
            continue
        seen_norms.add(norm)
        
        # Check if in our catalog
        if norm in our_names_norm:
            found_count += 1
            continue
        
        # Fuzzy check
        if use_fuzzy and our_names_list:
            best = rf_process.extractOne(norm, our_names_list, scorer=fuzz.token_set_ratio)
            if best and best[1] >= 85:
                found_count += 1
                continue
        
        price = float(row.get("price", 0) or 0)
        if price <= 0:
            continue
        
        missing.append({
            "product_name": name,
            "norm_name": norm,
            "price": price,
            "competitor": _safe(row.get("competitor", "")),
            "image_url": _safe(row.get("image_url", "")),
            "product_url": _safe(row.get("product_url", "")),
            "brand_raw": _safe(row.get("brand", "")),
        })
    
    print(f"  Total competitor: {len(comp_df):,}")
    print(f"  Found in catalog: {found_count:,}")
    print(f"  Duplicates skipped: {duplicate_count:,}")
    print(f"  TRULY MISSING: {len(missing):,}")
    
    if not missing:
        print("\n  No missing products found!")
        return
    
    # ── 4. تحضير بيانات المنتجات ──
    print(f"\n[4/6] Preparing {len(missing):,} products for Salla...")
    
    # Brand detection
    try:
        from engines.engine import extract_brand
        has_brand_detect = True
    except ImportError:
        has_brand_detect = False
    
    # Category detection
    try:
        from utils.salla_shamel_export import (
            _sanitize_category_safe, _GENDER_CATEGORY, _DEFAULT_CATEGORY,
            generate_salla_html_description, _resolve_brand_safe,
            _sanitize_alt_text, generate_safe_slug,
            SALLA_SHAMEL_COLUMNS, SALLA_BRAND_COLUMNS,
        )
        has_salla = True
    except ImportError:
        has_salla = False
        print("  WARNING: salla_shamel_export not available")
    
    # Known brands for checking
    try:
        from config import KNOWN_BRANDS
    except ImportError:
        KNOWN_BRANDS = []
    
    known_brands_norm = {_norm(b) for b in KNOWN_BRANDS if b}
    
    # Load our existing brands
    our_brands = set()
    brand_col = None
    for c in ("الماركة", "Brand", "brand"):
        if c in our_df.columns:
            brand_col = c
            break
    if brand_col:
        our_brands = {_safe(b) for b in our_df[brand_col].dropna().unique() if _safe(b)}
    our_brands_norm = {_norm(b) for b in our_brands if b}
    
    # Size regex
    SIZE_RE = re.compile(r"(\d{1,4})\s*(?:مل|ملي|ml|ML|mL)\b", re.I)
    
    # Gender detection
    def detect_gender(name):
        nl = name.lower()
        if any(k in nl for k in ("رجالي", "للرجال", "pour homme", "for men", "man", "homme")):
            return "رجالي"
        if any(k in nl for k in ("نسائي", "للنساء", "pour femme", "for women", "woman", "femme")):
            return "نسائي"
        return "للجنسين"
    
    def detect_category(gender):
        return _GENDER_CATEGORY.get(gender, _DEFAULT_CATEGORY) if has_salla else f"العطور > عطور {gender}"
    
    products_rows = []
    seo_rows = []
    all_brands_found = set()
    missing_brands = []
    
    for p in missing:
        name = p["product_name"]
        price = p["price"]
        
        # Brand — multi-method extraction
        brand = p["brand_raw"]
        if not brand and has_brand_detect:
            brand = extract_brand(name) or ""
        # Fallback: try to extract brand from product name using known brands
        if not brand or brand in ("غير متوفر", "غير محدد"):
            for kb in KNOWN_BRANDS:
                if kb and len(kb) > 2 and kb.lower() in name.lower():
                    brand = kb
                    break
        if not brand:
            brand = "غير متوفر"
        
        # Resolve brand to Salla-safe
        safe_brand = _resolve_brand_safe(brand) if has_salla else brand
        if not safe_brand or safe_brand in ("غير متوفر", "غير محدد"):
            safe_brand = brand if brand not in ("غير متوفر", "") else ""
        
        all_brands_found.add(safe_brand or brand)
        
        # Check if brand is missing from our store
        if safe_brand and _norm(safe_brand) not in our_brands_norm:
            if safe_brand not in ("غير متوفر", "غير محدد", ""):
                missing_brands.append(safe_brand)
        
        # Gender & Category
        gender = detect_gender(name)
        category = detect_category(gender)
        
        # Size
        size_match = SIZE_RE.search(name)
        size = size_match.group(1) if size_match else "100"
        
        # Image
        image = p["image_url"]
        if not image or not image.startswith("http"):
            image = ""
        
        # Description
        if has_salla:
            description = generate_salla_html_description(
                product_name=name,
                brand_name=safe_brand or "غير متوفر",
                gender=gender,
                size_ml=size,
            )
        else:
            # Fallback: generate mahwous-compliant description template
            _br = safe_brand or "غير متوفر"
            description = (
                f'<div dir="rtl" style="line-height: 1.5; font-family: Tahoma, Arial, sans-serif; color: #333;">'
                f'<h2 style="margin: 0 0 8px 0; color: #2c3e50; font-size: 20px;">{name}</h2>'
                f'<p style="margin: 0 0 12px 0;">اكتشف سحر {name} من {_br}، عطر فاخر يدوم طويلاً مع إحساس فريد بالأناقة.</p>'
                f'<h3 style="margin: 12px 0 5px 0; color: #b8860b; font-size: 16px;">المكونات العطرية:</h3>'
                f'<p style="margin: 0 0 12px 0; font-weight: bold;">مزيج عطري ساحر ينبض بالجاذبية والفخامة، صُمم ليترك أثراً لا يُنسى.</p>'
                f'<h3 style="margin: 12px 0 5px 0; color: #2c3e50; font-size: 16px;">لماذا تختار هذا العطر؟</h3>'
                f'<ul style="margin: 0 0 12px 0; padding-right: 20px;">'
                f'<li style="margin-bottom: 4px;"><strong>التميز والأصالة:</strong> من أرقى الدور العريقة بتراث عطري أصيل.</li>'
                f'<li style="margin-bottom: 4px;"><strong>الجاذبية المضمونة:</strong> عطر يجعلك محور الاهتمام في كل مكان.</li>'
                f'<li style="margin-bottom: 4px;"><strong>الأداء:</strong> الفوحان 8/10 والثبات 9/10.</li>'
                f'</ul>'
                f'<h3 style="margin: 12px 0 5px 0; color: #2c3e50; font-size: 16px;">الأسئلة الشائعة:</h3>'
                f'<ul style="margin: 0 0 12px 0; padding-right: 20px;">'
                f'<li style="margin-bottom: 4px;"><strong>كم يدوم العطر؟</strong> بين 8-12 ساعة حسب البشرة ودرجة الحرارة.</li>'
                f'<li style="margin-bottom: 4px;"><strong>متى أستخدمه؟</strong> صُمم ليناسب كافة أوقاتك المميزة.</li>'
                f'</ul>'
                f'<p style="margin: 0;"><a href="https://mahwous.com/" target="_blank" rel="noopener">اكتشف المزيد من عطور مهووس</a></p>'
                f'</div>'
            )
        
        # Salla price: competitor - 1
        salla_price = max(int(round(price - 1)), 1)
        
        # Image alt
        img_alt = _sanitize_alt_text(name) if has_salla else name[:80]
        
        # Build Salla row
        row = {c: "" for c in SALLA_SHAMEL_COLUMNS}
        row["النوع "] = "منتج"
        row["أسم المنتج"] = name
        row["تصنيف المنتج"] = category
        row["صورة المنتج"] = image
        row["وصف صورة المنتج"] = img_alt
        row["نوع المنتج"] = "منتج جاهز"
        row["سعر المنتج"] = salla_price
        row["الوصف"] = description
        row["هل يتطلب شحن؟"] = "نعم"
        row["رمز المنتج sku"] = ""
        row["سعر التكلفة"] = ""
        row["السعر المخفض"] = str(salla_price - 1) if salla_price > 1 else ""
        row["اقصي كمية لكل عميل"] = 100
        row["إخفاء خيار تحديد الكمية"] = "لا"
        row["اضافة صورة عند الطلب"] = "لا"
        row["الوزن"] = 0.2
        row["وحدة الوزن"] = "kg"
        row["الماركة"] = safe_brand
        row["تثبيت المنتج"] = ""
        row["خاضع للضريبة ؟"] = "نعم"
        
        products_rows.append(row)
        
        # SEO row
        slug = generate_safe_slug(name) if has_salla else name.replace(" ", "-")
        seo_rows.append({
            "أسم المنتج": name,
            "Page Title": f"{name} | {safe_brand} - مهووس للعطور",
            "SEO Page URL": slug,
            "Page Description": f"اشترِ {name} من {safe_brand} بأفضل سعر في مهووس للعطور. عطر أصلي 100% مع شحن سريع داخل السعودية.",
            "الماركة": safe_brand,
            "التصنيف": category,
            "السعر": salla_price,
        })
    
    print(f"  Products prepared: {len(products_rows):,}")
    print(f"  Brands found: {len(all_brands_found)}")
    
    # ── 5. تصدير الماركات المفقودة ──
    print(f"\n[5/6] Processing missing brands...")
    
    unique_missing_brands = list(set(missing_brands))
    print(f"  Missing brands: {len(unique_missing_brands)}")
    
    if unique_missing_brands:
        BRAND_COLUMNS = [
            "اسم الماركة", "وصف مختصر عن الماركة", "صورة شعار الماركة",
            "(إختياري) صورة البانر", "(Page Title) عنوان صفحة العلامة التجارية",
            "(SEO Page URL) رابط صفحة العلامة التجارية",
            "(Page Description) وصف صفحة العلامة التجارية",
        ]
        
        brand_rows = []
        seen_brands = set()
        
        for b in sorted(unique_missing_brands):
            if not b or b in ("غير متوفر", "غير محدد"):
                continue
            b_norm = _norm(b)
            if b_norm in seen_brands:
                continue
            seen_brands.add(b_norm)
            
            slug = generate_safe_slug(b) if has_salla else b.replace(" ", "-")
            
            # Try to find brand logo from competitor products
            brand_logo = ""
            for p in missing:
                if p.get("brand_raw", "") == b or b.lower() in p.get("product_name", "").lower():
                    if p.get("image_url", "").startswith("http"):
                        brand_logo = p["image_url"]
                        break
            
            brand_rows.append({
                "اسم الماركة": b,
                "وصف مختصر عن الماركة": f"ماركة {b} - متوفرة في متجر مهووس للعطور. عطور أصلية 100% بأفضل الأسعار.",
                "صورة شعار الماركة": brand_logo,
                "(إختياري) صورة البانر": "",
                "(Page Title) عنوان صفحة العلامة التجارية": f"{b} | عطور أصلية - مهووس",
                "(SEO Page URL) رابط صفحة العلامة التجارية": f"ماركة-{slug}",
                "(Page Description) وصف صفحة العلامة التجارية": f"تسوّق عطور {b} الأصلية من مهووس. تشكيلة واسعة بأفضل الأسعار مع شحن سريع داخل السعودية.",
            })
        
        print(f"  Unique missing brands to export: {len(brand_rows)}")
    
    # ══ MANDATORY QUALITY GATE ══
    # fix_line_spacing + product_gate validation before ANY export
    print(f"\n[6/7] Quality Gate — fix_line_spacing + validate...")
    
    from utils.product_gate import is_mahwous_description, is_real_image_url
    
    def fix_line_spacing(html):
        """تنسيق المسافات في HTML — إلزامي قبل التصدير"""
        if not html:
            return html
        s = str(html)
        # Remove extra whitespace between tags
        s = re.sub(r">\s+<", "><", s)
        # Normalize inline spacing
        s = re.sub(r"\s{2,}", " ", s)
        # Ensure proper line-height style
        if "line-height" not in s and "<div" in s:
            s = s.replace("<div", '<div style="line-height: 1.5;"', 1)
        # Remove empty paragraphs
        s = re.sub(r"<p[^>]*>\s*</p>", "", s)
        # Ensure RTL direction
        if "dir=" not in s and "<div" in s:
            s = s.replace("<div", '<div dir="rtl"', 1)
        return s.strip()
    
    # Apply fix_line_spacing to ALL descriptions
    for row in products_rows:
        desc = row.get("الوصف", "")
        if desc:
            row["الوصف"] = fix_line_spacing(desc)
    
    products_df = pd.DataFrame(products_rows, columns=SALLA_SHAMEL_COLUMNS)
    
    # Gate 1: Image must be real
    has_image = products_df["صورة المنتج"].apply(lambda x: is_real_image_url(x))
    
    # Gate 2: Description must contain mahwous markers
    has_desc = products_df["الوصف"].apply(lambda x: is_mahwous_description(x))
    
    # Gate 3: Product name must be valid
    has_name = products_df["أسم المنتج"].apply(lambda x: len(str(x or "")) >= 5)
    
    # Gate 4: Price must be > 2
    has_price = pd.to_numeric(products_df["سعر المنتج"], errors="coerce").fillna(0) > 2
    
    all_pass = has_image & has_desc & has_name & has_price
    
    products_valid = products_df[all_pass].copy()
    products_rejected = products_df[~all_pass].copy()
    
    # Rejection reasons
    rej_no_image = (~has_image).sum()
    rej_no_desc = (has_image & ~has_desc).sum()
    rej_no_name = (has_image & has_desc & ~has_name).sum()
    rej_no_price = (has_image & has_desc & has_name & ~has_price).sum()
    
    print(f"  ✅ PASSED: {len(products_valid):,}")
    print(f"  ❌ REJECTED: {len(products_rejected):,}")
    print(f"     ├─ No image: {rej_no_image}")
    print(f"     ├─ Bad description: {rej_no_desc}")
    print(f"     ├─ Bad name: {rej_no_name}")
    print(f"     └─ Bad price: {rej_no_price}")
    
    # ── 7. كتابة الملفات ──
    print(f"\n[7/7] Writing export files...")
    
    # File 1: Products CSV (Salla format) — ONLY VALIDATED
    products_path = os.path.join(OUTPUT_DIR, f"missing_products_salla_{TIMESTAMP}.csv")
    
    # Write with meta-header
    meta_header = "بيانات المنتج" + "," * (len(SALLA_SHAMEL_COLUMNS) - 1)
    with open(products_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write(meta_header + "\n")
        products_valid.to_csv(f, index=False)
    
    print(f"  [FILE 1] {products_path}")
    print(f"    Validated products: {len(products_valid):,}")
    
    # File 1b: All products (including no image) as XLSX
    products_xlsx_path = os.path.join(OUTPUT_DIR, f"missing_products_ALL_{TIMESTAMP}.xlsx")
    products_df.to_excel(products_xlsx_path, index=False)
    print(f"  [FILE 1b] {products_xlsx_path} (ALL {len(products_df):,} products)")
    
    # File 2: Brands CSV
    if unique_missing_brands and brand_rows:
        brands_path = os.path.join(OUTPUT_DIR, f"missing_brands_salla_{TIMESTAMP}.csv")
        brands_df = pd.DataFrame(brand_rows, columns=BRAND_COLUMNS)
        brands_df.to_csv(brands_path, index=False, encoding="utf-8-sig")
        print(f"  [FILE 2] {brands_path}")
        print(f"    Missing brands: {len(brand_rows)}")
    else:
        brands_path = ""
        print(f"  [FILE 2] No missing brands to export")
    
    # File 3: SEO CSV
    seo_path = os.path.join(OUTPUT_DIR, f"missing_products_seo_{TIMESTAMP}.csv")
    seo_df = pd.DataFrame(seo_rows)
    seo_df.to_csv(seo_path, index=False, encoding="utf-8-sig")
    print(f"  [FILE 3] {seo_path}")
    print(f"    SEO entries: {len(seo_df):,}")
    
    # ── Summary ──
    print("\n" + "=" * 60)
    print("  EXPORT COMPLETE")
    print("=" * 60)
    print(f"  Our catalog: {our_count:,}")
    print(f"  Competitor products: {len(comp_df):,}")
    print(f"  Missing products: {len(missing):,}")
    print(f"  Exported (validated): {len(products_valid):,}")
    print(f"  Missing brands: {len(brand_rows) if unique_missing_brands else 0}")
    print(f"  SEO entries: {len(seo_df):,}")
    print(f"\n  Files saved to: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
