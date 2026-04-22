# -*- coding: utf-8 -*-
"""Trial: build one product, generate export CSV + dry-run Make payload."""
import sys, os, json, io
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from utils import make_helper, salla_shamel_export as sse

OUT_DIR = r"C:/Users/Hp/Downloads/mahwous-smart-pricing-v30-master (4)"
CSV_PATH = os.path.join(OUT_DIR, "test_export_trial.csv")
XLSX_PATH = os.path.join(OUT_DIR, "test_export_trial.xlsx")
JSON_PATH = os.path.join(OUT_DIR, "test_make_payload.json")

IMG = "https://cdn.salla.sa/dEqVm/test-perfume-image-100ml.jpg"

product = {
    "NO": "9001",
    "product_id": "9001",
    "name": "عطر تجريبي مهووس فاخر 100 مل",
    "أسم المنتج": "عطر تجريبي مهووس فاخر 100 مل",
    "المنتج": "عطر تجريبي مهووس فاخر 100 مل",
    "brand": "ديور",
    "الماركة": "ديور",
    "price": 450,
    "السعر": 450,
    "سعر_المنافس": 480,
    "comp_price": 480,
    "image_url": IMG,
    "صورة المنتج": IMG,
    "صورة_المنافس": IMG,
    "sku": "TEST-9001",
    "description": "وصف تجريبي للمنتج",
    "top_notes": "برغموت، ليمون",
    "heart_notes": "ياسمين، ورد",
    "base_notes": "عنبر، مسك",
    "الجنس": "للجنسين",
    "الحجم": "100",
}

# ── (a) Trial export ─────────────────────────────────────
missing_df = pd.DataFrame([product])
salla_df, _ = sse.build_salla_shamel_dataframe(missing_df, our_catalog_df=None, verify_missing=False)
print(f"\n=== EXPORT DF rows: {len(salla_df)} ===")
if not salla_df.empty:
    img_cell = salla_df.iloc[0]["صورة المنتج"]
    print(f"Column name: 'صورة المنتج'")
    print(f"Image cell value: {img_cell!r}")
    print(f"Type: {type(img_cell).__name__}, contains comma: {',' in str(img_cell)}")

n = len(salla_df)
# CSV with UTF-8 BOM for Excel/Salla Arabic compatibility
salla_df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
csv_size = os.path.getsize(CSV_PATH)
print(f"CSV written: {CSV_PATH} ({n} products, {csv_size} bytes, utf-8-sig)")

# XLSX via openpyxl
salla_df.to_excel(XLSX_PATH, index=False, engine="openpyxl")
xlsx_size = os.path.getsize(XLSX_PATH)
print(f"XLSX written: {XLSX_PATH} ({n} products, {xlsx_size} bytes)")

# ── (b) Dry-run Make payload ─────────────────────────────
captured = {}
def fake_post(url, payload):
    captured.setdefault("calls", []).append({"url": url, "payload": payload})
    return {"success": True, "message": "DRY-RUN", "status_code": 200}

make_helper._post_to_webhook = fake_post

# Ensure description passes the gate
from utils.product_gate import _generate_mahwous_description
product["الوصف"] = _generate_mahwous_description(product)
product["description"] = product["الوصف"]

result = make_helper.send_missing_products([product])
print(f"\n=== MAKE DRY-RUN ===")
print(f"Result: {result.get('message')}")
calls = captured.get("calls", [])
print(f"Calls captured: {len(calls)}")
if calls:
    p = calls[0]["payload"]
    item = p["data"][0]
    print(f"Top-level keys: {list(p.keys())}")
    print(f"Item keys: {list(item.keys())}")
    img_field = item.get("صورة المنتج")
    print(f"Image key: 'صورة المنتج'")
    print(f"Image value: {img_field!r}")
    print(f"Type: {type(img_field).__name__}")

with open(JSON_PATH, "w", encoding="utf-8") as f:
    json.dump(calls[0]["payload"] if calls else {}, f, ensure_ascii=False, indent=2)
print(f"JSON written: {JSON_PATH}")
