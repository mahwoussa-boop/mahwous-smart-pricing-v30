#!/usr/bin/env python3
"""
run_advanced_scraper.py — واجهة تشغيل نظام الكشط المتقدم v30
═══════════════════════════════════════════════════════════════
يكشط صفحات المنتجات التي لديها URLs لكن بدون أسعار (price=0).
الاستخدام:
    python run_advanced_scraper.py              # كل المتاجر
    python run_advanced_scraper.py "قولدن سنت"  # متجر محدد
    python run_advanced_scraper.py "" 3000       # كل المتاجر، حد 3000
"""

import asyncio
import sys
import os
from pathlib import Path

# إضافة المسار الحالي
sys.path.insert(0, str(Path(__file__).parent))

from engines.scraper_v30_advanced import run_advanced_price_scraping


async def run_v30_cycle(store_filter: str = "", limit: int = 5000, interval_hours: int = 12):
    """
    Background automation cycle:
    - discover/update competitor products
    - refresh missing prices via advanced scraper
    - repeat every interval
    """
    while True:
        try:
            await run_advanced_price_scraping(store_filter=store_filter, limit=limit)
        except Exception as exc:
            print(f"⚠️ v30 cycle error: {exc}")
        await asyncio.sleep(max(1, int(interval_hours)) * 3600)


def main():
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║     🕷️  نظام الكشط المتقدم v30 - Advanced Scraper        ║
    ║     استخراج الأسعار من متاجر المنافسين بدقة عالية         ║
    ╚════════════════════════════════════════════════════════════╝
    """)

    store_filter = sys.argv[1] if len(sys.argv) > 1 else ""
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    _cycle_mode = str(sys.argv[3]).strip().lower() == "cycle" if len(sys.argv) > 3 else False

    if store_filter:
        print(f"🏪 المتجر المختار: {store_filter}")
    else:
        print("🏪 سيتم كشط جميع المتاجر")

    print(f"📊 الحد الأقصى: {limit} منتج")
    print("\n⏳ جاري بدء الكشط...")
    print("💡 يمكنك إيقاف العملية بـ Ctrl+C\n")

    try:
        if _cycle_mode:
            print("🔁 وضع الدورة التلقائية مفعّل (كل 12 ساعة)")
            asyncio.run(run_v30_cycle(store_filter=store_filter, limit=limit, interval_hours=12))
        else:
            result = asyncio.run(run_advanced_price_scraping(store_filter, limit))
            print(f"\n{result['message']}")
            print(f"   أخطاء: {result.get('errors', 0)}")
    except KeyboardInterrupt:
        print("\n\n⚠️ تم إيقاف الكشط من قبل المستخدم")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ خطأ: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
