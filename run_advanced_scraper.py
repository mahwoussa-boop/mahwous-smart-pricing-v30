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


def main():
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║     🕷️  نظام الكشط المتقدم v30 - Advanced Scraper        ║
    ║     استخراج الأسعار من متاجر المنافسين بدقة عالية         ║
    ╚════════════════════════════════════════════════════════════╝
    """)

    store_filter = sys.argv[1] if len(sys.argv) > 1 else ""
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

    if store_filter:
        print(f"🏪 المتجر المختار: {store_filter}")
    else:
        print("🏪 سيتم كشط جميع المتاجر")

    print(f"📊 الحد الأقصى: {limit} منتج")
    print("\n⏳ جاري بدء الكشط...")
    print("💡 يمكنك إيقاف العملية بـ Ctrl+C\n")

    try:
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
