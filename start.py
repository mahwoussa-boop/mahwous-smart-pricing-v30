#!/usr/bin/env python3
"""
start.py — Smart Launcher for Mahwous Smart Pricing v30
═══════════════════════════════════════════════════════
يفحص البيئة، يثبت المتطلبات الناقصة، ويشغل Streamlit.
Usage:
    python start.py              # تشغيل عادي
    python start.py --check      # فحص فقط بدون تشغيل
    python start.py --port 8502  # تشغيل على منفذ مختلف
"""
import os
import sys
import subprocess
import importlib
from pathlib import Path

# ── تحميل .env ──────────────────────────────────────────────────────────────
def load_dotenv_manual():
    """تحميل .env يدوياً بدون مكتبة خارجية."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return False
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and not key.startswith("#"):
                    os.environ.setdefault(key, value)
    return True


def check_python_version():
    """التحقق من إصدار Python."""
    v = sys.version_info
    if v.major < 3 or (v.major == 3 and v.minor < 9):
        print(f"❌ Python {v.major}.{v.minor} غير مدعوم — يرجى استخدام Python 3.9+")
        return False
    print(f"✅ Python {v.major}.{v.minor}.{v.micro}")
    return True


def check_requirements():
    """التحقق من المكتبات الأساسية."""
    required = {
        "streamlit": "streamlit",
        "pandas": "pandas",
        "rapidfuzz": "rapidfuzz",
        "google.generativeai": "google-generativeai",
        "requests": "requests",
        "aiohttp": "aiohttp",
        "bs4": "beautifulsoup4",
        "openpyxl": "openpyxl",
    }
    missing = []
    for module, pip_name in required.items():
        try:
            importlib.import_module(module)
        except ImportError:
            missing.append(pip_name)

    if missing:
        print(f"⚠️ مكتبات ناقصة: {', '.join(missing)}")
        return missing
    print("✅ جميع المكتبات الأساسية مثبتة")
    return []


def install_requirements():
    """تثبيت المتطلبات من requirements.txt."""
    req_file = Path(__file__).parent / "requirements.txt"
    if not req_file.exists():
        print("❌ ملف requirements.txt غير موجود!")
        return False
    print("📦 تثبيت المتطلبات ...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req_file)],
        capture_output=False,
    )
    return result.returncode == 0


def check_gemini_key():
    """التحقق من مفتاح Gemini API."""
    key = os.environ.get("GEMINI_API_KEY", "")
    if key and len(key) > 20:
        print(f"✅ Gemini API Key: {key[:12]}...")
        return True
    # محاولة من streamlit secrets
    try:
        secrets_path = Path(__file__).parent / ".streamlit" / "secrets.toml"
        if secrets_path.exists():
            with open(secrets_path, encoding="utf-8") as f:
                for line in f:
                    if "GEMINI_API_KEY" in line and "=" in line:
                        print("✅ Gemini API Key: موجود في secrets.toml")
                        return True
    except Exception:
        pass
    print("⚠️ مفتاح Gemini API غير موجود — أضفه في .env أو secrets.toml")
    return False


def check_data_dir():
    """التحقق من مجلد البيانات."""
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    print(f"✅ مجلد البيانات: {data_dir}")
    return True


def run_streamlit(port=8501):
    """تشغيل Streamlit."""
    app_path = Path(__file__).parent / "app.py"
    if not app_path.exists():
        print("❌ ملف app.py غير موجود!")
        return False

    print()
    print("═" * 50)
    print("  🚀 تشغيل نظام التسعير الذكي - مهووس v30")
    print(f"  📍 العنوان: http://localhost:{port}")
    print("═" * 50)
    print()

    cmd = [
        sys.executable, "-m", "streamlit", "run", str(app_path),
        "--server.port", str(port),
        "--server.headless", "false",
        "--browser.gatherUsageStats", "false",
    ]
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\n✅ تم إيقاف النظام.")
    return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Mahwous Smart Pricing Launcher")
    parser.add_argument("--check", action="store_true", help="فحص فقط بدون تشغيل")
    parser.add_argument("--port", type=int, default=8501, help="منفذ التشغيل")
    args = parser.parse_args()

    print()
    print("🧪 نظام التسعير الذكي - مهووس v30")
    print("=" * 40)
    print()

    # تحميل .env
    if load_dotenv_manual():
        print("✅ ملف .env محمّل")
    else:
        print("⚠️ ملف .env غير موجود — سيستخدم الإعدادات الافتراضية")

    # فحوصات
    ok = True
    ok = check_python_version() and ok
    check_data_dir()
    check_gemini_key()

    # التحقق من المكتبات
    missing = check_requirements()
    if missing:
        print()
        resp = input("هل تريد تثبيت المكتبات الناقصة؟ (y/n): ").strip().lower()
        if resp in ("y", "yes", "نعم"):
            if not install_requirements():
                print("❌ فشل التثبيت!")
                sys.exit(1)
            # إعادة التحقق
            missing = check_requirements()
            if missing:
                print(f"❌ لا تزال هناك مكتبات ناقصة: {missing}")
                sys.exit(1)
        elif not args.check:
            print("⚠️ قد لا يعمل النظام بدون المكتبات المطلوبة")

    if args.check:
        print()
        print("✅ الفحص اكتمل" if ok else "⚠️ الفحص كشف مشاكل")
        return

    # تشغيل
    print()
    run_streamlit(port=args.port)


if __name__ == "__main__":
    main()
