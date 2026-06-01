@echo off
chcp 65001 >nul 2>&1
title نظام التسعير الذكي - مهووس v30
color 0A

echo.
echo  ╔══════════════════════════════════════════════════╗
echo  ║     نظام التسعير الذكي - مهووس v30              ║
echo  ║     Mahwous Smart Pricing System                 ║
echo  ╚══════════════════════════════════════════════════╝
echo.

:: ── Check Python ──
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python غير مثبت! يرجى تثبيت Python 3.10+ من python.org
    pause
    exit /b 1
)

:: ── Change to script directory ──
cd /d "%~dp0"

:: ── Load .env if exists ──
if exist ".env" (
    echo [INFO] تحميل الإعدادات من .env ...
    for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
        set "line=%%a"
        if not "!line:~0,1!"=="#" (
            set "%%a=%%b"
        )
    )
)

:: ── Check/Install requirements ──
echo [INFO] التحقق من المتطلبات ...
pip show streamlit >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [INFO] تثبيت المتطلبات لأول مرة ...
    pip install -r requirements.txt
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] فشل تثبيت المتطلبات!
        pause
        exit /b 1
    )
)

:: ── Create data directory ──
if not exist "data" mkdir data

echo.
echo  ══════════════════════════════════════════════════
echo   تشغيل النظام ... سيفتح المتصفح تلقائياً
echo  ══════════════════════════════════════════════════
echo.

:: ── Run Streamlit ──
streamlit run app.py --server.port 8501 --server.headless false --browser.gatherUsageStats false

pause
