"""
pages/scraper_advanced.py — لوحة كشط مهووس v4.5 (Parallel Multi-Store Edition)
══════════════════════════════════════════════════════════════════════════
▸ تشغيل جماعي متوازي لجميع المتاجر بضغطة زر واحدة
▸ عرض فوري للمنتجات أثناء الكشط (Real-time streaming إلى SQLite)
▸ بطاقات منافسين احترافية مع شريط تقدم حي لكل متجر
▸ إدارة كاملة: إضافة / حذف / إعادة ضبط / تخطي
▸ جدول حي يتحدث كل 3 ثوانٍ أثناء الكشط
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

# ── مسارات ─────────────────────────────────────────────────────────────────
_DATA_DIR        = os.environ.get("DATA_DIR", "data")
_COMPETITORS_FILE = os.path.join(_DATA_DIR, "competitors_list.json")
_PROGRESS_FILE   = os.path.join(_DATA_DIR, "scraper_progress.json")
_STATE_FILE      = os.path.join(_DATA_DIR, "scraper_state.json")
_OUTPUT_CSV      = os.path.join(_DATA_DIR, "competitors_latest.csv")

os.makedirs(_DATA_DIR, exist_ok=True)

_STATE_LOCK    = threading.Lock()
_RESULT_LOCK   = threading.Lock()

# ── CSS ─────────────────────────────────────────────────────────────────────
_CSS = """
<style>
.sc-card{
    background:linear-gradient(135deg,#0d1b2a,#0a1520);
    border:1.5px solid #1e3a5f;border-radius:12px;
    padding:16px 18px 12px;margin-bottom:10px;
    transition:border-color .3s,box-shadow .3s;
}
.sc-card:hover{box-shadow:0 4px 18px rgba(79,195,247,.12);}
.sc-card.done   {border-color:#00C853;}
.sc-card.error  {border-color:#FF1744;}
.sc-card.running{border-color:#4fc3f7;animation:pulse 2s infinite;}
.sc-card.pending{border-color:#37474f;}
.sc-card.skipped{border-color:#FFA000;}
@keyframes pulse{0%,100%{box-shadow:none}50%{box-shadow:0 0 14px rgba(79,195,247,.35)}}
.sc-badge{display:inline-flex;align-items:center;gap:4px;padding:3px 12px;
          border-radius:20px;font-size:.72rem;font-weight:700;}
.done-b  {background:rgba(0,200,83,.15);color:#00C853;border:1px solid #00C853;}
.error-b {background:rgba(255,23,68,.15);color:#FF1744;border:1px solid #FF1744;}
.run-b   {background:rgba(79,195,247,.18);color:#4fc3f7;border:1px solid #4fc3f7;}
.pend-b  {background:rgba(96,125,139,.15);color:#90a4ae;border:1px solid #37474f;}
.skip-b  {background:rgba(255,160,0,.15);color:#FFA000;border:1px solid #FFA000;}
.sc-bar-bg{background:#0a1520;border-radius:6px;height:8px;overflow:hidden;margin-top:6px;}
.sc-bar-fill{height:100%;background:linear-gradient(90deg,#4fc3f7,#0091ea);
             border-radius:6px;transition:width .4s ease;}
.sc-meta{font-size:.75rem;color:#78909c;display:flex;gap:12px;flex-wrap:wrap;margin-top:5px;}
.sc-kpi{background:#0d1b2a;border:1px solid #1e3a5f;border-radius:10px;
        padding:12px 16px;text-align:center;flex:1;min-width:100px;}
.sc-kpi .num{font-size:1.8rem;font-weight:900;color:#4fc3f7;}
.sc-kpi .lbl{font-size:.75rem;color:#607d8b;margin-top:2px;}
.product-row{display:flex;align-items:center;gap:10px;padding:6px 10px;
             background:#0d1b2a;border-radius:8px;margin:3px 0;border:1px solid #1e3a5f;}
.product-row img{width:36px;height:36px;object-fit:cover;border-radius:5px;flex-shrink:0;}
.product-row .pname{flex:1;font-size:.82rem;color:#e0e0e0;overflow:hidden;
                    text-overflow:ellipsis;white-space:nowrap;}
.product-row .pprice{font-size:.82rem;font-weight:700;color:#ff9800;white-space:nowrap;}
.product-row .pstore{font-size:.7rem;color:#4fc3f7;white-space:nowrap;}
.live-count{font-size:2.5rem;font-weight:900;color:#00C853;text-align:center;line-height:1;}
</style>
"""

# ── دوال مساعدة ────────────────────────────────────────────────────────────────
def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "").strip() or url
    except Exception:
        return url

def _effective_concurrency() -> int:
    return int(st.session_state.get("sc_concurrency_adv", 6))

def _load_stores() -> list:
    try:
        with open(_COMPETITORS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save_stores(lst: list) -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)
    with _STATE_LOCK:
        with open(_COMPETITORS_FILE, "w", encoding="utf-8") as f:
            json.dump(lst, f, ensure_ascii=False, indent=2)

def _load_progress() -> dict:
    try:
        with open(_PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"running": False}

def _load_state() -> dict:
    try:
        with _STATE_LOCK:
            with open(_STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return {}

def _save_state(s: dict) -> None:
    with _STATE_LOCK:
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(s, f, ensure_ascii=False, indent=2)

def _live_path(domain: str) -> str:
    return os.path.join(_DATA_DIR, f"_sc_live_{domain}.json")

def _read_live(domain: str) -> dict:
    try:
        with open(_live_path(domain), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _result_path(domain: str) -> str:
    return os.path.join(_DATA_DIR, f"_sc_result_{domain}.json")

def _read_result(domain: str) -> dict | None:
    try:
        with _RESULT_LOCK:
            with open(_result_path(domain), encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return None

def _write_result(domain: str, data: dict) -> None:
    with _RESULT_LOCK:
        try:
            with open(_result_path(domain), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception:
            pass

def _reset_store(domain: str) -> None:
    state = _load_state()
    if domain in state:
        state[domain].update({
            "status": "pending", "last_url_index": 0,
            "last_page": 0, "urls_done": 0, "error": "", "finished_at": "",
        })
        _save_state(state)
    for p in [_live_path(domain), _result_path(domain)]:
        try: os.remove(p)
        except: pass

def _get_db_count(domain: str) -> int:
    try:
        from utils.db_manager import get_competitor_products_df
        df = get_competitor_products_df(domain)
        return len(df)
    except: return 0

def _get_all_db_products(domain: str = "", limit: int = 50) -> pd.DataFrame:
    try:
        from utils.db_manager import get_competitor_products_df
        df = get_competitor_products_df(domain)
        if df.empty: return pd.DataFrame()
        return df.tail(limit).iloc[::-1]
    except: return pd.DataFrame()

def _total_db_products() -> dict:
    try:
        from utils.db_manager import get_competitor_store_stats
        return get_competitor_store_stats()
    except: return {"total_products": 0, "by_competitor": {}}

# ── إدارة الخيوط (Threads) ───────────────────────────────────────────────────────
def _run_store_bg(store_url: str, concurrency: int = 6, max_products: int = 0, force: bool = False) -> None:
    domain = _domain(store_url)
    try:
        import sys
        sys.path.insert(0, ".")
        from engines.async_scraper import run_single_store
        result = run_single_store(store_url, concurrency=concurrency, max_products=max_products, force=force)
        _write_result(domain, result)
    except Exception as e:
        _write_result(domain, {"success": False, "rows": 0, "message": str(e)[:300], "domain": domain})

def _launch_store(store_url: str, concurrency: int = 6, max_products: int = 0, force: bool = False) -> None:
    domain = _domain(store_url)
    state = _load_state()
    state[domain] = state.get(domain, {})
    state[domain].update({"status": "running", "store_url": store_url, "domain": domain, "started_at": datetime.now().isoformat()})
    _save_state(state)
    t = threading.Thread(target=_run_store_bg, args=(store_url, concurrency, max_products, force), daemon=True, name=f"scraper-{domain}")
    t.start()
    if "sc_threads" not in st.session_state: st.session_state["sc_threads"] = {}
    st.session_state["sc_threads"][domain] = t

def _is_thread_alive(domain: str) -> bool:
    threads = st.session_state.get("sc_threads", {})
    t = threads.get(domain)
    return bool(t and t.is_alive())

# ── واجهة المستخدم ───────────────────────────────────────────────────────────────
def show_scraper_advanced():
    st.markdown(_CSS, unsafe_allow_html=True)
    st.markdown("## 🕷️ لوحة الكشط المتقدم (Parallel v4.5)")
    
    # إحصاءات عامة
    stats = _total_db_products()
    total_prods = stats.get("total_products", 0)
    by_comp     = stats.get("by_competitor", {})
    
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(f'<div class="sc-kpi"><div class="num">{len(_load_stores())}</div><div class="lbl">متاجر</div></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="sc-kpi"><div class="num">{total_prods:,}</div><div class="lbl">منتجات</div></div>', unsafe_allow_html=True)
    
    tab_main, tab_add, tab_live, tab_settings = st.tabs(["🏪 إدارة المنافسين", "➕ إضافة منافس", "📡 بث مباشر", "⚙️ الإعدادات"])
    
    with tab_main:
        stores = _load_stores()
        if not stores:
            st.info("لم تُضف أي متجر منافس بعد. اذهب لتبويب «إضافة منافس».")
            return

        # أزرار التشغيل الجماعي
        ca, cb, cc = st.columns([2, 2, 4])
        with ca:
            if st.button("🚀 تشغيل جماعي متوازي", type="primary", use_container_width=True, help="تشغيل جميع المتاجر معاً"):
                count = 0
                for s in stores:
                    d = _domain(s)
                    if not _is_thread_alive(d):
                        _launch_store(s, concurrency=_effective_concurrency())
                        count += 1
                if count > 0:
                    st.success(f"✅ تم إطلاق {count} متجر بالتوازي!")
                    time.sleep(1)
                    st.rerun()
        with cb:
            if st.button("⏹️ إيقاف الكل", use_container_width=True):
                try:
                    with open(_PROGRESS_FILE, "w", encoding="utf-8") as f:
                        json.dump({"running": False, "stop_signal": True}, f)
                    st.warning("⚠️ تم إرسال إشارة الإيقاف للجميع")
                    time.sleep(1)
                except: pass
                st.rerun()

        st.markdown("---")
        
        # عرض بطاقات المنافسين
        state = _load_state()
        for store_url in stores:
            domain = _domain(store_url)
            cp     = state.get(domain, {})
            status = cp.get("status", "pending")
            
            if status == "running" and not _is_thread_alive(domain):
                res = _read_result(domain)
                status = "done" if (res and res.get("success")) else "error"
                cp["status"] = status
                state[domain] = cp
                _save_state(state)
            
            live = _read_live(domain)
            db_count = _get_db_count(domain)
            
            icon = {"done": "✅", "error": "❌", "running": "⏳", "pending": "⏸️"}.get(status, "⏸️")
            badge = {"done": "done-b", "error": "error-b", "running": "run-b", "pending": "pend-b"}.get(status, "pend-b")
            sc_cls = status if status in ["done", "error", "running", "pending"] else "pending"
            
            pct = 0
            if status == "running": pct = max(0, min(100, int(live.get("pct", 0))))
            elif status == "done": pct = 100
            
            st.markdown(f"""
            <div class="sc-card {sc_cls}">
                <div style="display:flex;justify-content:space-between;align-items:center">
                    <div style="font-weight:700;color:#e0e0e0">{domain}</div>
                    <div class="sc-badge {badge}">{icon} {status.upper()}</div>
                </div>
                <div class="sc-bar-bg"><div class="sc-bar-fill" style="width:{pct}%"></div></div>
                <div class="sc-meta">
                    <span>📦 {db_count:,} منتج</span>
                    <span>⏱️ {live.get('updated_at', cp.get('last_checkpoint_at', ''))}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # أزرار تحكم لكل بطاقة
            bc1, bc2, bc3 = st.columns(3)
            with bc1:
                if st.button(f"▶️ تشغيل {domain}", key=f"run_{domain}", disabled=(status=="running")):
                    _launch_store(store_url, concurrency=_effective_concurrency())
                    st.rerun()
            with bc2:
                if st.button(f"🔄 تصفير {domain}", key=f"rst_{domain}"):
                    _reset_store(domain)
                    st.rerun()
            with bc3:
                if st.button(f"🗑️ حذف {domain}", key=f"del_{domain}"):
                    stores.remove(store_url)
                    _save_stores(stores)
                    _reset_store(domain)
                    st.rerun()

    with tab_add:
        with st.form("add_store"):
            new_url = st.text_input("🔗 رابط المتجر الجديد")
            submit = st.form_submit_button("✅ إضافة")
            if submit and new_url:
                new_url = new_url.strip().rstrip("/")
                if not new_url.startswith("http"): new_url = "https://" + new_url
                s_list = _load_stores()
                if new_url not in s_list:
                    s_list.append(new_url)
                    _save_stores(s_list)
                    st.success(f"أُضيف {new_url}")
                    st.rerun()

    with tab_live:
        st.markdown("### 📡 المنتجات المكشوطة حديثاً")
        live_df = _get_all_db_products("", limit=100)
        if not live_df.empty:
            st.dataframe(live_df[["product_name", "price", "competitor", "updated_at"]], use_container_width=True)
        else:
            st.info("لا توجد بيانات حالياً.")

    with tab_settings:
        st.slider("التزامن (Concurrency)", 2, 20, value=6, key="sc_concurrency_adv")
        if st.button("🗑️ مسح كل البيانات", type="primary"):
            from utils.db_manager import clear_competitor_store
            clear_competitor_store()
            st.success("تم مسح قاعدة البيانات")
            st.rerun()

if __name__ == "__main__":
    show_scraper_advanced()
