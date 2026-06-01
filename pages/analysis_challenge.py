"""
pages/analysis_challenge.py — محرك تحليل وتسعير تلقائي للعطور
═══════════════════════════════════════════════════════════════
واجهة كاملة لتحليل منتجات المنافسين مقارنةً بكتالوج المتجر الأساسي.
المخرجات: 5 ملفات Excel + إحصاءات بصرية شاملة.
"""
from __future__ import annotations

import io
import sys
import os
import traceback
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
import streamlit as st

# إضافة مسار المشروع إلى sys.path
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    from engines.challenge_engine import (
        run_challenge_analysis,
        read_file,
        detect_columns,
        export_to_excel_bytes,
        export_all_to_excel_bytes,
        ChallengeResult,
    )
    _ENGINE_OK = True
except ImportError as e:
    _ENGINE_OK = False
    _ENGINE_ERROR = str(e)

try:
    import plotly.graph_objects as go
    _HAS_PLOTLY = True
except ImportError:
    _HAS_PLOTLY = False

# ─── دوال مساعدة ─────────────────────────────────────────────────────────────

def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", "،")


def _badge(label: str, value: int, color: str) -> str:
    return f"""
    <div style="
        background:{color}22;border:2px solid {color};border-radius:12px;
        padding:16px 20px;text-align:center;margin:6px 0;
    ">
        <div style="font-size:2.2em;font-weight:bold;color:{color};">{_fmt(value)}</div>
        <div style="font-size:0.95em;color:#555;margin-top:4px;">{label}</div>
    </div>
    """


def _section_header(icon: str, title: str, count: int, color: str = "#1a73e8"):
    st.markdown(
        f"""<div style="
            background:linear-gradient(135deg,{color}18,{color}08);
            border-right:5px solid {color};border-radius:10px;
            padding:12px 20px;margin:16px 0 8px 0;
        ">
        <span style="font-size:1.4em;">{icon}</span>
        <span style="font-size:1.2em;font-weight:bold;color:{color};margin-right:8px;">
            {title}
        </span>
        <span style="background:{color};color:white;border-radius:20px;
                     padding:2px 12px;font-size:0.9em;">{_fmt(count)}</span>
        </div>""",
        unsafe_allow_html=True,
    )


def _render_stats(stats: dict, result: ChallengeResult):
    """عرض الإحصاءات الرئيسية."""
    st.markdown("---")
    st.markdown("### 📊 إحصاءات التحليل")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(_badge("📦 منتجات المتجر", stats.get("total_store", 0), "#1a73e8"), unsafe_allow_html=True)
    with c2:
        st.markdown(_badge("🏪 منتجات المنافسين", stats.get("total_competitor", 0), "#6f42c1"), unsafe_allow_html=True)
    with c3:
        st.markdown(_badge("✅ مطابق مؤكد", stats.get("confirmed_match", 0), "#28a745"), unsafe_allow_html=True)
    with c4:
        st.markdown(_badge("⚠️ تحت المراجعة", stats.get("under_review", 0), "#fd7e14"), unsafe_allow_html=True)
    with c5:
        st.markdown(_badge("🔍 مفقود مؤكد", stats.get("confirmed_missing", 0), "#dc3545"), unsafe_allow_html=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown(_badge("🛒 فرص الاستحواذ", stats.get("acquisition_opportunities", 0), "#17a2b8"), unsafe_allow_html=True)
    with col_b:
        st.markdown(_badge("💰 تنبيهات سعرية", stats.get("price_alerts", 0), "#e83e8c"), unsafe_allow_html=True)

    # رسم بياني دائري
    if _HAS_PLOTLY:
        total_classified = (
            stats.get("confirmed_match", 0)
            + stats.get("under_review", 0)
            + stats.get("confirmed_missing", 0)
        )
        if total_classified > 0:
            fig = go.Figure(data=[go.Pie(
                labels=["✅ مطابق مؤكد", "⚠️ تحت المراجعة", "🔍 مفقود مؤكد"],
                values=[
                    stats.get("confirmed_match", 0),
                    stats.get("under_review", 0),
                    stats.get("confirmed_missing", 0),
                ],
                hole=0.4,
                marker=dict(colors=["#28a745", "#fd7e14", "#dc3545"]),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>العدد: %{value}<br>النسبة: %{percent}<extra></extra>",
            )])
            fig.update_layout(
                title="توزيع تصنيف منتجات المنافسين",
                showlegend=True,
                height=360,
                font=dict(family="Cairo, Arial, sans-serif"),
                margin=dict(t=50, b=20, l=20, r=20),
            )
            st.plotly_chart(fig, use_container_width=True)


def _render_preview(df: pd.DataFrame, title: str, icon: str, color: str, key: str):
    """عرض معاينة DataFrame مع خيار التنزيل."""
    if df is None or df.empty:
        st.info(f"لا توجد بيانات في قسم: {icon} {title}")
        return

    _section_header(icon, title, len(df), color)
    cols_to_show = [c for c in df.columns if c not in ("صورة_المنافس", "صورة_منتجنا")]
    st.dataframe(
        df[cols_to_show].head(50),
        use_container_width=True,
        hide_index=True,
    )
    if len(df) > 50:
        st.caption(f"← عرض أول 50 صف من أصل {_fmt(len(df))}")

    excel_bytes = export_to_excel_bytes(df, sheet_name=title[:31])
    st.download_button(
        label=f"⬇️ تنزيل {title} ({_fmt(len(df))} منتج)",
        data=excel_bytes,
        file_name=f"{key}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"dl_{key}",
    
        on_click="ignore"
    )


def _render_column_map_ui(df: pd.DataFrame, prefix: str) -> Dict[str, Optional[str]]:
    """واجهة تخصيص أعمدة الملف."""
    auto = detect_columns(df)
    cols = ["— (لا يوجد)"] + list(df.columns)

    def _sel(label, key, auto_val):
        default = cols.index(auto_val) if auto_val in cols else 0
        return st.selectbox(label, cols, index=default, key=f"{prefix}_{key}")

    with st.expander("⚙️ تخصيص أعمدة الملف (يُكتشف تلقائياً)", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            name = _sel("📛 عمود اسم المنتج", "name", auto.get("name") or "")
            price = _sel("💰 عمود السعر", "price", auto.get("price") or "")
            sku = _sel("🔢 عمود رقم/SKU المنتج", "sku", auto.get("sku") or "")
        with c2:
            img = _sel("🖼️ عمود الصورة", "image", auto.get("image") or "")
            url = _sel("🔗 عمود الرابط", "url", auto.get("url") or "")

    def _none_if_empty(v):
        return None if v == "— (لا يوجد)" or not v else v

    return {
        "name": _none_if_empty(name),
        "price": _none_if_empty(price),
        "sku": _none_if_empty(sku),
        "image": _none_if_empty(img),
        "url": _none_if_empty(url),
    }


# ─── الصفحة الرئيسية ─────────────────────────────────────────────────────────

def render():
    """نقطة الدخول الرئيسية للصفحة."""

    st.markdown("""
    <div style="
        background:linear-gradient(135deg,#1a73e8,#6f42c1);
        color:white;padding:20px 24px;border-radius:14px;margin-bottom:20px;
    ">
        <h2 style="margin:0;font-size:1.6em;">🧬 محرك تحليل وتسعير العطور</h2>
        <p style="margin:6px 0 0 0;opacity:0.9;font-size:0.95em;">
            رفع كتالوج المتجر + ملفات المنافسين → تحليل تلقائي → 5 ملفات نتائج
        </p>
    </div>
    """, unsafe_allow_html=True)

    if not _ENGINE_OK:
        st.error(f"❌ خطأ في تحميل المحرك: {_ENGINE_ERROR}")
        st.code("pip install rapidfuzz openpyxl pandas")
        return

    # ── الشريط الجانبي: رفع الملفات ──────────────────────────────────────
    with st.sidebar:
        st.markdown("## 📁 رفع الملفات")
        st.markdown("---")
        st.markdown("#### 🏪 كتالوج المتجر الأساسي")
        store_file = st.file_uploader(
            "ارفع ملف متجرك (Excel/CSV)",
            type=["xlsx", "xls", "csv"],
            key="challenge_store_file",
            help="هذا هو مرجعك الثابت — كل منتجات المنافسين ستُقارن به",
        )
        st.markdown("---")
        st.markdown("#### 🏆 ملفات المنافسين")
        comp_files = st.file_uploader(
            "ارفع ملف منافس أو أكثر",
            type=["xlsx", "xls", "csv"],
            accept_multiple_files=True,
            key="challenge_comp_files",
            help="يمكن رفع ملفات متعددة لمنافسين مختلفين",
        )
        st.markdown("---")
        st.markdown("#### ⚡ تشغيل التحليل")
        run_btn = st.button(
            "🚀 تشغيل التحليل الآن",
            type="primary",
            use_container_width=True,
            key="challenge_run_btn",
        )

    # ── محتوى الصفحة ─────────────────────────────────────────────────────
    if not store_file and not comp_files:
        st.markdown("""
        <div style="text-align:center;padding:40px;color:#666;">
            <div style="font-size:4em;">📂</div>
            <h3>ابدأ برفع الملفات من القائمة الجانبية</h3>
            <p>1. ارفع ملف كتالوج متجرك (المرجع الثابت)</p>
            <p>2. ارفع ملفات المنافسين (مصادر التحليل)</p>
            <p>3. اضغط "تشغيل التحليل"</p>
        </div>
        """, unsafe_allow_html=True)
        _render_help_guide()
        return

    # ── معاينة الملفات المرفوعة ──────────────────────────────────────────
    store_df = None
    store_col_map = None
    competitor_dfs: Dict[str, pd.DataFrame] = {}

    if store_file:
        with st.expander("🏪 معاينة كتالوج المتجر", expanded=False):
            store_df, err = read_file(store_file)
            if err:
                st.error(f"❌ خطأ في قراءة ملف المتجر: {err}")
                store_df = None
            else:
                st.success(f"✅ تم تحميل الكتالوج: {len(store_df):,} منتج، {len(store_df.columns)} عمود")
                st.dataframe(store_df.head(5), use_container_width=True, hide_index=True)
                store_col_map = _render_column_map_ui(store_df, "store")
                if store_col_map.get("name"):
                    st.info(f"📛 عمود الاسم المُكتشف: **{store_col_map['name']}** | "
                            f"💰 السعر: **{store_col_map.get('price') or 'غير محدد'}** | "
                            f"🔢 SKU: **{store_col_map.get('sku') or 'غير محدد'}**")

    if comp_files:
        with st.expander(f"🏆 معاينة ملفات المنافسين ({len(comp_files)} ملف)", expanded=False):
            for cf in comp_files:
                comp_df, err = read_file(cf)
                if err:
                    st.warning(f"⚠️ {cf.name}: {err}")
                    continue
                # استخدام اسم الملف كمعرف للمنافس
                comp_name = cf.name.replace(".xlsx", "").replace(".xls", "").replace(".csv", "")
                competitor_dfs[comp_name] = comp_df
                st.success(f"✅ {comp_name}: {len(comp_df):,} منتج")
                st.dataframe(comp_df.head(3), use_container_width=True, hide_index=True)

    # ── التحقق قبل التشغيل ──────────────────────────────────────────────
    ready_to_run = (
        run_btn
        and store_df is not None
        and not store_df.empty
        and len(competitor_dfs) > 0
    )

    if run_btn and not ready_to_run:
        if store_df is None or store_df.empty:
            st.error("❌ يرجى رفع ملف كتالوج المتجر أولاً")
        if not competitor_dfs:
            st.error("❌ يرجى رفع ملف منافس واحد على الأقل")
        return

    if not ready_to_run:
        if store_df is not None and not competitor_dfs:
            st.info("📋 ارفع ملف منافس واحد على الأقل ثم اضغط 'تشغيل التحليل'")
        elif competitor_dfs and store_df is None:
            st.info("📋 ارفع ملف كتالوج المتجر أولاً")
        return

    # ── التشغيل ──────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### ⚙️ جارٍ تشغيل التحليل...")

    progress_bar = st.progress(0)
    status_text = st.empty()

    def _on_progress(pct: float, msg: str):
        progress_bar.progress(min(pct, 1.0))
        status_text.text(f"⚙️ {msg}")

    try:
        result: ChallengeResult = run_challenge_analysis(
            store_df=store_df,
            competitor_dfs=competitor_dfs,
            store_col_map=store_col_map,
            progress_callback=_on_progress,
        )
    except Exception as e:
        st.error(f"❌ خطأ أثناء التحليل: {e}")
        with st.expander("تفاصيل الخطأ"):
            st.code(traceback.format_exc())
        return

    progress_bar.progress(1.0)
    status_text.success("✅ اكتمل التحليل!")

    # خطأ في النتيجة
    if "error" in result.stats:
        st.error(f"❌ {result.stats['error']}")
        return

    # ── عرض الإحصاءات ────────────────────────────────────────────────────
    _render_stats(result.stats, result)

    # ── تنزيل جميع النتائج دفعة واحدة ────────────────────────────────────
    st.markdown("---")
    st.markdown("### ⬇️ تنزيل جميع النتائج")

    all_bytes = export_all_to_excel_bytes(result)
    fname = f"نتائج_التحليل_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    st.download_button(
        label=f"📦 تنزيل ملف النتائج الشامل (5 أوراق)",
        data=all_bytes,
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_all",
        type="primary",
    
        on_click="ignore"
    )

    st.markdown("أو قم بتنزيل كل ملف على حدة:")

    # ── عرض النتائج في تبويبات ────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        f"✅ مطابقات ({_fmt(len(result.confirmed_matches))})",
        f"⚠️ مراجعة ({_fmt(len(result.under_review))})",
        f"🔍 مفقودات ({_fmt(len(result.confirmed_missing))})",
        f"🛒 فرص ({_fmt(len(result.acquisition_opportunities))})",
        f"📋 تدقيق ({_fmt(len(result.audit_log))})",
    ])

    with tab1:
        _render_preview(
            result.confirmed_matches,
            "المطابقات المؤكدة", "✅", "#28a745", "confirmed_matches",
        )
        if not result.confirmed_matches.empty and "تنبيه_السعر" in result.confirmed_matches.columns:
            price_alerts = result.confirmed_matches[
                result.confirmed_matches["تنبيه_السعر"].notna()
                & (result.confirmed_matches["تنبيه_السعر"] != "—")
            ]
            if not price_alerts.empty:
                st.markdown("#### 💰 تنبيهات سعرية")
                st.warning(f"وُجد {len(price_alerts)} منتج بفارق سعر > 20%")
                cols_pa = [c for c in ["اسم_منتج_المنافس", "سعر_المنافس", "سعر_متجرنا",
                                        "فرق_السعر", "نسبة_فرق_السعر", "تنبيه_السعر"]
                           if c in price_alerts.columns]
                _pa_display = price_alerts[cols_pa]
                if len(_pa_display) > 500:
                    st.caption(f"⚠️ يعرض أول 500 تنبيه من {len(_pa_display):,}. البيانات كاملة في ملف التصدير.")
                    _pa_display = _pa_display.head(500)
                st.dataframe(_pa_display, use_container_width=True, hide_index=True)

    with tab2:
        _render_preview(
            result.under_review,
            "المنتجات تحت المراجعة", "⚠️", "#fd7e14", "under_review",
        )
        st.info(
            "💡 هذه المنتجات تحتاج مراجعة بشرية — قد تكون مطابقة أو مفقودة. "
            "أسباب الإرسال للمراجعة موضحة في عمود 'سبب_القرار'."
        )

    with tab3:
        _render_preview(
            result.confirmed_missing,
            "المفقودات المؤكدة", "🔍", "#dc3545", "confirmed_missing",
        )

    with tab4:
        _render_preview(
            result.acquisition_opportunities,
            "فرص الاستحواذ", "🛒", "#17a2b8", "acquisition_opportunities",
        )
        st.info(
            "🛒 هذه منتجات تجارية حقيقية (غير عينات أو طواقم) موجودة عند المنافسين "
            "ولا تتوفر في كتالوجنا — فرص إضافة محتملة."
        )

    with tab5:
        _render_preview(
            result.audit_log,
            "سجل التدقيق الكامل", "📋", "#6c757d", "audit_log",
        )
        st.info("📋 هذا السجل يحتوي على قرار كل منتج مع سببه للمراجعة والمتابعة.")

    # ── ملخص نهائي ──────────────────────────────────────────────────────
    st.markdown("---")
    _render_final_summary(result.stats)


def _render_final_summary(stats: dict):
    """ملخص إحصائي نهائي."""
    total = stats.get("total_competitor", 0)
    matched = stats.get("confirmed_match", 0)
    review = stats.get("under_review", 0)
    missing = stats.get("confirmed_missing", 0)
    excl = stats.get("excluded_samples", 0)
    opps = stats.get("acquisition_opportunities", 0)
    alerts = stats.get("price_alerts", 0)

    classified = matched + review + missing
    coverage = round(matched / classified * 100, 1) if classified > 0 else 0

    st.markdown(f"""
    <div style="
        background:#f8f9fa;border:1px solid #dee2e6;border-radius:12px;
        padding:20px 24px;font-family:Cairo,Arial,sans-serif;
    ">
        <h4 style="margin:0 0 12px 0;color:#1a73e8;">📊 ملخص التحليل النهائي</h4>
        <table style="width:100%;border-collapse:collapse;">
            <tr>
                <td style="padding:6px 12px;color:#555;">إجمالي منتجات المنافسين:</td>
                <td style="padding:6px 12px;font-weight:bold;">{_fmt(total)}</td>
                <td style="padding:6px 12px;color:#555;">منتجات المتجر:</td>
                <td style="padding:6px 12px;font-weight:bold;">{_fmt(stats.get('total_store',0))}</td>
            </tr>
            <tr style="background:#f0f8f0;">
                <td style="padding:6px 12px;color:#28a745;">✅ مطابق مؤكد:</td>
                <td style="padding:6px 12px;font-weight:bold;color:#28a745;">{_fmt(matched)}</td>
                <td style="padding:6px 12px;color:#555;">نسبة التغطية:</td>
                <td style="padding:6px 12px;font-weight:bold;">{coverage}%</td>
            </tr>
            <tr>
                <td style="padding:6px 12px;color:#fd7e14;">⚠️ تحت المراجعة:</td>
                <td style="padding:6px 12px;font-weight:bold;color:#fd7e14;">{_fmt(review)}</td>
                <td style="padding:6px 12px;color:#dc3545;">🔍 مفقود مؤكد:</td>
                <td style="padding:6px 12px;font-weight:bold;color:#dc3545;">{_fmt(missing)}</td>
            </tr>
            <tr style="background:#f0f8ff;">
                <td style="padding:6px 12px;color:#17a2b8;">🛒 فرص الاستحواذ:</td>
                <td style="padding:6px 12px;font-weight:bold;color:#17a2b8;">{_fmt(opps)}</td>
                <td style="padding:6px 12px;color:#e83e8c;">💰 تنبيهات سعرية:</td>
                <td style="padding:6px 12px;font-weight:bold;color:#e83e8c;">{_fmt(alerts)}</td>
            </tr>
            <tr>
                <td style="padding:6px 12px;color:#6c757d;">⚪ مستبعد (عينات):</td>
                <td style="padding:6px 12px;font-weight:bold;color:#6c757d;">{_fmt(excl)}</td>
                <td></td><td></td>
            </tr>
        </table>
        <div style="margin-top:12px;padding:10px;background:#fff3cd;border-radius:8px;
                    border-right:4px solid #ffc107;font-size:0.9em;">
            <strong>✅ ضمان صفر فقدان:</strong>
            إجمالي المُصنَّف = {_fmt(classified)} | الإجمالي الأصلي = {_fmt(total)}
            {'✅ صحيح' if classified + excl == total else f'⚠️ فرق = {total - classified - excl}'}
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_help_guide():
    """دليل استخدام سريع."""
    with st.expander("📖 دليل الاستخدام السريع", expanded=True):
        st.markdown("""
        ### كيف يعمل المحرك؟

        **1. رفع الملفات:**
        - **كتالوج المتجر**: ملف Excel أو CSV يحتوي على منتجاتك مع الأسعار
        - **ملفات المنافسين**: ملف أو أكثر من تصدير بيانات المنافسين

        **2. اكتشاف تلقائي للأعمدة:**
        - يكتشف المحرك تلقائياً أعمدة الاسم والسعر والصورة والرابط
        - يمكن تخصيص الأعمدة يدوياً من قسم "تخصيص أعمدة الملف"

        **3. طبقات المطابقة:**
        - تطبيع عربي/إنجليزي ذكي للأسماء
        - معالجة اختلافات التهجئة للماركات والعطور
        - استخراج: الحجم | التركيز | الجنس | نوع المنتج
        - تمييز صارم بين: تستر/عادي | عينة/تجاري | طقم/فردي | إصدارات مختلفة

        **4. التصنيف الثلاثي المحافظ:**
        | التصنيف | المعنى |
        |---------|--------|
        | ✅ مطابق مؤكد | درجة تشابه ≥ 88% + لا تعارض في الحجم/التركيز/الإصدار |
        | ⚠️ تحت المراجعة | درجة 65-87% أو اختلاف في صفة واحدة غير حاسمة |
        | 🔍 مفقود مؤكد | درجة < 65% بعد جميع طبقات التحقق |

        **5. المخرجات:**
        - ✅ المطابقات المؤكدة + تنبيهات سعرية (فرق > 20%)
        - ⚠️ المنتجات تحت المراجعة
        - 🔍 المفقودات المؤكدة
        - 🛒 فرص الاستحواذ (منتجات تجارية حقيقية غير موجودة في كتالوجنا)
        - 📋 سجل التدقيق الكامل

        > **🛡️ ضمان صفر فقدان:** كل منتج يُصنَّف — لا يُحذف أي سجل بصمت.
        """)


# ─── نقطة الدخول من app.py ──────────────────────────────────────────────────

if __name__ == "__main__":
    render()
