"""
pages/product_cards.py — Visual Product Grid (v1.0)
═══════════════════════════════════════════════════
صفحة بديلة لعرض منتجات المنافسين ككروت مرئية مع صورة + اسم + سعر
بدل الجدول. تقرأ من نفس قاعدة البيانات `competitor_products_store`.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

st.set_page_config(page_title="🎴 بطاقات المنتجات", page_icon="🎴", layout="wide")
st.title("🎴 بطاقات المنتجات المرئية")

try:
    from utils.db_manager import get_db
except Exception as e:
    st.error(f"تعذّر تحميل قاعدة البيانات: {e}")
    st.stop()


@st.cache_data(ttl=60)
def load_products(store: str = "", limit: int = 200, only_priced: bool = True) -> pd.DataFrame:
    conn = get_db()
    try:
        where = ["product_url != ''"]
        params: list = []
        if store:
            where.append("competitor = ?")
            params.append(store)
        if only_priced:
            where.append("(price IS NOT NULL AND price > 0)")
        sql = (
            "SELECT competitor, product_name, price, product_url, image_url "
            "FROM competitor_products_store "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY rowid DESC LIMIT ?"
        )
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=["competitor", "product_name", "price", "product_url", "image_url"])


# ─── Filters ───────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    try:
        conn = get_db()
        stores = [r[0] for r in conn.execute(
            "SELECT DISTINCT competitor FROM competitor_products_store ORDER BY competitor"
        ).fetchall()]
        conn.close()
    except Exception:
        stores = []
    store_filter = col1.selectbox("🏪 المتجر", [""] + stores)
with col2:
    limit = col2.slider("عدد العرض", 12, 600, 60, step=12)
with col3:
    only_priced = col3.toggle("أسعار فقط", value=True)

df = load_products(store_filter, limit, only_priced)

if df.empty:
    st.info("لا توجد منتجات مطابقة.")
    st.stop()

st.caption(f"✨ يعرض {len(df)} منتج")

# ─── Grid: 4 cards per row ────────────────────────────────────────────────
CARDS_PER_ROW = 4
for i in range(0, len(df), CARDS_PER_ROW):
    cols = st.columns(CARDS_PER_ROW)
    for j, col in enumerate(cols):
        idx = i + j
        if idx >= len(df):
            break
        row = df.iloc[idx]
        with col:
            with st.container(border=True):
                img = row.get("image_url") or ""
                if isinstance(img, str) and img.startswith(("http", "//")):
                    try:
                        st.image(img, use_container_width=True)
                    except Exception:
                        st.caption("🖼 صورة غير متاحة")
                else:
                    st.caption("🖼 بدون صورة")
                name = str(row["product_name"])[:70]
                st.markdown(f"**{name}**")
                price = float(row["price"]) if row["price"] else 0.0
                st.markdown(f"💰 **{price:,.2f}** ر.س")
                st.caption(f"🏪 {row['competitor']}")
                url = row.get("product_url") or ""
                if url:
                    st.link_button("🔗 فتح المنتج", url, use_container_width=True)
