"""
SoftCart Analytics Dashboard — Tabbed version with AI chat
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import sqlalchemy
import requests
import sys
import os
from urllib.parse import quote_plus
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from src.databases.postgresconn import PostgreSQLConnection
from configparser import ConfigParser

# ─── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="SoftCart Analytics", page_icon="📊", layout="wide")
st.title("📊 SoftCart Analytics Dashboard")

# ─── Database Connection ─────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    password = quote_plus("akshat123@")
    return sqlalchemy.create_engine(
        f"postgresql+psycopg2://softcart_user:{password}@localhost:5433/softcart_staging"
    )


def run_query(sql):
    with get_engine().connect() as conn:
        return pd.read_sql(sqlalchemy.text(sql), conn)

FASTAPI_URL = "http://localhost:8000"

# ─── Tabs ────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs([
    "📦 Products & Trends",
    "🛍️ Business Insights",
    "🤖 Ask AI"
])

# ─── Tab 1: Product Category Analysis ────────────────────────────────────────
with tab1:
    st.header("Product Category Analysis")

    # Revenue & Quantity by Category
    q1 = run_query("""
        SELECT dp.product_type,
               SUM(fs.product_price * fs.product_quantity) AS total_revenue,
               SUM(fs.product_quantity) AS total_quantity
        FROM analytics.fact_sales fs
        JOIN analytics.dim_product dp ON fs.product_id = dp.product_id
        GROUP BY dp.product_type
        ORDER BY total_revenue DESC
    """)
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(q1, x="product_type", y="total_revenue", title="Revenue by Category",
                     color="product_type", text_auto=",.0f")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(q1, x="product_type", y="total_quantity", title="Quantity by Category",
                     color="product_type", text_auto=",.0f")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Monthly Trends
    st.subheader("Monthly Sales Trends")
    q2 = run_query("""
        SELECT d.calendar_year, d.calendar_month, dp.product_type,
               SUM(fs.product_price * fs.product_quantity) AS total_revenue
        FROM analytics.fact_sales fs
        JOIN analytics.dim_product dp ON fs.product_id = dp.product_id
        JOIN analytics.dim_date d ON fs.date_id = d.date_id
        GROUP BY d.calendar_year, d.calendar_month, dp.product_type
        ORDER BY d.calendar_year, d.calendar_month
    """)
    q2["period"] = pd.to_datetime(q2["calendar_year"].astype(str) + "-" + q2["calendar_month"].astype(str) + "-01")
    fig = px.line(q2, x="period", y="total_revenue", color="product_type",
                  title="Monthly Revenue Trend by Category", markers=True)
    st.plotly_chart(fig, use_container_width=True)

# ─── Tab 2: Business Insights (Customers + Top Products + Channel/Promo) ───
with tab2:
    st.header("Customer Behavior")
    q3 = run_query("""
        SELECT c.customer_id, c.segment,
               COUNT(*) AS order_count,
               SUM(fs.product_price * fs.product_quantity) AS total_spend
        FROM analytics.fact_sales fs
        JOIN analytics.dim_customer c ON fs.customer_id = c.customer_id
        GROUP BY c.customer_id, c.segment
    """)
    q3["buyer_type"] = q3["order_count"].apply(
        lambda n: "One-Time" if n == 1 else ("Repeat (2-5)" if n <= 5 else "Loyal (6+)"))

    col1, col2 = st.columns(2)
    with col1:
        buyer_counts = q3["buyer_type"].value_counts().reset_index()
        buyer_counts.columns = ["buyer_type", "count"]
        fig = px.pie(buyer_counts, names="buyer_type", values="count",
                     title="Buyer Type Distribution", hole=0.4)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        seg = q3.groupby("segment", as_index=False).agg(
            avg_spend=("total_spend", "mean"),
            avg_orders=("order_count", "mean"),
            customers=("customer_id", "count")
        )
        fig = px.bar(seg, x="segment", y="avg_spend", title="Average Spend by Segment",
                     color="segment", text_auto=",.0f")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Top Products
    st.subheader("Top 15 Products by Revenue")
    q4 = run_query("""
        SELECT dp.product_model,
               SUM(fs.product_price * fs.product_quantity) AS total_revenue
        FROM analytics.fact_sales fs
        JOIN analytics.dim_product dp ON fs.product_id = dp.product_id
        GROUP BY dp.product_model
        ORDER BY total_revenue DESC
        LIMIT 15
    """)
    fig = px.bar(q4, x="total_revenue", y="product_model", orientation="h",
                 title="Top 15 Products by Revenue", text_auto=",.0f")
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Channel & Promotion
    st.subheader("Channel & Promotion Impact")
    q5_channel = run_query("""
        SELECT dc.channel_name, SUM(fs.product_price * fs.product_quantity) AS total_revenue
        FROM analytics.fact_sales fs
        JOIN analytics.dim_channel dc ON fs.channel_id = dc.channel_id
        GROUP BY dc.channel_name ORDER BY total_revenue DESC
    """)
    q5_promo = run_query("""
        SELECT dp.promo_code, SUM(fs.product_price * fs.product_quantity) AS total_revenue
        FROM analytics.fact_sales fs
        JOIN analytics.dim_promotion dp ON fs.promo_id = dp.promo_id
        GROUP BY dp.promo_code ORDER BY total_revenue DESC
    """)
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(q5_channel, x="channel_name", y="total_revenue",
                     title="Revenue by Channel", color="channel_name", text_auto=",.0f")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(q5_promo, x="promo_code", y="total_revenue",
                     title="Revenue by Promotion", color="promo_code", text_auto=",.0f")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

# ─── Tab 3: Ask AI (Ollama via FastAPI) ──────────────────────────────────────
with tab3:
    st.header("🤖 Ask Your Data in Plain English")
    st.caption("Powered by Ollama (local LLM) + FastAPI + Snowflake")

    # Example questions
    with st.expander("💡 Example questions"):
        st.markdown("""
        - What are the top 5 products by revenue?
        - Which channel generates the most revenue?
        - Show me monthly sales for 2025
        - What's the average spend by customer segment?
        - Which promotion drove the most orders?
        """)

    question = st.text_input("Ask a question:", placeholder="e.g., What are the top 5 products by revenue?")

    if st.button("🔍 Ask", type="primary") and question:
        with st.spinner("Generating SQL and fetching data..."):
            try:
                response = requests.post(
                    f"{FASTAPI_URL}/ask",
                    json={"question": question},
                    timeout=60
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    # Show generated SQL
                    with st.expander("🔧 Generated SQL"):
                        st.code(result.get("sql", ""), language="sql")
                    
                    # Show results as dataframe
                    data = result.get("data", [])
                    if data:
                        df = pd.DataFrame(data)
                        st.subheader("📊 Results")
                        st.dataframe(df, use_container_width=True)
                        
                        # Auto-chart if numeric columns exist
                        numeric_cols = df.select_dtypes(include="number").columns
                        if len(numeric_cols) > 0 and len(df.columns) >= 2:
                            non_numeric = [c for c in df.columns if c not in numeric_cols]
                            if non_numeric:
                                fig = px.bar(df, x=non_numeric[0], y=numeric_cols[0],
                                           title=f"Visualization")
                                st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No data returned.")
                else:
                    error = response.json()
                    st.error(f"Error: {error.get('detail', error)}")
            except requests.exceptions.ConnectionError:
                st.error("⚠️ Cannot connect to FastAPI. Make sure it's running at " + FASTAPI_URL)
            except Exception as e:
                st.error(f"Error: {e}")



