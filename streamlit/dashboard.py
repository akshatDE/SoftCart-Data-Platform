import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Sales Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("📊 Sales Analytics Dashboard")
st.markdown("---")

# Connect to PostgreSQL
try:
    conn = st.connection("postgresql", type="sql")
    st.success("✅ Connected to PostgreSQL Database")
except Exception as e:
    st.error(f"❌ Database Connection Error: {e}")
    st.stop()

# Check available schemas and tables
@st.cache_data
def get_available_tables():
    """Get list of available tables to help with debugging"""
    try:
        query = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
        """
        return conn.query(query)
    except:
        return None

# Debug: Show available tables
with st.expander("🔧 Available Tables & Schemas"):
    tables_df = get_available_tables()
    if tables_df is not None:
        st.dataframe(tables_df, use_container_width=True)
    else:
        st.warning("Could not fetch table information")

st.markdown("---")
st.header("1️⃣ Revenue vs Quantity by Product Category")

query1 = """
SELECT p.product_type,
    SUM(s.product_price * s.product_quantity) AS total_revenue,
    SUM(s.product_quantity) AS total_quantity
FROM analytics.fact_sales s
JOIN analytics.dim_product p
    ON s.product_id = p.product_id
GROUP BY p.product_type
ORDER BY total_revenue DESC;
"""

try:
    df_revenue = conn.query(query1, ttl=600)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Revenue by Product Category")
        fig_revenue = px.bar(
            df_revenue,
            x="product_type",
            y="total_revenue",
            title="Total Revenue by Product Category",
            labels={"product_type": "Product Category", "total_revenue": "Revenue ($)"},
            color="total_revenue",
            color_continuous_scale="Viridis"
        )
        fig_revenue.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_revenue, use_container_width=True)
    
    with col2:
        st.subheader("Quantity Sold by Product Category")
        fig_quantity = px.bar(
            df_revenue,
            x="product_type",
            y="total_quantity",
            title="Total Quantity Sold by Product Category",
            labels={"product_type": "Product Category", "total_quantity": "Quantity (Units)"},
            color="total_quantity",
            color_continuous_scale="Blues"
        )
        fig_quantity.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_quantity, use_container_width=True)
    
    # Data table
    st.subheader("📋 Detailed View")
    df_revenue_display = df_revenue.copy()
    df_revenue_display["total_revenue"] = df_revenue_display["total_revenue"].apply(lambda x: f"${x:,.2f}")
    df_revenue_display["total_quantity"] = df_revenue_display["total_quantity"].apply(lambda x: f"{int(x):,}")
    st.dataframe(df_revenue_display, use_container_width=True)
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Revenue", f"${df_revenue['total_revenue'].sum():,.2f}")
    with col2:
        st.metric("Total Units Sold", f"{df_revenue['total_quantity'].sum():,.0f}")
    with col3:
        st.metric("Number of Categories", len(df_revenue))
        
except Exception as e:
    st.error(f"❌ Error executing Query 1: {e}")

st.markdown("---")

# Query 2: Sales Trending Over Time
st.header("2️⃣ Sales Trending Over Time")

query2 = """
SELECT d.calendar_year,
    d.calendar_month,
    d.calendar_year::text || '-' || LPAD(d.calendar_month::text, 2, '0') AS year_month,
    p.product_type,
    SUM(s.product_price * s.product_quantity) AS total_revenue
FROM analytics.fact_sales s
JOIN analytics.dim_product p
    ON s.product_id = p.product_id
JOIN analytics.dim_date d
    ON s.date_id = d.date_id
GROUP BY
    d.calendar_year,
    d.calendar_month,
    p.product_type
ORDER BY
    d.calendar_year,
    d.calendar_month,
    total_revenue DESC;
"""

try:
    df_trends = conn.query(query2, ttl=600)
    
    # Create year-month sorting key
    df_trends['sort_date'] = df_trends['calendar_year'].astype(str) + '-' + df_trends['calendar_month'].astype(str).str.zfill(2)
    df_trends = df_trends.sort_values('sort_date')
    
    # Line chart for all categories
    st.subheader("Revenue Trend Over Time - All Categories")
    fig_trend = px.line(
        df_trends,
        x="year_month",
        y="total_revenue",
        color="product_type",
        title="Sales Revenue Trend by Product Category",
        labels={"year_month": "Month", "total_revenue": "Revenue ($)", "product_type": "Category"},
        markers=True,
        hover_data=["total_revenue"]
    )
    fig_trend.update_layout(height=400, hovermode='x unified')
    st.plotly_chart(fig_trend, use_container_width=True)
    
    # Category filter for detailed view
    st.subheader("📈 Analyze Specific Category")
    categories = sorted(df_trends['product_type'].unique())
    selected_category = st.selectbox("Select Product Category:", categories, key="category_select")
    
    df_category = df_trends[df_trends['product_type'] == selected_category]
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_category_line = px.line(
            df_category,
            x="year_month",
            y="total_revenue",
            title=f"Revenue Trend - {selected_category}",
            labels={"year_month": "Month", "total_revenue": "Revenue ($)"},
            markers=True
        )
        fig_category_line.update_layout(height=350)
        st.plotly_chart(fig_category_line, use_container_width=True)
    
    with col2:
        # Growth metrics for selected category
        st.metric(
            f"{selected_category} - Total Revenue",
            f"${df_category['total_revenue'].sum():,.2f}"
        )
        st.metric(
            f"{selected_category} - Avg Monthly Revenue",
            f"${df_category['total_revenue'].mean():,.2f}"
        )
        
        max_revenue = df_category['total_revenue'].max()
        max_month = df_category[df_category['total_revenue'] == max_revenue]['year_month'].values[0]
        st.metric(
            f"{selected_category} - Peak Month",
            f"{max_month} (${max_revenue:,.2f})"
        )
    
    # Detailed data table
    st.subheader("📋 Detailed Trend Data")
    df_trends_display = df_trends[df_trends['product_type'] == selected_category][['year_month', 'product_type', 'total_revenue']].copy()
    df_trends_display['total_revenue'] = df_trends_display['total_revenue'].apply(lambda x: f"${x:,.2f}")
    st.dataframe(df_trends_display, use_container_width=True)
    
except Exception as e:
    st.error(f"❌ Error executing Query 2: {e}")

st.markdown("---")

# Footer
st.markdown("""
<div style='text-align: center; color: gray; margin-top: 20px;'>
    <p>Last Updated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</p>
    <p>📊 SoftCart Sales Analytics Dashboard</p>
</div>
""", unsafe_allow_html=True)
