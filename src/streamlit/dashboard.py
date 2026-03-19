import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
import streamlit as st
import pandas as pd
import psycopg2
from urllib.parse import quote_plus

# Title of the dashboard
st.title("SoftCart Sales Revenue by category")


conn = psycopg2.connect(
    host="localhost",
    port=5433,
    user="softcart_user",
    password="akshat123@",
    database="softcart_staging"
)



# Connect to Postgres and fetch data
try:
    query = """
            SELECT dp.product_type,
                ROUND(SUM(fs.product_price * fs.product_quantity), 2) AS total_revenue
            FROM analytics.fact_sales as fs
            JOIN analytics.dim_product as dp
                ON fs.product_id = dp.product_id
            GROUP BY dp.product_type
            ORDER BY total_revenue DESC;
        """
    revenue_df = pd.read_sql(query, conn)
    st.dataframe(revenue_df)
    st.bar_chart(revenue_df.set_index("product_type"))
except Exception as e:
    st.error(f"Error fetching data: {e}")

