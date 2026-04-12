from fastapi import FastAPI
from src.databases.mysqlconn import MySqlConnection
from src.datawarehouse.snowflakeconn import SnowflakeConnection
from configparser import ConfigParser
from loguru import logger
import pandas as pd 
import os

config = ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "resources", "config_file.ini")
config.read(config_path)

app = FastAPI()

def run_query(query: str):
    snowflake_conn = SnowflakeConnection.get_instance()
    engine = snowflake_conn.get_engine()
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/revenue/by-category")
async def revenue_by_category():

    query = """
            SELECT dp.product_type,
                SUM(fs.product_price * fs.product_quantity) AS total_revenue
            FROM analytics.fact_sales fs
            JOIN analytics.dim_product dp ON fs.product_id = dp.product_id
            GROUP BY dp.product_type
            ORDER BY total_revenue DESC;

            """
    return run_query(query=query)

@app.get("/trends/monthly")
async def trends_monthly():
    # Q2 - Monthly revenue by category
    query = """
        SELECT d.calendar_year, d.calendar_month, dp.product_type,
               SUM(fs.product_price * fs.product_quantity) AS total_revenue
        FROM analytics.fact_sales fs
        JOIN analytics.dim_product dp ON fs.product_id = dp.product_id
        JOIN analytics.dim_date d ON fs.date_id = d.date_id
        GROUP BY d.calendar_year, d.calendar_month, dp.product_type
        ORDER BY d.calendar_year, d.calendar_month;
    """
    return run_query(query=query)


@app.get("/customers/behavior")
async def customer_behavior():
    # Q3 - Customer purchasing behavior
    query = """
        SELECT c.customer_id, c.segment,
               COUNT(*) AS order_count,
               SUM(fs.product_price * fs.product_quantity) AS total_spend
        FROM analytics.fact_sales fs
        JOIN analytics.dim_customer c ON fs.customer_id = c.customer_id
        GROUP BY c.customer_id, c.segment;
    """
    return run_query(query=query)


@app.get("/revenue/concentration")
async def revenue_concentration():
    # Q4 - Top 15 products by revenue
    query = """
        SELECT dp.product_model,
               SUM(fs.product_price * fs.product_quantity) AS total_revenue
        FROM analytics.fact_sales fs
        JOIN analytics.dim_product dp ON fs.product_id = dp.product_id
        GROUP BY dp.product_model
        ORDER BY total_revenue DESC
        LIMIT 15;
    """
    return run_query(query=query)

@app.get("/channels/promotions")
async def channels_promotions():
    # Q5 - Channel and promo impact
    query = """
        SELECT dc.channel_name, dp.promo_code,
               SUM(fs.product_price * fs.product_quantity) AS total_revenue,
               SUM(fs.product_quantity) AS total_quantity
        FROM analytics.fact_sales fs
        JOIN analytics.dim_channel dc ON fs.channel_id = dc.channel_id
        JOIN analytics.dim_promotion dp ON fs.promo_id = dp.promo_id
        GROUP BY dc.channel_name, dp.promo_code;
    """
    return run_query(query=query)

