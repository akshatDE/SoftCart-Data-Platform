from src.databases.postgresconn import PostgreSQLConnection
from src.utility.custom_logger import logger
from configparser import ConfigParser
from sqlalchemy import text
import pandas as pd
import os

config = ConfigParser()

config_path = "/opt/airflow/resources/config_file.ini"
config.read(config_path)

# Set up the connection to the postgres analytics schema
# Truncate the tables in the analytics schema tables:
# Load the dimension tables first and then the fact tables
def load_analytics():
    try:
# Step 1: Set up the connection to the postgres analytics schema
        postgres_conn = PostgreSQLConnection.get_instance(config=config)
        postgres_eng = postgres_conn.get_engine()

        # Truncate the tables in the analytics schema & Load the dimension tables first and then the fact tables

        with postgres_eng.begin() as conn:
            logger.info("Truncating tables...")
            conn.execute(text("TRUNCATE analytics.fact_sales CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_date CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_channel RESTART IDENTITY CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_promotion RESTART IDENTITY CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_customer CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_product CASCADE"))
            logger.info("Tables truncated successfully")

            conn.execute(text("""
            INSERT INTO analytics.dim_channel (channel_name)
            SELECT DISTINCT channel
            FROM staging.sales_data;
            """))
            logger.info("Dim channel loaded successfully")

            conn.execute(text("""
            INSERT INTO analytics.dim_customer (customer_id, first_name, last_name, email, segment)
            SELECT DISTINCT customer_id, first_name, last_name, email, segment
            FROM staging.customers;
            """))
            logger.info("Dim customer loaded successfully")

            conn.execute(text("""
            INSERT INTO analytics.dim_date (date_id, date, day_of_week, calendar_month, calendar_year, weekday_indicator)
            SELECT DISTINCT
                EXTRACT(YEAR FROM time_stamp) * 10000 + EXTRACT(MONTH FROM time_stamp) * 100 + EXTRACT(DAY FROM time_stamp) AS date_id,
                time_stamp::DATE AS date,
                TO_CHAR(time_stamp, 'Day') AS day_of_week,
                EXTRACT(MONTH FROM time_stamp) AS calendar_month,
                EXTRACT(YEAR FROM time_stamp) AS calendar_year,
                CASE WHEN EXTRACT(DOW FROM time_stamp) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS weekday_indicator
            FROM staging.sales_data;
            """))
            logger.info("Dim date loaded successfully")

            conn.execute(text("""
                INSERT INTO analytics.dim_promotion (promo_code, discount_percent)
                SELECT DISTINCT promo_code, discount_percent
                FROM staging.sales_data;
            """))
            logger.info("Dim promotion loaded successfully")

            conn.execute(text("""
                INSERT INTO analytics.dim_product (product_id, product_model, product_type)
                SELECT DISTINCT product_id, product_model, product_type
                FROM staging.catalog;
                """))
            logger.info("Dim product loaded successfully")

            conn.execute(text("""
                INSERT INTO analytics.fact_sales
                (product_id, customer_id, date_id, channel_id, promo_id, product_quantity, product_price)
            SELECT
                sd.product_id,
                sd.customer_id,
                CAST(EXTRACT(YEAR FROM sd.time_stamp) * 10000 + EXTRACT(MONTH FROM sd.time_stamp) * 100 + EXTRACT(DAY FROM sd.time_stamp) AS INT),
                dc.channel_id,
                dp.promo_id,
                sd.quantity,
                sd.price
            FROM staging.sales_data sd
            JOIN analytics.dim_channel dc
                ON sd.channel = dc.channel_name
            JOIN analytics.dim_promotion dp
                ON sd.promo_code = dp.promo_code
                AND sd.discount_percent = dp.discount_percent;
                """))
            logger.info("Fact sales loaded successfully")
            conn.close()
    except Exception as e:
        logger.error(f"Error loading analytics data: {e}")
        raise e


