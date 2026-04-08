from src.datawarehouse.snowflakeconn import SnowflakeConnection
from src.databases.postgresconn import PostgreSQLConnection
from configparser import ConfigParser
import os 
from loguru import logger
from dotenv import load_dotenv
import pandas as pd



config = ConfigParser()
config.read("/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/src/datawarehouse/config.ini")



# Extract data from Postgres staging as dataframes
def extract_data_from_postgres(pg_conn):
    try:

        catalog_query = "SELECT * FROM staging.catalog;"
        sales_data_query = "SELECT * FROM staging.sales_data;"
        customers_query = "SELECT * FROM staging.customers;"

        engine = pg_conn.get_engine()

        catalog_df = pd.read_sql(catalog_query, engine)
        logger.info("Catalog data extracted successfully with shape: {}".format(catalog_df.shape))
        sales_data_df = pd.read_sql(sales_data_query, engine)
        logger.info("Sales data extracted successfully with shape: {}".format(sales_data_df.shape))
        customers_df = pd.read_sql(customers_query, engine)
        logger.info("Customers data extracted successfully with shape: {}".format(customers_df.shape))

        logger.info("Data extracted from PostgreSQL successfully and saved as DataFrames")
        return catalog_df, sales_data_df, customers_df
    
    except Exception as e:
        logger.error(f"Error extracting data from PostgreSQL: {e}")
        raise

def ddl_for_snowflake_staging(snowflake_conn):
    try:
        engine = snowflake_conn.get_engine()
        with engine.connect() as connection:
            # Create staging tables in Snowflake
            connection.execute("""
                CREATE SCHEMA IF NOT EXISTS staging;
            """)

            connection.execute("""
                CREATE OR REPLACE TABLE staging.catalog (
                        product_id INT PRIMARY KEY,
                        product_model STRING,
                        product_type STRING,
                        product_price NUMBER(10,2));""")
            
            connection.execute("""
                CREATE OR REPLACE TABLE staging.customers (
                        customer_id INT PRIMARY KEY,
                        first_name STRING NOT NULL,
                        last_name STRING NOT NULL,
                        email STRING NOT NULL,
                        segment STRING NOT NULL
                    );""")
            connection.execute("""
                
                CREATE OR REPLACE TABLE staging.sales_data (
                    id NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
                    product_id INT NOT NULL,
                    customer_id INT NOT NULL,
                    quantity INT NOT NULL,
                    price NUMBER(10,2) NOT NULL,
                    time_stamp TIMESTAMP_NTZ NOT NULL,
                    channel STRING NOT NULL,
                    promo_code STRING NOT NULL,
                    discount_percent NUMBER(5,2) NOT NULL);""")
        # Tables created successfully
        logger.info("Staging tables created in Snowflake successfully")                        
    except Exception as e:
        logger.error(f"Error creating staging tables in Snowflake: {e}")
        raise

def load_data_to_snowflake(snowflake_conn, catalog_df, sales_data_df, customers_df):
    try:
        engine = snowflake_conn.get_engine()

        catalog_df.to_sql("catalog", con=engine, schema="staging", if_exists="replace", index=False)
        logger.info("Catalog data loaded to Snowflake successfully with shape: {}".format(catalog_df.shape))
        sales_data_df.to_sql("sales_data", con=engine, schema="staging", if_exists="replace", index=False)
        logger.info("Sales data loaded to Snowflake successfully with shape: {}".format(sales_data_df.shape))
        customers_df.to_sql("customers", con=engine, schema="staging", if_exists="replace", index=False)
        logger.info("Customers data loaded to Snowflake successfully with shape: {}".format(customers_df.shape))

        logger.info("Data loaded to Snowflake successfully")
    except Exception as e:
        logger.error(f"Error loading data to Snowflake: {e}")
        raise

if __name__ == "__main__":
    try:
        # Initialize connections
        pg_conn = PostgreSQLConnection.get_instance(config=config)
        snowflake_conn = SnowflakeConnection.get_instance()
        # Extract data from PostgreSQL
        catalog_df, sales_data_df, customers_df = extract_data_from_postgres(pg_conn)

        # Create staging tables in Snowflake
        #ddl_for_snowflake_staging(snowflake_conn)

        # Load data to Snowflake staging
        load_data_to_snowflake(snowflake_conn, catalog_df, sales_data_df, customers_df)

    except Exception as e:
        logger.error(f"ETL process failed: {e}")


