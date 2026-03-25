from src.databases.postgresconn import PostgreSQLConnection
from src.databases.mysqlconn import MySqlConnection
from src.databases.mongoconn import MongoConnection
from src.utility.custom_logger import logger
from urllib.parse import quote_plus
from configparser import ConfigParser
import pandas as pd 
import os

config = ConfigParser()
config_path="/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/resources/config_file.ini"
config.read(config_path)

# Connect to MySQL
def get_mysql():
    try:
        table_names = ["sales_data","customers"]
        mysql_conn = MySqlConnection.get_instance(config=config)
        logger.info(f"Connected to MySQL with object {mysql_conn}")
        mysql_eng = mysql_conn.get_engine()
        logger.info(f"Connected to MySQL with engine {mysql_eng}")
        sales_df = pd.read_sql("SELECT * FROM sales_data",mysql_eng)
        customer_df = pd.read_sql("SELECT * FROM customers",mysql_eng)
        logger.info(f"DF loaded with {len(sales_df)}")
        return sales_df, customer_df
    except Exception as e:
        logger.info(f"Got some error {e}")
# Connect to postgres
def load_mysql_postgres(sales_df, customer_df):
    try:

        postgres_conn = PostgreSQLConnection.get_instance(config=config)
        postgres_eng =postgres_conn.get_engine()
        sales_df.to_sql("sales_data",postgres_eng,schema="staging",if_exists="replace",index=False)
        customer_df.to_sql("customers",postgres_eng,schema="staging",if_exists="replace",index=False)
        logger.info("Sales and customer data loaded....")

    except Exception as e:
        logger.info(f"Got some error{e}")

def get_mongo():
    try:
        client = MongoConnection.get_instance(config=config)
        catalog_df = pd.DataFrame(list(client.mongo_db["catalog"].find()))
        catalog_df['product_price'] = catalog_df['product_price'].astype(float)
        catalog_df.drop(columns="_id",inplace=True)
        return catalog_df
    except Exception as e:
        logger.info(f"Got some error {e}")

def load_mongo_postgres(catalog_df):
    try:
        postgres_conn = PostgreSQLConnection.get_instance(config=config)
        postgres_eng = postgres_conn.get_engine()
        catalog_df.to_sql("catalog",postgres_eng,schema="staging",if_exists="replace",index=False)
        logger.info("Catalog data loaded....")
    except Exception as e:
        logger.info(f"Got some error....{e}")

if __name__ == "__main__":
    try:
        sales_df, customer_df = get_mysql()
        catalog_df = get_mongo()
        load_mysql_postgres(sales_df, customer_df)
        load_mongo_postgres(catalog_df)
    except Exception as e:
        logger.info(f"Got some error {e}")