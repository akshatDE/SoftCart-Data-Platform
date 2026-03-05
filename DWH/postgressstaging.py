from databases.postgresconn import PostgreSQLConnection
from databases.mysqlconn import MySqlConnection
from databases.mongoconn import MongoConnection
from urllib.parse import quote_plus
from configparser import ConfigParser
from loguru import logger
import pandas as pd 


config = ConfigParser()
path = "/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCart-Data-Platform/resources/config_file.ini"
config.read(path)


# Connect to MySQL
def get_mysql():
    try:

        mysql_conn = MySqlConnection(config=config)
        mysql_eng = mysql_conn.get_engine()
        sales_df = pd.read_sql("SELECT * FROM sales_data",mysql_eng)
        logger.info(f"DF loaded with {len(sales_df)}")
        return sales_df
    except Exception as e:
        logger.info(f"Got some error {e}")
# Connect to postgres
def load_mysql_postgres(sales_df):
    try:

        postgres_conn = PostgreSQLConnection(config=config)
        postgres_eng = postgres_conn.get_engine()
        sales_df.to_sql("sales_data",postgres_eng,schema="staging",if_exists="replace",index=False)
        logger.info("Sales data loaded....")

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
        postgres_conn = PostgreSQLConnection(config=config)
        postgres_eng = postgres_conn.get_engine()
        catalog_df.to_sql("catalog",postgres_eng,schema="staging",if_exists="replace",index=False)
    except Exception as e:
        logger.info(f"Got some error....{e}")




if __name__ == "__main__":
    #df = mysql_conn()
    #load_mysql_postgres(sales_df=df)
    df = get_mongo()
    logger.info(df.head())
    load_mongo_postgres(catalog_df=df)
