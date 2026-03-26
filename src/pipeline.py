from src.datasource.GenerateTransactions import generate_transactions
from src.datasource.GenerateCatalog import generate_catalog_data
from src.datasource.GenerateCustomers import generate_customers
from src.datasource.GenerateProducts import generate_products, product_config
from src.databases.mysqlconn import MySqlConnection
from src.databases.postgresconn import PostgreSQLConnection
from src.databases.mongoconn import MongoConnection
from src.utility.custom_logger import logger
from src.services.load_mongo import load_mongo
from src.services.load_mysql import load_mysql
from src.services.staging import get_mysql,load_mysql_postgres,get_mongo,load_mongo_postgres
from src.services.analytics import load_analytics
import pandas as pd
import os

def data_to_csv():
    try:
        config = product_config()
        models = {}
        for data in config:
            models[data] = generate_products(**config[data])
            
        catalog_data = generate_catalog_data(models=models)
        catalog_df = pd.DataFrame(catalog_data)
        logger.info(f"Catalog data generated with {len(catalog_df)} records.")

        transactional_data = generate_transactions(catalog=catalog_data,n=100000)
        transactional_df = pd.DataFrame(transactional_data)
        logger.info(f"transactional data generated with {len(transactional_df)} records")

        customer_data = generate_customers()
        customer_df = pd.DataFrame(customer_data)
        logger.info(f"Customer data generated with {len(customer_df)} records.")


        # Writing data to data  as json and csv 
        catalog_df.to_json("/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/data/catalog.json",orient="records")
        transactional_df.to_csv("/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/data/sales_data.csv",index=False)
        customer_df.to_csv("/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/data/customers.csv",index=False)

    except Exception as e:
            logger.error(f"Failed to generate data: {e}")
            raise e


# Execute the pipeline
if __name__ == "__main__":
    try:
        data_to_csv()
        logger.info("Step 1: Data Generated")

        # Load OTLP
        load_mysql()
        load_mongo()
        logger.info("Step 2: OLTP Loaded")
        
        # ETL to stagging 
        sales_df, customer_df = get_mysql()
        catalog_df = get_mongo()
        load_mysql_postgres(sales_df, customer_df)
        load_mongo_postgres(catalog_df)

        logger.info("Step 3: Stagging loaded")

        # Load to analytics schema
        load_analytics()
        logger.info("Step 4: Analytics loaded")


    except Exception as e:
        logger.error(f"Failed to execute pipeline: {e}")
        raise e
    