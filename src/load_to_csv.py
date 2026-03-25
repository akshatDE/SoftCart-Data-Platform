# This is the main entry point that generates the data and loads it to the data warehouse. It also has the code to read the config file and set up the logging.
from src.datasource.GenerateTransactions import generate_transactions
from src.datasource.GenerateCatalog import generate_catalog_data
from src.datasource.GenerateCustomers import generate_customers
from src.datasource.GenerateProducts import generate_products, product_config
from src.databases.mysqlconn import MySqlConnection
from src.databases.postgresconn import PostgreSQLConnection
from src.databases.mongoconn import MongoConnection
from src.utility.custom_logger import logger
from configparser import ConfigParser
import os
import pandas as pd


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
            logger.info(f"Got some exception {e}")



