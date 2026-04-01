from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from src.datasource.GenerateTransactions import generate_transactions
from src.datasource.GenerateCatalog import generate_catalog_data
from src.datasource.GenerateCustomers import generate_customers
from src.datasource.GenerateProducts import generate_products, product_config
from src.databases.mysqlconn import MySqlConnection
from src.databases.postgresconn import PostgreSQLConnection
from src.databases.mongoconn import MongoConnection
from loguru import logger
from src.services.load_mongo import load_mongo
from src.services.load_mysql import load_mysql
from src.services.staging import get_mysql,load_mysql_postgres,get_mongo,load_mongo_postgres
from src.services.analytics import load_analytics
from datetime import datetime, timedelta
import pandas as pd
import os
import time
from datetime import datetime, timedelta

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
        catalog_df.to_json("/opt/airflow/data/catalog.json",orient="records")
        transactional_df.to_csv("/opt/airflow/data/transactional.csv",index=False)
        customer_df.to_csv("/opt/airflow/data/customers.csv",index=False)

    except Exception as e:
            logger.error(f"Failed to generate data: {e}")
            raise e


    


# Creating an Object of DAG Class
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(dag_id="SoftCart_ETL_Pipeline")


# Task 1: Generate Data
generate_data_task = PythonOperator(
    task_id='generate_data',
    python_callable=data_to_csv,
    dag=dag
)

# Testing the generated data
test_data_task = BashOperator(
    task_id='test_data',
    bash_command='head -n 5 /opt/airflow/data/catalog.json && head -n 5 /opt/airflow/data/transactional.csv && head -n 5 /opt/airflow/data/customers.csv',
    dag=dag
)


# Dependency
generate_data_task >> test_data_task


