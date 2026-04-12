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
from src.services.load_mongo import load_mongo
from src.services.staging import get_mysql,load_mysql_postgres,get_mongo,load_mongo_postgres
from src.services.analytics import load_analytics
from src.datawarehouse.snowflakeconn import SnowflakeConnection
from src.datawarehouse.snowflake_etl import extract_data_from_postgres, ddl_for_snowflake_staging, load_data_to_snowflake
from configparser import ConfigParser
import os
from loguru import logger
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from configparser import ConfigParser
import pandas as pd
import os
import time
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy import text, create_engine
from urllib.parse import quote_plus
from loguru import logger


config = ConfigParser()
config_path = "/opt/airflow/resources/config_file.ini"
config.read(config_path)
print(config.sections()) 

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

def load_mysql():
    try:
  
        data_path = ["/opt/airflow/data/transactional.csv",
                     "/opt/airflow/data/customers.csv"
                     ]
        
        mysql_con = MySqlConnection.get_instance(config=config)
        logger.info(f"Connected to mysql ready for data loading.....")
        mysql_cur =mysql_con.connection.cursor()
        for file in data_path:
            if "transactional" in file:
                mysql_cur.execute("SELECT COUNT(*) FROM sales_data;")
                count_before = mysql_cur.fetchone()[0]
                logger.info(f"Existing data in sales_data table is {count_before} records.....")
                if count_before > 0:
                    mysql_cur.execute("TRUNCATE TABLE sales_data;")
                    logger.info("Existing data in sales_data table truncated.....")
                mysql_cur.execute(f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE sales_data FIELDS TERMINATED BY ',' IGNORE 1 LINES;")
                logger.info(f"Sales data loaded to mysql.....")
            elif "customers" in file:
                mysql_cur.execute("SELECT COUNT(*) FROM customers;")
                count_before = mysql_cur.fetchone()[0]
                logger.info(f"Existing data in customers table is {count_before} records.....")
                if count_before > 0:
                    mysql_cur.execute("TRUNCATE TABLE customers;")
                    logger.info("Existing data in customers table truncated.....")
                mysql_cur.execute(f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE customers FIELDS TERMINATED BY ',' IGNORE 1 LINES;")
                logger.info(f"Customer data loaded to mysql.....")

        logger.info("Data loading to mysql completed.....")
        mysql_con.connection.commit()
        mysql_cur.close()
        mysql_con.connection.close()

    except Exception as e:
        logger.error(f"Got some error while loading data to mysql {e}")
        raise e
    
def load_mongo():
    try:
        data_path = "/opt/airflow/data/catalog.json"
        
        client = MongoConnection.get_instance(config=config)
        mongo_db = client.mongo_db
        with open(data_path,"r") as f:
            # if collection is aleady there drop it
            if "catalog" in mongo_db.list_collection_names():
                logger.info("Collection already exists, dropping existing collection and creating new collection.....")
                mongo_db["catalog"].drop()
            else:
                logger.info("Collection does not exist, creating new collection.....")
            mongo_db["catalog"].insert_many(eval(f.read()))
            logger.info("Data loaded to mongo.....")
    except Exception as e:
        logger.error(f"Got some error while loading data to mongo {e}")
        raise e

# Load to staging PostgreSQL
def load_staging():
    try:
        sales_df, customer_df = get_mysql()
        catalog_df = get_mongo()
        load_mysql_postgres(sales_df, customer_df)
        load_mongo_postgres(catalog_df)
        logger.info("Data loaded to staging.....")
    except Exception as e:
        logger.error(f"Got some error while loading data to staging {e}")
        raise e
    
# Load to analytics schema in PostgreSQL  
def load_analytics():
    try:
        cfg = config["postgresql"]
        password = quote_plus(cfg["password"])
        engine = create_engine(
            f"postgresql+psycopg2://{cfg['user']}:{password}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )

        with engine.begin() as conn:
            logger.info("Truncating tables...")
            conn.execute(text("TRUNCATE analytics.fact_sales CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_date CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_channel RESTART IDENTITY CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_promotion RESTART IDENTITY CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_customer CASCADE"))
            conn.execute(text("TRUNCATE analytics.dim_product CASCADE"))
            # ... rest of your INSERT statements stay the same
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

    except Exception as e:
        logger.error(f"Error loading analytics: {e}")
        raise

def load_staging_snowflake():
    try:
        # Get Snowflake connection
        snowflake_conn = SnowflakeConnection.get_instance()
        logger.info("Connected to Snowflake ready for data loading.....")

         # Extract data from PostgreSQL staging
        pg_conn = PostgreSQLConnection.get_instance(config=config)
        catalog_df, sales_data_df, customers_df = extract_data_from_postgres(pg_conn)
    except Exception as e:
        logger.error(f"Got some error while extracting data from PostgreSQL: {e}")
        raise e


# Creating an Object of DAG Class
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 29),
    'start_date': datetime.now(),
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

# Task 2: Load data to OLTP
load_mysql_task = PythonOperator(
    task_id='load_mysql',
    python_callable=load_mysql,
    dag=dag
)

# Task 3: Load to MongoDB
load_mongo_task = PythonOperator(
    task_id='load_mongo',
    python_callable=load_mongo,
    dag=dag
)
# Task 4: Load to staging PostgreSQL
load_staging_task = PythonOperator(
    task_id='load_staging',
    python_callable=load_staging,
    dag=dag
)
# Task 4: Load to staging Snowflake
load_staging_snowflake_task = PythonOperator(
    task_id='load_staging_snowflake',
    python_callable=load_staging_snowflake,
    dag=dag
)

# Task 5: Load to analytics
load_analytics_task = PythonOperator(
    task_id='load_analytics',
    python_callable=load_analytics,
    dag=dag)

# Setting up dependencies
generate_data_task >> [load_mysql_task, load_mongo_task] >> load_staging_task >> load_staging_snowflake_task >> load_analytics_task

