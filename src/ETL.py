from src.pipeline import pipeline
from src.loaddata.load_mongo import load_mongo
from src.loaddata.load_mysql import load_mysql
from src.dwh.postgressstaging import get_mysql,load_mysql_postgres,get_mongo,load_mongo_postgres
from src.utility.custom_logger import logger
import pandas as pd

# Execute the pipeline
if __name__ == "__main__":
    try:
        #pipeline()
        logger.info("Pipeline executed successfully data saved in data directory.")

        # Load data to MySQL and MongoDB
        #load_mysql()
        #load_mongo()
        
        # Load data from MySQL and MongoDB
        get_mysql()
        #catalog_df = get_mongo()

        # Load data to Postgres staging
        #load_mysql_postgres(sales_df, customer_df)
        #load_mongo_postgres(catalog_df)

        #logger.info("Data loaded From MySQL, MongoDB to Postgres staging successfully.")

    except Exception as e:
        logger.info(f"Got some exception {e}")
    
    