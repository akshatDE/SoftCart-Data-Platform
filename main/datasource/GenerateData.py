from datasource.GenerateCatalog import generate_catalog_data
from datasource.GenerateProducts import generate_products,product_config
from datasource.GenerateCustomers import generate_customers
import pandas as pd 
from utility.custom_logger import logger
import random
from faker import Faker

faker = Faker()

def generate_transactions(catalog,n):
    try: 

        sales_data = []
        for _ in range(n):

            prod = random.choice(catalog)
            prod_id = prod['product_id']
            prod_price = float(prod['product_price'])
            customer_id = random.randint(1,1000)
            quantity = random.randint(1,20)
            timestp = faker.date_time_between(start_date="-2y", end_date="now")

            sales_dict ={
                 "product_id":prod_id,
                "customer_id":customer_id,
                "quantity":quantity,
                "price":prod_price,
                "time_stamp":timestp
            }
            sales_data.append(sales_dict)
        return sales_data
    
    except Exception as e:
         logger.info(f"Got some error {e}")
     


if __name__ == "__main__":
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
        catalog_df.to_json("/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCart-Data-Platform/data/catalog.json",orient="records")
        transactional_df.to_csv("/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCart-Data-Platform/data/sales.csv",index=False)
        customer_df.to_csv("/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCart-Data-Platform/data/customers.csv",index=False)

    except Exception as e:
            logger.info(f"Got some exception {e}")