from src.datasource.GenerateCatalog import generate_catalog_data
from src.datasource.GenerateProducts import generate_products,product_config
from src.datasource.GenerateCustomers import generate_customers
from src.utility.custom_logger import logger
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
     
