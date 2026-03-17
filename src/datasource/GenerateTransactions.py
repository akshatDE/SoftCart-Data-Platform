from src.datasource.GenerateCatalog import generate_catalog_data
from src.datasource.GenerateProducts import generate_products,product_config
from src.datasource.GenerateCustomers import generate_customers
from src.utility.custom_logger import logger
import random
from faker import Faker

faker = Faker()

def generate_transactions(catalog, n):
    try:
        sales_data = []
        channels = ["Online", "In-Store", "Mobile App"]
        promotions = [
            {"promo_code": "None", "discount_percent": 0},
            {"promo_code": "None", "discount_percent": 0},
            {"promo_code": "None", "discount_percent": 0},
            {"promo_code": "Holiday Sale", "discount_percent": 10},
            {"promo_code": "Black Friday", "discount_percent": 20},
            {"promo_code": "Cyber Monday", "discount_percent": 15},
        ]

        for _ in range(n):
            prod = random.choice(catalog)
            promo = random.choice(promotions)
            prod_price = float(prod['product_price'])
            discounted_price = round(prod_price * (1 - promo["discount_percent"] / 100), 2)

            sales_dict = {
                "product_id": prod['product_id'],
                "customer_id": random.randint(1, 1000),
                "quantity": random.randint(1, 20),
                "price": discounted_price,
                "time_stamp": faker.date_time_between(start_date="-2y", end_date="now"),
                "channel": random.choice(channels),
                "promo_code": promo["promo_code"],
                "discount_percent": promo["discount_percent"]
            }
            sales_data.append(sales_dict)
        return sales_data

    except Exception as e:
        logger.info(f"Got some error {e}")
    
     
