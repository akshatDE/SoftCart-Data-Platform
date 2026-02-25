from loguru import logger
import random
import pandas as pd 


def generate_catalog_data(models):
    products_catalog = []
    price_category = {
        "watches": range(50,401),
        "phones": range(500,1201),
        "tablets": range(300,801),
        "laptops": range(500,2501)
    }
    start_id = 1000
    try:

        for category, model_list in models.items():
            for product_id, model_name in enumerate(model_list, start_id):
                product = {
                    "product_id": product_id,
                    "product_model": model_name,
                    "product_type": category,
                    "product_price": random.choice(price_category[category])
                }
                products_catalog.append(product)
            start_id += len(model_list)
        logger.info(f"Succesfully genrate products catalog with {len(products_catalog)} products.")
        return products_catalog
    
    except Exception as e:
        logger.error(f"Got some error while genrating products catalog data {e}")



