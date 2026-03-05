from faker import Faker
import random
from loguru import logger

faker = Faker()
def generate_customers():
    try:

        customer_details = []

        for i in range(1,1000):
            while True:
                fname = faker.first_name().strip()
                lname = faker.last_name().strip()
                if fname.isalpha() and lname.isalpha():
                    email = fname+lname+"@gmail.com"
                    customer_id = i
                    break
                else:
                    continue
            customer_dict = {
                        "customer_id":customer_id,
                        "first_name":fname,
                        "last_name":lname,
                        "email":email
            }

            customer_details.append(customer_dict)
        return customer_details
    except Exception as e:
        logger.info(f"Got some error{e}")
