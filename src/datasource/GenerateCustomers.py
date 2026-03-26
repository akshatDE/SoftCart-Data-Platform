from faker import Faker
from src.utility.custom_logger import logger
import random


faker = Faker()
def generate_customers():
    try:
        customer_segment = ["Consumer","Corporate","Home Office"]
        customer_details = []

        for i in range(1,1001):
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
                        "email":email,
                        "segment":random.choice(customer_segment)
            }

            customer_details.append(customer_dict)
        return customer_details
    except Exception as e:
        logger.error(f"Error generating customer data: {e}")
        raise e
