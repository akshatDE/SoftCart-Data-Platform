from dotenv import load_dotenv
from snowflake.connector import connect
from loguru import logger
import os
load_dotenv()


# snowflake connection test
def test_snowflake_connection():
    try:
        conn = connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT")
        )
        logger.info("Snowflake connection successful")
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise e

# test_snowflake_connection()

test_snowflake_connection()


