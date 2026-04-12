from snowflake.connector import connect
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from loguru import logger
from dotenv import load_dotenv
import os
import configparser


load_dotenv("/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/airflow/.env")  # Load environment variables from .env file

class SnowflakeConnection:
    """Singleton class to manage Snowflake connection"""
    _instance = None

    def __init__(self, config=None):
        if SnowflakeConnection._instance is not None:
            raise Exception("Use get_instance() instead of creating a new object.")
        self.connection = None
        self.connect()

    @classmethod
    def get_instance(cls, config=None):
        if cls._instance is None:
            cls._instance = cls(config=config)
        return cls._instance

    def connect(self):
        try:
            self.connection = connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA")
            )
            logger.info("Snowflake Connection successful")
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise

    def get_engine(self):
        try:
            url = URL(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA")
            )
            engine = create_engine(url)
            logger.info(f"Snowflake engine created {engine}")
            return engine
        except Exception as e:
            logger.error(f"Error creating Snowflake engine: {e}")
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("Snowflake Connection closed")
