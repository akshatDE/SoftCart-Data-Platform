from src.utility.encrypt_decrypt import decrypt
from src.utility.custom_logger import logger
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import mysql.connector
import configparser
import pandas as pd


class MySqlConnection:
    _instance = None  # Singleton instance

    def __init__(self, config):
        if MySqlConnection._instance is not None:
            raise Exception("Use get_instance() instead of creating a new object.")

        self.config = config
        self.connection = None
        self.connect()
    
    @classmethod
    def get_instance(cls, config=None):
        if cls._instance is None:
            cls._instance = cls(config)
        return cls._instance

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.config["mysql"]["host"],
                port=int(self.config["mysql"]["port"]),
                user="root",
                password=decrypt(self.config["mysql"]["root_password"]),
                database=self.config["mysql"]["database"],
                allow_local_infile=True
            )
            logger.info("MySQL Connection successful")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise e
        
    def get_engine(self):
        try:
            pswd = decrypt(self.config["mysql"]["password"]) 
            engine = create_engine(f"mysql+mysqlconnector://{self.config["mysql"]["user"]}:{pswd}@{self.config["mysql"]["host"]}:{self.config["mysql"]["port"]}/{self.config["mysql"]["database"]}")
            logger.info(f"Connection successful {engine}")
            return engine


        except Exception as e:
            logger.info(f"Got Some error {e}")

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL Connection closed")



