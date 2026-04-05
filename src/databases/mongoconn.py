from pymongo import MongoClient
from urllib.parse import quote_plus
from src.utility.custom_logger import logger


class MongoConnection:
    _instance = None 

    def __init__(self,config):
        if MongoConnection._instance is not None:
            raise Exception("Use get_instance() instead of creating a new object.")
        self.config = config
        self.connect_mongo()

    @classmethod
    def get_instance(cls, config=None):
        if cls._instance is None:
            cls._instance = cls(config)
        return cls._instance
    
    def connect_mongo(self):
        try:
            mongo = self.config["mongodb"]
            password = quote_plus(mongo["password"])
            uri = (
                f"mongodb://{mongo['user']}:{password}"
                f"@{mongo['host']}:{mongo['port']}"
                f"/{mongo['database']}?authSource={mongo['auth_source']}"
            )
            self.mongo_client = MongoClient(uri)
            self.mongo_db = self.mongo_client[mongo["database"]]

            # verify connection is alive
            self.mongo_client.admin.command("ping")
            logger.info("MongoDB Connection successful")
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            raise e
        
    def close_mongo(self):
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB Connection closed")



