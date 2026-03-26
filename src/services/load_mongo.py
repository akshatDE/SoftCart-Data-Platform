from src.databases.mongoconn import MongoConnection
from configparser import ConfigParser
from loguru import logger

def load_mongo():
    try:
        config = ConfigParser()
        config_path = "/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/resources/config_file.ini"
        config.read(config_path)

        data_path = "/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/data/catalog.json"
        
        client = MongoConnection.get_instance(config=config)
        mongo_db = client.mongo_db
        with open(data_path,"r") as f:
            # if collection is aleady there drop it
            if "catalog" in mongo_db.list_collection_names():
                logger.info("Collection already exists, dropping existing collection and creating new collection.....")
                mongo_db["catalog"].drop()
            else:
                logger.info("Collection does not exist, creating new collection.....")
            mongo_db["catalog"].insert_many(eval(f.read()))
            logger.info("Data loaded to mongo.....")
    except Exception as e:
        logger.error(f"Got some error while loading data to mongo {e}")
        raise e 
    
