import psycopg2
import psycopg2.extras
from loguru import logger
import json
from sqlalchemy import create_engine
from urllib.parse import quote_plus

class PostgreSQLConnection:
    """
    A singleton PostgreSQL database connection manager.
    
    This class implements the Singleton pattern to ensure only one PostgreSQL connection
    instance exists throughout the application lifecycle. It handles connection
    establishment, management, and cleanup for PostgreSQL databases.
    
    Attributes:
        _instance (PostgreSQLConnection): Class-level singleton instance
        config (dict): Database configuration dictionary
        connection: PostgreSQL database connection object
    """
    
    _instance = None  # Singleton instance

    def __init__(self, config):
        """
        Initialize the PostgreSQL connection instance.
        
        Note: This constructor should not be called directly. Use get_instance() instead.
        
        Args:
            config (dict): Database configuration dictionary containing PostgreSQL connection parameters
            
        Raises:
            Exception: If an instance already exists (violates singleton pattern)
        """
        if PostgreSQLConnection._instance is not None:
            raise Exception("Use get_instance() instead of creating a new object.")

        self.config = config
        self.connection = None
        self.connect()
    
    @classmethod
    def get_instance(cls, config=None):
        """
        Get the singleton instance of PostgreSQLConnection.
        
        Creates a new instance if none exists, otherwise returns the existing instance.
        
        Args:
            config (dict, optional): Database configuration dictionary. Required for first call.
            
        Returns:
            PostgreSQLConnection: The singleton instance of the PostgreSQL connection manager
        """
        if cls._instance is None:
            cls._instance = cls(config)
        return cls._instance

    def connect(self):
        """
        Establish a connection to the PostgreSQL database.
        
        Uses the configuration provided during initialization to connect to PostgreSQL.
        The password is decrypted before establishing the connection.
        
        Raises:
            Exception: If the database connection fails for any reason
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config["postgresql"]["host"],
                port=self.config["postgresql"].get("port", 5432),  # Default PostgreSQL port
                user=self.config["postgresql"]["user"],
                password= self.config["postgresql"]["password"],
                database=self.config["postgresql"]["database"],
            )
            # Set autocommit to False (default behavior)
            self.connection.autocommit = False
            logger.info("PostgreSQL Connection successful")
        except Exception as e:
            logger.error(f"PostgreSQL connection error: {e}")
            raise e
    def get_engine(self):
        try:
            pswd = quote_plus(self.config["postgresql"]["password"])
            engine = create_engine(f"postgresql+psycopg2://{self.config["postgresql"]["user"]}:{pswd}@{self.config["postgresql"]["host"]}:{self.config["postgresql"]["port"]}/{self.config["postgresql"]["database"]}")
            logger.info(f"Postgres engine created {engine}")
            return engine
        except Exception as e:
            logger.info(f"Got some error {e} ")

        

    
