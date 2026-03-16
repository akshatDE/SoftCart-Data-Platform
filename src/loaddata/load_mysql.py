from src.databases.mysqlconn import MySqlConnection
from configparser import ConfigParser
from loguru import logger

def load_mysql():
    try:
        config = ConfigParser()
        config_path = "/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/resources/config_file.ini"
        config.read(config_path)

        data_path = ["/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/data/sales_data.csv",
                     "/Users/akshatsharma/Desktop/Personal_Projects/Soft_Cart_Data_Platform/SoftCartDataPlatform/data/customers.csv"
                     ]
        

        mysql_con = MySqlConnection.get_instance(config=config)
        logger.info(f"Connected to mysql ready for data loading.....")
        mysql_cur =mysql_con.connection.cursor()
        for file in data_path:
            if "sales_data" in file:
                mysql_cur.execute(f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE sales_data FIELDS TERMINATED BY ',' IGNORE 1 LINES;")
                logger.info(f"Sales data loaded to mysql.....")
            elif "customers" in file:
                mysql_cur.execute(f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE customers FIELDS TERMINATED BY ',' IGNORE 1 LINES;")
                logger.info(f"Customer data loaded to mysql.....")

        logger.info("Data loading to mysql completed.....")
        mysql_con.connection.commit()
        mysql_cur.close()
        mysql_con.connection.close()

    except Exception as e:
        logger.info(f"Got some error while loading data to mysql {e}")

