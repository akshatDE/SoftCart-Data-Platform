from loguru import logger
import sys
from configparser import ConfigParser
import os
config_path = os.getenv('config_path')

config = ConfigParser()
config_path = os.getenv('config_path')
config.read(config_path)

log_level = config["default"]["log_level"]

log_dir = "/Users/akshatsharma/Desktop/Personal_Projects/Soft-Cart/SoftCartCapstone/logs"

# create logs folder if it does not exist
os.makedirs(log_dir, exist_ok=True)

# full path of log file
log_file = os.path.join(log_dir, "app_log.log")

logger.remove()

logger.add(sys.stderr, level=log_level)

logger.add(
    log_file,
    level=log_level,
    rotation="10 MB",
    retention="10 days",
    compression="zip"
)

logger.info("Logger initialized")