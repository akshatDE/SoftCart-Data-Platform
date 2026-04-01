from loguru import logger
import sys
from configparser import ConfigParser
import os

config = ConfigParser()
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, "..", "..", "resources", "config_file.ini")
config.read(config_path)

log_level = config.get("default", "log_level", fallback="INFO")

log_dir = os.path.join(base_dir, "..", "..", "logs")
os.makedirs(log_dir, exist_ok=True)

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