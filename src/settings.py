import logging.config
import sys
from loguru import logger

DEBUG = True

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
        },
    },
    "loggers": {
        "app": {
            "handlers": ["console"],
            "level": "INFO" if not DEBUG else "DEBUG",
        },
    },
}

logging.config.dictConfig(LOGGING)

DATABASE_URL = "sqlite:///./test.db"

logger.level("INFO" if not DEBUG else "DEBUG")
