import logging.config

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
            "level": "INFO",
        },
    },
}

logging.config.dictConfig(LOGGING)

DATABASE_URL = "sqlite:///./test.db"
