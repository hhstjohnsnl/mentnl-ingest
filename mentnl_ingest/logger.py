import logging.config
import datetime

# https://stackoverflow.com/a/59241472/583830
DEFAULT_LOGGING = {
    "version": 1,
    "loggers": {
        "": {  # root logger
            "level": "INFO",
            "handlers": [
                "debug_console_handler",
                "info_rotating_file_handler",
                "error_file_handler",
            ],
        },
        "googleapiclient.discovery_cache": {
            "level": "WARNING",
        },
        "smart_open": {
            "level": "WARNING",
        },
    },
    "handlers": {
        "debug_console_handler": {
            "level": "DEBUG",
            "formatter": "info",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "info_rotating_file_handler": {
            "level": "INFO",
            "formatter": "info",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "info.log",
            "mode": "a",
            "maxBytes": 1048576,
            "backupCount": 10,
        },
        "error_file_handler": {
            "level": "WARNING",
            "formatter": "error",
            "class": "logging.FileHandler",
            "filename": "error.log",
            "mode": "a",
        },
    },
    "formatters": {
        "info": {
            "format": "%(asctime)s-%(levelname)s-%(name)s::%(module)s|%(lineno)s:: %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
        "error": {
            "format": "%(asctime)s-%(levelname)s-%(name)s-%(process)d::%(module)s|%(lineno)s:: %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
    },
}
logging.config.dictConfig(DEFAULT_LOGGING)

# Add milliseconds
# https://stackoverflow.com/a/58777937/583830
logging.Formatter.formatTime = (
    lambda self, record, datefmt=None: datetime.datetime.fromtimestamp(
        record.created, datetime.timezone.utc
    )
    .astimezone()
    .isoformat(sep="T", timespec="milliseconds")
)
