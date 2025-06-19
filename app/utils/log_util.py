def gen_logging_config():
    """Configure logging with Uvicorn's handlers."""
    # Base logging configuration
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "default",
                "stream": "ext://sys.stdout"
            }
        },
        "loggers": {
            "app": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False
            },
            # Add specific configuration for Alpaca loggers
            "alpaca": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False
            },
            "alpaca.trading": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False
            },
            "alpaca.data": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False
            }
        },
        "root": {
            "level": "INFO",
            "handlers": ["console"]
        }
    }
    return logging_config