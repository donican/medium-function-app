import logging
import os

def setup_logging():
    """
    Sets the default logger for the application.
    """

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    root_logger = logging.getLogger()

    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("application.log", mode="a"),
        ]
    )

