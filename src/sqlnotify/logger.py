import logging

from .constants import PACKAGE_NAME


def get_logger(name: str = PACKAGE_NAME, enabled: bool = True) -> logging.Logger:
    """
    Get a logger for the SQLNotify package.

    Args:
        name (str): The name of the logger. Defaults to the package name.
        enable (bool): If False, adds a NullHandler to disable logging. Defaults to True.

    Returns:
        logging.Logger: Configured logger instance
    """

    logger = logging.getLogger(name)

    logger.handlers.clear()

    if enabled:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    else:
        logger.addHandler(logging.NullHandler())
        logger.setLevel(logging.CRITICAL + 1)

    return logger
