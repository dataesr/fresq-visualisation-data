import logging
import sys

FORMATTER = '%(asctime)s | %(name)s | %(levelname)s | %(message)s'


def get_formatter() -> logging.Formatter:
    formatter = logging.Formatter(FORMATTER)
    return formatter


def get_console_handler() -> logging.StreamHandler:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(get_formatter())
    return console_handler

def get_file_handler():
    file_handler = logging.FileHandler('logs.log')
    file_handler.setFormatter(get_formatter())
    return file_handler

def get_logger(name: str = __name__, level: int = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    return logger
