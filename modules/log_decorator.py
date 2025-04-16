import functools
import logging

def log_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        logger.info("Iniciando função: %s", func.__name__)
        result = func(*args, **kwargs)
        logger.info("Função finalizada: %s", func.__name__)
        return result
    return wrapper
