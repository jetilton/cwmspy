import functools
from functools import wraps


def log_decorator(logger):
    def real_decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            name = function.__name__
            logger.debug(f"Start {name}")
            out = function(*args, **kwargs)
            logger.debug(f"End {name}")
            return out

        return wrapper

    return real_decorator

