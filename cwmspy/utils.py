# reference: https://stackoverflow.com/a/23726462/4296857
import logging
import functools


def LogDecorator(logger):
    class _MyDecorator(object):
        def __init__(self, fn):
            self.fn = fn

        def __get__(self, obj, type=None):
            return functools.partial(self, obj)

        def __call__(self, *args, **kwargs):
            name = self.fn.__name__
            logger.info(f"Start {name}")
            result = self.fn(*args, **kwargs)
            logger.info(f"End {name}")
            return result

    return _MyDecorator

