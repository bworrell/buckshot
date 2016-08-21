"""
Logging utility functions.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import time
import logging
import inspect
import functools


def _wrap_function(func, logger):
    """Wrapper for `tracelog` to be used on functions."""
    @functools.wraps(func)
    def inner(*args, **kwargs):
        funcname = func.__name__
        start = time.time()
        logger.debug("Entering function %s", funcname)

        try:
            return func(*args, **kwargs)
        finally:
            duration = time.time() - start
            logger.debug("Leaving function %s: %s", funcname, duration)

    return inner


def _wrap_generator(func, logger):
    """Wrapper for `@attach_profiler` to be used on generators.
    Source:
        https://github.com/rkern/line_profiler/blob/master/kernprof.py
    """
    @functools.wraps(func)
    def inner(*args, **kwargs):
        funcname = func.__name__
        start = time.time()
        logger.debug("Entering generator %s", funcname)

        try:
            g = func(*args, **kwargs)
            input_ = None

            while True:
                item = g.send(input_)
                input_ = yield item

        finally:
            duration = time.time() - start
            logger.debug("Leaving generator %s: %s", funcname, duration)

    return inner


def tracelog(logger_or_func):
    def decorator(func):
        if inspect.isgeneratorfunction(func):
            return _wrap_generator(func, logger)
        return _wrap_function(func, logger)

    if callable(logger_or_func):
        func, logger = logger_or_func, logging.getLogger("buckshot.tracelog")
        return decorator(func)

    logger = logger_or_func
    return decorator