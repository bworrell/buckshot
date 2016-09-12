from __future__ import absolute_import
from __future__ import unicode_literals

import inspect
import logging
import functools
import multiprocessing


LOG = logging.getLogger(__name__)


def is_generator(func):
    """Return True if `func` is a generator function."""
    return inspect.isgeneratorfunction(func)


def distributed_memoize(func):
    """A memoization decorator that works across multiple processes.

    Warning:
        This is a bad design that will almost certainly cause memory
        issues.
    """
    manager = multiprocessing.Manager()
    cache = manager.dict()

    def keyfunc(args):
        return unicode(args)

    @functools.wraps(func)
    def inner(*args):
        key = keyfunc(args)

        try:
            retval = cache[key]
        except KeyError:
            cache[key] = retval = func(*args)
        return retval

    return inner


def patch_recursion(func):
    """Allows recursion to work with @distributed functions.

    Note:
        This only works with immediate recursion. If the @distributed function
        is called by descendant functions, this will not work.
    """
    func.__globals__[func.__name__] = func