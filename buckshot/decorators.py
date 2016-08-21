"""
Context managers which can distribute workers (functions) across multiple
processes.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import functools

from buckshot.contexts import distributed


LOG = logging.getLogger(__name__)


def distribute(processes):
    """Decorator which turns a function into a mappable, distributed task
    worker.

    The input function will be wrapped in a new generator which accepts
    an iterable. This iterable will contain values expected by the original
    function.

    Each value in the iterable will be mapped to subprocess workers which
    executed the original function and return the result to the parent
    process.

    Example:
    >>> @distribute(processes=4)
    ... def foo(x):
    ...     return expensive_calulation(x) + another_expensive_calculation(x)
    ...
    >>> values = range(1000)
    >>> for result in foo(values):  # Map each item in `values` to the original function.
    ...     print result
    ...
    """
    def decorator(func):
        @functools.wraps(func)
        def inner(*args):
            iterable = args[-1]  # Kind of a hack to work with instance methods.

            with distributed(func, processes) as mapfunc:
                for result in mapfunc(iterable):
                    yield result

        return inner

    if callable(processes):
        func, processes = processes, None
        return decorator(func)
    return decorator
