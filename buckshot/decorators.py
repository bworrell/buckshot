"""
Context managers which can distribute workers (functions) across multiple
processes.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

__all__ = ["distribute"]

import logging
import functools

from buckshot import contexts


LOG = logging.getLogger(__name__)


def distribute(*func, **opts):
    """Decorator which distributes the wrapped function across multiple
    processes.

    The input function will be wrapped in a new generator which accepts
    an iterable collection of argument tuples. If a function only takes
    one parameter, a flat list of items can be passed in.

    Each value in the iterable will be mapped to subprocess workers which
    executed the original function and return the result to the parent
    process.

    Example:
    >>> @distribute(processes=4)
    ... def foo(x, y):
    ...     return expensive_calulation(x) + another_expensive_calculation(y)
    ...
    >>> values = zip("abc", [1,2,3])  # [(a, 1), (b, 2), (c, 3)]
    >>> for result in foo(values):    # Map each item in `values` to the original function.
    ...     print result

    Warning:
        Because this decorator replaces the existing function with a generator,
        recursion will not work!

    Keyword Arguments:
        processes (int): Number of worker processes to use. If None,
            the number of CPUs on the host system will be used.
        ordered (bool): If True, results are returned in the order of their
            respective inputs.
        timeout (float): Number of seconds to wait before killing a worker
            process. If None, no timeout is used.
    """
    if func and opts:
        raise ValueError("Cannot provide positional arguments.")

    def wrapper(func):
        @functools.wraps(func)
        def inner(*args):
            iterable = args[-1]  # Kind of a hack to work with instance methods.

            with contexts.distributed(func, **opts) as distributed_function:
                for result in distributed_function(iterable):
                    yield result

        return inner

    if func:
        func = func[0]
        return wrapper(func)
    return wrapper
