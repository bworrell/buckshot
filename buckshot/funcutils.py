from __future__ import absolute_import
from __future__ import unicode_literals

import inspect
import logging

LOG = logging.getLogger(__name__)


def is_generator(func):
    """Return True if `func` is a generator function."""
    return inspect.isgeneratorfunction(func)


def patch_recursion(func):
    """Allows recursion to work with @distributed functions.

    Note:
        This only works with immediate recursion. If the @distributed function
        is called by descendant functions, this will not work.
    """
    func.__globals__[func.__name__] = func