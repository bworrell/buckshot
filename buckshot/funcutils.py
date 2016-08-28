from __future__ import absolute_import
from __future__ import unicode_literals

import inspect
import logging

LOG = logging.getLogger(__name__)


def is_generator(func):
    """Return True if `func` is a generator function."""
    return inspect.isgeneratorfunction(func)
