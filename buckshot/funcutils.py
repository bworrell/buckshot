from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import functools

LOG = logging.getLogger(__name__)


def assert_unlocked(obj):
    """Assert that an input object has a falsey __is_locked value or
    no __is_locked value set.
    """
    if getattr(obj, "__is_locked", False):
        raise RuntimeError("Item is locked.")


def lock_instance(func):
    """Set an __is_locked=True attr on the method owner. Unlock if an
    error occurs during execution of the method.
    """
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        assert_unlocked(func)
        self.__is_locked = True
        try:
            retval = func(self, *args, **kwargs)
        except:
            self.__is_locked = False
            raise
        return retval
    return inner


def unlock_instance(func):
    """Set an __is_locked=False attr on the method owner after finishing
    execution of the method.
    """
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        finally:
            self.__is_locked = False
    return inner
