from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import functools
import multiprocessing

from buckshot import funcutils

LOG = logging.getLogger(__name__)


class LockManager(object):
    """Controls object locking.

    We wrap these methods in a class so that the lock attribute is
    namespaced as _LockManager__is_locked.
    """

    @classmethod
    def acquire_lock(cls, obj):
        """Acquire the __lock on `obj`. If no __lock exists, add one and
        acquire().
        """
        try:
            mutex = obj.__lock
        except AttributeError:
            obj.__lock = mutex = multiprocessing.Lock()
        mutex.acquire()

    @classmethod
    def release_lock(cls, obj):
        """Release the __lock attribute on `obj`. """
        try:
            obj.__lock.release()
        except AttributeError:
            pass
        except ValueError as ex:  # lock release failed (maybe not acquired).
            LOG.warning(ex)

    @classmethod
    def is_locked(cls, obj):
        try:
            locked = obj.__lock.acquire(block=False) is False
        except AttributeError:
            locked = False
        else:
            if not locked:
                obj.__lock.release()
        return locked


def lock_instance(method):
    """Lock the object associated with the instance `method` and execute
    the method. Release the lock when completed.
    """
    def wrap_method(method):
        @functools.wraps(method)
        def inner(self, *args, **kwargs):
            LockManager.acquire_lock(self)
            try:
                retval = method(self, *args, **kwargs)
            finally:
                LockManager.release_lock(self)
            return retval
        return inner

    def wrap_generator(method):
        @functools.wraps(method)
        def inner(self, *args, **kwargs):
            LockManager.acquire_lock(self)

            try:
                g = method(self, *args, **kwargs)
                input_ = None

                while True:
                    item = g.send(input_)
                    input_ = yield item

            finally:
                LockManager.release_lock(self)
        return inner

    if funcutils.is_generator(method):
        return wrap_generator(method)
    return wrap_method(method)


def unlock_instance(method):
    """Execute the input `method` and release the __lock on the associated
    instance.
    """
    def wrap_generator(method):
        @functools.wraps(method)
        def inner(self, *args, **kwargs):
            try:
                g = method(self, *args, **kwargs)
                input_ = None

                while True:
                    item = g.send(input_)
                    input_ = yield item

            finally:
                LockManager.release_lock(self)
        return inner

    def wrap_method(method):
        @functools.wraps(method)
        def inner(self, *args, **kwargs):
            try:
                retval = method(self, *args, **kwargs)
            finally:
                LockManager.release_lock(self)
            return retval
        return inner

    if funcutils.is_generator(method):
        return wrap_generator(method)
    return wrap_method(method)


def is_locked(obj):
    return LockManager.is_locked(obj)

