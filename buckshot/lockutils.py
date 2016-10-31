from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import functools

from buckshot import funcutils

LOG = logging.getLogger(__name__)


class LockManager(object):
    """Controls object locking.

    We wrap these methods in a class so that the lock attribute is
    namespaced as _LockManager__is_locked.
    """

    @classmethod
    def acquire_lock(cls, obj, lockattr):
        """Acquire the __lock on `obj`. If no __lock exists, add one and
        acquire().
        """
        getattr(obj, lockattr).acquire()

    @classmethod
    def release_lock(cls, obj, lockattr):
        """Release the __lock attribute on `obj`. """
        try:
            getattr(obj, lockattr).release()
        except AttributeError:
            pass
        except Exception as ex:  # lock release failed (maybe not acquired).
            LOG.warning(ex)

    @classmethod
    def is_locked(cls, obj, lockattr):
        """Return True if the input object is locked.

        Warning:
            If two threads attempt to check is_locked at the same time
            there is a chance that an unlocked object will return True.
        """
        lock = getattr(obj, lockattr)

        try:
            locked = lock.acquire(False) is False
        except AttributeError:
            locked = False
        else:
            if not locked:
                lock.release()
        return locked

def lock_instance(lockattr):
    def decorate(method):
        """Lock the object associated with the instance `method` and execute
        the method.
        """
        def wrap_method(method):
            @functools.wraps(method)
            def inner(self, *args, **kwargs):
                LockManager.acquire_lock(self, lockattr)
                try:
                    retval = method(self, *args, **kwargs)
                finally:
                    LockManager.release_lock(self, lockattr)
                return retval
            return inner

        def wrap_generator(method):
            @functools.wraps(method)
            def inner(self, *args, **kwargs):
                LockManager.acquire_lock(self, lockattr)

                try:
                    g = method(self, *args, **kwargs)
                    input_ = None

                    while True:
                        item = g.send(input_)
                        input_ = yield item
                finally:
                    LockManager.release_lock(self, lockattr)

            return inner

        if funcutils.is_generator(method):
            return wrap_generator(method)
        return wrap_method(method)

    return decorate


def unlock_instance(lockattr):
    def decorate(method):
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
                    LockManager.release_lock(self, lockattr)
            return inner

        def wrap_method(method):
            @functools.wraps(method)
            def inner(self, *args, **kwargs):
                try:
                    retval = method(self, *args, **kwargs)
                finally:
                    LockManager.release_lock(self, lockattr)
                return retval
            return inner

        if funcutils.is_generator(method):
            return wrap_generator(method)
        return wrap_method(method)

    return decorate


def with_lock(lockattr):
    def decorate(method):
        """Execute the input `method` and release the __lock on the associated
        instance.
        """
        def wrap_generator(method):
            @functools.wraps(method)
            def inner(self, *args, **kwargs):
                LockManager.acquire_lock(self, lockattr)

                try:
                    g = method(self, *args, **kwargs)
                    input_ = None

                    while True:
                        item = g.send(input_)
                        input_ = yield item

                finally:
                    LockManager.release_lock(self, lockattr)
            return inner

        def wrap_method(method):
            @functools.wraps(method)
            def inner(self, *args, **kwargs):
                LockManager.acquire_lock(self, lockattr)

                try:
                    retval = method(self, *args, **kwargs)
                finally:
                    LockManager.release_lock(self, lockattr)
                return retval
            return inner

        if funcutils.is_generator(method):
            return wrap_generator(method)
        return wrap_method(method)
    return decorate


def is_locked(obj, lockattr):
    return LockManager.is_locked(obj, lockattr)

