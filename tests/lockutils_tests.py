from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import unittest
import threading

from buckshot import lockutils

LOG = logging.getLogger(__name__)


class Object(object):
    def __init__(self):
        self._lock = threading.Lock()

    @lockutils.lock_instance("_lock")
    def foo(self):
        while True:
            yield 1

    @lockutils.unlock_instance("_lock")
    def unlock(self):
        pass


class NoDecoratorObject(object):
    def __init__(self):
        self._lock = threading.Lock()


class LockUtilsTests(unittest.TestCase):
    def test_lock_manager(self):
        """Test that LockManager correctly locks and unlocks objects."""
        LockManager = lockutils.LockManager
        obj = NoDecoratorObject()

        # check that the initial state is unlocked
        self.assertFalse(LockManager.is_locked(obj, "_lock"))

        # lock it and check state
        LockManager.acquire_lock(obj, "_lock")
        self.assertTrue(LockManager.is_locked(obj, "_lock"))

        # unlock it and check state
        LockManager.release_lock(obj, "_lock")
        self.assertFalse(LockManager.is_locked(obj, "_lock"))


    def test_decorators(self):
        mock = Object()
        self.assertFalse(lockutils.is_locked(mock, "_lock"))

        g = mock.foo()
        next(g)  # Start the generator and initiate the lock
        self.assertTrue(lockutils.is_locked(mock, "_lock"))

        mock.unlock()
        self.assertFalse(lockutils.is_locked(mock, "_lock"))


if __name__ == "__main__":
    unittest.main()