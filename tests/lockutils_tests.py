from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import unittest

from buckshot import lockutils

LOG = logging.getLogger(__name__)


class Object(object):

    @lockutils.lock_instance
    def foo(self):
        while True:
            yield 1

    @lockutils.unlock_instance
    def unlock(self):
        pass


class NoDecoratorObject(object):
    pass


class LockUtilsTests(unittest.TestCase):
    def test_lock_manager(self):
        """Test that LockManager correctly locks and unlocks objects."""
        LockManager = lockutils.LockManager
        obj = NoDecoratorObject()

        # check that the initial state is unlocked
        self.assertFalse(LockManager.is_locked(obj))

        # lock it and check state
        LockManager.acquire_lock(obj)
        self.assertTrue(LockManager.is_locked(obj))

        # unlock it and check state
        LockManager.release_lock(obj)
        self.assertFalse(LockManager.is_locked(obj))


    def test_decorators(self):
        mock = Object()
        self.assertFalse(lockutils.is_locked(mock))

        g = mock.foo()
        next(g)  # Start the generator and initiate the lock
        self.assertTrue(lockutils.is_locked(mock))

        mock.unlock()
        self.assertFalse(lockutils.is_locked(mock))


if __name__ == "__main__":
    unittest.main()