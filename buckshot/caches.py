from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import functools
import multiprocessing

from buckshot import lockutils

LOG = logging.getLogger(__name__)
DEFAULT_LRU_CACHE_SIZE = 256


class LRUCache(object):
    """A multiprocess LRU cache."""

    def __init__(self, size=DEFAULT_LRU_CACHE_SIZE):
        if not size:
            raise ValueError("LRUCache size must be > 0")

        self._size = 0
        self._max_size = size
        self._manager = multiprocessing.Manager()
        self._order = self._manager.list()
        self._storage = self._manager.dict()
        self._lock = multiprocessing.Lock()

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.put(key, value)

    @lockutils.with_lock("_lock")
    def keys(self):
        return list(self._order)

    @lockutils.with_lock("_lock")
    def values(self):
        return self._storage.values()

    @lockutils.with_lock("_lock")
    def get(self, key):
        value = self._storage[key]
        self._order.remove(key)  # This sucks and is O(n). Make me faster!
        self._order.insert(0, key)
        return value

    @lockutils.with_lock("_lock")
    def put(self, key, item):
        self._storage[key] = item

        try:
            self._order.remove(key)
        except ValueError:
            pass
        finally:
            self._order.insert(0, key)
            self._size += 1

        self._maintain_size()

    def _maintain_size(self):
        while self._size > self._max_size:
            key = self._order.pop()
            del self._storage[key]
            self._size -= 1


def memoize(cache_or_func):
    def keyfunc(args, kwargs):
        return unicode(args) + unicode(sorted(kwargs.iteritems()))

    def decorator(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            key = keyfunc(args, kwargs)
            try:
                retval = cache[key]
            except KeyError:
                cache[key] = retval = func(*args, **kwargs)
            return retval
        return inner

    if callable(cache_or_func):
        cache, func = LRUCache(), cache_or_func
        return decorator(func)

    cache = cache_or_func
    return decorator