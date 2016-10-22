from __future__ import absolute_import
from __future__ import unicode_literals

import os
import collections

from buckshot import datautils


class Task(object):
    """Encapsulates worker function inputs"""

    __slots__ = ["id", "args"]

    def __init__(self, id, args):
        self.id = id
        self.args = args

    def __repr__(self):
        return "Task(%r, %r)" % (self.id, self.args)


class Result(object):
    """Encapsulates worker function return values."""

    __slots__ = ["task_id", "value", "pid"]

    def __init__(self, task_id, value):
        self.task_id = task_id
        self.value = value
        self.pid = os.getpid()

    def __repr__(self):
        return "Result(%r, %r)" % (self.task_id, self.value)


class TaskIterator(collections.Iterator):
    """Iterator which yields Task objects for the input argument tuples.

    Args:
        args: An iterable collection of argument tuples. E.g., [(0,1), (2,3), ...]
    """

    def __init__(self, args):
        args = datautils.iter_tuples(args)
        self._iter = (Task(id, arguments) for id, arguments in enumerate(args))

    def next(self):
        return next(self._iter)
