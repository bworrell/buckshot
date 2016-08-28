from __future__ import absolute_import
from __future__ import unicode_literals

import os
import collections
import multiprocessing

from buckshot import datautils


class Task(object):
    """Encapsulates worker function inputs"""

    __slots__ = ["id", "args"]

    def __init__(self, id, args):
        self.id = id
        self.args = args


class Result(object):
    """Encapsulates worker function return values."""

    __slots__ = ["task_id", "value"]

    def __init__(self, task_id, value):
        self.task_id = task_id
        self.value = value


class TaskIterator(collections.Iterator):
    """Iterator which yields Task objects for the input argument tuples.

    Args:
        args: An iterable collection of argument tuples. E.g., [(0,1), (2,3), ...]
    """

    def __init__(self, args):
        args = datautils.iterargs(args)
        self._iter = (Task(id, arguments) for id, arguments in enumerate(args))

    def next(self):
        return next(self._iter)


class TaskRegistry(object):
    """Registry of Task objects.

    When worker subprocesses pick up tasks, they notify the registry of that
    they received it and are working on it.

    Consumers of task Results should remove registry items before returning
    results to their caller.
    """

    def __init__(self):
        manager = multiprocessing.Manager()
        self._task2pid = manager.dict()

    def register(self, task):
        self._task2pid[task.id] = os.getpid()

    def remove(self, task_id):
        del self._task2pid[task_id]

    def processes(self):
        return sorted(set(self._task2pid.itervalues()))

    def tasks(self):
        return self._task2pid.keys()
