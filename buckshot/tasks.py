from __future__ import absolute_import
from __future__ import unicode_literals

import collections

from buckshot import datautils


class NoResult(object):
    pass


class Task(object):
    __slots__ = ["id", "args"]

    def __init__(self, id, args):
        self.id = id
        self.args = args


class Result(object):
    __slots__ = ["task_id", "value"]

    def __init__(self, task_id, value):
        self.task_id = task_id
        self.value = value


class TaskIterator(collections.Iterator):
    def __init__(self, args):
        args = datautils.iterargs(args)
        self._iter = (Task(id, arguments) for id, arguments in enumerate(args))

    def next(self):
        return next(self._iter)



