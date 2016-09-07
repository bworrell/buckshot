from __future__ import absolute_import
from __future__ import unicode_literals

import os

class SubprocessError(object):
    """Encapsulates an exception which may be raised in a worker subprocess."""

    def __init__(self, ex):
        self.pid = os.getpid()
        self.exception = ex

    def __unicode__(self):
        return unicode(self.exception)

    def __str__(self):
        return unicode(self).encode("utf-8")


class TaskTimeout(object):
    def __init__(self, task):
        self.pid = os.getpid()
        self.task = task

    @property
    def task_id(self):
        return self.task.id

    def __repr__(self):
        return "TaskTimeout(task=%s)" % (self.task_id)


class ThreadTimeout(Exception):
    """Raised when a Thread does not complete in the allowed time window."""
    pass
