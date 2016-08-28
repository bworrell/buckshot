from __future__ import absolute_import
from __future__ import unicode_literals

import os


class SubprocessError(Exception):
    """Encapsulates an exception which may be raised in a worker subprocess."""

    def __init__(self, ex):
        super(SubprocessError, self).__init__(unicode(ex))
        self.pid = os.getpid()
        self.exception = ex


class TaskTimeout(Exception):
    def __init__(self, msg, task):
        super(TaskTimeout, self).__init__(msg)
        self.task = task

