from __future__ import absolute_import
from __future__ import unicode_literals


class SubprocessError(object):
    """Encapsulates an exception which may be raised in a worker subprocess."""

    def __init__(self, ex):
        self.exception = ex

    def __unicode__(self):
        return unicode(self.exception)

    def __str__(self):
        return unicode(self).encode("utf-8")

