"""
Context managers which can distribute workers (functions) across multiple
processes.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

__all__ = ["distributed"]

import logging

from buckshot import logutils
from buckshot.distributors import ProcessPoolDistributor

LOG = logging.getLogger(__name__)


class distributed(object):
    """Context manager that distributes an input worker function across
    multiple subprocesses.

    The object returned from ``with`` accepts an iterable object. Each item
    in the iterable object must match the input worker function *args.

    Args:
        func: A callable object to be distributed across subprocesses.
        processes (int): The number of subprocesses to spawn. If not
            provided, the number of CPUs on the system will be used.
    """

    def __init__(self, func, processes=None, ordered=True, timeout=None):
        self._ordered = bool(ordered)
        self._distributor = ProcessPoolDistributor(
            func=func,
            num_processes=processes,
            timeout=timeout
        )

    @logutils.tracelog(LOG)
    def __enter__(self):
        self._distributor.start()
        return self

    @logutils.tracelog(LOG)
    def __exit__(self, ex_type, ex_value, traceback):
        """Kill any spawned subprocesses."""
        self._distributor.stop()

    @logutils.tracelog(LOG)
    def __call__(self, iterable):
        """Map each item in the input `iterable` to our worker subprocesses.
        When results become available, yield them to the caller.

        Args:
            iterable: An iterable collection of *args to be passed to the
                worker function. For example: [(1,), (2,), (3,)]

        Yields:
            Results from the worker function.
        """
        if self._ordered:
            imap = self._distributor.imap
        else:
            imap = self._distributor.imap_unordered

        for result in imap(iterable):
            yield result
