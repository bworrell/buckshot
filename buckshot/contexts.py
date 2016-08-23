"""
Context managers which can distribute workers (functions) across multiple
processes.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import os
import signal
import logging
import itertools
import collections
import multiprocessing
import Queue


from buckshot import errors
from buckshot import logutils
from buckshot import procutils
from buckshot import datautils
from buckshot import constants
from buckshot.tasks import TaskIterator
from buckshot.listener import Listener

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

    def __init__(self, func, processes=None, ordered=True):
        self._func = func
        self._size = processes or constants.CPU_COUNT
        self._ordered = ordered
        self._process_map = {}

        self._in_queue = None
        self._out_queue = None

        self._num_sent = 0  # Number of tasks sent
        self._num_recv = 0  # Number of results received

        self._task_queue  = collections.deque()
        self._uncollected = {}

    def __len__(self):
        """Return the number of processes registered."""
        return len(self._process_map)

    @logutils.tracelog(LOG)
    def __enter__(self):
        """Initialize our subprocesses and return self."""

        self._process_map = {}
        self._in_queue = multiprocessing.Queue(maxsize=self._size)
        self._out_queue = multiprocessing.Queue()

        listener = Listener(
            func=self._func,
            input_queue=self._in_queue,
            output_queue=self._out_queue
        )

        for _ in range(self._size):
            process = multiprocessing.Process(target=listener)
            process.start()
            self._process_map[process.pid] = process

        return self

    @logutils.tracelog(LOG)
    def __exit__(self, ex_type, ex_value, traceback):
        """Kill any spawned subprocesses."""
        self._kill_subprocesses()

    @logutils.tracelog(LOG)
    def __call__(self, iterable):
        """Map each item in the input `iterable` to our worker subprocesses.
        When results become availble, yield them to the caller.

        Args:
            iterable: An iterable collection of *args to be passed to the
                worker function. For example: [(1,), (2,), (3,)]

        Yields:
            Results from the worker function. If a subprocess error occurs,
            the result value will be an instance of errors.SubprocessError.
        """
        # Convert the input iterable into Task objects.
        itertasks = TaskIterator(iterable)
        task = next(itertasks)

        while True:
            try:
                self._send_task(task)
                task = next(itertasks)
            except Queue.Full:
                self._recv_result()

                for result in self._results_to_yield():  # I wish I had `yield from`  :(
                    yield result.value

            except StopIteration:
                break

        while not self.is_finished:
            self._recv_result()
            for result in self._results_to_yield():
                yield result.value

    @property
    def is_finished(self):
        return not len(self._uncollected)

    @property
    def results_ready(self):
        if not self._task_queue:
            return False

        return self._task_queue[0].id in self._uncollected

    def _send_task(self, task):
        self._in_queue.put_nowait(task)
        self._task_queue.append(task)

    def _recv_result(self):
        result = self._out_queue.get()

        if result is errors.SubprocessError:
            raise result  # One of our workers failed.

        self._uncollected[result.task_id] = result

    def _results_to_yield(self):
        while self.results_ready:
            task   = self._task_queue.popleft()
            result = self._uncollected.pop(task.id)
            yield result

    def _lookup_process(self, pid):
        """Get the Process object associated with the pid."""
        return self._process_map[pid]

    def _register_process(self, process):
        """Register the process in the internal process map."""
        assert process.pid not in self._process_map
        self._process_map[process.pid] = process

    def _unregister_process(self, pid):
        """Attempt to remove the processes associated with the pid from
        the internal process map.
        """
        try:
            del self._process_map[pid]
        except KeyError:
            LOG.warning("Attempted to unregister missing pid: %s", pid)

    @procutils.suppress(signal.SIGCHLD)
    def _kill_subprocesses(self):
        """Handle any SIGCHLD signals by attempting to kill all spawned
        processes.

        This removes all processes from the internal process map.

        Raises:
            RuntimeError: If a SIGCHLD signal was caught during processing.
        """
        for pid in self._process_map.keys():
            LOG.debug("Killing subprocess %s.", pid)
            os.kill(pid, signal.SIGTERM)
            self._unregister_process(pid)


