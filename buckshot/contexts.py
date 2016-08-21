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
import multiprocessing
import Queue

from buckshot import logutils
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

    def __init__(self, func, processes=None):
        self._func = func
        self._size = processes or multiprocessing.cpu_count()
        self._process_map = {}
        self._in_queue = None
        self._out_queue = None

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

        # If any of our child processes are killed or die unexpectedly,
        # abort. TODO: Make this more durable.
        signal.signal(signal.SIGCHLD, self._kill_subprocesses)

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
                worker function.

        Yields:
            Results from the worker function. If a subprocess error occurs,
            the result value will be an instance of errors.SubprocessError.
        """
        send_counter = itertools.count(1)
        recv_counter = itertools.count(1)
        num_sent = num_recv = 0

        iteritems = iter(iterable)
        item = next(iteritems)

        while True:
            try:
                self._in_queue.put_nowait(item)
                num_sent = next(send_counter)
                item = next(iteritems)
            except Queue.Full:
                retval = self._out_queue.get()
                num_recv = next(recv_counter)
                yield retval
            except StopIteration:
                break

        while num_recv < num_sent:
            yield self._out_queue.get()
            num_recv = next(recv_counter)

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

    def _kill_subprocesses(self, *args, **kwargs):
        """Handle any SIGCHLD signals by attempting to kill all spawned
        processes.

        This removes all processes from the internal process map.

        Raises:
            RuntimeError: If a SIGCHLD signal was caught during processing.
        """
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)

        for pid in self._process_map.keys():
            LOG.debug("Killing subprocess %s.", pid)
            os.kill(pid, signal.SIGTERM)
            self._unregister_process(pid)

        # FIXME: Make this better.
        # If a SIGCHLD signal occurred during processes, raise a RuntimeError.
        if args or kwargs:
            raise RuntimeError("SIGCHLD occurred")




