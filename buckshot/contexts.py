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
    def __init__(self, func, processes=None):
        self._func = func
        self._size = processes or multiprocessing.cpu_count()
        self._process_map = {}
        self._in_queue = None
        self._out_queue = None

    def __len__(self):
        return len(self._process_map)

    @logutils.tracelog(LOG)
    def __enter__(self):
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
        self._kill_subprocesses()

    @logutils.tracelog(LOG)
    def __call__(self, iterable):
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
        return self._process_map[pid]

    def _register_process(self, process):
        self._process_map[process.pid] = process

    def _unregister_process(self, pid):
        try:
            del self._process_map[pid]
        except KeyError:
            LOG.warning("Attempted to unregister missing pid: %s", pid)

    def _kill_subprocesses(self, *args, **kwargs):
        """Handle any SIGCHLD signals by attempting to kill all spawned
        processes and raising a RuntimeError.

        This removes all processes from the internal process map.
        """
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)

        for pid in self._process_map.keys():
            LOG.debug("Killing subprocess %s.", pid)
            os.kill(pid, signal.SIGTERM)
            self._unregister_process(pid)




