from __future__ import absolute_import
from __future__ import unicode_literals

import os
import Queue
import signal
import logging
import collections
import multiprocessing

from buckshot import errors
from buckshot import funcutils
from buckshot import constants
from buckshot.listener import Listener
from buckshot.tasks import TaskIterator, TaskRegistry


LOG = logging.getLogger(__name__)


class ProcessPoolDistrubor(object):
    def __init__(self, func, num_processes=None):
        self._num_processes = num_processes or constants.CPU_COUNT
        self._func = func
        self._processes = None

        self._in_queue = None   # worker tasks
        self._out_queue = None  # worker results

        self._task_registry = None
        self._tasks_in_progress = None  # tasks started with unreturned results
        self._task_results_waiting = None

    @property
    def is_started(self):
        return bool(self._processes)

    @property
    def is_completed(self):
        if not self.is_started:
            return False
        elif self._tasks_in_progress:
            return False
        return True

    def _is_alive(self, pid):
        process = next(x for x in self._processes if x.pid == pid)
        return  process.is_alive()

    @funcutils.lock_instance
    def start(self):
        self._processes = []
        self._task_registry = TaskRegistry()
        self._in_queue = multiprocessing.Queue(maxsize=self._num_processes)
        self._out_queue = multiprocessing.Queue()

        self._tasks_in_progress = collections.OrderedDict()
        self._task_results_waiting = {}

        listener = Listener(
            func=self._func,
            registry=self._task_registry,
            input_queue=self._in_queue,
            output_queue=self._out_queue
        )

        for _ in range(self._num_processes):
            process = multiprocessing.Process(target=listener)
            process.daemon = True  # Prevent zombies
            process.start()
            self._processes.append(process)

        return self

    def _send_task(self, task):
        self._in_queue.put_nowait(task)
        self._tasks_in_progress[task.id] = task

    def _recv_result(self):
        result = self._out_queue.get()  # blocks

        if result is errors.SubprocessError:
            raise result  # One of our workers failed.

        LOG.debug("Received task: %s", result.task_id)
        self._task_results_waiting[result.task_id] = result
        self._task_registry.remove(result.task_id)

    @funcutils.unlock_instance
    def imap(self, iterable):
        if not self.is_started:
            raise RuntimeError("Cannot process inputs: must call start() first.")

        def get_results():
            """Get a result from the worker output queue and try to yield
            results back to the caller.

            This yields results back in the order of their associated tasks.
            """
            self._recv_result()  # Get a result off the worker return queue

            # All this junk is to make sure we yield results in the order
            # of their associated tasks.
            tasks   = self._tasks_in_progress
            results = self._task_results_waiting

            for task_id in tasks.keys():
                if task_id not in results:
                    break

                del tasks[task_id]
                result = results.pop(task_id)
                yield result.value

        tasks = TaskIterator(iterable)
        task  = next(tasks)

        while True:
            try:
                self._send_task(task)
                task = next(tasks)
            except Queue.Full:
                for result in get_results():  # I wish I had `yield from`  :(
                    yield result
            except StopIteration:
                break

        while not self.is_completed:
            for result in get_results():
                yield result

    @funcutils.unlock_instance
    def imap_unordered(self, iterable):
        if not self.is_started:
            raise RuntimeError("Cannot process inputs: must call start() first.")

        def get_results():
            """Get a result from the worker output queue and try to yield
            results back to the caller.

            This yields results back in the order of their associated tasks.
            """
            self._recv_result()  # Get a result off the worker return queue

            tasks   = self._tasks_in_progress
            results = self._task_results_waiting

            for task_id in tasks.keys():
                if task_id in results:
                    del tasks[task_id]
                    result = results.pop(task_id)
                    yield result.value

        tasks = TaskIterator(iterable)
        task  = next(tasks)

        while True:
            try:
                self._send_task(task)
                task = next(tasks)
            except Queue.Full:
                for result in get_results():  # I wish I had `yield from`  :(
                    yield result
            except StopIteration:
                break

        while not self.is_completed:
            for result in get_results():
                yield result

    @funcutils.unlock_instance
    def stop(self):
        while self._processes:
            process = self._processes.pop()
            LOG.debug("Killing subprocess %s.", process.pid)
            os.kill(process.pid, signal.SIGTERM)

