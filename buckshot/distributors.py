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
    """Distributes an input function across multiple processes.

    Args:
        func: The function to run in each process.
        num_processes: The number of worker processes to spawn.
        timeout: The maximum amount of time to wait for a result from
            a worker process. Default is None (unbounded).
    """

    def __init__(self, func, num_processes=None, timeout=None):
        self._num_processes = num_processes or constants.CPU_COUNT
        self._func = func
        self._processes = None
        self._timeout = None

        self._task_queue = None   # worker tasks
        self._result_queue = None  # worker results

        self._task_registry = None
        self._tasks_in_progress = None  # tasks started with unreturned results
        self._task_results_waiting = None

    @property
    def is_started(self):
        """Return True if the worker have been started."""
        return bool(self._processes)

    @property
    def is_completed(self):
        """Return True if all tasks have been picked up and associated results
        have been returned to the caller.
        """
        if not self.is_started:
            return False
        elif self._tasks_in_progress:
            return False
        return True

    def _is_alive(self, pid):
        """Return True if the worker process associated with the pid is alive."""
        process = next(x for x in self._processes if x.pid == pid)
        return  process.is_alive()

    @funcutils.lock_instance
    def start(self):
        """Start the worker processes and return self.

        * Create an input and output queue for worker processes to receive
          tasks and send results.
        * Create a task registry so worker processes can identify what
          task they are working on.

        Note:
            This creates Processes with `daemon=True`, so if the parent process
            dies the child processes will be killed.
        """

        self._processes = []
        self._task_registry = TaskRegistry()
        self._result_queue = multiprocessing.Queue(maxsize=self._num_processes)
        self._task_queue = multiprocessing.Queue()

        self._tasks_in_progress = collections.OrderedDict()
        self._task_results_waiting = {}

        listener = Listener(
            func=self._func,
            registry=self._task_registry,
            input_queue=self._task_queue,
            output_queue=self._result_queue
        )

        for _ in xrange(self._num_processes):
            process = multiprocessing.Process(target=listener)
            process.daemon = True  # This will die if parent process dies.
            process.start()
            self._processes.append(process)

        return self

    def _send_task(self, task):
        self._task_queue.put_nowait(task)
        self._tasks_in_progress[task.id] = task

    def _recv_result(self):
        result = self._result_queue.get(timeout=self._timeout)  # blocks

        if result is errors.SubprocessError:
            raise result  # One of our workers failed.

        LOG.debug("Received task: %s", result.task_id)
        self._task_results_waiting[result.task_id] = result
        self._task_registry.remove(result.task_id)

    @funcutils.unlock_instance
    def imap(self, iterable):
        """Send each argument tuple in `iterable` to a worker process and
        yield results.

        Args:
            iterable: An iterable collection of argument tuples. These tuples
                are in the form expected of the work function. E.g., if the
                work function signature is ``def foo(x, y)`` the `iterable`
                will look like [(1, 2), (3, 4), ...].

        Yields:
            Results from the work function. The results will be returned in
            order of their associated inputs.
        """
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
        """Send each argument tuple in `iterable` to a worker process and
        yield results.

        Args:
            iterable: An iterable collection of argument tuples. These tuples
                are in the form expected of the work function. E.g., if the
                work function signature is ``def foo(x, y)`` the `iterable`
                will look like [(1, 2), (3, 4), ...].

        Yields:
            Results from the work function. The results are yielded in the
            order they are received from worker processes.
        """
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
        """Kill all child processes and clear results."""

        while self._processes:
            process = self._processes.pop()
            LOG.debug("Killing subprocess %s.", process.pid)
            os.kill(process.pid, signal.SIGTERM)

        self._processes = None
        self._task_queue = None
        self._result_queue = None
        self._task_registry = None
        self._tasks_in_progress = None
        self._task_results_waiting = None
