from __future__ import absolute_import
from __future__ import unicode_literals

import os
import logging
import threading

from buckshot import errors
from buckshot import signals
from buckshot import tasks
from buckshot import constants

LOG = logging.getLogger(__name__)


class Suicide(Exception):
    """Raised when a Listener kills itself."""
    pass


class Listener(object):
    """Listens for input messages, hands it off to the registered handler
    and sends the handler results back on on the output queue.

    If we receive a signals.StopProcessing object, we send back our process
    id and die.
    """

    def __init__(self, func, registry, input_queue, output_queue, timeout=None):
        self._func = func
        self._registry = registry
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._timeout = timeout or constants.TASK_TIMEOUT

    def _recv(self):
        """Get a message off of the input queue. Block until something is
        received.

        If a signals.StopProcessing message is received, die.
        """
        task = self._input_queue.get()

        if task is signals.StopProcessing:
            self._die()

        self._registry.register(task)
        return task

    def _send(self, result):
        """Put the `value` on the output queue."""
        self._output_queue.put(result)

    def _die(self):
        """Send a signals.Stopped message across the output queue and raise
        a Suicide exception.
        """
        LOG.debug("Received StopProcessing")
        self._send(signals.Stopped(os.getpid()))
        raise Suicide()

    def _thread_worker(self, task, result_list):
        try:
            result = self._func(*task.args)
        except Exception as ex:
            result = ex
        result_list.append(tasks.Result(task.id, result))

    def _run_worker(self, task):
        queue  = []  # Queue.Queue doesn't seem to work...
        thread = threading.Thread(target=self._thread_worker, args=(task, queue))
        thread.daemon = True
        thread.start()
        thread.join(self._timeout)  # Block until thread is done or timeout is reached

        try:
            result = queue.pop()
        except IndexError:
            raise errors.TaskTimeout("Timeout error", task.id)
        return result

    def __call__(self, *args):
        """Listen for values on the input queue, hand them off to the worker
        function, and send results across the output queue.
        """
        is_running = True

        while is_running:
            try:
                task = self._recv()
            except Suicide:
                return
            except errors.TaskTimeout as ex:
                retval = ex
                is_running = False
            except Exception as ex:
                retval = errors.SubprocessError(ex)
            else:
                retval = self._run_worker(task)
            self._send(retval)
