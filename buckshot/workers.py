from __future__ import absolute_import
from __future__ import unicode_literals

import os
import logging

from buckshot import errors
from buckshot import signals
from buckshot import tasks
from buckshot import threads

LOG = logging.getLogger(__name__)


class Suicide(Exception):
    """Raised when a Listener kills itself."""
    pass


class Worker(object):
    """Listens for tasks on an input queue, passes the task to the worker
    function, and returns the results on the output queue.

    If we receive a signals.StopProcessing object, we send back our process
    id and die.

    If a task times out, send back a errors.TaskTimeout object.
    """

    def __init__(self, func, input_queue, output_queue, timeout=None):
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._thread_func = threads.isolated(
            target=func,
            daemon=True,
            timeout=timeout
        )

    def _recv(self):
        """Get a message off of the input queue. Block until something is
        received.

        If a signals.StopProcessing message is received, die.
        """
        task = self._input_queue.get()

        if task is signals.StopProcessing:
            self._die()

        return task

    def _send(self, result):
        """Put the `value` on the output queue."""
        LOG.debug("Sending result: %s", os.getpid())
        self._output_queue.put(result)

    def _die(self):
        """Send a signals.Stopped message across the output queue and raise
        a Suicide exception.
        """
        LOG.debug("Received StopProcessing")
        self._send(signals.Stopped(os.getpid()))
        raise Suicide()

    def _process_task(self, task):
        try:
            LOG.info("%s starting task %s", os.getpid(), task.id)
            success, result = True, self._thread_func(*task.args)
        except threads.ThreadTimeout:
            LOG.error("Task %s timed out", task.id)
            success, result = False, errors.TaskTimeout(task)
        return success, tasks.Result(task.id, result)

    def __call__(self, *args):
        """Listen for values on the input queue, hand them off to the worker
        function, and send results across the output queue.
        """
        continue_ = True

        while continue_:
            try:
                task = self._recv()
            except Suicide:
                return
            except Exception as ex:
                retval = errors.SubprocessError(ex)
            else:
                continue_, retval = self._process_task(task)
            self._send(retval)

