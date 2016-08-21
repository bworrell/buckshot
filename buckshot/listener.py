from __future__ import absolute_import
from __future__ import unicode_literals

import os
import logging

from buckshot import errors
from buckshot import signals

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

    def __init__(self, func, input_queue, output_queue):
        self._func = func
        self._input_queue = input_queue
        self._output_queue = output_queue

    def _recv(self):
        """Get a message off of the input queue. Block until something is
        received.

        If a signals.StopProcessing message is received, die.
        """
        val = self._input_queue.get()

        if val is signals.StopProcessing:
            self._die()

        return val

    def _send(self, value):
        """Put the `value` on the output queue."""
        self._output_queue.put(value)  # FIXME: add a timeout?

    def _die(self):
        """Send a signals.Stopped message across the output queue and raise
        a Suicide exception.
        """
        LOG.debug("Received StopProcessing")
        self._send(signals.Stopped(os.getpid()))
        raise Suicide()

    def __call__(self, *args):
        """Listen for values on the input queue, hand them off to the worker
        function, and send results across the output queue.
        """
        while True:
            try:
                args = self._recv()
                retval = self._func(*(args,))  # This is kinda ugly...
            except Suicide:
                return
            except Exception as ex:
                retval = errors.SubprocessError(ex)
            self._send(retval)
