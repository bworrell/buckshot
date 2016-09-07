from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import threading

from buckshot import errors


LOG = logging.getLogger(__name__)


def isolated(target, daemon=False, timeout=None):
    """Run the input function in an isolated thread.

    Note:
        If a timeout is specified and it expires, the thread does not stop.
        There is no way (that I know of) to kill a thread without killing
        the parent process.

    Args:
        func: The thread target function.
        daemon: If True, daemonize the thread.
        timeout: The maximum allowable time the thread can spend executing.
            If None, there is no timeout.
    """
    def wrapped(func):
        """Call the target function and append the result to the queue."""
        def inner(queue, *args):
            queue.append(func(*args))
        return inner

    def inner(*args):
        queue = []
        args = (queue,) + args
        thread = threading.Thread(target=wrapped(target), args=args)
        thread.daemon = daemon
        thread.start()
        thread.join(timeout)

        try:
            result = queue.pop()
        except IndexError:
            raise errors.ThreadTimeout("Thread timed out.")
        return result

    return inner

