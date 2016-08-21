"""
Process utility functions.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import signal
import logging
import functools

LOG = logging.getLogger(__name__)

def suppress(sig, default=True):
    """Suppressthe input signal during the execution of the wrapped
    function. Upon exit, return the signal handler to its default value.
    """
    def decorator(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            handler = signal.SIG_DFL if default else signal.getsignal(sig)
            signal.signal(sig, signal.SIG_IGN)

            try:
                return func(*args, **kwargs)
            finally:
                signal.signal(sig, handler)
        return inner
    return decorator