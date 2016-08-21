"""
Special objects that are passed between parent and child processes to
communicate state.
"""

class StopProcessing(object):
    """Tells a Listner to stop processing."""
    pass


class Stopped(object):
    """Notifies a process manager that a subprocess has been stopped."""

    def __init__(self, pid):
        self.pid = pid
