import logging

from buckshot.version import __version__
from buckshot.contexts import distributed
from buckshot.decorators import distribute

# Fixes the "No handler found..." error.
logging.getLogger("buckshot").addHandler(logging.NullHandler())
