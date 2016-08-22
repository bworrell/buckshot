from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import itertools

LOG = logging.getLogger(__name__)


def as_tuple(x):
    """Convert the input value into a tuple of one item."""
    try:
        return tuple(x)
    except TypeError:
        return (x,)


def iterargs(it):
    """Convert each item in the input iterable `it` into a tuple.

    Note:
        This assumes all items in `it` are the same type. If the first
        item is a tuple, it is assumed the rest are as well. If not,
        all will be converted into tuples.

    Yields:
        Each item `it` as a tuple.
    """
    items = iter(it)
    first = next(items)
    items = itertools.chain((first,), items)

    if not isinstance(first, tuple):
        items = (as_tuple(x) for x in items)

    for item in items:
        yield item

