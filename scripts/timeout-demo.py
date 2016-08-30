#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

import os
import sys
import time
import logging

from buckshot import distribute


@distribute(timeout=2.0)
def sleep_and_unicode(x):
    print("%s sleeping for %f seconds" % (os.getpid(), x))
    time.sleep(x)
    return unicode(x)


def main():
    values = [1, 3, 1, 3]
    results = list(sleep_and_unicode(values))

    print("\nReceived %s values" % len(results))
    print(results)


if __name__ == "__main__":
    if "-d" in sys.argv:
        logging.basicConfig(level=logging.DEBUG)
    main()