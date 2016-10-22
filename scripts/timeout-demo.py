#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

import os
import sys
import time
import logging

from buckshot import distribute


TIMEOUT = 1.5


@distribute(timeout=TIMEOUT)
def sleep_and_unicode(x):
    print("Process %s sleeping for %f seconds" % (os.getpid(), x))
    time.sleep(x)
    return unicode(x)


def main():
    print("Using timeout value of %s seconds" % TIMEOUT)

    values = [1, 25] * 3
    results = list(sleep_and_unicode(values))

    print("\nReceived %s values" % len(results))
    print(results)


if __name__ == "__main__":
    if "-d" in sys.argv:
        logging.basicConfig(level=logging.DEBUG)
    main()
