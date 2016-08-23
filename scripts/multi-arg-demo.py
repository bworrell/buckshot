#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

import os
import time
import random

from buckshot import distribute


@distribute
def sleep_and_square(x, y):
    pid = os.getpid()

    print("%s sleeping for %f seconds" % (pid, x))
    time.sleep(x)

    return x, y


def main():
    values = [(random.random(), x) for x in xrange(1, 25)]
    results = list(sleep_and_square(values))

    print("\nReceived %s values" % len(results))
    print(results)


if __name__ == "__main__":
    main()