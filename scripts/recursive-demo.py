"""
Demonstrate/test @distribute on recursive functions. Basically show that
it doesn't work correctly :(
"""
#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

import sys
import timeit
import logging
import functools

from buckshot import distribute
from buckshot import caches


def fib(x):
    if x < 2:
        return x
    return fib(x-1) + fib(x-2)

distributed_fib = distribute(fib)

@caches.memoize
def memoized_fib(x):
    if x < 2:
        return x
    return memoized_fib(x-1) + memoized_fib(x-2)

distributed_memoized_fib = distribute(memoized_fib)


def serial(numbers):
    return [fib(x) for x in numbers]


def main():
    numbers = range(32)
    benchmark = functools.partial(timeit.repeat, number=1, repeat=3)

    print("serial:                 ", benchmark(lambda: serial(numbers)))
    print("distributed:            ", benchmark(lambda: list(distributed_fib(numbers))))
    print("distributed + memoized: ", benchmark(lambda: list(distributed_memoized_fib(numbers))))

if __name__ == "__main__":
    if "-d" in sys.argv:
        logging.basicConfig(level=logging.DEBUG)
    main()
