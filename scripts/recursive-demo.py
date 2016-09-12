"""
Demonstrate/test @distribute on recursive functions.
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


def fib(x):
    if x < 2:
        return x
    return fib(x-1) + fib(x-2)


# Make a @distribute version of `fib`
fastfib = distribute(fib)


def serial(numbers):
    return [fib(x) for x in numbers]


def distributed(numbers):
    return list(fastfib(numbers))


def main():
    numbers = range(32)
    benchmark = functools.partial(timeit.repeat, number=1, repeat=3)
    print("serial:      ", benchmark(lambda: serial(numbers)))
    print("distributed: ", benchmark(lambda: distributed(numbers)))


if __name__ == "__main__":
    if "-d" in sys.argv:
        logging.basicConfig(level=logging.DEBUG)
    main()
