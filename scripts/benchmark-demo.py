#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

import sys
import timeit
import random
import logging
import functools
import fractions

import buckshot


def harmonic_sum(x):
    hsum = 0
    for x in xrange(1, x + 1):
        hsum += fractions.Fraction(1, x)
    return hsum


@buckshot.distribute(ordered=False)
def unordered_distributed_harmonic_sum(x):
    return harmonic_sum(x)


@buckshot.distribute(ordered=True)
def ordered_distributed_harmonic_sum(x):
    return harmonic_sum(x)


def run_serial(values):
    return [harmonic_sum(x) for x in values]


def main():
    values = range(10)
    random.shuffle(values)

    print("Verifying results are the same across functions...", end=" ")
    r1 = run_serial(values)
    r2 = list(unordered_distributed_harmonic_sum(values))
    r3 = list(ordered_distributed_harmonic_sum(values))

    assert r1 == r3
    assert sorted(r1) == sorted(r2) == sorted(r3)
    print("All good!")

    print("Benchmarking...")
    values = range(1000, 2000, 50)


    benchmark = functools.partial(timeit.repeat, number=1, repeat=3)
    print("serial:                     ", benchmark(lambda: run_serial(values)))
    print("@distribute(ordered=False): ", benchmark(lambda: list(ordered_distributed_harmonic_sum(values))))
    print("@distribute(ordered=True):  ", benchmark(lambda: list(unordered_distributed_harmonic_sum(values))))


if __name__ == "__main__":
    if "-d" in sys.argv:
        logging.basicConfig(level=logging.DEBUG)

    main()