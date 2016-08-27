#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

import timeit
import random
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


def run_distribute(values, ordered):
    if ordered:
        harmonic_sum = ordered_distributed_harmonic_sum
    else:
        harmonic_sum = unordered_distributed_harmonic_sum
    return list(harmonic_sum(values))


def main():
    values = range(10)
    random.shuffle(values)

    print("Verifying results are the same across functions...", end=" ")
    r1 = run_serial(values)
    r2 = run_distribute(values, ordered=False)
    r3 = run_distribute(values, ordered=True)
    assert sorted(r1) == sorted(r2) == sorted(r3)
    print("All good!")

    print("Benchmarking...")
    values = range(1, 1000, 5)
    random.shuffle(values)

    benchmark = functools.partial(timeit.repeat, number=1, repeat=5)
    print("serial:                     ", benchmark(lambda: run_serial(values)))
    print("@distribute(ordered=False): ", benchmark(lambda: run_distribute(values, ordered=False)))
    print("@distribute(ordered=True):  ", benchmark(lambda: run_distribute(values, ordered=True)))

if __name__ == "__main__":
    main()