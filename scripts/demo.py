#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

import time
import fractions

import buckshot


def harmonic_sum(x):
    hsum = 0
    for x in xrange(1, x + 1):
        hsum += fractions.Fraction(1, x)
    return hsum


@buckshot.distribute(ordered=False)
def distributed_harmonic_sum(x):
    return harmonic_sum(x)


@buckshot.distribute(ordered=True)
def ordered_distributed_harmonic_sum(x):
    return harmonic_sum(x)


def run_single(values):
    print("Starting single process run...")
    results = []
    single_process_time = time.time()

    for val in values:
        results.append(harmonic_sum(val))

    print("Single process: %s" % (time.time() - single_process_time))
    return results


def run_distribute(values, ordered):
    print("Starting multi-process run via @distribute with ordered = %s..." % ordered)

    multi_process_time = time.time()
    results = list(distributed_harmonic_sum(values))

    print("Multi process via @distribute: %s" % (time.time() - multi_process_time))
    return results


def main():
    values = range(750, 0, -1)

    # Generate the harmonic sum for each value in values over a single thread.
    r1 = run_single(values)

    print()

    # Generate the harmonic sum for each value in values using the @distrubute
    # decorated function and ordered=False

    r2 = run_distribute(values, ordered=False)

    print()

    # Generate the harmonic sum for each value in values using the @distrubute
    # decorated function and ordered=True.

    r3 = run_distribute(values, ordered=True)

    assert sorted(r1) == sorted(r2) == sorted(r3)

if __name__ == "__main__":
    main()