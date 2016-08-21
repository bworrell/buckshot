#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

import time
import fractions

from buckshot.contexts import distributed
from buckshot.decorators import distribute


def harmonic_sum(x):
    hsum = 0
    for x in xrange(1, x + 1):
        hsum += fractions.Fraction(1, x)
    return hsum


@distribute
def distributed_harmonic_sum(x):
    return harmonic_sum(x)


def run_single(values):
    print("Starting single process run...")
    results = []
    single_process_time = time.time()

    for val in values:
        results.append(harmonic_sum(val))

    print("Single process: %s" % (time.time() - single_process_time))
    return results


def run_multi(values):
    print("Starting multi-process run...")

    multi_process_time = time.time()

    with distributed(harmonic_sum) as func:
        results = list(func(values))

    print("Multi process: %s" % (time.time() - multi_process_time))
    return results


def run_distribute(values):
    print("Starting multi-process run via @distribute...")

    multi_process_time = time.time()
    results = list(distributed_harmonic_sum(values))

    print("Multi process via @distribute: %s" % (time.time() - multi_process_time))
    return results


def main():
    values = range(500, 1, -1)

    # Generate the harmonic sum for each value in values over a single thread.
    r1 = run_single(values)

    print()

    # Generate the harmonic sum for each value in values over multiple processes.
    r2 = run_multi(values)

    print()

    # Generate the harmonic sum for each value in values using the @distrubute
    # decorated function.

    r3 = run_distribute(values)

    assert sorted(r1) == sorted(r2) == sorted(r3)

if __name__ == "__main__":
    main()