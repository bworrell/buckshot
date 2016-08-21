#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

import time
import fractions

from buckshot import distributed


def harmonic_sum(x):
    hsum = 0
    for x in xrange(1, x + 1):
        hsum += fractions.Fraction(1, x)
    return hsum


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


def main():
    values = range(500, 1, -1)

    # Generate the harmonic sum for each value in values over a single thread.
    r1 = run_single(values)

    print()

    # Generate the harmonc sum for each value in values over multiple processes.
    r2 = run_multi(values)

    assert sorted(r1) == sorted(r2)

if __name__ == "__main__":
    main()