buckshot
--------

Multiprocessing tools built on top of the Python ``multiprocessing``
module.


Overview
--------

I tried to use a ``multiprocessing.Pool`` on with a nested function and then
an instance method as my ``target`` and it exploded because those can't be
pickled. Argh.

I decided to make something similar to ``Pool`` that could map inputs to
target nested functions/instance methods.

Then I went off the rails and decided to go for a quasi-RAII-but-not-actually-RAII
design (e.g., using ``with`` for process creation and teardown).


Installation
------------

Coming soon...


Usage
-----

Right now there is only really two components of ``buckshot`` worth using:
``distributed`` and ``distribute``:

::

    import fractions

    from buckshot import distributed

    def harmonic_sum(x)
        F = fractions.Fraction
        return sum(F(1, d) for d in xrange(1, x + 1))

    with distributed(harmonic_sum, processes=4) as mapfunc:
        # mapfunc is an object which can be called with an iterable input.
        # The values in that iterable will be passed to worker processes
        # that will execute the `harmonic_sum` function on each input value.
        # This is similar to the imap() method on multiprocessing.Pool.

        for result in mapfunc(xrange(1, 100)):
            print result

    # All spawned subprocesses will be destroyed when exiting the context
    # manager.

Another way of writing the above code is using the ``distribute`` decorator:

::

    import fractions

    from buckshot import distribute

    @distribute(processes=4)  # <-- Added the decorator
    def harmonic_sum(x)
        F = fractions.Fraction
        return sum(F(1, d) for d in xrange(1, x + 1))

    # The harmonic_sum(...) function has been rewritten as a generator
    # which leverages the `distributed` context manager underneath to
    # pass values from the input iterable to worker processes.
    for result in harmonic_sum(xrange(1, 100)):
        print result

    # All spawned subprocesses will be destroyed when exiting the decorated
    # function.


Disclaimer
----------

I probably should have read the ``multiprocessing`` documentation more.
Everything in ``buckshot`` is likely already implemented there, and done much
much better.

**USE AT YOUR OWN RISK!**


LICENSE
-------

Read the LICENSE file for details.
