buckshot
========

The ``buckshot`` library contains multiprocessing functions designed to help
developers create distributed applications quickly.


Installation
------------

The easiest way to install ``buckshot`` is via ``pip``:

::

    $ pip install buckshot


Usage
-----

Currently, there are two ways of leveraging ``buckshot`` to distribute work
across processes: the ``@distribute`` decorator and ``with distributed(...)``
context manager.

The following example shows how we can distibute the work of calculating
many harmonic sum values across processes.

Serial Approach
~~~~~~~~~~~~~~~

::

    import fractions

    def harmonic_sum(x):
        F = fractions.Fraction
        return sum(F(1, d) for d in xrange(1, x + 1))
    
    for value in range(1, 100):
        result = harmonic_sum(value)
        print result
        

Using ``@distribute``
~~~~~~~~~~~~~~~~~~~~~

::

    import fractions

    from buckshot import distribute

    @distribute(processes=4)
    def harmonic_sum(x):
        F = fractions.Fraction
        return sum(F(1, d) for d in xrange(1, x + 1))

    for result in harmonic_sum(range(1, 100)):  # Pass in values list.
        print result


Using ``with distributed(...)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    import fractions

    from buckshot import distributed

    def harmonic_sum(x):
        F = fractions.Fraction
        return sum(F(1, d) for d in xrange(1, x + 1))

    with distributed(harmonic_sum, processes=4) as distributed_harmonic_sum:
        for result in distributed_harmonic_sum(range(1, 100)):
            print result

All processes are destroyed when inputs are exhausted and/or the context is exited.


Features
~~~~~~~~

Both ``@distribute`` and ``with distributed`` accept arguments which alter the
behavior of the task distribution:

* ``processes``: The number of worker processes to spawn.
* ``timeout``: The number of seconds to wait before stopping work on a task
  and grabbing the next input. This will result in a ``TaskTimeout`` being
  passed back for that input.


Known Issues
------------

* Function inputs must be picklable.
* Function outputs must be picklable.
* If a child process is killed externally, ``buckshot`` will block forever waiting
  for results.
* This uses ``os.fork()`` under the hood, so there is a risk of rapidly exhausting
  memory.
* Recursion does not work well with the @distribute decorator and should be
  avoided.


LICENSE
-------

Read the LICENSE file for details.
