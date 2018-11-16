===============================================================================
Lapis is an adaptable, performant, and interactive scheduling (Lapis) simulator
===============================================================================

The ``lapis`` library provides a framework and runtime for simulating the scheduling and usage of opportunistic
and static resources.

Command Line Interface
----------------------

Currently the library provides a simple command line interface that allows three modes of operation:

* static provisioning of resources,
* dynamic provisioning of resources, and
* hybrid provisioning of resources.

In the most simple case you can apply a given workload, e.g. downloaded from the parallel workload archive to a
static resource configuration:


.. code:: bash

    python3 simulate.py --log-file - static --job-file <path-to-workload> swf --pool-file <path-to-pool-definition> htcondor

The output of simulation is given to stdout. You have further options you can explore via

.. code:: bash

    python3 simulate.py --help

and more specifically for the different operation modes with

.. code:: bash

    python3 simulate.py static --help
