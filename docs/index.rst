.. lapis documentation master file, created by
   sphinx-quickstart on Tue Mar 26 16:10:43 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

LAPIS -- Simulations for Opportunistic Resources
================================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   source/topics/overview
   source/glossary

The LAPIS simulator enables the simulation of job execution and scheduling with
a focus on :term:`opportunistic resources <Opportunistic Resource>`. The
scheduling internally builds on concepts from `HTCondor`_. The
:term:`opportunistic resources <Opportunistic Resource>` are managed building on
the projects `TARDIS`_ and `COBalD`_.
The simulation builds on importing well-established input formats to generate
the jobs and set up the infrastructure either in an opportunistic or
classical fashion.

Simple Command Line Interface
-----------------------------

Although LAPIS is written to provide an extensive framework for setting up
advanced simulation, it also provides a simple command line interface to get you
started quickly.

You have the options to start in 1) static, 2) dynamic as well as 3) hybrid
mode enabling you to compare the various simulation outputs.

.. code-block:: bash

   python cli/simulate.py --log-file - static --job-file <path-to-workload> swf \
      --pool-file <path-to-pool-definition> htcondor

As you can see from the example, you can even mix and match different input
formats to create your required simulation environment. An extensive documentation
about the CLI can be found in the :doc:`source/topics/cli` chapter.

Simple Framework for Advanced Use Cases
---------------------------------------

The implementation of the simulation itself builds on the lightweight simulation
framework `μSim`_. Due to the human-centric API of μSim, it is a charm to actually
read and extend the simulation for adaptation to various use cases.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _HTCondor: https://research.cs.wisc.edu/htcondor/
.. _COBalD: https://cobald.readthedocs.io/en/latest/
.. _TARDIS: https://cobald-tardis.readthedocs.io/en/latest
.. _μSim: https://usim.readthedocs.io/en/latest/
