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
   source/changelog

The LAPIS simulator enables the simulation of :term:`job` execution and scheduling
with a focus on :term:`opportunistic resources <Opportunistic Resource>`. The
scheduling internally builds on concepts from `HTCondor`_. The
:term:`opportunistic resources <Opportunistic Resource>` are managed building on
the projects `TARDIS`_ and `COBalD`_.
The simulation builds on importing well-established input formats to generate
the :term:`jobs <Job>` and set up the infrastructure either in an opportunistic
or classical fashion.

Simple Command Line Interface
-----------------------------

Although LAPIS is written to provide an extensive framework for setting up
advanced simulation, it also provides a simple command line interface to get you
started quickly.

You have the options to start in 1) static, 2) dynamic, or 3) hybrid
mode enabling you to compare the various simulation scenarios.

.. content-tabs::

   .. tab-container:: static
      :title: Static

      The *static* environment provides a classical setup where all resources
      are available exclusively for processing the :term:`jobs <Job>` for the
      whole runtime of the simulation.

      .. code-block:: bash

         python cli/simulate.py --log-file - static --job-file <path-to-workload> swf \
            --pool-file <path-to-pool-definition> htcondor

   .. tab-container:: dynamic
      :title: Dynamic

      The *dynamic* environment builds on volatile, opportunistic resources
      exclusively. Based on the amount of :term:`jobs <Job>` being processed
      within the simulation COBalD controllers decide about the integration and
      disintegration of resources.

      .. code-block:: bash

         python cli/simulate.py --log-file - dynamic --job-file <path-to-workload> swf \
            --pool-file <path-to-pool-definition> htcondor

   .. tab-container:: hybrid
      :title: Hybrid

      The *hybrid* simulation environment provides a baseline of static resources
      that are available for the whole runtime of the simulation. These static
      resources are dynamically complemented with volatile, opportunistic
      resources based on current :term:`job` pressure.

      .. code-block:: bash

         python cli/simulate.py --log-file - hybrid --job-file <path-to-workload> swf \
            --static-pool-file <path-to-pool-definition> htcondor \
            --dynamic-pool-file <path-to-pool-definition> htcondor

As you can see from the example above, you can even mix and match different input
formats to create your required simulation environment. An extensive documentation
about the CLI can be found in the :doc:`source/topics/cli` chapter.

Simple Framework for Advanced Use Cases
---------------------------------------

The simulation is event-driven and builds on the lightweight simulation
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
