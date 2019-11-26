Glossary of Terms
=================

.. Using references in the glossary itself:
   When mentioning other items, always reference them.
   When mentioning the current item, never reference it.

.. glossary::

   Allocation
      Information about the amount of resources being acquired by a :term:`drone`
      without evaluating the effectiveness of use.

      .. note::

         The general concept of allocation and :term:`utilisation` is introduced
         by COBalD to internally decide which resources to integrate or
         disintegrate.

   Controller
      Manages the demand of :term:`drones <Drone>` for a given :term:`pool`.
      The controller continuously evaluates the :term:`allocation` and
      :term:`utilisation` of resources for available :term:`drones <Drone>`
      for a given :term:`pool` and regulates the current demand to ensure that
      best used :term:`drones <Drone>` are available via the overlay batch system.

      .. note::

         Controllers are also initiated for :term:`static pools <Pool>`.
         Their functionality is different from those of opportunistic resources
         by initialising the :term:`drones <Drone>` only once.

   Drone
      Partitionable placeholder for :term:`jobs <Job>`. In the current state of
      LAPIS a drone represents a single worker node provided by a specific
      :term:`pool`.

      .. note::

         The concept of drones is introduced by TARDIS. Drones integrate
         themselves into an HTCondor overlay batch system and thereby provision
         the resources for :term:`jobs <Job>`. They act nearly autonomously to
         e.g. manage shutdown and error handling if required.

   Job
      A task that requires a defined set of resources to be successfully
      processed. Processing of jobs is done by :term:`drones <Drone>`.

   Job Generator
      The Job Generator takes care to continuously create :term:`jobs <Job>`
      that are appended to a central :term:`Job Queue` based on job information
      provided by one or several job input files.

   Job Queue
      Wait queue that contains the :term:`jobs <Job>` in order of creation time.

   Opportunistic Resource
      Any resources not permanently dedicated to but temporarily available for
      a specific task, user, or group.

   Pool
      A collection of indistinguishable resources. This means that a pool
      defines the number of worker nodes having a specific combination of
      available resources e.g. number of cores, memory, or disk. A resource
      provider can provide a number of pools.

      The simulation differentiates between static and dynamic pools. While
      the specified number of :term:`drones <Drone>` is initialised once for
      static pools, the demand for :term:`drones <Drone>` is continually updated
      by a given :term:`controller` for dynamic pools.

   Scheduler
      An autonomous process that assigns :term:`jobs <Job>` for execution from
      the :term:`Job Queue` to any appropriate :term:`drone`. The process
      of job-to-drone-assignment builds on a specified matchmaking logic.

   Utilisation
      Information about the effectiveness of use of resources acquired by a
      :term:`drone`.

      .. note::

         The general concept of :term:`allocation` and utilisation is introduced
         by COBalD to internally decide about which resources to integrate or
         disintegrate.
