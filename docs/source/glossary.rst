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
         by COBalD to internally decide about which resources to integrate or
         disintegrate.

   Drone
      Partitionable placeholder for jobs. In the current state of LAPIS a drone
      represents a single worker node provided by a specific :term:`pool`.

      .. note::

         The concept of drones is introduced by TARDIS. Drones integrate
         themselves into an HTCondor overlay batch system and thereby provision
         the resources for jobs. They act nearly autonomously to e.g. manage
         shutdown and error handling if required.

   Opportunistic Resource
      Any resources not permanently dedicated to but temporarily available for
      a specific task, user, or group.

   Pool
      A collection of indistinguishable resources. This means that a pool
      defines the number of worker nodes having a specific combination of
      available resources e.g. number of cores, memory, or disk. A resource
      provider can provide a number of pools.

   Utilisation
      Information about the effectiveness of use of resources acquired by a
      :term:`drone`.

      .. note::

         The general concept of :term:`allocation` and utilisation is introduced
         by COBalD to internally decide about which resources to integrate or
         disintegrate.
