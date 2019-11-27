Simulation Concept
==================

Background
----------

.. TODO::

    HEP context.

Components
----------

The core simulation builds on several components, and concepts:

* :term:`Job Generator`,
* :term:`Job Queue`,
* :term:`Pools <Pool>` and their :term:`Controllers <Controller>`,
* :term:`Drones <Drone>`, and
* the :term:`Scheduler`,

If you are planning to adapt the simulation for your specific use case, please
consider the different components to determine what and where to extend functionality.

Job Generator
~~~~~~~~~~~~~

The Job Generator processes any job input files. It takes care to
translate time-based characteristics of the :term:`jobs <Job>` into simulation
time. For this the timestamp of the first :term:`job` of each job input file is
taken as the ``base`` timestamp, resulting in a time value of ``0`` for the
first :term:`job`. All following :term:`jobs <Job>` are adapted accordingly,
i.e. time is ``time - base``.

The Job Generator itself acts as a generator, meaning that a :term:`job` is put
into the simulations :term:`Job Queue` as soon as the simulation time corresponds
to the translated :term:`job` queueing time.

Job Queue
~~~~~~~~~

The Job Queue is filled with :term:`jobs <Job>` in creation-time order by the
:term:`Job Generator`. The queue is managed by the :term:`scheduler` and contains
all :term:`jobs <Job>` that are not yet scheduled to a :term:`drone` as well as
:term:`jobs <Job>` that have not yet been processed succesfully.

Pools
~~~~~

Pools are created based on the pool input files. Each pool is characterised by
a set of defined resources. Further, pools have a ``capacity`` number of
:term:`drones <Drone>` that can be created from a given pool. If the capacity
is not specified, a maximum capacity of ``float("inf")`` is assumed.

For pools, we differentiate static and dynamic pools. While static pools are
intialised with a fixed amount of :term:`drones <Drone>`, the number of
:term:`drones <Drone>` is adapted dynamically by the
:term:`pool controller <Controller>` for dynamic pools.

.. autoclass:: lapis.pool.Pool
.. autoclass:: lapis.pool.StaticPool

Controllers
~~~~~~~~~~~

Each :term:`pool` is managed by a controller. Each controller runs
periodically to check :term:`allocation` and :term:`utilisation` of assigned
:term:`pool(s) <Pool>` to regulate the demand of :term:`drones <Drone>` for the
given :term:`pool`.

The concept of controllers is introduced by COBalD. The controllers implemented
in LAPIS share the general concept as well as implementation by subclassing
provided controllers such as :py:class:`cobald.controller.linear.LinearController`
or :py:class:`cobald.controller.relative_supply.RelativeSupplyController` and
overwriting :py:meth:`lapis.controller.SimulatedLinearController.run`. In
this way, we enable validation of current TARDIS/COBalD setup as well as simulation
of future extensions.

Available controller implementations from COBalD in LAPIS are:

.. autoclass:: lapis.controller.SimulatedLinearController
    :members:

.. autoclass:: lapis.controller.SimulatedRelativeSupplyController
    :members:

And there is also an implementation considered as an extension for COBalD:

.. autoclass:: lapis.controller.SimulatedCostController
    :members:

Drones
~~~~~~

Drones provide instances of the set of resources defined by a given :term:`pool`.
Drones are the only objects in the simulation that are able to process
:term:`jobs <Job>`. Simplified, drones represent worker nodes.

The concept of drones is introduced by TARDIS. A drone is a generalisation of
the pilot concept used for example in High Energy Physics and is a placeholder
for the real workloads to be processed. A drone is expected to autonomously
manage its lifecycle, meaning, that it handles failures and termination
independently from other components within the system.

.. warning::

    Drones are not yet fully employed in LAPIS. They already run independently
    but do not handle termination themselves.

Scheduler
~~~~~~~~~

The scheduler is the connecting component between the :term:`jobs <Job>` in the
:term:`job queue` and the running :term:`drones <Drone>`. It does the matchmaking
between :term:`jobs <Job>` and :term:`drones <Drone>` to assign the
:term:`jobs <Job>` to the best evaluated :term:`drone`. Whenever a :term:`job`
is assigned to a :term:`drone`, the :term:`job` is removed from the
:term:`job queue`. The scheduler is notified as soon as the :term:`job` is
terminated independent from the state of termination. It is the task of the
scheduler to decide to either remove the :term:`job` from the simulation in case
of success or to re-insert the :term:`job` into the :term:`job queue` to retry
processing.

LAPIS currently supports an HTCondor-like implementation of a scheduler:

.. autoclass:: lapis.scheduler.CondorJobScheduler
    :members:

.. warning::

    The implementation of the HTCondor scheduler is still very rough.
    The matchmaking currently does not rely on given ``requirements``, but only
    considers required and provided ``resources`` for :term:`jobs <Job>` and
    :term:`drones <Drone>`. The automatic clustering, therefore, also only relies
    on the type and number of ``resources`` and is applied to :term:`drones <Drone>`
    only at the moment.
