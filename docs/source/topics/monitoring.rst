Monitoring Simulation Data
==========================

Monitoring information is critical information in simulations. However, the
monitoring overhead can be significant. For this reason, LAPIS provides an object-based
monitoring. Whenever a monitoring-relevant object does change during simulation
the object is put into a monitoring :py:class:`usim.Queue` for further processing.

When running a simulation you should register your required logging callable
with the monitoring component. There is already a number of predefined logging
callables that can easily be used, see :ref:`predefined_monitoring_functions`.
Each of these logging functions is parameterised with the objects it is able to
process. Whenever an object becomes available in the monitoring queue, it is
checked if matching logging callables have been registered to handle the specific
object. The monitoring itself runs asynchronously: Whenever elements become
available in the monitoring queue, the logging process starts.

If you want to define your own logging callable that for example logs information
about changes to a :term:`drone` it should follow the following format:

.. code-block:: python3

    def log_object(the_object: Drone) -> List[Dict]:
        return []
    log_object.name: str = "identifying_name"
    log_object.whitelist: Tuple = (Drone,)
    log_object.logging_formatter: Dict = {
        LoggingSocketHandler.__name__: JsonFormatter(),
    }

Information about the object types being processed by your callable is given as a
:py:class:`tuple` in :py:attr:`whitelist`. You further need to set an identifying
:py:attr:`name` for your callable as well as :py:class:`logging.Formatter` for
specific logging options.

Registering your logging callable is very easy then, you just need to call

.. code-block:: python3

    simulator.monitoring.register_statistic(log_object)

That's it!

LAPIS currently supports logging to

* TCP,
* File, and/or
* Telegraf.

See :doc:`cli` for details on how to utilise the different logging options.

.. _predefined_monitoring_functions:

Predefined Monitoring Functions
-------------------------------

Lapis provides some predefined functions that provide monitoring of relevant
information about your :term:`pools <pool>`, resources, and jobs. Further,
information relevant to COBalD are provided.

General Monitoring
~~~~~~~~~~~~~~~~~~

.. autofunction:: lapis.monitor.general.resource_statistics
.. autofunction:: lapis.monitor.general.user_demand
.. autofunction:: lapis.monitor.general.job_statistics
.. autofunction:: lapis.monitor.general.job_events
.. autofunction:: lapis.monitor.general.pool_status
.. autofunction:: lapis.monitor.general.configuration_information

COBalD-specific Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: lapis.monitor.cobald.drone_statistics
.. autofunction:: lapis.monitor.cobald.pool_statistics

Caching-specific Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. TODO::

    Will be added as soon as the caching branch is merged.

Telegraf
--------

LAPIS supports sending monitoring information to telegraf via the CLI option
``--log-telegraf``. The monitoring information for telegraf are sent to the
default UDP logging port ``logging.handlers.DEFAULT_UDP_LOGGING_PORT`` that is
port ``9021``.

Resource Status
~~~~~~~~~~~~~~~

=========== ================== ============================= =======
type        name               values                        comment
----------- ------------------ ----------------------------- -------
measurement resource_status    --
tag         tardis             uuid
tag         resource_type      [memory | disk | cores | ...]
tag         pool_configuration [``None`` | uuid]
tag         pool_type          [pool | drone]
tag         pool               uuid
field       used_ratio         ``float``
field       requested_ratio    ``float``
timestamp   time               ``float``
=========== ================== ============================= =======

COBalD Status
~~~~~~~~~~~~~

=========== ================== ================= ============
type        name               values            comment
----------- ------------------ ----------------- ------------
measurement cobald_status      --
tag         tardis             uuid
tag         pool_configuration [``None`` | uuid]
tag         pool_type          [pool | drone]
tag         pool               uuid
field       allocation         ``float``
field       utilization        ``float``
field       demand             ``float``
field       supply             ``float``
field       job_count          ``int``           Running jobs
timestamp   time               ``float``
=========== ================== ================= ============

Pool Status
~~~~~~~~~~~

=========== ================== ================================ =======
type        name               values                           comment
----------- ------------------ -------------------------------- -------
measurement system_status      --
tag         tardis             uuid
tag         parent_pool        uuid
tag         pool_configuration [``None`` | uuid]
tag         pool_type          [pool | drone]
tag         pool               uuid
field       status             [DownState | CleanupState | ...]
timestamp   time               ``float``
=========== ================== ================================ =======

User Demand
~~~~~~~~~~~

=========== =========== ========= =======
type        name        values    comment
----------- ----------- --------- -------
measurement user_demand --
tag         tardis      uuid
field       value       ``int``
timestamp   time        ``float``
=========== =========== ========= =======

Configuration
~~~~~~~~~~~~~

=========== ================== ============================= =======
type        name               values                        comment
----------- ------------------ ----------------------------- -------
measurement configuration      --
tag         tardis             uuid
tag         pool_configuration uuid
tag         resource_type      [memory | disk | cores | ...]
field       value              ``float``
timestamp   time               ``float``
=========== ================== ============================= =======
