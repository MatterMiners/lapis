Monitoring Simulation Data
==========================

Lapis provides some predefined functions that provide monitoring of relevant
information about your :term:`pools <pool>`, resources, and jobs. Further,
information relevant to COBalD are provided.

In the following you find tables summarising the available information.

The CLI of LAPIS currently supports logging to

* TCP,
* File, or
* Telegraf.

See :doc:`cli` for details.

Telegraf
--------

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
