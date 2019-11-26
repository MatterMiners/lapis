Supported File Formats
======================

TARDIS
------

.. warning::

    Import of TARDIS configuration files not supported yet, but will be
    available in the future.

HTCondor
--------

Job Imports
~~~~~~~~~~~

:term:`Jobs <Job>` can be created directly from HTCondor outputs. Via the
``condor_history`` command from HTCondor, ClassAds describing a :term:`jobs <Job>`
requested and used resources can be gathered and saved to a csv file.
To sufficiently describe a :term:`job` for the simulation information about
requested and used resources should be included in the export:

requested resources:
    RequestCpus, RequestWalltime, RequestMemory, RequestDisk

used resources:
    RemoteWallClockTime, MemoryUsage, DiskUsage_RAW, RemoteSysCpu, RemoteUserCpu

additional job information:
    QDate, GlobalJobId

In the csv file format every line represents a :term:`job`. The columns are
separated by spaces, and comments are marked by simple quotation marks.

.. note::

    If information about the input files of a :term:`jobs <Job>` should be passed
    to LAPIS, a separate csv file is required. This feature is not provided yet,
    but will be added in one of the next versions.

Input file information of jobs are not part of the standard :term:`jobs <Job>`
ClassAds in HTCondor but can be extracted via external tools (e.g. job submission
tools).

SWF Format
----------
