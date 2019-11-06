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

Jobs can be created directly from HTCondor outputs. Via the ``condor_history``
command from HTCondor, ClassAds describing a jobs requested and used resources
can be gathered and saved to a csv or json file.
To sufficiently describe a job for the simulation information about requested
and used resources should be included in the export:

requested resources:
    RequestCpus, RequestWalltime, RequestMemory, RequestDisk

used resources:
    RemoteWallClockTime, MemoryUsage, DiskUsage_RAW, RemoteSysCpu, RemoteUserCpu

additional job information:
    QDate

If csv is chosen as input file format every line represents a job, columns
should be separated by spaces, comments should be marked by simple quotation
marks.

If information about a jobs input files are passed to lapis a json file should
contain job descriptions because this file format allows for nested structures.
In this case the json file should contain an array of objects, each representing
a job.

ClassAds containing information about a jobs input files are not part of a jobs
standard ClassAds in HTCondor but can be extracted via external tools (e.g. job
submission tools) and stored as Inputfiles ClassAd.
Alternatively this information can be added to the job input file manually.

The ``Inputfile`` ClassAd contains dictionary with the input file names serving
as keys and subdictionaries further describing the input files.
These subdictionaries provide

filesize:
    the files total size in MB

usedsize:
    the amount of data the job actually reads from this file in MB

.. code-block:: csv

    TODO

.. code-block:: json

    [
        {
            "QDate": 1567169672,
            "RequestCpus": 1,
            "RequestWalltime": 60,
            "RequestMemory": 2000,
            "RequestDisk": 6000000,
            "RemoteWallClockTime": 100.0,
            "MemoryUsage": 2867,
            "DiskUsage_RAW": 41898,
            "RemoteSysCpu": 10.0,
            "RemoteUserCpu": 40.0,
        },
        {
            "QDate": 1567169672,
            "RequestCpus": 1,
            "RequestWalltime": 60,
            "RequestMemory": 2000,
            "RequestDisk": 6000000,
            "RemoteWallClockTime": 100.0,
            "MemoryUsage": 2867,
            "DiskUsage_RAW": 41898,
            "RemoteSysCpu": 10.0,
            "RemoteUserCpu": 40.0,
            "Inputfiles": {
                "a.root": {
                    "filesize": 25000,
                    "usedsize": 20000
                },
                "b.root": {
                    "filesize": 25000,
                    "usedsize": 20000
                }
            }
        }
    ]

SWF Format
----------
