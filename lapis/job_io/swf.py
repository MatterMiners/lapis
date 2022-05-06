"""
Import of jobs from the parallel workload archive.
Current implementation is based on version 2.2 of the
[Standard Workload Format](http://www.cs.huji.ac.il/labs/parallel/workload/swf.html).
"""
import csv
from typing import Dict, Iterable

from lapis.job import Job


default_resource_name_mapping: Dict[str, str] = {
    "cores": "Requested Number of Processors",
    "walltime": "Requested Time",  # s
    "memory": "Requested Memory",  # KiB
}
default_used_resource_name_mapping: Dict[str, str] = {
    "walltime": "Run Time",  # s
    "cores": "Number of Allocated Processors",
    "memory": "Used Memory",  # KiB
    "queuetime": "Submit Time",
}
default_unit_conversion_mapping: Dict[str, float] = {
    "Used Memory": 1024,
    "Requested Memory": 1024,
}


def swf_job_reader(
    iterable,
    resource_name_mapping: Dict[str, str] = None,
    used_resource_name_mapping: Dict[str, str] = None,
    unit_conversion_mapping: Dict[str, float] = None,
) -> Iterable[Job]:
    if resource_name_mapping is None:
        resource_name_mapping = default_resource_name_mapping
    if used_resource_name_mapping is None:
        used_resource_name_mapping = default_used_resource_name_mapping
    if unit_conversion_mapping is None:
        unit_conversion_mapping = default_unit_conversion_mapping
    header = {
        "Job Number": 0,
        "Submit Time": 1,
        "Wait Time": 2,  # s
        "Run Time": 3,  # s
        "Number of Allocated Processors": 4,
        "Average CPU Time Used": 5,  # s
        "Used Memory": 6,  # average kB per processor
        "Requested Number of Processors": 7,
        "Requested Time": 8,
        "Requested Memory": 9,  # kB per processor
        "Status": 10,
        "User ID": 11,
        "Group ID": 12,
        "Executable (Application) Number": 13,
        "Queue Number": 14,
        "Partition Number": 15,
        "Preceding Job Number": 16,
        "Think Time from Preceding Job": 17,  # s
    }
    reader = csv.reader(
        (line for line in iterable if line[0] != ";"),
        delimiter=" ",
        skipinitialspace=True,
    )
    for row in reader:
        resources = {}
        used_resources = {}
        # correct request parameters
        for key in ["cores", "walltime", "memory"]:
            if float(row[header[resource_name_mapping[key]]]) < 0:
                row[header[resource_name_mapping[key]]] = 0
        for key in ["cores", "walltime"]:
            value = float(row[header[resource_name_mapping[key]]])
            used_value = float(row[header[used_resource_name_mapping[key]]])
            if value >= 0:
                resources[key] = value * unit_conversion_mapping.get(
                    resource_name_mapping[key], 1
                )
            if used_value >= 0:
                used_resources[key] = used_value * unit_conversion_mapping.get(
                    used_resource_name_mapping[key], 1
                )
        # handle memory
        key = "memory"
        resources[key] = int(
            (
                float(row[header[resource_name_mapping[key]]])
                * float(row[header[resource_name_mapping["cores"]]])
            )
            * unit_conversion_mapping.get(resource_name_mapping[key], 1)
        )
        used_resources[key] = int(
            (
                float(row[header[used_resource_name_mapping[key]]])
                * float(row[header[used_resource_name_mapping["cores"]]])
            )
            * unit_conversion_mapping.get(used_resource_name_mapping[key], 1)
        )
        yield Job(
            resources=resources,
            used_resources=used_resources,
            queue_date=float(row[header[used_resource_name_mapping["queuetime"]]]),
            name=row[header["Job Number"]],
        )
