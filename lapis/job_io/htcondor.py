import csv
import json
import logging
from typing import Dict, Iterable

from lapis.job import Job
from copy import deepcopy


default_resource_name_mapping: Dict[str, str] = {
    "cores": "RequestCpus",
    "walltime": "RequestWalltime",  # s
    "memory": "RequestMemory",  # MiB
    "disk": "RequestDisk",  # KiB
}
default_used_resource_name_mapping: Dict[str, str] = {
    "queuetime": "QDate",
    "walltime": "RemoteWallClockTime",  # s
    "memory": "MemoryUsage",  # MB
    "disk": "DiskUsage_RAW",  # KiB
}
default_unit_conversion_mapping: Dict[str, float] = {
    "RequestCpus": 1,
    "RequestWalltime": 1,
    "RequestMemory": 1024 * 1024,
    "RequestDisk": 1024,
    "queuetime": 1,
    "RemoteWallClockTime": 1,
    "MemoryUsage": 1000 * 1000,
    "DiskUsage_RAW": 1024,
}


def htcondor_job_reader(
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

    input_file_type = iterable.name.split(".")[-1].lower()
    if input_file_type == "json":
        htcondor_reader = json.load(iterable)
    elif input_file_type == "csv":
        htcondor_reader = csv.DictReader(iterable, delimiter=" ", quotechar="'")
    else:
        logging.getLogger("implementation").error(
            "Invalid input file %s. Job input file can not be read." % iterable.name
        )
        return
    for entry in htcondor_reader:
        if float(entry[used_resource_name_mapping["walltime"]]) <= 0:
            logging.getLogger("implementation").warning(
                "removed job from htcondor import (%s)", entry
            )
            continue
        resources = {}
        for key, original_key in resource_name_mapping.items():
            try:
                resources[key] = int(
                    float(entry[original_key])
                    * unit_conversion_mapping.get(original_key, 1)
                )
            except ValueError:
                pass

        used_resources = {
            "cores": (
                (float(entry["RemoteSysCpu"]) + float(entry["RemoteUserCpu"]))
                / float(entry[used_resource_name_mapping["walltime"]])
            )
            * unit_conversion_mapping.get(resource_name_mapping["cores"], 1)
        }
        for key in ["memory", "walltime", "disk"]:
            original_key = used_resource_name_mapping[key]
            used_resources[key] = int(
                float(entry[original_key])
                * unit_conversion_mapping.get(original_key, 1)
            )

        try:
            resources["inputfiles"] = deepcopy(entry["Inputfiles"])
            used_resources["inputfiles"] = deepcopy(entry["Inputfiles"])
            for filename, filespecs in entry["Inputfiles"].items():
                if "usedsize" in filespecs:
                    del resources["inputfiles"][filename]["usedsize"]
                if "filesize" in filespecs:
                    if "usedsize" not in filespecs:
                        used_resources["inputfiles"][filename]["usedsize"] = filespecs[
                            "filesize"
                        ]
                    del used_resources["inputfiles"][filename]["filesize"]

        except KeyError:
            pass
        yield Job(
            resources=resources,
            used_resources=used_resources,
            queue_date=float(entry[used_resource_name_mapping["queuetime"]]),
        )
