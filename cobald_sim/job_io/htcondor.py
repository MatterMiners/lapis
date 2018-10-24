import csv

from cobald_sim.job import Job


def htcondor_job_reader(env, iterable, resource_name_mapping={
    "cores": "RequestCpus",
    "walltime": "RemoteWallClockTime",
    "memory": "RequestMemory",
    "disk": "RequestDisk"
}, used_resource_name_mapping={
    "queuetime": "QDate",
    "walltime": "RemoteWallClockTime",
    "cores": "Number of Allocated Processors",
    "memory": "MemoryUsage",
    "disk": "DiskUsage_RAW"
}):
    htcondor_reader = csv.DictReader(iterable, delimiter=' ', quotechar="'")

    for row in htcondor_reader:
        yield Job(
            env,
            walltime=row[resource_name_mapping["walltime"]],
            resources={
                "cores": int(row[resource_name_mapping["cores"]]),
                # "disk": float(row[resource_name_mapping["disk"]]),
                "memory": float(row[resource_name_mapping["memory"]])
            }, used_resources={
                "cores": (float(row["RemoteSysCpu"]) + float(row["RemoteUserCpu"])) /
                         float(row[used_resource_name_mapping["walltime"]]),
                "memory": float(row[used_resource_name_mapping["memory"]]),
                # "disk": float(row[used_resource_name_mapping["disk"]])
            }, queue_date=float(row[used_resource_name_mapping["queuetime"]]))
