import csv

from lapis.job import Job


def swf_job_reader(env, iterable, resource_name_mapping={
    "cores": "Requested Number of Processors",
    "walltime": "Requested Time",
    "memory": "Requested Memory"
}, used_resource_name_mapping={
    "walltime": "Run Time",
    "cores": "Number of Allocated Processors",
    "memory": "Used Memory",
    "queuetime": "Submit Time"
}):
    header = {
        "Job Number": 0,
        "Submit Time": 1,
        "Wait Time": 2,
        "Run Time": 3,
        "Number of Allocated Processors": 4,
        "Average CPU Time Used": 5,
        "Used Memory": 6,
        "Requested Number of Processors": 7,
        "Requested Time": 8,
        "Requested Memory": 9,
        "Status": 10,
        "User ID": 11,
        "Group ID": 12,
        "Executable (Application) Number": 13,
        "Queue Number": 14,
        "Partition Number": 15,
        "Preceding Job Number": 16,
        "Think Time from Preceding Job": 17
    }
    reader = csv.reader((line for line in iterable if line[0] != ';'), delimiter=' ', skipinitialspace=True)
    for row in reader:
        yield Job(
            env,
            resources={
                key: float(row[header[resource_name_mapping[key]]])
                for key in ("cores", "memory", "walltime")
                if float(row[header[resource_name_mapping[key]]]) >= 0
            },
            used_resources={
                key: float(row[header[used_resource_name_mapping[key]]])
                for key in used_resource_name_mapping.keys()
                if float(row[header[used_resource_name_mapping[key]]]) >= 0
            }, queue_date=float(row[header[used_resource_name_mapping["queuetime"]]]), name=row[header["Job Number"]])
