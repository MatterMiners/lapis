import csv

from cobald_sim.job import Job


def swf_job_reader(env, iterable, resource_name_mapping={
    "cores": "Requested Number of Processors",
    "walltime": "Requested Time",
    "memory": "Requested Memory"
}, used_resource_name_mapping={
    "walltime": "Run Time",
    "cores": "Number of Allocated Processors",
    "memory": "Used Memory",
    "scheduletime": "Submit Time"
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
            walltime=row[header[resource_name_mapping["walltime"]]],
            resources={
                "cores": row[header[resource_name_mapping["cores"]]],
                "memory": row[header[resource_name_mapping["memory"]]]
            },
            used_resources={
                "cores": row[header[used_resource_name_mapping["cores"]]],
                "memory": row[header[used_resource_name_mapping["memory"]]]
            }, schedule_date=float(row[header[used_resource_name_mapping["scheduletime"]]]))

