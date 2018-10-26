import csv

from ..pool import Pool


def htcondor_pool_reader(env, iterable, resource_name_mapping={
    "cores": "TotalSlotCPUs",
    "disk": "TotalSlotDisk",
    "memory": "TotalSlotMemory"
}, pool_type=Pool):
    """
    Load a pool configuration that was exported via htcondor from files or iterables

    :param iterable: an iterable yielding lines of CSV, such as an open file
    :param resource_name_mapping: Mapping from given header names to well-defined resources in simulation
    :param pool_type: The type of pool to be yielded
    :return: Yields the :py:class:`StaticPool`s found in the given iterable
    """
    reader = csv.DictReader(iterable, delimiter=' ', skipinitialspace=True)
    for row_idx, row in enumerate(reader):
        yield pool_type(
            env,
            capacity=int(row["Count"]),
            resources={key: float(row[value]) for key, value in resource_name_mapping.items()})
