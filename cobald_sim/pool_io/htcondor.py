import csv

from ..pool import StaticPool


def htcondor_pool_reader(env, iterable, resource_name_mapping={
    "cores": "TotalSlotCPUs",
    "disk": "TotalSlotDisk",
    "memory": "TotalSlotMemory"
}):
    """
    Load a pool configuration that was exported via htcondor from files or iterables

    :param iterable: an iterable yielding lines of CSV, such as an open file
    :param resource_name_mapping: Mapping from given header names to well-defined resources in simulation
    :return: Yields the :py:class:`StaticPool`s found in the given iterable
    """
    reader = csv.DictReader(iterable, delimiter=' ', skipinitialspace=True)
    for row_idx, row in enumerate(reader):
        yield StaticPool(
            env,
            init=int(row["Count"]),
            resources={key: row[value] for key, value in resource_name_mapping.items()})
