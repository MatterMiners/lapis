import csv

from ..pool import StaticPool


def htcondor_pool_reader(env, iterable, resource_name_mapping={
    "TotalSlotCPUs": "cores",
    "TotalSlotDisk": "disk",
    "TotalSlotMemory": "memory"
}):
    """
    Load a pool configuration that was exported via htcondor from files or iterables

    :param iterable: an iterable yielding lines of CSV, such as an open file
    :param resource_name_mapping: Mapping from given header names to well-defined resources in simulation
    :return: Yields the :py:class:`StaticPool`s found in the given iterable
    """
    reader = csv.reader(iterable, delimiter=' ', skipinitialspace=True)
    first_line = next(reader)
    for row_idx, row in enumerate(reader):
        yield StaticPool(
            env,
            init=int(row[0]),
            resources={value: row[first_line.index(key)] for key, value in resource_name_mapping.items()})
