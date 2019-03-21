import csv
from functools import partial

from typing import Callable
from ..pool import Pool


def htcondor_pool_reader(iterable, resource_name_mapping: dict={
    "cores": "TotalSlotCPUs",
    "disk": "TotalSlotDisk",
    "memory": "TotalSlotMemory"
}, pool_type: Callable=Pool, make_drone: Callable=None):
    """
    Load a pool configuration that was exported via htcondor from files or iterables

    :param iterable: an iterable yielding lines of CSV, such as an open file
    :param resource_name_mapping: Mapping from given header names to well-defined resources in simulation
    :param pool_type: The type of pool to be yielded
    :param make_drone:
    :return: Yields the :py:class:`StaticPool`s found in the given iterable
    """
    assert make_drone
    reader = csv.DictReader(iterable, delimiter=' ', skipinitialspace=True)
    for row_idx, row in enumerate(reader):
        yield pool_type(
            capacity=int(row["Count"]),
            make_drone=partial(make_drone, {key: float(row[value]) for key, value in resource_name_mapping.items()}))
