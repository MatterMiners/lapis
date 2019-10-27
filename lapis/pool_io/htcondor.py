import csv
from functools import partial

from typing import Callable
from ..pool import Pool


def htcondor_pool_reader(
    iterable,
    resource_name_mapping: dict = {  # noqa: B006
        "cores": "TotalSlotCPUs",
        "disk": "TotalSlotDisk",  # MiB
        "memory": "TotalSlotMemory",  # MiB
    },
    unit_conversion_mapping: dict = {  # noqa: B006
        "TotalSlotCPUs": 1,
        "TotalSlotDisk": 1.024 / 1024,
        "TotalSlotMemory": 1.024 / 1024,
    },
    pool_type: Callable = Pool,
    make_drone: Callable = None,
):
    """
    Load a pool configuration that was exported via htcondor from files or
    iterables

    :param iterable: an iterable yielding lines of CSV, such as an open file
    :param resource_name_mapping: Mapping from given header names to well-defined
                                  resources in simulation
    :param pool_type: The type of pool to be yielded
    :param make_drone:
    :return: Yields the :py:class:`Pool`s found in the given iterable
    """
    assert make_drone
    reader = csv.DictReader(iterable, delimiter=" ", skipinitialspace=True)
    for row in reader:
        try:
            capacity = int(row["Count"])
        except ValueError:
            if row["Count"] == "None":
                capacity = float("Inf")
        yield pool_type(
            capacity=capacity,
            make_drone=partial(
                make_drone,
                {
                    key: float(row[value]) * unit_conversion_mapping.get(value, 1)
                    for key, value in resource_name_mapping.items()
                },
                ignore_resources=["disk"],
            ),
        )
