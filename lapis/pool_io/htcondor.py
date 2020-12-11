import csv
from functools import partial

from typing import Callable, Dict, Iterable
from ..pool import Pool


default_resource_name_mapping: Dict[str, str] = {
    "cores": "TotalSlotCPUs",
    "disk": "TotalSlotDisk",  # MiB
    "memory": "TotalSlotMemory",  # MiB
}
default_unit_conversion_mapping: Dict[str, float] = {
    "TotalSlotCPUs": 1,
    "TotalSlotDisk": 1024 * 1024,
    "TotalSlotMemory": 1024 * 1024,
}


def htcondor_pool_reader(
    iterable,
    resource_name_mapping: Dict[str, str] = None,
    unit_conversion_mapping: Dict[str, float] = None,
    pool_type: Callable = Pool,
    make_drone: Callable = None,
) -> Iterable[Pool]:
    """
    Load a pool configuration that was exported via htcondor from files or
    iterables

    :param unit_conversion_mapping:
    :param iterable: an iterable yielding lines of CSV, such as an open file
    :param resource_name_mapping: Mapping from given header names to well-defined
                                  resources in simulation
    :param pool_type: The type of pool to be yielded
    :param make_drone:
    :return: Yields the :py:class:`Pool`s found in the given iterable
    """
    if resource_name_mapping is None:
        resource_name_mapping = default_resource_name_mapping
    if unit_conversion_mapping is None:
        unit_conversion_mapping = default_unit_conversion_mapping

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
                    key: int(float(row[value]) * unit_conversion_mapping.get(value, 1))
                    for key, value in resource_name_mapping.items()
                },
                ignore_resources=["disk"],
            ),
        )
