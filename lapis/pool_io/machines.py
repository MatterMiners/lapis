import csv
from functools import partial

from typing import Callable, Dict, Iterable
from ..pool import Pool


default_resource_name_mapping: Dict[str, str] = {
    "cores": "CPUs_per_node",
    "memory": "RAM_per_node_in_KB",
}
default_unit_conversion_mapping: Dict[str, float] = {
    "CPUs_per_node": 1,
    "RAM_per_node_in_KB": 1000,
}


def machines_pool_reader(
    iterable,
    resource_name_mapping: Dict[str, str] = None,
    unit_conversion_mapping: Dict[str, float] = None,
    pool_type: Callable = Pool,
    make_drone: Callable = None,
) -> Iterable[Pool]:
    """
    Load a pool configuration that was exported via htcondor from files or
    iterables

    :param make_drone: The callable to create the drone
    :param iterable: an iterable yielding lines of CSV, such as an open file
    :param resource_name_mapping: Mapping from given header names to well-defined
                                  resources in simulation
    :param pool_type: The type of pool to be yielded
    :return: Yields the :py:class:`StaticPool`s found in the given iterable
    """
    if resource_name_mapping is None:
        resource_name_mapping = default_resource_name_mapping
    if unit_conversion_mapping is None:
        unit_conversion_mapping = default_unit_conversion_mapping

    assert make_drone
    reader = csv.DictReader(iterable, delimiter=" ", skipinitialspace=True)
    for row in reader:
        yield pool_type(
            capacity=int(row["number_of_nodes"]),
            make_drone=partial(
                make_drone,
                {
                    key: int(float(row[value]) * unit_conversion_mapping.get(value, 1))
                    for key, value in resource_name_mapping.items()
                },
            ),
            name=row["cluster_name"],
        )
