import csv
from functools import partial

from typing import Callable
from ..pool import Pool


def machines_pool_reader(
    iterable,
    resource_name_mapping: dict = {  # noqa: B006
        "cores": "CPUs_per_node",
        "memory": "RAM_per_node_in_KB",
    },
    pool_type: Callable = Pool,
    make_drone: Callable = None,
):
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
    assert make_drone
    reader = csv.DictReader(iterable, delimiter=" ", skipinitialspace=True)
    for row in reader:
        yield pool_type(
            capacity=int(row["number_of_nodes"]),
            make_drone=partial(
                make_drone,
                {
                    key: float(row[value])
                    for key, value in resource_name_mapping.items()
                },
            ),
            name=row["cluster_name"],
        )
