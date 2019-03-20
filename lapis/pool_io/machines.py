import csv

from ..pool import Pool


def machines_pool_reader(env, iterable, resource_name_mapping={
    "cores": "CPUs_per_node",
    "memory": "RAM_per_node_in_KB"
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
            capacity=int(row["number_of_nodes"]),
            resources={key: float(row[value]) for key, value in resource_name_mapping.items()},
            name=row["cluster_name"]
        )
