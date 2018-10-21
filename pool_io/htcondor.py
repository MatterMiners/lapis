import pandas as pd

from pool import StaticPool


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
    df = pd.read_csv(iterable, sep='\s{1,}', header=0, engine='python', thousands=',')
    df = df.rename(columns=resource_name_mapping)
    header = list(df.columns.values)
    for row_idx, *row in df.itertuples():
        yield StaticPool(env, init=row[0], resources={key: row[header.index(key)] for key in
                                                      resource_name_mapping.values()})
