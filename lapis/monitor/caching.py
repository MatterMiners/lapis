import logging

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter
from usim import Pipe

from lapis.monitor import LoggingSocketHandler, LoggingUDPSocketHandler
from lapis.storage import Storage


def storage_status(storage: Storage) -> list:
    """
    Log information about current storage object state
    :param storage:
    :return: list of records for logging
    """
    results = [
        {
            "storage": repr(storage),
            "usedstorage": storage.usedstorage,
            "storagesize": storage.size,
            "numberoffiles": len(storage.files),
        }
    ]
    return results


storage_status.name = "storage_status"
storage_status.whitelist = (Storage,)
storage_status.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "storage"}, resolution=1
    ),
}


def storage_connection(storage: Storage) -> list:
    """
    Log information about the storages connection
    :param storage:
    :return:
    """
    results = [
        {
            "storage": repr(storage),
            "throughput": storage.connection.throughput,
            "requested_throughput": sum(storage.connection._subscriptions.values()),
            "throughput_scale": storage.connection._throughput_scale,
        }
    ]
    return results


storage_connection.name = "storage_connection"
storage_connection.whitelist = (Storage,)
storage_connection.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "storage"}, resolution=1
    ),
}


def remote_connection(remote: Pipe) -> list:
    """
    Log information about the remote connection
    :param remote:
    :return:
    """
    results = [
        {
            "throughput": remote.throughput,
            "requested_throughput": sum(remote._subscriptions.values()),
            "throughput_scale": remote._throughput_scale,
        }
    ]
    return results


remote_connection.name = "remote_connection"
remote_connection.whitelist = (Pipe,)
remote_connection.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1
    ),
}
