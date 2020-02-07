import logging

from typing import NamedTuple, Optional

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.monitor import LoggingSocketHandler, LoggingUDPSocketHandler
from lapis.storageelement import StorageElement


class MonitoredPipeInfo(NamedTuple):
    requested_throughput: float
    available_throughput: float
    pipename: Optional[str]
    throughputscale: float
    no_subscriptions: int


def storage_status(storage: StorageElement) -> list:
    """
    Log information about current storage object state
    :param storage:
    :return: list of records for logging
    """
    results = [
        {
            "storage": repr(storage),
            "usedstorage": storage.used,
            "storagesize": storage.size,
            "numberoffiles": len(storage.files),
        }
    ]
    return results


storage_status.name = "storage_status"
storage_status.whitelist = (StorageElement,)
storage_status.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "storage"}, resolution=1
    ),
}


def pipe_status(pipeinfo: MonitoredPipeInfo) -> list:
    """
    #     Log information about the pipes
    #     :param storage:
    #     :return:
    #     """
    results = [
        {
            "pipe": repr(pipeinfo.pipename),
            "throughput": pipeinfo.available_throughput,
            "requested_throughput": pipeinfo.requested_throughput,
            "throughput_scale": pipeinfo.throughputscale,
            "no_subscribers": pipeinfo.no_subscriptions,
        }
    ]
    return results


pipe_status.name = "pipe_status"
pipe_status.whitelist = (MonitoredPipeInfo,)
pipe_status.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pipe"}, resolution=1
    ),
}


# def storage_connection(storage: StorageElement) -> list:
#     """
#     Log information about the storages connection
#     :param storage:
#     :return:
#     """
#     results = [
#         {
#             "storage": repr(storage),
#             "throughput": storage.connection.throughput,
#             "requested_throughput": sum(storage.connection._subscriptions.values()),
#             "throughput_scale": storage.connection._throughput_scale,
#         }
#     ]
#     return results
#
#
# storage_connection.name = "storage_connection"
# storage_connection.whitelist = (StorageElement,)
# storage_connection.logging_formatter = {
#     LoggingSocketHandler.__name__: JsonFormatter(),
#     logging.StreamHandler.__name__: JsonFormatter(),
#     LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
#         tags={"tardis", "storage"}, resolution=1
#     ),
# }
#
#
# def remote_connection(remote: Pipe) -> list:
#     """
#     Log information about the remote connection
#     :param remote:
#     :return:
#     """
#     results = [
#         {
#             "throughput": remote.throughput,
#             "requested_throughput": sum(remote._subscriptions.values()),
#             "throughput_scale": remote._throughput_scale,
#         }
#     ]
#     return results
#
#
# remote_connection.name = "remote_connection"
# remote_connection.whitelist = (Pipe,)
# remote_connection.logging_formatter = {
#     LoggingSocketHandler.__name__: JsonFormatter(),
#     logging.StreamHandler.__name__: JsonFormatter(),
#     LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
#         tags={"tardis"}, resolution=1
#     ),
# }
