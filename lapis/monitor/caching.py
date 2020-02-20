import logging

from typing import NamedTuple, Optional

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.monitor import (
    LoggingSocketHandler,
    LoggingUDPSocketHandler,
    SIMULATION_START,
)
from lapis.storageelement import StorageElement
from monitoredpipe import MonitoredPipe

import time as pytime
from usim import time


class MonitoredPipeInfo(NamedTuple):
    requested_throughput: float
    available_throughput: float
    pipename: Optional[str]
    throughputscale: float
    no_subscriptions: int


class HitrateInfo(NamedTuple):
    hitrate: float
    volume: float
    provides_file: int


class SimulationInfo(NamedTuple):
    input: list
    identifier: str


def simulation_id(simulationinfo) -> list:
    results = [
        {
            "input": str(simulationinfo.input),
            "id": simulationinfo.identifier,
            "time": pytime.ctime(SIMULATION_START),
        }
    ]
    return results


simulation_id.name = "simulation_id"
simulation_id.whitelist = (SimulationInfo,)
simulation_id.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1
    ),
}


def hitrate_evaluation(hitrateinfo: HitrateInfo) -> list:
    results = [
        {
            "hitrate": hitrateinfo.hitrate,
            "volume": hitrateinfo.volume / 1000.0 / 1000.0 / 1000.0,
            "providesfile": hitrateinfo.provides_file,
        }
    ]
    return results


hitrate_evaluation.name = "hitrate_evaluation"
hitrate_evaluation.whitelist = (HitrateInfo,)
hitrate_evaluation.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1
    ),
}


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
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "storage"}, resolution=1
    ),
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
            "throughput": pipeinfo.available_throughput / 1000.0 / 1000.0 / 1000.0,
            "requested_throughput": pipeinfo.requested_throughput
            / 1000.0
            / 1000.0
            / 1000.0,
            "throughput_scale": pipeinfo.throughputscale,
            "no_subscribers": pipeinfo.no_subscriptions,
        }
    ]
    print(time.now, "monitoring:", results)
    return results


pipe_status.name = "pipe_status"
pipe_status.whitelist = (MonitoredPipeInfo,)
pipe_status.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pipe"}, resolution=1
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pipe"}, resolution=1
    ),
}


def pipe_data_volume(pipe: MonitoredPipe):
    """
    Total amount of data transferred by the pipe up to this point
    :param pipe:
    :return:
    """
    results = [{"pipe": repr(pipe), "current_total": pipe.transferred_data}]
    print(results)
    return results


pipe_data_volume.name = "pipe_data_volume"
pipe_data_volume.whitelist = (MonitoredPipe,)
pipe_data_volume.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pipe"}, resolution=1
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pipe"}, resolution=1
    ),
}
