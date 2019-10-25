from typing import TYPE_CHECKING

import logging.handlers

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.monitor import LoggingSocketHandler, LoggingUDPSocketHandler

if TYPE_CHECKING:
    from lapis.simulator import Simulator


def resource_statistics(simulator: "Simulator") -> list:
    """
    Log ratio of used and requested resources for drones.

    :param simulator: the simulator
    :return: list of records for logging
    """
    results = []
    for drone in simulator.job_scheduler.drone_list:
        resources = drone.theoretical_available_resources
        used_resources = drone.available_resources
        for resource_type in resources:
            results.append({
                "resource_type": resource_type,
                "pool_configuration": "None",
                "pool_type": "drone",
                "pool": repr(drone),
                "used_ratio":
                    1 - used_resources[resource_type]
                    / drone.pool_resources[resource_type],
                "requested_ratio":
                    1 - resources[resource_type] / drone.pool_resources[resource_type]
            })
    return results


resource_statistics.name = "resource_status"
resource_statistics.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "resource_type", "pool_configuration", "pool_type", "pool"},
        resolution=1
    )
}


def user_demand(simulator: "Simulator") -> list:
    """
    Log global user demand.

    :param simulator: the simulator
    :return: list of records for logging
    """
    return [{
        "value": len(simulator.job_scheduler.job_queue)
    }]


user_demand.name = "user_demand"
user_demand.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis"},
        resolution=1
    )
}


def job_statistics(simulator: "Simulator") -> list:
    """
    Log number of jobs running in a drone.

    :param simulator: the simulator
    :return: list of records for logging
    """
    result = 0
    for drone in simulator.job_scheduler.drone_list:
        result += drone.jobs
    return [{
        "job_count": result
    }]


job_statistics.name = "cobald_status"
job_statistics.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type", "pool"},
        resolution=1
    )
}


def pool_status(simulator: "Simulator") -> list:
    """
    Log state changes of pools and drones.

    :param simulator: the simulator
    :return: list of records for logging
    """
    return []


pool_status.name = "pool_status"
pool_status.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "parent_pool", "pool_configuration", "pool_type", "pool"},
        resolution=1
    )
}


def configuration_information(simulator: "Simulator") -> list:
    """
    Log information how pools and drones are configured, e.g. provided resources.

    :param simulator: the simulator
    :return: list of records for logging
    """
    return []


configuration_information.name = "configuration"
configuration_information.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "resource_type"},
        resolution=1
    )
}
