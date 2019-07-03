import logging

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter
from typing import TYPE_CHECKING

from lapis.monitor import LoggingSocketHandler, LoggingUDPSocketHandler

if TYPE_CHECKING:
    from lapis.simulator import Simulator


def drone_statistics(simulator: "Simulator") -> list:
    """
    Collect allocation, utilisation, demand and supply of drones.

    :param simulator: the simulator
    :return: list of records for logging
    """
    results = []
    for drone in simulator.job_scheduler.drone_list:
        results.append({
            "pool_configuration": "None",
            "pool_type": "drone",
            "pool": repr(drone),
            "allocation": drone.allocation,
            "utilisation": drone.utilisation,
            "demand": drone.demand,
            "supply": drone.supply,
            "job_count": drone.jobs
        })
    return results


drone_statistics.name = "cobald_status"
drone_statistics.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type", "pool"},
        resolution=1
    )
}


def pool_statistics(simulator: "Simulator") -> list:
    """
    Collect allocation, utilisation, demand and supply of pools.

    :param simulator: the simulator
    :return: list of records to log
    """
    results = []
    for pool in simulator.pools:
        results.append({
            "pool_configuration": "None",
            "pool_type": "pool",
            "pool": repr(pool),
            "allocation": pool.allocation,
            "utilisation": pool.utilisation,
            "demand": pool.demand,
            "supply": pool.supply,
        })
    return results


pool_statistics.name = "cobald_status"
pool_statistics.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type", "pool"},
        resolution=1
    )
}
