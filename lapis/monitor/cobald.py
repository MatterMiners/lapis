import logging

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.drone import Drone
from lapis.monitor import LoggingSocketHandler, LoggingUDPSocketHandler
from lapis.pool import Pool


def drone_statistics(drone: Drone) -> list:
    """
    Collect allocation, utilisation, demand and supply of drones.

    :param drone: the drone
    :return: list of records for logging
    """
    results = [
        {
            "pool_configuration": "None",
            "pool_type": "drone",
            "pool": repr(drone),
            "allocation": drone.allocation,
            "utilisation": drone.utilisation,
            "demand": drone.demand,
            "supply": drone.supply,
            "job_count": drone.jobs,
        }
    ]
    return results


drone_statistics.name = "cobald_status"
drone_statistics.whitelist = (Drone,)
drone_statistics.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type", "pool"}, resolution=1
    ),
}


def pool_statistics(pool: Pool) -> list:
    """
    Collect allocation, utilisation, demand and supply of pools.

    :param pool: the pool
    :return: list of records to log
    """
    results = [
        {
            "pool_configuration": "None",
            "pool_type": "pool",
            "pool": repr(pool),
            "allocation": pool.allocation,
            "utilisation": pool.utilisation,
            "demand": pool.demand,
            "supply": pool.supply,
        }
    ]
    return results


pool_statistics.name = "cobald_status"
pool_statistics.whitelist = (Pool,)
pool_statistics.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: JsonFormatter(),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type", "pool"}, resolution=1
    ),
}
